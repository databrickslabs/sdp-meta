"""SDP-META MCP 2.x server.

The server intentionally exposes local bundle scaffolding and inspection only.
Live onboarding and deployment remain CLI operations until they have dedicated
integration coverage and confirmation semantics.
"""

from __future__ import annotations

import io
import json
import logging
import os
import re
from dataclasses import asdict
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from databricks.labs.sdp_meta.__about__ import __version__
from databricks.labs.sdp_meta.identifiers import validate_uc_identifier
from mcp.server import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations
from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger("databricks.labs.sdp_meta.mcp")

_EXAMPLES_ENV_VAR = "SDP_META_EXAMPLES_DIR"
_MCP_ROOT_ENV_VAR = "SDP_META_MCP_ROOT"
_PACKAGED_EXAMPLES_MODULE = "databricks.labs.sdp_meta._packaged_examples"
_RESOURCE_PREFIX = "sdp-meta://templates/"

_CLI_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.-]*$")
_MAX_CLI_IDENTIFIER_LEN = 128


def _directory_contains_examples(candidate: Path) -> bool:
    """Return whether a candidate has at least one supported template file."""
    for fmt in ("json", "yml"):
        fmt_dir = candidate / fmt
        if fmt_dir.is_dir() and any(path.is_file() for path in fmt_dir.rglob("*")):
            return True
    return False


def _locate_examples_dir() -> Optional[Path]:
    """Resolve packaged onboarding examples, with explicit overrides first."""
    env_override = os.environ.get(_EXAMPLES_ENV_VAR)
    if env_override:
        candidate = Path(env_override).expanduser().resolve()
        if _directory_contains_examples(candidate):
            return candidate
        logger.warning(
            "%s=%r does not contain json/ or yml/ templates; "
            "falling back to packaged/source-tree resolution.",
            _EXAMPLES_ENV_VAR,
            env_override,
        )

    try:
        from importlib.resources import files

        package_root = Path(str(files(_PACKAGED_EXAMPLES_MODULE)))
        if _directory_contains_examples(package_root):
            return package_root
    except (ImportError, ModuleNotFoundError, TypeError):
        pass

    repo_candidate = Path(__file__).resolve().parents[4] / "examples"
    if _directory_contains_examples(repo_candidate):
        return repo_candidate
    return None


def _mcp_root() -> Path:
    """Return the explicitly configured filesystem boundary for MCP tools."""
    raw_root = os.environ.get(_MCP_ROOT_ENV_VAR)
    if not raw_root:
        raise RuntimeError(
            f"{_MCP_ROOT_ENV_VAR} must be set to the project directory before "
            "using filesystem-mutating SDP-META MCP tools. Requiring an explicit "
            "root prevents desktop MCP hosts with an unexpected working directory "
            "from granting access to a broader part of the filesystem."
        )
    root = Path(raw_root).expanduser().resolve()
    if not root.is_dir():
        raise ValueError(
            f"{_MCP_ROOT_ENV_VAR}={raw_root!r} is not an existing directory."
        )
    return root


def _is_within(path: Path, root: Path) -> bool:
    root_text = str(root)
    path_text = str(path)
    return path == root or path_text.startswith(root_text.rstrip(os.sep) + os.sep)


def _resolve_within_root(raw_path: str, *, kind: str) -> Path:
    """Resolve a caller path and enforce the configured MCP filesystem root."""
    if not isinstance(raw_path, str) or not raw_path:
        raise ValueError(f"{kind} must be a non-empty path string.")
    root = _mcp_root()
    lexical = Path(raw_path).expanduser()
    if not lexical.is_absolute():
        lexical = root / lexical
    # ``resolve`` catches static symlink escapes while accepting equivalent
    # absolute spellings such as macOS ``/tmp`` -> ``/private/tmp``.
    resolved = lexical.resolve()
    if not _is_within(resolved, root):
        raise ValueError(
            f"{kind} {raw_path!r} resolves to {str(resolved)!r}, which is "
            f"outside the MCP filesystem root {str(root)!r}."
        )
    return resolved


def _validate_cli_identifier(
    value: Optional[str], *, kind: str
) -> Optional[str]:
    """Validate a value that is forwarded as one Databricks CLI argument."""
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise ValueError(
            f"{kind} must be a string, got {type(value).__name__}: {value!r}"
        )
    if len(value) > _MAX_CLI_IDENTIFIER_LEN:
        raise ValueError(
            f"{kind} {value!r} exceeds {_MAX_CLI_IDENTIFIER_LEN} characters."
        )
    if not _CLI_IDENTIFIER_RE.fullmatch(value):
        raise ValueError(
            f"{kind} {value!r} is invalid. Use letters, digits, underscore, "
            "dash, or dot, and begin with a letter, digit, or underscore."
        )
    return value


def _validate_profile(profile: Optional[str]) -> Optional[str]:
    return _validate_cli_identifier(profile, kind="profile")


def _validate_target(target: Optional[str]) -> Optional[str]:
    return _validate_cli_identifier(target, kind="target")


def _list_template_files() -> List[Tuple[str, Path]]:
    """List supported templates without following files outside the source."""
    examples_dir = _locate_examples_dir()
    if examples_dir is None:
        raise RuntimeError(
            "SDP-META MCP templates are unavailable. Reinstall "
            "'databricks-labs-sdp-meta[mcp]' or set "
            f"{_EXAMPLES_ENV_VAR} to a directory containing json/ and yml/."
        )
    return list(_list_template_files_under(str(examples_dir.resolve())))


@lru_cache(maxsize=None)
def _list_template_files_under(root_text: str) -> Tuple[Tuple[str, Path], ...]:
    """Cache the immutable template index for one server-lifetime source."""
    root = Path(root_text)
    templates: List[Tuple[str, Path]] = []
    for fmt in ("json", "yml"):
        fmt_dir = root / fmt
        if not fmt_dir.is_dir():
            continue
        for path in sorted(fmt_dir.rglob("*")):
            if not path.is_file():
                continue
            resolved = path.resolve()
            if not _is_within(resolved, root):
                logger.warning("Ignoring template symlink outside examples root: %s", path)
                continue
            name = f"{fmt}/{path.relative_to(fmt_dir).as_posix()}"
            templates.append((name, resolved))
    return tuple(templates)


def _mime_for_template(name: str) -> str:
    if ".template" in name:
        return "text/plain"
    if name.startswith("json/"):
        return "application/json"
    if name.startswith("yml/"):
        return "text/yaml"
    return "text/plain"


def _run_bundle_init(
    output_dir: str = ".",
    quickstart: bool = True,
    config_file: Optional[str] = None,
    overrides: Optional[Dict[str, Any]] = None,
    profile: Optional[str] = None,
) -> Dict[str, Any]:
    from databricks.labs.sdp_meta.bundle import (
        BundleInitCommand,
        bundle_init,
        write_quickstart_config_file,
    )

    resolved_output = _resolve_within_root(output_dir, kind="output_dir")
    validated_profile = _validate_profile(profile)
    if quickstart:
        resolved_output.mkdir(parents=True, exist_ok=True)
        generated_config = write_quickstart_config_file(
            resolved_output, overrides=overrides
        )
        resolved_config = str(generated_config)
    else:
        if not config_file:
            raise ValueError("config_file is required when quickstart=false.")
        resolved_config = str(
            _resolve_within_root(config_file, kind="config_file")
        )

    output = io.StringIO()
    returncode = bundle_init(
        BundleInitCommand(
            output_dir=str(resolved_output),
            config_file=resolved_config,
            profile=validated_profile,
        ),
        output=output,
    )
    payload = {
        "returncode": returncode,
        "output": output.getvalue(),
        "output_dir": str(resolved_output),
    }
    if returncode:
        raise RuntimeError(json.dumps(payload, default=str))
    return payload


def _run_bundle_validate(
    bundle_dir: str = ".",
    target: Optional[str] = None,
    profile: Optional[str] = None,
) -> Dict[str, Any]:
    from databricks.labs.sdp_meta.bundle import BundleValidateCommand, bundle_validate

    resolved_bundle = _resolve_within_root(bundle_dir, kind="bundle_dir")
    output = io.StringIO()
    returncode = bundle_validate(
        BundleValidateCommand(
            bundle_dir=str(resolved_bundle),
            target=_validate_target(target),
            profile=_validate_profile(profile),
        ),
        output=output,
    )
    payload = {"returncode": returncode, "output": output.getvalue()}
    return payload


class FlowInput(BaseModel):
    """Validated input model for a single SDP-META flow."""

    model_config = ConfigDict(extra="forbid")

    source_format: str = "cloudFiles"
    source_path: Optional[str] = None
    source_database: Optional[str] = None
    source_table: Optional[str] = None
    source_schema_path: Optional[str] = None
    kafka_bootstrap_servers: Optional[str] = None
    kafka_topic: Optional[str] = None
    starting_offsets: str = "earliest"
    snapshot_format: str = "delta"
    bronze_table: Optional[str] = None
    silver_table: Optional[str] = None
    layer: Optional[str] = None
    bronze_target_schema: Optional[str] = None
    silver_target_schema: Optional[str] = None
    data_flow_id: str = "auto"
    data_flow_group: Optional[str] = None
    source_system: str = "auto_added"
    cloudfiles_format: str = "json"


class PipelineInput(BaseModel):
    """Validated input model for one bundle pipeline topology."""

    model_config = ConfigDict(extra="forbid")

    name: str
    layer: str
    dataflow_group: str
    pipeline_mode: str = "split"
    bronze_target_schema: Optional[str] = None
    silver_target_schema: Optional[str] = None


def _run_bundle_add_pipeline(
    pipeline: PipelineInput,
    bundle_dir: str = ".",
    dry_run: bool = False,
) -> Dict[str, Any]:
    from databricks.labs.sdp_meta.bundle import (
        BundleAddPipelineCommand,
        PipelineSpec,
        bundle_add_pipeline,
    )

    resolved_bundle = _resolve_within_root(bundle_dir, kind="bundle_dir")
    pipeline_spec = PipelineSpec(**pipeline.model_dump(exclude_none=True))
    output = io.StringIO()
    returncode = bundle_add_pipeline(
        BundleAddPipelineCommand(
            bundle_dir=str(resolved_bundle),
            pipeline=pipeline_spec,
            dry_run=dry_run,
        ),
        output=output,
    )
    payload = {
        "returncode": returncode,
        "output": output.getvalue(),
        "pipeline_added": asdict(pipeline_spec),
    }
    if returncode:
        raise RuntimeError(json.dumps(payload, default=str))
    return payload


def _run_bundle_add_flow(
    flows: List[FlowInput],
    bundle_dir: str = ".",
    onboarding_file: Optional[str] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    from databricks.labs.sdp_meta.bundle import (
        BundleAddFlowCommand,
        FlowSpec,
        bundle_add_flow,
    )

    if not flows:
        raise ValueError("flows must contain at least one entry.")
    flow_specs = [
        FlowSpec(**flow.model_dump(exclude_none=True)) for flow in flows
    ]
    for index, flow in enumerate(flow_specs):
        for field_name in ("source_database", "source_table", "data_flow_group"):
            value = getattr(flow, field_name, None)
            if value:
                validate_uc_identifier(value, kind=f"flows[{index}].{field_name}")

    resolved_bundle = _resolve_within_root(bundle_dir, kind="bundle_dir")
    resolved_onboarding = (
        str(_resolve_within_root(onboarding_file, kind="onboarding_file"))
        if onboarding_file
        else None
    )
    output = io.StringIO()
    returncode = bundle_add_flow(
        BundleAddFlowCommand(
            bundle_dir=str(resolved_bundle),
            onboarding_file=resolved_onboarding,
            flows=flow_specs,
            dry_run=dry_run,
        ),
        output=output,
    )
    payload = {
        "returncode": returncode,
        "output": output.getvalue(),
        "flows_added": [asdict(flow) for flow in flow_specs],
    }
    if returncode:
        raise RuntimeError(json.dumps(payload, default=str))
    return payload


def _list_templates() -> Dict[str, Any]:
    names = [name for name, _ in _list_template_files()]
    return {"templates": names}


def _get_onboarding_template(name: str) -> Dict[str, Any]:
    if not name:
        raise ValueError("`name` is required.")
    matches = [path for candidate, path in _list_template_files() if candidate == name]
    if not matches:
        available = [candidate for candidate, _ in _list_template_files()]
        raise FileNotFoundError(
            f"Template {name!r} not found. Available templates: {available}"
        )
    return {"name": name, "content": matches[0].read_text()}


_DIRECT_HANDLERS = {
    "sdp_meta_bundle_init": lambda args: _run_bundle_init(**args),
    "sdp_meta_bundle_validate": lambda args: _run_bundle_validate(**args),
    "sdp_meta_bundle_add_flow": lambda args: _run_bundle_add_flow(
        flows=[FlowInput.model_validate(flow) for flow in args.get("flows", [])],
        bundle_dir=args.get("bundle_dir", "."),
        onboarding_file=args.get("onboarding_file"),
        dry_run=args.get("dry_run", False),
    ),
    "sdp_meta_bundle_add_pipeline": lambda args: _run_bundle_add_pipeline(
        pipeline=PipelineInput.model_validate(args.get("pipeline")),
        bundle_dir=args.get("bundle_dir", "."),
        dry_run=args.get("dry_run", False),
    ),
    "sdp_meta_list_templates": lambda args: _list_templates(),
    "sdp_meta_get_onboarding_template": lambda args: _get_onboarding_template(
        args.get("name")
    ),
}


def call_tool(name: str, arguments: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Direct dispatcher retained for focused unit tests."""
    if name not in _DIRECT_HANDLERS:
        raise ValueError(f"Unknown tool: {name}. Available: {list(_DIRECT_HANDLERS)}")
    return _DIRECT_HANDLERS[name](arguments or {})


def _invoke_mcp_tool(function, *args, **kwargs) -> Dict[str, Any]:
    """Expose expected, actionable failures without leaking unexpected errors."""
    try:
        return function(*args, **kwargs)
    except (FileNotFoundError, RuntimeError, ValueError) as exc:
        raise ToolError(str(exc)) from exc


def build_server(_sdp_meta: Any = None) -> MCPServer:
    """Build the MCP 2.x server without opening a transport."""
    del _sdp_meta
    server = MCPServer(
        name="sdp-meta",
        version=__version__,
        description="Scaffold, inspect, and validate local SDP-META bundles.",
        instructions=(
            "Use these tools only inside SDP_META_MCP_ROOT. Live onboarding and "
            "deployment are intentionally not exposed."
        ),
    )

    @server.tool(
        name="sdp_meta_bundle_init",
        description="Scaffold a new SDP-META Databricks Asset Bundle.",
        annotations=ToolAnnotations(
            readOnlyHint=False,
            destructiveHint=True,
            idempotentHint=False,
            openWorldHint=True,
        ),
        structured_output=True,
    )
    def bundle_init_tool(
        output_dir: str = ".",
        quickstart: bool = True,
        config_file: Optional[str] = None,
        overrides: Optional[Dict[str, Any]] = None,
        profile: Optional[str] = None,
    ) -> Dict[str, Any]:
        return _invoke_mcp_tool(
            _run_bundle_init,
            output_dir,
            quickstart,
            config_file,
            overrides,
            profile,
        )

    @server.tool(
        name="sdp_meta_bundle_validate",
        description="Validate a bundle with Databricks CLI and SDP-META checks.",
        annotations=ToolAnnotations(
            readOnlyHint=True,
            destructiveHint=False,
            idempotentHint=True,
            openWorldHint=True,
        ),
        structured_output=True,
    )
    def bundle_validate_tool(
        bundle_dir: str = ".",
        target: Optional[str] = None,
        profile: Optional[str] = None,
    ) -> Dict[str, Any]:
        return _invoke_mcp_tool(
            _run_bundle_validate, bundle_dir, target, profile
        )

    @server.tool(
        name="sdp_meta_bundle_add_flow",
        description="Append one or more flows to a scaffolded bundle.",
        annotations=ToolAnnotations(
            readOnlyHint=False,
            destructiveHint=True,
            idempotentHint=False,
            openWorldHint=False,
        ),
        structured_output=True,
    )
    def bundle_add_flow_tool(
        flows: List[FlowInput],
        bundle_dir: str = ".",
        onboarding_file: Optional[str] = None,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        return _invoke_mcp_tool(
            _run_bundle_add_flow,
            flows,
            bundle_dir,
            onboarding_file,
            dry_run,
        )

    @server.tool(
        name="sdp_meta_bundle_add_pipeline",
        description="Add and wire an independently configured bundle pipeline.",
        annotations=ToolAnnotations(
            readOnlyHint=False,
            destructiveHint=True,
            idempotentHint=False,
            openWorldHint=False,
        ),
        structured_output=True,
    )
    def bundle_add_pipeline_tool(
        pipeline: PipelineInput,
        bundle_dir: str = ".",
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        return _invoke_mcp_tool(
            _run_bundle_add_pipeline,
            pipeline,
            bundle_dir,
            dry_run,
        )

    @server.tool(
        name="sdp_meta_list_templates",
        description="List packaged onboarding, DQE, and transformation templates.",
        annotations=ToolAnnotations(
            readOnlyHint=True,
            destructiveHint=False,
            idempotentHint=True,
            openWorldHint=False,
        ),
        structured_output=True,
    )
    def list_templates_tool() -> Dict[str, Any]:
        return _invoke_mcp_tool(_list_templates)

    @server.tool(
        name="sdp_meta_get_onboarding_template",
        description="Read one packaged template selected by its logical name.",
        annotations=ToolAnnotations(
            readOnlyHint=True,
            destructiveHint=False,
            idempotentHint=True,
            openWorldHint=False,
        ),
        structured_output=True,
    )
    def get_onboarding_template_tool(
        name: str = Field(description="Name returned by sdp_meta_list_templates")
    ) -> Dict[str, Any]:
        return _invoke_mcp_tool(_get_onboarding_template, name)

    try:
        template_files = _list_template_files()
    except RuntimeError as exc:
        logger.error(
            "Template resources are unavailable; starting with bundle tools only: %s",
            exc,
        )
        template_files = []

    for logical_name, template_path in template_files:
        def make_reader(path: Path):
            def read_template() -> str:
                return path.read_text()

            return read_template

        server.resource(
            f"{_RESOURCE_PREFIX}{logical_name}",
            name=logical_name,
            description=f"Packaged SDP-META template {logical_name}",
            mime_type=_mime_for_template(logical_name),
        )(make_reader(template_path))

    return server


def run_stdio(sdp_meta: Any = None) -> None:
    """Run the MCP server over stdio until the client disconnects."""
    build_server(sdp_meta).run(transport="stdio")
