"""CLI helpers that expose sdp-meta as a Declarative Automation Bundle.

The entry points consumed by the `databricks labs sdp-meta bundle-*`
commands are module-level functions:

- :func:`bundle_init` — scaffold a new bundle from the packaged template.
- :func:`bundle_prepare_wheel` — build and upload the local sdp-meta wheel
  to a UC volume for use as the bundle's ``sdp_meta_dependency``.
- :func:`bundle_validate` — run ``databricks bundle validate`` plus
  sdp-meta-specific sanity checks on a rendered bundle.
- :func:`bundle_add_pipeline` — add an independent pipeline topology and
  wire it into the bundle's pipelines job.

All three shell out to the Databricks CLI for bundle-level work. Everything
else is deliberately thin so behavior is easy to audit and mock in tests.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, TextIO, Tuple

import yaml

from databricks.labs.sdp_meta.identifiers import (
    SUPPORTED_SOURCE_FORMATS,
    prompt_uc_identifier,
    validate_source_format,
    validate_uc_identifier,
)

logger = logging.getLogger("databricks.labs.sdp_meta")


TEMPLATE_DIR = Path(__file__).parent / "templates" / "dab"


def _template_path() -> Path:
    """Return the absolute path to the packaged bundle template.

    Raises FileNotFoundError with a helpful message if the template is missing
    (e.g. a bad install that stripped non-``.py`` files from the wheel).
    """
    if not TEMPLATE_DIR.is_dir():
        raise FileNotFoundError(
            f"sdp-meta bundle template not found at {TEMPLATE_DIR}. "
            "Reinstall the package (e.g. `pip install --force-reinstall "
            "databricks-labs-sdp-meta`) and try again."
        )
    schema = TEMPLATE_DIR / "databricks_template_schema.json"
    if not schema.is_file():
        raise FileNotFoundError(
            f"sdp-meta bundle template at {TEMPLATE_DIR} is missing "
            "databricks_template_schema.json."
        )
    return TEMPLATE_DIR


def _resolve_databricks_cli() -> str:
    """Return the path to the `databricks` CLI or raise with a usable hint."""
    path = shutil.which("databricks")
    if not path:
        raise RuntimeError(
            "Could not find the `databricks` CLI on PATH. Install it first: "
            "https://docs.databricks.com/en/dev-tools/cli/tutorial.html"
        )
    return path


def _run(
    cmd: List[str],
    *,
    cwd: Optional[Path] = None,
    check: bool = True,
    capture_output: bool = False,
) -> subprocess.CompletedProcess:
    """Thin wrapper around subprocess.run that logs the command."""
    logger.info("$ %s", " ".join(cmd))
    return subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        check=check,
        stdout=subprocess.PIPE if capture_output else None,
        stderr=subprocess.STDOUT if capture_output else None,
        text=capture_output,
    )


def _write_process_output(
    result: subprocess.CompletedProcess, output: Optional[TextIO]
) -> None:
    """Forward captured subprocess output to a non-protocol stream."""
    if output is not None and result.stdout:
        print(result.stdout, file=output, end="")


# ---------------------------------------------------------------------------
# bundle init
# ---------------------------------------------------------------------------

@dataclass
class BundleInitCommand:
    """Parameters for `databricks labs sdp-meta bundle init`."""

    output_dir: str = "."
    config_file: Optional[str] = None
    profile: Optional[str] = None


def _discover_bundle_dir(output_dir: Path, config_file: Optional[str]) -> Optional[Path]:
    """Locate the folder ``databricks bundle init`` actually created.

    ``--output-dir`` is the *parent*; the template creates its project folder
    (named after the ``bundle_name`` answer) inside it, so the real bundle
    lives at ``output_dir / <bundle_name>``. The template's own success
    message prints ``cd <bundle_name>`` with no knowledge of ``--output-dir``,
    which is misleading whenever the two differ. Recover the true path so the
    wrapper can print an accurate ``cd``.

    Prefers the ``bundle_name`` recorded in the non-interactive config file
    (quickstart always writes one); falls back to the newest immediate
    subdirectory that contains a ``databricks.yml``.
    """
    if config_file:
        try:
            data = json.loads(Path(config_file).read_text())
            name = (data or {}).get("bundle_name")
            if name:
                candidate = output_dir / name
                if (candidate / "databricks.yml").is_file():
                    return candidate
        except (OSError, ValueError):
            pass
    try:
        candidates = [
            p for p in output_dir.iterdir()
            if p.is_dir() and (p / "databricks.yml").is_file()
        ]
    except OSError:
        return None
    if candidates:
        return max(candidates, key=lambda p: p.stat().st_mtime)
    return None


def _stamp_sdp_meta_version(bundle_dir: Path) -> None:
    """Replace the ``sdp_meta_version`` default in a scaffolded bundle.

    The template ships ``sdp_meta_version`` defaulting to the ``"true"``
    sentinel so a raw ``databricks bundle init`` still produces a discoverable
    (non-empty) ``sdp_meta`` pipeline tag. When the scaffold goes through the
    sdp-meta CLI we know the installed version, so stamp it in — that makes the
    bundle-deployed pipelines carry ``sdp_meta=<version>``, matching what
    ``sdp-meta deploy`` writes and letting the SDP-META App show the version.

    Best-effort: if the file or the sentinel default is missing (e.g. the
    template was customised), leave the scaffold untouched.
    """
    from databricks.labs.sdp_meta.__about__ import __version__

    variables_yml = bundle_dir / "resources" / "variables.yml"
    if not variables_yml.is_file():
        return
    text = variables_yml.read_text()
    # Anchor on the ``sdp_meta_version:`` key and rewrite the first ``default:``
    # that follows it (its own), stopping before the next 2-space top-level key.
    patched, count = re.subn(
        r'(^  sdp_meta_version:\n(?:.*\n)*?    default: )"true"',
        rf'\g<1>"{__version__}"',
        text,
        count=1,
        flags=re.MULTILINE,
    )
    if count:
        variables_yml.write_text(patched)


def bundle_init(cmd: BundleInitCommand, *, output: Optional[TextIO] = None) -> int:
    """Scaffold a new sdp-meta DAB from the packaged template.

    Delegates to ``databricks bundle init <template> --output-dir ...``.
    If ``config_file`` is provided, the template's prompts are pre-answered
    from that JSON file (used by tests and by non-interactive callers).
    """
    template_dir = _template_path()
    databricks_cli = _resolve_databricks_cli()

    output_dir = Path(cmd.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    argv = [databricks_cli, "bundle", "init", str(template_dir), "--output-dir", str(output_dir)]
    if cmd.config_file:
        argv.extend(["--config-file", str(Path(cmd.config_file).resolve())])
    if cmd.profile:
        argv.extend(["--profile", cmd.profile])

    result = _run(argv, check=False, capture_output=output is not None)
    _write_process_output(result, output)
    if result.returncode != 0:
        logger.error("databricks bundle init failed with exit code %s", result.returncode)
        return result.returncode

    bundle_dir = _discover_bundle_dir(output_dir, cmd.config_file)
    if bundle_dir is not None:
        # Stamp the installed sdp-meta version into resources/variables.yml so
        # bundle-deployed pipelines carry sdp_meta=<version> (App discovery +
        # version display), matching `sdp-meta deploy`.
        _stamp_sdp_meta_version(bundle_dir)
        # Authoritative, output-dir-aware message. Note the template also
        # prints its own `cd <bundle_name>` hint above; that hint ignores
        # --output-dir, so this line is the one to trust.
        print(
            f"\nBundle scaffolded at {bundle_dir}.\n"
            "Next:\n"
            f"  cd {bundle_dir}\n"
            "  # edit conf/onboarding.* with your real sources, then:\n"
            "  databricks bundle deploy --target dev",
            file=output,
        )
    else:
        print(
            "\nBundle scaffolded under "
            f"{output_dir}. Next: edit conf/onboarding.* with your real sources, "
            "then `databricks bundle deploy --target dev`.",
            file=output,
        )
    return 0


# ---------------------------------------------------------------------------
# bundle prepare-wheel
# ---------------------------------------------------------------------------

@dataclass
class BundlePrepareWheelCommand:
    """Parameters for `databricks labs sdp-meta bundle prepare-wheel`."""

    uc_catalog: str
    uc_schema: str
    uc_volume: str
    profile: Optional[str] = None
    # Forwarded to `pip wheel` as `--index-url`. Use this to point at a private
    # pip mirror (e.g. https://pypi.internal.example.com/simple) when the
    # build host can't reach pypi.org. If unset, falls back to the PIP_INDEX_URL
    # environment variable; if that is also unset, pip uses its default index.
    pip_index_url: Optional[str] = None
    # Forwarded to `pip wheel` as one or more `--extra-index-url` flags. Useful
    # when the primary index is internal but a few packages still come from
    # pypi.org. Falls back to PIP_EXTRA_INDEX_URL when unset.
    pip_extra_index_urls: Optional[List[str]] = None
    # When True (the default) the schema and volume are auto-created if they
    # don't already exist under the (existing) catalog. Catalogs are never
    # auto-created since that almost always requires metastore-admin perms.
    # Set False if your principal is read-only on the schema namespace and
    # you want a hard failure instead of an attempted CREATE SCHEMA.
    create_if_missing: bool = True

    def __post_init__(self) -> None:
        # Reject illegal UC names up-front so we never try to splice a
        # hyphenated catalog into a CREATE SCHEMA / CREATE VOLUME later
        # (issue #261). Must mirror the strict regular-identifier rule used
        # by the rest of the input boundaries (CLI, DAB template).
        validate_uc_identifier(self.uc_catalog, kind="uc_catalog")
        validate_uc_identifier(self.uc_schema, kind="uc_schema")
        validate_uc_identifier(self.uc_volume, kind="uc_volume")


def bundle_prepare_wheel(cmd: BundlePrepareWheelCommand) -> str:
    """Build the local sdp-meta wheel and upload it to a UC volume.

    Returns the resulting ``/Volumes/...`` path which the caller should paste
    into ``resources/variables.yml`` as the default for ``sdp_meta_dependency``.
    """
    from databricks.sdk import WorkspaceClient  # local import keeps tests light
    from databricks.sdk.service.catalog import VolumeType

    from databricks.labs.sdp_meta.__about__ import __version__

    if not cmd.uc_catalog or not cmd.uc_schema or not cmd.uc_volume:
        raise ValueError("--uc-catalog, --uc-schema and --uc-volume are all required")

    repo_root = Path(__file__).resolve().parents[4]
    setup_py = repo_root / "setup.py"
    if not setup_py.is_file():
        raise FileNotFoundError(
            "setup.py not found — bundle prepare-wheel must be run from the "
            "sdp-meta source tree (where `setup.py` lives)."
        )

    dist_dir = repo_root / "dist"
    dist_dir.mkdir(exist_ok=True)
    logger.info("Building sdp-meta wheel in %s ...", dist_dir)

    # ``SDP_META_NO_BUILD_ISOLATION`` defaults to ``"1"`` here so the
    # pip wheel build skips PEP 517 build isolation and reuses the build
    # tools already installed in the active interpreter (setuptools +
    # wheel). With isolation ON, pip downloads a fresh setuptools into
    # an ephemeral venv on every invocation; that download routinely
    # fails on Databricks-internal pip proxies that lag the public
    # PyPI index for newly-published setuptools releases (404 on the
    # ``.metadata`` file). The mirror of this knob in ``install.py``
    # uses the same env var; both default-on so users don't have to
    # opt in.
    env = os.environ.copy()
    env.setdefault("SDP_META_NO_BUILD_ISOLATION", "1")

    pip_argv: List[str] = [
        sys.executable, "-m", "pip", "wheel", "--no-deps",
        "--wheel-dir", str(dist_dir),
    ]
    if env.get("SDP_META_NO_BUILD_ISOLATION", "").lower() in ("1", "true", "yes"):
        pip_argv.append("--no-build-isolation")
    index_url = cmd.pip_index_url or os.environ.get("PIP_INDEX_URL")
    if index_url:
        pip_argv.extend(["--index-url", index_url])
        logger.info("Using pip --index-url %s", index_url)
    extras = cmd.pip_extra_index_urls
    if not extras and os.environ.get("PIP_EXTRA_INDEX_URL"):
        # PIP_EXTRA_INDEX_URL accepts space-separated URLs per pip docs.
        extras = [u for u in os.environ["PIP_EXTRA_INDEX_URL"].split() if u]
    for extra in extras or []:
        pip_argv.extend(["--extra-index-url", extra])

    pip_argv.append(str(repo_root))
    _run(pip_argv)

    wheels = sorted(dist_dir.glob(f"databricks_labs_sdp_meta-{__version__}*.whl"))
    if not wheels:
        raise RuntimeError(
            f"Wheel build produced no file matching "
            f"databricks_labs_sdp_meta-{__version__}*.whl under {dist_dir}"
        )
    wheel = wheels[-1]

    ws = WorkspaceClient(profile=cmd.profile) if cmd.profile else WorkspaceClient()

    # Catalogs are never auto-created (it's almost always a metastore-admin
    # action), so surface a clear, actionable error if it's missing.
    try:
        ws.catalogs.get(name=cmd.uc_catalog)
    except Exception as exc:
        raise RuntimeError(
            f"Catalog {cmd.uc_catalog!r} not found or not accessible with the "
            f"current credentials: {exc}. Catalogs are not auto-created — ask "
            f"a metastore admin to create it, or pass a different --uc-catalog."
        ) from exc

    schema_full = f"{cmd.uc_catalog}.{cmd.uc_schema}"
    try:
        ws.schemas.get(full_name=schema_full)
        logger.info("Using existing schema %s", schema_full)
    except Exception as exc:
        if not cmd.create_if_missing:
            raise RuntimeError(
                f"Schema {schema_full} not found and create_if_missing=False: {exc}. "
                f"Either create it manually (CREATE SCHEMA {schema_full}) or re-run "
                f"with create_if_missing=True (the CLI default)."
            ) from exc
        logger.info("Schema %s does not exist; creating it (create_if_missing=True)",
                    schema_full)
        try:
            ws.schemas.create(name=cmd.uc_schema, catalog_name=cmd.uc_catalog)
        except Exception as create_exc:
            raise RuntimeError(
                f"Schema {schema_full} does not exist and could not be auto-created: "
                f"{create_exc}. Either create it manually or pass a different "
                f"--uc-schema."
            ) from create_exc

    try:
        ws.volumes.create(
            catalog_name=cmd.uc_catalog,
            schema_name=cmd.uc_schema,
            name=cmd.uc_volume,
            # Recent SDKs require the VolumeType enum (not the string "MANAGED");
            # passing a bare string raises "'str' object has no attribute 'value'".
            volume_type=VolumeType.MANAGED,
        )
        logger.info("Created MANAGED volume %s.%s.%s",
                    cmd.uc_catalog, cmd.uc_schema, cmd.uc_volume)
    except Exception as exc:  # pragma: no cover — ``exists`` path is tested via mock
        # `volumes.create` is the only "test by trying" path here because the
        # SDK has no `volumes.exists`. ALREADY_EXISTS is the expected case on
        # re-runs; everything else (PERMISSION_DENIED, INVALID_PARAMETER, ...)
        # we want to surface so the user actually sees it.
        msg = str(exc)
        if "already exists" in msg.lower() or "ALREADY_EXISTS" in msg:
            logger.info("Volume %s.%s.%s already exists; reusing it",
                        cmd.uc_catalog, cmd.uc_schema, cmd.uc_volume)
        else:
            raise RuntimeError(
                f"Failed to ensure volume {cmd.uc_catalog}.{cmd.uc_schema}."
                f"{cmd.uc_volume}: {exc}"
            ) from exc

    volume_path = f"/Volumes/{cmd.uc_catalog}/{cmd.uc_schema}/{cmd.uc_volume}/{wheel.name}"
    with wheel.open("rb") as fh:
        ws.files.upload(file_path=volume_path, contents=fh, overwrite=True)

    print(
        "\nUploaded wheel to:\n"
        f"  {volume_path}\n\n"
        "Paste this path into your bundle's resources/variables.yml as the\n"
        "default for `sdp_meta_dependency`:\n\n"
        "  sdp_meta_dependency:\n"
        f"    default: {volume_path}\n"
    )
    return volume_path


# ---------------------------------------------------------------------------
# bundle validate
# ---------------------------------------------------------------------------

@dataclass
class BundleValidateCommand:
    """Parameters for `databricks labs sdp-meta bundle validate`."""

    bundle_dir: str = "."
    target: Optional[str] = None
    profile: Optional[str] = None


def _load_yaml_or_json(path: Path):
    text = path.read_text()
    if path.suffix.lower() in (".yml", ".yaml"):
        return yaml.safe_load(text)
    return json.loads(text)


# Matches the convention used by every seeded onboarding placeholder
# (e.g. `<your-kafka-host>`, `<your-secret-name>`, `<your-eventhub-namespace>`).
# Validates only on the well-known prefix `your-` so it can't false-positive
# against a real value that legitimately contains angle brackets (rare, but
# possible in eg. JSON-typed comments).
_PLACEHOLDER_RE = re.compile(r"<your-[^>]+>")


def _find_edit_me_placeholders(
    onboarding_doc: List[Any],
) -> List[Tuple[Any, str, str]]:
    """Walk the parsed onboarding document and return every `<your-...>` hit.

    Each hit is a tuple of ``(data_flow_id, dotted_field_path, raw_value)``.
    Recurses into nested dicts (notably ``source_details``) but not into
    lists, since none of the per-flow keys we ship use list values for the
    affected placeholders.
    """
    hits: List[Tuple[Any, str, str]] = []

    def _walk(node: Any, prefix: str, flow_id: Any) -> None:
        if isinstance(node, dict):
            for k, v in node.items():
                _walk(v, f"{prefix}.{k}" if prefix else str(k), flow_id)
        elif isinstance(node, str) and _PLACEHOLDER_RE.search(node):
            hits.append((flow_id, prefix, node))

    for flow in onboarding_doc:
        if not isinstance(flow, dict):
            continue
        flow_id = flow.get("data_flow_id")
        for key, value in flow.items():
            _walk(value, str(key), flow_id)
    return hits


def _find_yaml_placeholders(doc: Any) -> List[Tuple[str, str]]:
    """Walk an arbitrary parsed YAML/JSON document and return `<your-...>` hits.

    Unlike :func:`_find_edit_me_placeholders` (which knows the onboarding
    schema and reports per-flow), this is a generic dotted-path walker used
    for files like ``databricks.yml`` where the schema is open-ended. PyYAML
    discards comments at parse time, so commented-out blocks (e.g. the
    suggested ``run_as:`` block in the prod target) are *not* flagged --
    placeholders only fire when the user uncomments and forgets to fill in
    the real value.

    Each hit is ``(dotted_path, raw_value)``. Recurses into both dicts and
    lists so deeply-nested structures (``targets.prod.run_as.service_principal_name``,
    ``permissions[0].user_name``, ...) are covered.
    """
    hits: List[Tuple[str, str]] = []

    def _walk(node: Any, prefix: str) -> None:
        if isinstance(node, dict):
            for k, v in node.items():
                _walk(v, f"{prefix}.{k}" if prefix else str(k))
        elif isinstance(node, list):
            for i, v in enumerate(node):
                _walk(v, f"{prefix}[{i}]")
        elif isinstance(node, str) and _PLACEHOLDER_RE.search(node):
            hits.append((prefix, node))

    _walk(doc, "")
    return hits


_VARIABLE_REF_RE = re.compile(r"^\$\{var\.([A-Za-z0-9_]+)\}$")
_PIPELINE_REF_RE = re.compile(
    r"^\$\{resources\.pipelines\.([A-Za-z0-9_-]+)\.id\}$"
)


def _resolved_variable(
    variables: Dict[str, Any],
    databricks_doc: Dict[str, Any],
    name: str,
    target: Optional[str],
) -> Any:
    """Resolve a bundle variable default with an optional target override."""
    node = variables.get(name) or {}
    value = node.get("default") if isinstance(node, dict) else None
    if target:
        target_doc = ((databricks_doc.get("targets") or {}).get(target) or {})
        overrides = target_doc.get("variables") or {}
        if name in overrides:
            override = overrides[name]
            value = (
                override.get("value")
                if isinstance(override, dict) and "value" in override
                else override
            )
    return value


def _resolve_bundle_reference(
    value: Any,
    variables: Dict[str, Any],
    databricks_doc: Dict[str, Any],
    target: Optional[str],
) -> Any:
    """Resolve a lone ``${var.name}`` reference for static sanity checks."""
    if not isinstance(value, str):
        return value
    match = _VARIABLE_REF_RE.fullmatch(value)
    if not match:
        return value
    return _resolved_variable(variables, databricks_doc, match.group(1), target)


def _legacy_topology_errors(
    layer: Optional[str],
    pipeline_mode: str,
    pipes: Dict[str, Any],
) -> List[str]:
    """Validate pre-#446 resource stubs that do not expose configuration."""
    errors: List[str] = []
    has_bronze = "bronze" in pipes
    has_silver = "silver" in pipes
    has_combined = "bronze_silver" in pipes
    if layer == "bronze" and (not has_bronze or has_silver or has_combined):
        errors.append(
            "layer=bronze but sdp_meta_pipelines.yml has "
            f"{'bronze' if has_bronze else 'no bronze'}, "
            f"{'silver' if has_silver else 'no silver'}, "
            f"{'bronze_silver' if has_combined else 'no bronze_silver'} pipelines"
        )
    elif layer == "silver" and (has_bronze or not has_silver or has_combined):
        errors.append(
            "layer=silver but sdp_meta_pipelines.yml has "
            f"{'bronze' if has_bronze else 'no bronze'}, "
            f"{'silver' if has_silver else 'no silver'}, "
            f"{'bronze_silver' if has_combined else 'no bronze_silver'} pipelines"
        )
    elif layer == "bronze_silver":
        if pipeline_mode == "combined" and (
            not has_combined or has_bronze or has_silver
        ):
            errors.append(
                "layer=bronze_silver, pipeline_mode=combined expects exactly "
                "one `bronze_silver` pipeline; got "
                f"bronze={has_bronze}, silver={has_silver}, "
                f"bronze_silver={has_combined}"
            )
        elif pipeline_mode != "combined" and (
            not (has_bronze and has_silver) or has_combined
        ):
            errors.append(
                "layer=bronze_silver, pipeline_mode=split expects both `bronze` "
                "and `silver` pipelines and no `bronze_silver` pipeline; got "
                f"bronze={has_bronze}, silver={has_silver}, "
                f"bronze_silver={has_combined}"
            )
    return errors


def _sdp_meta_sanity_checks(
    bundle_dir: Path, target: Optional[str] = None
) -> List[str]:
    """sdp-meta-specific checks layered on top of `databricks bundle validate`.

    Returns a list of human-readable error strings (empty list = all good).
    These checks are all static: no workspace calls.
    """
    errors: List[str] = []
    conf_dir = bundle_dir / "conf"
    resources_dir = bundle_dir / "resources"

    # databricks.yml itself: catch un-filled `<your-...>` placeholders that
    # users may have introduced by uncommenting the run_as block (or any
    # other guidance comment we ship with placeholder values). PyYAML drops
    # comments at parse time, so this only fires once a user actually
    # uncomments and forgets to substitute their real value.
    databricks_yml = bundle_dir / "databricks.yml"
    db_yml_doc: Dict[str, Any] = {}
    if databricks_yml.is_file():
        try:
            db_yml_doc = yaml.safe_load(databricks_yml.read_text()) or {}
        except yaml.YAMLError as exc:
            errors.append(f"databricks.yml: invalid YAML ({exc})")
            db_yml_doc = {}
        if db_yml_doc:
            for dotted_field, value in _find_yaml_placeholders(db_yml_doc):
                errors.append(
                    f"databricks.yml: field `{dotted_field}` is still the "
                    f"placeholder {value!r}. Replace it with a real value "
                    "before deploying (e.g. uncomment + fill in run_as."
                    "service_principal_name with your prod service principal "
                    "application_id)."
                )

    variables_yml = resources_dir / "variables.yml"
    if not variables_yml.is_file():
        errors.append(f"Missing {variables_yml.relative_to(bundle_dir)}")
        return errors

    try:
        variables_doc = yaml.safe_load(variables_yml.read_text()) or {}
    except yaml.YAMLError as exc:
        errors.append(f"{variables_yml.relative_to(bundle_dir)}: invalid YAML ({exc})")
        return errors

    variables = variables_doc.get("variables", {}) or {}

    def _default(name: str):
        return _resolved_variable(variables, db_yml_doc, name, target)

    onboarding_file_name = _default("onboarding_file_name")
    groups_in_file = set()
    onboarding_flows: List[Dict[str, Any]] = []
    if not onboarding_file_name:
        errors.append("variables.yml: `onboarding_file_name` has no default value")
    else:
        onboarding_path = conf_dir / onboarding_file_name
        if not onboarding_path.is_file():
            errors.append(
                f"Onboarding file {onboarding_path.relative_to(bundle_dir)} "
                f"referenced by variables.yml is missing"
            )
        else:
            try:
                onboarding_doc = _load_yaml_or_json(onboarding_path)
            except (yaml.YAMLError, json.JSONDecodeError) as exc:
                errors.append(
                    f"{onboarding_path.relative_to(bundle_dir)}: invalid "
                    f"{onboarding_path.suffix} ({exc})"
                )
                onboarding_doc = None

            if onboarding_doc is not None:
                if not isinstance(onboarding_doc, list):
                    errors.append(
                        f"{onboarding_path.relative_to(bundle_dir)}: expected a list "
                        "of flow dicts at the top level"
                    )
                else:
                    onboarding_flows = [
                        flow for flow in onboarding_doc
                        if isinstance(flow, dict)
                    ]
                    groups_in_file = {
                        flow.get("data_flow_group")
                        for flow in onboarding_flows
                    }
                    placeholder_hits = _find_edit_me_placeholders(onboarding_doc)
                    rel = onboarding_path.relative_to(bundle_dir)
                    for flow_id, dotted_field, value in placeholder_hits:
                        errors.append(
                            f"{rel}: flow data_flow_id={flow_id!r} field "
                            f"`{dotted_field}` is still the placeholder "
                            f"{value!r}. Replace it with a real value before "
                            "deploying."
                        )

    sdp_meta_dep = _default("sdp_meta_dependency")
    wheel_source = _default("wheel_source")
    if sdp_meta_dep == "__SET_ME__" or not sdp_meta_dep:
        errors.append(
            "variables.yml: `sdp_meta_dependency` is still the `__SET_ME__` "
            "sentinel. Replace it with either a PyPI coordinate (e.g. "
            "`databricks-labs-sdp-meta==0.1.0`) or a UC-volume wheel path "
            "(produced by `databricks labs sdp-meta bundle-prepare-wheel`)."
        )
    elif wheel_source == "volume_path" and not sdp_meta_dep.startswith("/Volumes/"):
        errors.append(
            f"variables.yml: wheel_source=volume_path but `sdp_meta_dependency` "
            f"({sdp_meta_dep!r}) does not start with `/Volumes/`. Run "
            "`databricks labs sdp-meta bundle-prepare-wheel` to upload the "
            "wheel, then paste the printed path here."
        )
    elif wheel_source == "pypi" and sdp_meta_dep.startswith("/Volumes/"):
        errors.append(
            f"variables.yml: wheel_source=pypi but `sdp_meta_dependency` "
            f"({sdp_meta_dep!r}) is a `/Volumes/` path. Either change "
            "wheel_source to `volume_path` or replace the dependency with a "
            "PyPI coordinate."
        )

    layer = _default("layer")
    pipeline_mode = _default("pipeline_mode") or "split"
    pipelines_yml = resources_dir / "sdp_meta_pipelines.yml"
    if not pipelines_yml.is_file():
        errors.append(f"Missing {pipelines_yml.relative_to(bundle_dir)}")
    else:
        try:
            pipelines_doc = yaml.safe_load(pipelines_yml.read_text()) or {}
        except yaml.YAMLError as exc:
            errors.append(f"{pipelines_yml.relative_to(bundle_dir)}: invalid YAML ({exc})")
            pipelines_doc = {}

        pipes = (pipelines_doc.get("resources", {}) or {}).get("pipelines", {}) or {}
        configured = {
            key: spec
            for key, spec in pipes.items()
            if isinstance(spec, dict) and isinstance(spec.get("configuration"), dict)
        }
        if pipes and not configured:
            # Keep compatibility with old/minimal bundles whose resource stubs
            # predate per-pipeline configuration. Fully rendered #446 bundles
            # use the configuration-driven validator below.
            errors.extend(_legacy_topology_errors(layer, pipeline_mode, pipes))
            dataflow_group = _default("dataflow_group")
            if dataflow_group and dataflow_group not in groups_in_file:
                errors.append(
                    f"dataflow_group `{dataflow_group}` (from variables.yml) "
                    "is not used by any onboarding flow; flows use: "
                    f"{sorted(g for g in groups_in_file if g)}"
                )
        else:
            if not pipes:
                errors.append("sdp_meta_pipelines.yml defines no pipeline resources")
            for key in sorted(set(pipes) - set(configured)):
                errors.append(
                    f"Pipeline `{key}` has no `configuration` mapping and cannot "
                    "be validated as an sdp-meta pipeline"
                )

            pipeline_groups: Dict[str, Any] = {}
            pipeline_layers: Dict[str, str] = {}
            for key, spec in configured.items():
                config = spec["configuration"]
                if not spec.get("schema"):
                    errors.append(
                        f"Pipeline `{key}` is missing its target `schema`"
                    )
                if not config.get("sdp_meta_dependency"):
                    errors.append(
                        f"Pipeline `{key}` is missing configuration "
                        "`sdp_meta_dependency`"
                    )
                pipeline_layer = config.get("layer")
                if pipeline_layer not in ("bronze", "silver", "bronze_silver"):
                    errors.append(
                        f"Pipeline `{key}` configuration.layer must be one of "
                        "bronze, silver, bronze_silver"
                    )
                    continue
                pipeline_layers[key] = pipeline_layer
                required_prefixes = (
                    ("bronze", "silver")
                    if pipeline_layer == "bronze_silver"
                    else (pipeline_layer,)
                )
                resolved_groups = []
                for prefix in required_prefixes:
                    table_key = f"{prefix}.dataflowspecTable"
                    group_key = f"{prefix}.group"
                    if not config.get(table_key):
                        errors.append(
                            f"Pipeline `{key}` ({pipeline_layer}) is missing "
                            f"configuration `{table_key}`"
                        )
                    raw_group = config.get(group_key)
                    if not raw_group:
                        errors.append(
                            f"Pipeline `{key}` ({pipeline_layer}) is missing "
                            f"configuration `{group_key}`"
                        )
                        continue
                    group = _resolve_bundle_reference(
                        raw_group, variables, db_yml_doc, target
                    )
                    resolved_groups.append(group)
                    if group not in groups_in_file:
                        errors.append(
                            f"Pipeline `{key}` references {group_key}={group!r}, "
                            "which is not used by any onboarding flow; flows use: "
                            f"{sorted(g for g in groups_in_file if g)}"
                        )
                    has_layer_row = any(
                        flow.get("data_flow_group") == group
                        and any(
                            field.startswith(f"{prefix}_database_") and value
                            for field, value in flow.items()
                        )
                        and flow.get(f"{prefix}_table")
                        for flow in onboarding_flows
                    )
                    if group in groups_in_file and not has_layer_row:
                        errors.append(
                            f"Pipeline `{key}` references {group_key}={group!r}, "
                            f"but that group has no {prefix} onboarding row with "
                            f"`{prefix}_database_<environment>` and "
                            f"`{prefix}_table`"
                        )
                if len(set(resolved_groups)) > 1:
                    errors.append(
                        f"Pipeline `{key}` uses different bronze/silver groups "
                        f"{resolved_groups}; a combined pipeline must use one group"
                    )
                if resolved_groups:
                    pipeline_groups[key] = resolved_groups[0]

            required_onboarding_layers = set()
            for pipeline_layer in pipeline_layers.values():
                if pipeline_layer == "bronze_silver":
                    required_onboarding_layers.update(("bronze", "silver"))
                else:
                    required_onboarding_layers.add(pipeline_layer)
            configured_onboarding_layers = (
                {"bronze", "silver"}
                if layer == "bronze_silver"
                else {layer} if layer in ("bronze", "silver") else set()
            )
            missing_onboarding_layers = (
                required_onboarding_layers - configured_onboarding_layers
            )
            if missing_onboarding_layers:
                errors.append(
                    f"Onboarding layer={layer!r} does not cover pipeline layer(s) "
                    f"{sorted(missing_onboarding_layers)}"
                )

            onboarding_job_yml = (
                resources_dir / "sdp_meta_onboarding_job.yml"
            )
            if not onboarding_job_yml.is_file():
                errors.append(
                    f"Missing {onboarding_job_yml.relative_to(bundle_dir)}"
                )
            else:
                try:
                    onboarding_job_doc = (
                        yaml.safe_load(onboarding_job_yml.read_text()) or {}
                    )
                except yaml.YAMLError as exc:
                    errors.append(
                        f"{onboarding_job_yml.relative_to(bundle_dir)}: "
                        f"invalid YAML ({exc})"
                    )
                    onboarding_job_doc = {}
                onboarding_tasks = (
                    (((onboarding_job_doc.get("resources") or {}).get("jobs") or {})
                     .get("onboarding") or {}).get("tasks") or []
                )
                onboard_task = next(
                    (
                        task for task in onboarding_tasks
                        if isinstance(task, dict)
                        and task.get("task_key") == "onboard_dataflowspecs"
                    ),
                    {},
                )
                named_parameters = (
                    (onboard_task.get("python_wheel_task") or {})
                    .get("named_parameters") or {}
                )
                for required_layer in required_onboarding_layers:
                    parameter = f"{required_layer}_dataflowspec_table"
                    if not named_parameters.get(parameter):
                        errors.append(
                            "Onboarding task `onboard_dataflowspecs` is missing "
                            f"named parameter `{parameter}` required by configured "
                            "pipelines"
                        )

            jobs = (pipelines_doc.get("resources", {}) or {}).get("jobs", {}) or {}
            pipeline_job = jobs.get("pipelines") or {}
            tasks = pipeline_job.get("tasks") or []
            if configured and not isinstance(tasks, list):
                errors.append("Job `pipelines.tasks` must be a list")
                tasks = []
            task_by_key = {
                task.get("task_key"): task
                for task in tasks
                if isinstance(task, dict) and task.get("task_key")
            }
            refs: Dict[str, List[str]] = {}
            for task_key, task in task_by_key.items():
                pipeline_id = (task.get("pipeline_task") or {}).get("pipeline_id")
                match = (
                    _PIPELINE_REF_RE.fullmatch(pipeline_id)
                    if isinstance(pipeline_id, str)
                    else None
                )
                if not match:
                    errors.append(
                        f"Job task `{task_key}` must reference a pipeline as "
                        "`${resources.pipelines.<key>.id}`"
                    )
                    continue
                ref = match.group(1)
                refs.setdefault(ref, []).append(task_key)
                if ref not in configured:
                    errors.append(
                        f"Job task `{task_key}` references unknown pipeline `{ref}`"
                    )
                dependencies = {
                    dep.get("task_key")
                    for dep in (task.get("depends_on") or [])
                    if isinstance(dep, dict)
                }
                unknown_dependencies = dependencies - set(task_by_key)
                if unknown_dependencies:
                    errors.append(
                        f"Job task `{task_key}` depends on unknown task(s) "
                        f"{sorted(unknown_dependencies)}"
                    )

            for key in configured:
                count = len(refs.get(key, []))
                if count != 1:
                    errors.append(
                        f"Pipeline `{key}` must be referenced by exactly one task "
                        f"in job `pipelines`; found {count}"
                    )

            # A silver pipeline sharing a group with a bronze pipeline is a
            # split topology. Its task must wait for at least one matching
            # bronze task so the upstream tables are refreshed first.
            for silver_key, silver_layer in pipeline_layers.items():
                if silver_layer != "silver":
                    continue
                matching_bronze = {
                    key
                    for key, candidate_layer in pipeline_layers.items()
                    if candidate_layer == "bronze"
                    and pipeline_groups.get(key) == pipeline_groups.get(silver_key)
                }
                if not matching_bronze or silver_key not in refs:
                    continue
                silver_task = task_by_key[refs[silver_key][0]]
                dependencies = {
                    dep.get("task_key")
                    for dep in (silver_task.get("depends_on") or [])
                    if isinstance(dep, dict)
                }
                matching_tasks = {
                    task_key
                    for key in matching_bronze
                    for task_key in refs.get(key, [])
                }
                if not dependencies.intersection(matching_tasks):
                    errors.append(
                        f"Silver pipeline `{silver_key}` shares dataflow group "
                        f"{pipeline_groups.get(silver_key)!r} with bronze pipeline(s) "
                        f"{sorted(matching_bronze)} but its job task does not depend "
                        "on a matching bronze task"
                    )

    return errors


def bundle_validate(
    cmd: BundleValidateCommand, *, output: Optional[TextIO] = None
) -> int:
    """Run `databricks bundle validate` plus sdp-meta sanity checks.

    Exit code is:
      - 0 if both the Databricks validator and the sdp-meta checks pass.
      - non-zero if either fails (Databricks exit code takes precedence).
    """
    bundle_dir = Path(cmd.bundle_dir).resolve()
    if not (bundle_dir / "databricks.yml").is_file():
        print(
            f"ERROR: {bundle_dir} does not look like a bundle (no databricks.yml)",
            file=output,
        )
        return 2

    errors = _sdp_meta_sanity_checks(bundle_dir, target=cmd.target)
    if errors:
        print("sdp-meta sanity checks FAILED:", file=output)
        for err in errors:
            print(f"  - {err}", file=output)
    else:
        print("sdp-meta sanity checks: OK", file=output)

    databricks_cli = _resolve_databricks_cli()
    argv = [databricks_cli, "bundle", "validate"]
    if cmd.target:
        argv.extend(["--target", cmd.target])
    if cmd.profile:
        argv.extend(["--profile", cmd.profile])

    result = _run(
        argv,
        cwd=bundle_dir,
        check=False,
        capture_output=output is not None,
    )
    _write_process_output(result, output)

    if result.returncode == 0 and not errors:
        print("\nAll checks passed.", file=output)
        return 0
    if result.returncode != 0:
        return result.returncode
    return 1


# ---------------------------------------------------------------------------
# bundle add-pipeline
# ---------------------------------------------------------------------------

_BUNDLE_RESOURCE_KEY_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")


@dataclass
class PipelineSpec:
    """One independently configured sdp-meta pipeline topology."""

    name: str
    layer: str
    dataflow_group: str
    pipeline_mode: str = "split"
    bronze_target_schema: Optional[str] = None
    silver_target_schema: Optional[str] = None

    def __post_init__(self) -> None:
        if not _BUNDLE_RESOURCE_KEY_RE.fullmatch(self.name):
            raise ValueError(
                "pipeline name must start with a letter and contain only "
                "letters, numbers, and underscores"
            )
        if self.layer not in ("bronze", "silver", "bronze_silver"):
            raise ValueError("layer must be one of: bronze, silver, bronze_silver")
        if self.pipeline_mode not in ("split", "combined"):
            raise ValueError("pipeline_mode must be one of: split, combined")
        if self.layer != "bronze_silver" and self.pipeline_mode != "split":
            raise ValueError(
                "pipeline_mode=combined is only valid with layer=bronze_silver"
            )
        if not str(self.dataflow_group).strip():
            raise ValueError("dataflow_group must be non-empty")
        if self.bronze_target_schema:
            validate_uc_identifier(
                self.bronze_target_schema, kind="bronze_target_schema"
            )
        if self.silver_target_schema:
            validate_uc_identifier(
                self.silver_target_schema, kind="silver_target_schema"
            )


@dataclass
class BundleAddPipelineCommand:
    """Parameters for `databricks labs sdp-meta bundle-add-pipeline`."""

    bundle_dir: str = "."
    pipeline: Optional[PipelineSpec] = None
    dry_run: bool = False


def _pipeline_resource(
    *,
    display_name: str,
    layer: str,
    group: str,
    schema: str,
    bronze_target_schema: Optional[str] = None,
    silver_target_schema: Optional[str] = None,
) -> Dict[str, Any]:
    config: Dict[str, Any] = {
        "layer": layer,
        "sdp_meta_dependency": "${var.sdp_meta_dependency}",
    }
    if layer in ("bronze", "bronze_silver"):
        config.update({
            "bronze.dataflowspecTable": (
                "${var.uc_catalog_name}.${var.sdp_meta_schema}."
                "${var.bronze_dataflowspec_table}"
            ),
            "bronze.group": group,
        })
    if layer in ("silver", "bronze_silver"):
        config.update({
            "silver.dataflowspecTable": (
                "${var.uc_catalog_name}.${var.sdp_meta_schema}."
                "${var.silver_dataflowspec_table}"
            ),
            "silver.group": group,
        })
    if bronze_target_schema:
        config["sdp_meta.bronzeTargetSchema"] = bronze_target_schema
    if silver_target_schema:
        config["sdp_meta.silverTargetSchema"] = silver_target_schema
    return {
        "name": f"${{bundle.name}} - {display_name}",
        "tags": {"sdp_meta": "${var.sdp_meta_version}"},
        "catalog": "${var.uc_catalog_name}",
        "schema": schema,
        "development": "${var.development_enabled}",
        "photon": "${var.photon_enabled}",
        "serverless": "${var.serverless}",
        "libraries": [{
            "notebook": {
                "path": "${workspace.file_path}/notebooks/init_sdp_meta_pipeline"
            }
        }],
        "configuration": config,
    }


def _pipeline_entries(spec: PipelineSpec) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    bronze_schema = spec.bronze_target_schema or "${var.bronze_target_schema}"
    silver_schema = spec.silver_target_schema or "${var.silver_target_schema}"
    resources: Dict[str, Any] = {}
    tasks: List[Dict[str, Any]] = []

    def add(layer: str, schema: str, depends_on: Optional[str] = None) -> str:
        key = f"{spec.name}_{layer}"
        resources[key] = _pipeline_resource(
            display_name=f"{spec.name} - {layer.replace('_', '+')}",
            layer=layer,
            group=spec.dataflow_group,
            schema=schema,
            bronze_target_schema=(
                bronze_schema if layer in ("bronze", "bronze_silver") else None
            ),
            silver_target_schema=(
                silver_schema if layer in ("silver", "bronze_silver") else None
            ),
        )
        task: Dict[str, Any] = {
            "task_key": key,
            "pipeline_task": {
                "pipeline_id": f"${{resources.pipelines.{key}.id}}",
                "full_refresh": False,
            },
        }
        if depends_on:
            task["depends_on"] = [{"task_key": depends_on}]
        tasks.append(task)
        return key

    if spec.layer == "bronze":
        add("bronze", bronze_schema)
    elif spec.layer == "silver":
        add("silver", silver_schema)
    elif spec.pipeline_mode == "combined":
        add("bronze_silver", bronze_schema)
    else:
        bronze_key = add("bronze", bronze_schema)
        add("silver", silver_schema, depends_on=bronze_key)
    return resources, tasks


def _onboarding_layer_updates(
    bundle_dir: Path,
    pipelines: Dict[str, Any],
    pipeline_spec: PipelineSpec,
) -> List[Tuple[Path, str]]:
    """Prepare variables/job updates so onboarding covers all pipeline layers."""
    variables_path = bundle_dir / "resources" / "variables.yml"
    onboarding_job_path = (
        bundle_dir / "resources" / "sdp_meta_onboarding_job.yml"
    )
    if not variables_path.is_file() or not onboarding_job_path.is_file():
        raise FileNotFoundError(
            "resources/variables.yml and resources/sdp_meta_onboarding_job.yml "
            "are required to add a pipeline"
        )

    variables_doc = yaml.safe_load(variables_path.read_text()) or {}
    variables = variables_doc.get("variables") or {}
    current_layer = _var_default(variables, "layer")
    required = set()
    if current_layer == "bronze_silver":
        required.update(("bronze", "silver"))
    elif current_layer in ("bronze", "silver"):
        required.add(current_layer)
    for pipeline in pipelines.values():
        if not isinstance(pipeline, dict):
            continue
        pipeline_layer = (pipeline.get("configuration") or {}).get("layer")
        if pipeline_layer == "bronze_silver":
            required.update(("bronze", "silver"))
        elif pipeline_layer in ("bronze", "silver"):
            required.add(pipeline_layer)
    onboarding_layer = (
        "bronze_silver" if required == {"bronze", "silver"}
        else next(iter(required), current_layer)
    )
    layer_node = variables.get("layer")
    if not isinstance(layer_node, dict):
        raise ValueError("resources/variables.yml: variable `layer` is missing")
    layer_node["default"] = onboarding_layer

    job_doc = yaml.safe_load(onboarding_job_path.read_text()) or {}
    jobs = (job_doc.get("resources") or {}).get("jobs") or {}
    tasks = (jobs.get("onboarding") or {}).get("tasks") or []
    onboard_task = next(
        (
            task for task in tasks
            if isinstance(task, dict)
            and task.get("task_key") == "onboard_dataflowspecs"
        ),
        None,
    )
    if onboard_task is None:
        raise ValueError(
            "resources/sdp_meta_onboarding_job.yml is missing task "
            "`onboard_dataflowspecs`"
        )
    named_parameters = (
        onboard_task.setdefault("python_wheel_task", {})
        .setdefault("named_parameters", {})
    )
    named_parameters["onboard_layer"] = "${var.layer}"
    if "bronze" in required:
        named_parameters["bronze_dataflowspec_table"] = (
            "${var.bronze_dataflowspec_table}"
        )
    if "silver" in required:
        named_parameters["silver_dataflowspec_table"] = (
            "${var.silver_dataflowspec_table}"
        )
    updates = [
        (variables_path, yaml.safe_dump(variables_doc, sort_keys=False)),
        (onboarding_job_path, yaml.safe_dump(job_doc, sort_keys=False)),
    ]
    onboarding_path = _resolve_onboarding_path(bundle_dir, None, variables)
    onboarding_rows = _load_existing_flows(onboarding_path)
    catalog = _var_default(variables, "uc_catalog_name") or "main"
    bronze_schema = (
        pipeline_spec.bronze_target_schema
        or _var_default(variables, "bronze_target_schema")
        or "sdp_meta_bronze"
    )
    silver_schema = (
        pipeline_spec.silver_target_schema
        or _var_default(variables, "silver_target_schema")
        or "sdp_meta_silver"
    )
    onboarding_ext = (
        "yml"
        if onboarding_path.suffix.lower() in (".yml", ".yaml")
        else "json"
    )
    changed = False
    silver_tables = []
    for row in onboarding_rows:
        if (
            not isinstance(row, dict)
            or row.get("data_flow_group") != pipeline_spec.dataflow_group
        ):
            continue
        original_row = dict(row)
        if pipeline_spec.layer in ("bronze", "bronze_silver"):
            bronze_table = (
                row.get("bronze_table")
                or row.get("silver_table")
                or (row.get("source_details") or {}).get("source_table")
            )
            if not bronze_table:
                raise ValueError(
                    f"Cannot add bronze pipeline for data_flow_group="
                    f"{pipeline_spec.dataflow_group!r}: flow "
                    f"{row.get('data_flow_id')!r} has no bronze_table, "
                    "silver_table, or source_details.source_table to use"
                )
            row.setdefault("bronze_table", bronze_table)
            row.setdefault("bronze_reader_options", {})
            row.setdefault("bronze_table_path_dev", "")
            row.setdefault("bronze_partition_columns", "")
            target = f"{catalog}.{bronze_schema}"
            if row.get("bronze_database_dev") != target:
                row["bronze_database_dev"] = target
                changed = True
            if "bronze_database_quarantine_dev" in row:
                row["bronze_database_quarantine_dev"] = target
        if pipeline_spec.layer in ("silver", "bronze_silver"):
            silver_table = (
                row.get("silver_table")
                or row.get("bronze_table")
                or (row.get("source_details") or {}).get("source_table")
            )
            if not silver_table:
                raise ValueError(
                    f"Cannot add silver pipeline for data_flow_group="
                    f"{pipeline_spec.dataflow_group!r}: flow "
                    f"{row.get('data_flow_id')!r} has no silver_table, "
                    "bronze_table, or source_details.source_table to use"
                )
            row.setdefault("silver_table", silver_table)
            row.setdefault("silver_table_path_dev", "")
            row.setdefault("silver_partition_columns", "")
            row.setdefault(
                "silver_transformation_json_dev",
                (
                    "${workspace.file_path}/conf/"
                    f"silver_transformations.{onboarding_ext}"
                ),
            )
            if silver_table not in silver_tables:
                silver_tables.append(silver_table)
            target = f"{catalog}.{silver_schema}"
            if row.get("silver_database_dev") != target:
                row["silver_database_dev"] = target
                changed = True
            if "silver_database_quarantine_dev" in row:
                row["silver_database_quarantine_dev"] = target
        if row != original_row:
            changed = True
    if changed:
        if onboarding_path.suffix.lower() in (".yml", ".yaml"):
            onboarding_text = yaml.safe_dump(onboarding_rows, sort_keys=False)
        else:
            onboarding_text = json.dumps(onboarding_rows, indent=2)
        updates.append((onboarding_path, onboarding_text))
    if silver_tables:
        transformations_path = (
            bundle_dir / "conf" / f"silver_transformations.{onboarding_ext}"
        )
        if transformations_path.is_file():
            transformations = _load_yaml_or_json(transformations_path) or []
        else:
            transformations = []
        if not isinstance(transformations, list):
            raise ValueError(
                f"{transformations_path.name}: expected a top-level list"
            )
        existing_tables = {
            row.get("target_table")
            for row in transformations
            if isinstance(row, dict)
        }
        for table in silver_tables:
            if table not in existing_tables:
                transformations.append({
                    "target_table": table,
                    "select_exp": ["*"],
                })
        if onboarding_ext == "yml":
            transformations_text = yaml.safe_dump(
                transformations, sort_keys=False
            )
        else:
            transformations_text = json.dumps(transformations, indent=2)
        updates.append((transformations_path, transformations_text))
    return updates


def bundle_add_pipeline(
    cmd: BundleAddPipelineCommand, *, output: Optional[TextIO] = None
) -> int:
    """Add one independently configured topology and its job task wiring."""
    bundle_dir = Path(cmd.bundle_dir).resolve()
    if not (bundle_dir / "databricks.yml").is_file():
        print(
            f"ERROR: {bundle_dir} does not look like a bundle (no databricks.yml)",
            file=output,
        )
        return 2
    if cmd.pipeline is None:
        print("ERROR: pipeline specification is required", file=output)
        return 2

    pipelines_path = bundle_dir / "resources" / "sdp_meta_pipelines.yml"
    if not pipelines_path.is_file():
        print(
            "ERROR: resources/sdp_meta_pipelines.yml not found; run this command "
            "from a bundle scaffolded by bundle-init",
            file=output,
        )
        return 2
    doc = yaml.safe_load(pipelines_path.read_text()) or {}
    resources_doc = doc.setdefault("resources", {})
    pipelines = resources_doc.setdefault("pipelines", {})
    jobs = resources_doc.setdefault("jobs", {})
    pipeline_job = jobs.setdefault(
        "pipelines",
        {
            "name": "${bundle.name} - run pipelines",
            "description": "Runs the sdp-meta SDP Pipeline(s) end-to-end.",
            "tasks": [],
        },
    )
    tasks = pipeline_job.setdefault("tasks", [])
    if not isinstance(pipelines, dict) or not isinstance(tasks, list):
        print(
            "ERROR: sdp_meta_pipelines.yml must contain mapping "
            "`resources.pipelines` and list `resources.jobs.pipelines.tasks`",
            file=output,
        )
        return 2

    new_resources, new_tasks = _pipeline_entries(cmd.pipeline)
    collisions = sorted(set(pipelines).intersection(new_resources))
    existing_task_keys = {
        task.get("task_key") for task in tasks if isinstance(task, dict)
    }
    task_collisions = sorted(
        task["task_key"] for task in new_tasks
        if task["task_key"] in existing_task_keys
    )
    if collisions or task_collisions:
        print(
            "ERROR: pipeline topology already exists; resource collisions="
            f"{collisions}, task collisions={task_collisions}",
            file=output,
        )
        return 2

    keys = list(new_resources)
    merged_pipelines = {**pipelines, **new_resources}
    try:
        onboarding_updates = _onboarding_layer_updates(
            bundle_dir, merged_pipelines, cmd.pipeline
        )
    except (FileNotFoundError, ValueError, yaml.YAMLError) as exc:
        print(f"ERROR: {exc}", file=output)
        return 2

    if cmd.dry_run:
        print(
            f"Would add pipeline resource(s) {keys} for group "
            f"{cmd.pipeline.dataflow_group!r}; no files changed.",
            file=output,
        )
        return 0

    pipelines.update(new_resources)
    tasks.extend(new_tasks)
    pipelines_path.write_text(yaml.safe_dump(doc, sort_keys=False))
    for path, text in onboarding_updates:
        path.write_text(text)
    print(
        f"Added pipeline resource(s) {keys} and {len(new_tasks)} job task(s) "
        f"for dataflow_group={cmd.pipeline.dataflow_group!r}. "
        "Run `databricks labs sdp-meta bundle-validate` to confirm.",
        file=output,
    )
    return 0


# ---------------------------------------------------------------------------
# bundle add-flow
# ---------------------------------------------------------------------------

# Field name → CSV column alias accepted by --from-csv. Anything not listed
# here is ignored when reading CSV rows so users can keep extra columns.
_CSV_FIELD_ALIASES = {
    "source_format": ["source_format"],
    "source_path": ["source_path", "source_path_dev"],
    "source_database": ["source_database", "src_db"],
    "source_table": ["source_table", "src_table"],
    "source_schema_path": ["source_schema_path"],
    "kafka_bootstrap_servers": ["kafka_bootstrap_servers", "bootstrap_servers"],
    "kafka_topic": ["kafka_topic", "subscribe", "topic"],
    "bronze_table": ["bronze_table"],
    "silver_table": ["silver_table"],
    "layer": ["layer"],
    "bronze_target_schema": ["bronze_target_schema"],
    "silver_target_schema": ["silver_target_schema"],
    "data_flow_id": ["data_flow_id", "id"],
    "data_flow_group": ["data_flow_group", "group"],
    "source_system": ["source_system"],
    "snapshot_format": ["snapshot_format"],
    "starting_offsets": ["starting_offsets", "startingOffsets"],
    # Per-flow override for the cloudFiles file format (json|csv|parquet|avro|...).
    # Defaults to "json" when omitted, matching prior behavior.
    "cloudfiles_format": ["cloudfiles_format", "cloudFiles.format", "format"],
}


@dataclass
class FlowSpec:
    """A single flow's user-supplied parameters. Field defaults map to the
    same shape that the seeded `conf/onboarding.{yml,json}` template uses."""

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
    # Per-flow override for cloudFiles.format (only used when source_format ==
    # "cloudFiles"). Defaults to "json" so existing CSVs that omit this column
    # behave identically to before.
    cloudfiles_format: str = "json"

    def __post_init__(self) -> None:
        if self.layer and self.layer not in ("bronze", "silver", "bronze_silver"):
            raise ValueError("layer must be one of: bronze, silver, bronze_silver")
        if self.bronze_target_schema:
            validate_uc_identifier(
                self.bronze_target_schema, kind="bronze_target_schema"
            )
        if self.silver_target_schema:
            validate_uc_identifier(
                self.silver_target_schema, kind="silver_target_schema"
            )


@dataclass
class BundleAddFlowCommand:
    """Parameters for `databricks labs sdp-meta bundle add-flow`."""

    bundle_dir: str = "."
    onboarding_file: Optional[str] = None  # auto-detect from variables.yml if None
    flows: List[FlowSpec] = field(default_factory=list)
    from_csv: Optional[str] = None
    dry_run: bool = False


def _read_variables_yml(bundle_dir: Path) -> Dict[str, Any]:
    variables_yml = bundle_dir / "resources" / "variables.yml"
    if not variables_yml.is_file():
        raise FileNotFoundError(
            f"{variables_yml.relative_to(bundle_dir)} not found. "
            "`bundle-add-flow` must be run from a bundle scaffolded by "
            "`databricks labs sdp-meta bundle-init`."
        )
    doc = yaml.safe_load(variables_yml.read_text()) or {}
    return (doc.get("variables") or {})


def _var_default(variables: Dict[str, Any], name: str) -> Optional[str]:
    node = variables.get(name) or {}
    return node.get("default") if isinstance(node, dict) else None


def _resolve_onboarding_path(bundle_dir: Path, override: Optional[str], variables: Dict[str, Any]) -> Path:
    name = override or _var_default(variables, "onboarding_file_name")
    if not name:
        raise ValueError(
            "Cannot determine onboarding file. Either pass --onboarding-file or set "
            "`onboarding_file_name` in resources/variables.yml."
        )
    path = (bundle_dir / "conf" / name).resolve()
    if not path.is_file():
        raise FileNotFoundError(
            f"Onboarding file not found at {path.relative_to(bundle_dir.resolve())}."
        )
    return path


def _load_existing_flows(path: Path) -> List[Dict[str, Any]]:
    text = path.read_text().strip()
    if not text:
        return []
    if path.suffix.lower() in (".yml", ".yaml"):
        doc = yaml.safe_load(text)
    else:
        doc = json.loads(text)
    if doc is None:
        return []
    if not isinstance(doc, list):
        raise ValueError(
            f"{path.name}: expected a top-level list of flow dicts, got {type(doc).__name__}"
        )
    return doc


def _next_data_flow_id(existing: List[Dict[str, Any]]) -> int:
    """Return max existing numeric id + 1, or 100 if no numeric ids exist.

    Non-numeric ids are ignored for the auto-increment purpose; collisions
    against them are still detected separately by the caller.
    """
    ids: List[int] = []
    for flow in existing:
        if not isinstance(flow, dict):
            continue
        raw = flow.get("data_flow_id")
        try:
            ids.append(int(str(raw)))
        except (TypeError, ValueError):
            continue
    return max(ids) + 1 if ids else 100


def _flow_to_dict(spec: FlowSpec, variables: Dict[str, Any], assigned_id: str) -> Dict[str, Any]:
    """Build a flow entry mirroring the shape produced by the seeded template.

    Pulls bundle-wide defaults (uc_catalog_name, target schemas, layer,
    onboarding_file_format, dataflow_group) from variables.yml so the new
    flow matches the surrounding bundle's conventions.
    """
    # Single source of truth lives in identifiers.py so the bundle CLI,
    # DAB template, and onboarding pre-flight all agree on the supported
    # set; ValueError message format is preserved for any caller that
    # was matching on it. The validator accepts case variants (v0.0.10
    # compat, issue #370) and returns the canonical spelling — write it
    # back onto the spec, because ``_build_source_details`` and the
    # cloudFiles-only reader-options branch compare exactly and would
    # otherwise fail with ``unhandled source_format`` on an input the
    # validator just accepted.
    spec.source_format = validate_source_format(spec.source_format)

    layer = (
        spec.layer or _var_default(variables, "layer") or "bronze_silver"
    ).lower()
    onboarding_format = (_var_default(variables, "onboarding_file_format") or "yaml").lower()
    catalog = _var_default(variables, "uc_catalog_name") or "main"
    bronze_schema = (
        spec.bronze_target_schema
        or _var_default(variables, "bronze_target_schema")
        or "sdp_meta_bronze"
    )
    silver_schema = (
        spec.silver_target_schema
        or _var_default(variables, "silver_target_schema")
        or "sdp_meta_silver"
    )
    bundle_group = _var_default(variables, "dataflow_group")

    # Validate any UC identifiers we read out of the bundle's
    # `variables.yml` before splicing them into onboarding rows. This
    # catches the case where the bundle was scaffolded outside of
    # `bundle-init` (e.g. hand-edited / generated from an older sdp-meta
    # version) and a hyphenated catalog snuck through (issue #261).
    validate_uc_identifier(catalog, kind="variables.yml uc_catalog_name")
    validate_uc_identifier(bronze_schema, kind="variables.yml bronze_target_schema")
    validate_uc_identifier(silver_schema, kind="variables.yml silver_target_schema")

    bronze_table = spec.bronze_table
    silver_table = spec.silver_table
    if layer in ("bronze", "bronze_silver") and not bronze_table:
        raise ValueError(
            "bronze_table is required when layer is `bronze` or `bronze_silver`."
        )
    if layer in ("silver", "bronze_silver") and not silver_table:
        # Mirror bronze name when the user only declared the bronze side.
        silver_table = bronze_table
        if not silver_table:
            raise ValueError(
                "silver_table is required when layer is `silver` (no bronze fallback)."
            )

    # Bronze/silver table names eventually get spliced unquoted into SQL
    # (`spark.read.table(...)`), so reject anything that isn't a regular
    # SQL identifier here at the input boundary (issue #261).
    if bronze_table:
        validate_uc_identifier(bronze_table, kind="bronze_table")
    if silver_table:
        validate_uc_identifier(silver_table, kind="silver_table")

    table_for_paths = bronze_table or silver_table
    ext = "yml" if onboarding_format == "yaml" else "json"

    flow: Dict[str, Any] = {
        "data_flow_id": assigned_id,
        "data_flow_group": spec.data_flow_group or bundle_group or "A1",
        "source_system": spec.source_system,
        "source_format": spec.source_format,
        "source_details": _build_source_details(spec, catalog),
    }

    if layer in ("bronze", "bronze_silver"):
        flow.update({
            "bronze_database_dev": f"{catalog}.{bronze_schema}",
            "bronze_table": bronze_table,
            "bronze_table_comment": f"bronze table for {bronze_table}",
            "bronze_reader_options": _bronze_reader_options(spec),
            "bronze_table_path_dev": "",
            "bronze_partition_columns": "",
            "bronze_data_quality_expectations_json_dev": (
                f"${{workspace.file_path}}/conf/dqe/{table_for_paths}/bronze_expectations.{ext}"
            ),
        })

    if layer in ("silver", "bronze_silver"):
        flow.update({
            "silver_database_dev": f"{catalog}.{silver_schema}",
            "silver_table": silver_table,
            "silver_table_comment": f"silver table for {silver_table}",
            "silver_partition_columns": "",
            "silver_table_path_dev": "",
            "silver_transformation_json_dev": (
                f"${{workspace.file_path}}/conf/silver_transformations.{ext}"
            ),
        })

    return flow


def _build_source_details(spec: FlowSpec, catalog: str) -> Dict[str, Any]:
    sf = spec.source_format
    if sf == "cloudFiles":
        details: Dict[str, Any] = {
            "source_database": spec.source_database or "landing",
            "source_table": spec.source_table or (spec.bronze_table or "example_table"),
            "source_path_dev": spec.source_path or (
                f"/Volumes/{catalog}/landing/files/{spec.bronze_table or 'example_table'}/"
            ),
        }
        if spec.source_schema_path:
            details["source_schema_path"] = spec.source_schema_path
        return details
    if sf == "delta":
        details = {
            "source_database": spec.source_database or f"{catalog}.landing",
            "source_table": spec.source_table or (spec.bronze_table or "example_table"),
            "source_path_dev": spec.source_path or "",
        }
        return details
    if sf == "kafka":
        if not spec.kafka_bootstrap_servers or not spec.kafka_topic:
            raise ValueError(
                "kafka_bootstrap_servers and kafka_topic are required for source_format=kafka"
            )
        details = {
            "kafka.bootstrap.servers": spec.kafka_bootstrap_servers,
            "subscribe": spec.kafka_topic,
            "startingOffsets": spec.starting_offsets,
        }
        if spec.source_schema_path:
            details["source_schema_path"] = spec.source_schema_path
        return details
    if sf == "eventhub":
        if not spec.kafka_topic:
            raise ValueError(
                "kafka_topic (treated as eventhub.name) is required for source_format=eventhub"
            )
        details = {
            "eventhub.namespace": spec.kafka_bootstrap_servers or "<your-eventhub-namespace>",
            "eventhub.name": spec.kafka_topic,
            "eventhub.port": "9093",
            "eventhub.accessKeyName": "<your-sas-policy-name>",
            "eventhub.accessKeySecretName": "<your-secret-name>",
            "eventhub.secretsScopeName": "<your-secret-scope>",
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.security.protocol": "SASL_SSL",
        }
        if spec.source_schema_path:
            details["source_schema_path"] = spec.source_schema_path
        return details
    if sf == "snapshot":
        return {
            "source_path_dev": spec.source_path or (
                f"/Volumes/{catalog}/landing/snapshots/{spec.bronze_table or 'example_table'}/"
            ),
            "snapshot_format": spec.snapshot_format,
        }
    raise ValueError(f"unhandled source_format={sf!r}")  # pragma: no cover


def _bronze_reader_options(spec: FlowSpec) -> Dict[str, Any]:
    if spec.source_format != "cloudFiles":
        return {}
    return {
        "cloudFiles.format": spec.cloudfiles_format or "json",
        "cloudFiles.inferColumnTypes": "true",
        "cloudFiles.rescuedDataColumn": "_rescued_data",
    }


def _flows_from_csv(csv_path: Path) -> List[FlowSpec]:
    """Read a CSV file into a list of FlowSpec.

    Recognized columns are listed in `_CSV_FIELD_ALIASES`. Unknown columns
    are silently ignored so users can carry through arbitrary metadata.

    Blank lines and comment lines (whose first non-space character is ``#``)
    are skipped before parsing, so the shipped ``conf/samples/flows.csv`` can
    stay self-documenting. The first surviving line is the header row. Original
    file line numbers are preserved in error messages.
    """
    raw_lines = csv_path.read_text().splitlines(keepends=True)
    # (original 1-based line number, text) for every non-blank, non-comment line.
    kept = [
        (lineno, line)
        for lineno, line in enumerate(raw_lines, start=1)
        if line.strip() and not line.lstrip().startswith("#")
    ]
    if not kept:
        raise ValueError(f"{csv_path.name} contains no rows")

    # csv.DictReader accepts any iterable of strings; feed it only the surviving
    # lines. kept[0] is the header; kept[1:] map row-by-row to data lines.
    reader = csv.DictReader([line for _, line in kept])
    data_line_numbers = [lineno for lineno, _ in kept[1:]]

    flows: List[FlowSpec] = []
    for idx, row in enumerate(reader):
        lineno = data_line_numbers[idx] if idx < len(data_line_numbers) else kept[0][0]
        kwargs: Dict[str, Any] = {}
        for field_name, aliases in _CSV_FIELD_ALIASES.items():
            for alias in aliases:
                if alias in row and row[alias] != "":
                    kwargs[field_name] = row[alias]
                    break
        try:
            flows.append(FlowSpec(**kwargs))
        except TypeError as exc:  # pragma: no cover — defensive
            raise ValueError(f"{csv_path.name}:{lineno}: {exc}") from exc
    if not flows:
        raise ValueError(f"{csv_path.name} contains no rows")
    return flows


def _apply_pipeline_defaults(
    bundle_dir: Path,
    variables: Dict[str, Any],
    spec: FlowSpec,
) -> None:
    """Fill a flow's layer/schemas from the pipeline serving its group."""
    group = spec.data_flow_group or _var_default(variables, "dataflow_group")
    if not group:
        return
    pipelines_path = bundle_dir / "resources" / "sdp_meta_pipelines.yml"
    if not pipelines_path.is_file():
        return
    doc = yaml.safe_load(pipelines_path.read_text()) or {}
    pipelines = (doc.get("resources") or {}).get("pipelines") or {}
    layers = set()
    bronze_schemas = set()
    silver_schemas = set()
    for pipeline in pipelines.values():
        if not isinstance(pipeline, dict):
            continue
        config = pipeline.get("configuration") or {}
        raw_groups = (
            config.get("bronze.group"),
            config.get("silver.group"),
        )
        resolved_groups = {
            _var_default(variables, match.group(1))
            if isinstance(raw_group, str)
            and (match := _VARIABLE_REF_RE.fullmatch(raw_group))
            else raw_group
            for raw_group in raw_groups
            if raw_group
        }
        if group not in resolved_groups:
            continue
        layer = config.get("layer")
        if layer == "bronze_silver":
            layers.update(("bronze", "silver"))
        elif layer in ("bronze", "silver"):
            layers.add(layer)
        bronze_schema = config.get("sdp_meta.bronzeTargetSchema")
        silver_schema = config.get("sdp_meta.silverTargetSchema")
        if bronze_schema:
            bronze_schemas.add(
                _resolve_bundle_reference(bronze_schema, variables, {}, None)
            )
        if silver_schema:
            silver_schemas.add(
                _resolve_bundle_reference(silver_schema, variables, {}, None)
            )
    if len(bronze_schemas) > 1 or len(silver_schemas) > 1:
        raise ValueError(
            f"data_flow_group={group!r} is served by pipelines with conflicting "
            "target schemas; set flow schema overrides explicitly"
        )
    if spec.layer is None and layers:
        spec.layer = (
            "bronze_silver" if layers == {"bronze", "silver"}
            else next(iter(layers))
        )
    if spec.bronze_target_schema is None and bronze_schemas:
        spec.bronze_target_schema = next(iter(bronze_schemas))
    if spec.silver_target_schema is None and silver_schemas:
        spec.silver_target_schema = next(iter(silver_schemas))


def bundle_add_flow(
    cmd: BundleAddFlowCommand, *, output: Optional[TextIO] = None
) -> int:
    """Append one or more flow entries to a bundle's onboarding file.

    Reads bundle defaults from `resources/variables.yml`, auto-increments
    `data_flow_id` when the user passes `auto`, refuses to write on id
    collisions, and preserves the file's existing format (YAML or JSON).
    Returns 0 on success, non-zero on validation errors.
    """
    bundle_dir = Path(cmd.bundle_dir).resolve()
    if not (bundle_dir / "databricks.yml").is_file():
        print(
            f"ERROR: {bundle_dir} does not look like a bundle (no databricks.yml)",
            file=output,
        )
        return 2

    variables = _read_variables_yml(bundle_dir)
    onboarding_path = _resolve_onboarding_path(bundle_dir, cmd.onboarding_file, variables)
    existing = _load_existing_flows(onboarding_path)

    pending: List[FlowSpec] = list(cmd.flows)
    if cmd.from_csv:
        csv_path = Path(cmd.from_csv).resolve()
        if not csv_path.is_file():
            print(f"ERROR: --from-csv path not found: {csv_path}", file=output)
            return 2
        pending.extend(_flows_from_csv(csv_path))

    if not pending:
        print(
            "ERROR: no flows to add. Pass at least one FlowSpec or --from-csv.",
            file=output,
        )
        return 2

    next_id = _next_data_flow_id(existing)
    existing_ids = {
        str(flow.get("data_flow_id"))
        for flow in existing
        if isinstance(flow, dict) and flow.get("data_flow_id") is not None
    }

    new_entries: List[Dict[str, Any]] = []
    for spec in pending:
        _apply_pipeline_defaults(bundle_dir, variables, spec)
        if spec.data_flow_id == "auto":
            assigned = str(next_id)
            next_id += 1
        else:
            assigned = str(spec.data_flow_id)
        if assigned in existing_ids:
            print(
                f"ERROR: data_flow_id={assigned!r} already exists in {onboarding_path.name}. "
                "Pick a different id or use `auto`.",
                file=output,
            )
            return 2
        existing_ids.add(assigned)
        new_entries.append(_flow_to_dict(spec, variables, assigned))

    if cmd.dry_run:
        print(
            f"Would append {len(new_entries)} flow(s) to "
            f"{onboarding_path.relative_to(bundle_dir)}:",
            file=output,
        )
        for entry in new_entries:
            print(
                f"  - data_flow_id={entry['data_flow_id']!r}, "
                f"source_format={entry['source_format']!r}, "
                f"bronze={entry.get('bronze_table')!r}, "
                f"silver={entry.get('silver_table')!r}",
                file=output,
            )
        return 0

    merged = existing + new_entries
    if onboarding_path.suffix.lower() in (".yml", ".yaml"):
        onboarding_path.write_text(yaml.safe_dump(merged, sort_keys=False))
    else:
        onboarding_path.write_text(json.dumps(merged, indent=2))

    # Keep silver_transformations in sync. The seeded transformations file
    # only ships with a `target_table: example_table` row; without a matching
    # row for each silver_table in onboarding.yml, the onboarding job's INNER
    # join (`silver_transformation_json_df.target_table == silverDataflowSpec
    # .targetDetails.table`) drops the row, the silver dataflowspec table is
    # written with zero rows, and the silver SDP Pipeline blows up at startup
    # with `[NO_TABLES_IN_PIPELINE] Pipelines are expected to have at least
    # one table defined`. Auto-seeding a `select_exp: ["*"]` row per new
    # silver_table makes the demo (and any auto-generated bundle) runnable
    # out of the box; users can always edit the row to add real projections.
    seeded = _ensure_silver_transformation_entries(bundle_dir, variables, new_entries)

    msg = f"Appended {len(new_entries)} flow(s) to {onboarding_path.relative_to(bundle_dir)}."
    if seeded:
        msg += f" Seeded {seeded} default silver transformation row(s) in conf/."
    print(
        msg + " Run `databricks labs sdp-meta bundle-validate` to confirm.",
        file=output,
    )
    return 0


def _ensure_silver_transformation_entries(
    bundle_dir: Path,
    variables: Dict[str, Any],
    new_entries: List[Dict[str, Any]],
) -> int:
    """Append a default `target_table: <silver_table>, select_exp: ["*"]`
    row to the bundle's silver_transformations.{yml,json} for each newly
    appended onboarding row that has a `silver_table` and isn't already
    represented. Returns the number of rows appended.

    No-op when `new_entries` carry no `silver_table`, or when the
    transformations file isn't present (the user may have replaced it with a
    custom path). Per-flow layer overrides take precedence over the bundle's
    original global layer, so the entries themselves are the source of truth.
    All other failures bubble up so users see them.
    """
    onboarding_format = (_var_default(variables, "onboarding_file_format") or "yaml").lower()
    ext = "yml" if onboarding_format == "yaml" else "json"
    transformations_path = bundle_dir / "conf" / f"silver_transformations.{ext}"
    silver_tables_to_add: List[str] = []
    for entry in new_entries:
        table = entry.get("silver_table")
        if table and table not in silver_tables_to_add:
            silver_tables_to_add.append(table)
    if not silver_tables_to_add:
        return 0

    text = (
        transformations_path.read_text().strip()
        if transformations_path.is_file()
        else ""
    )
    if not text:
        existing_rows: List[Dict[str, Any]] = []
    elif ext == "yml":
        existing_rows = yaml.safe_load(text) or []
    else:
        existing_rows = json.loads(text)
    if not isinstance(existing_rows, list):
        raise ValueError(
            f"{transformations_path.name}: expected a top-level list of "
            f"transformation entries, got {type(existing_rows).__name__}"
        )

    already_present = {
        row.get("target_table")
        for row in existing_rows
        if isinstance(row, dict) and row.get("target_table")
    }

    new_rows = [
        {"target_table": tbl, "select_exp": ["*"]}
        for tbl in silver_tables_to_add
        if tbl not in already_present
    ]
    if not new_rows:
        return 0

    merged_rows = existing_rows + new_rows
    if ext == "yml":
        transformations_path.write_text(yaml.safe_dump(merged_rows, sort_keys=False))
    else:
        transformations_path.write_text(json.dumps(merged_rows, indent=2))
    return len(new_rows)


# ---------------------------------------------------------------------------
# wiring helpers used by cli.py
# ---------------------------------------------------------------------------

# Backwards-compatible alias for the canonical prompt helper. Existing
# bundle.py call sites read ``_ident_prompt(wsi, text, kind=...)``;
# routing through :func:`prompt_uc_identifier` keeps the retry / error-
# print behavior in lockstep with ``cli.py::SDPMeta._ident_question``
# so a future tweak (e.g. softening the regex, changing the retry
# count) updates both call paths at once. See identifiers.py for the
# implementation.
_ident_prompt = prompt_uc_identifier


def _load_bundle_init_config(wsi) -> BundleInitCommand:
    """Interactive loader used by `databricks labs sdp-meta bundle init`."""
    output_dir = wsi._question("Output directory for the new bundle", default=".")
    return BundleInitCommand(output_dir=output_dir)


# Developer-friendly defaults used by `bundle-init --quickstart`. These mirror
# the schema's `default` values for everything except `sdp_meta_dependency`,
# which the user must still resolve (PyPI coordinate or `bundle-prepare-wheel`
# output) before deploy. Keeping the values here in one place means both the
# CLI wrapper and the test that asserts the produced config-file is sound
# read from the same source of truth.
QUICKSTART_BUNDLE_INIT_DEFAULTS: Dict[str, str] = {
    "bundle_name": "my_sdp_meta_pipeline",
    "uc_catalog_name": "main",
    "sdp_meta_schema": "sdp_meta_dataflowspecs",
    "bronze_target_schema": "sdp_meta_bronze",
    "silver_target_schema": "sdp_meta_silver",
    "layer": "bronze_silver",
    "pipeline_mode": "split",
    "source_format": "cloudFiles",
    "onboarding_file_format": "yaml",
    "dataflow_group": "my_group",
    "wheel_source": "pypi",
    "sdp_meta_dependency": "__SET_ME__",
    "author": "sdp-meta-user",
}


# Per-key validation for caller-supplied quickstart overrides. Each entry
# validates the override value and returns it unchanged on success (or raises
# ValueError). Keys not listed here (e.g. ``dataflow_group``, ``author``,
# ``sdp_meta_dependency``) accept any non-empty string. Only keys present in
# QUICKSTART_BUNDLE_INIT_DEFAULTS may be overridden at all.
_BUNDLE_NAME_RE = re.compile(r"^[A-Za-z0-9_\-]+$")


def _enum_override_validator(field: str, allowed):
    allowed = tuple(allowed)

    def _validate(value):
        if value not in allowed:
            raise ValueError(
                f"quickstart override {field}={value!r} is invalid; "
                f"allowed values: {list(allowed)}"
            )
        return value

    return _validate


def _nonempty_str_override(field: str):
    def _validate(value):
        if not isinstance(value, str) or not value.strip():
            raise ValueError(
                f"quickstart override {field} must be a non-empty string, "
                f"got {value!r}"
            )
        return value

    return _validate


def _bundle_name_override(value):
    if not isinstance(value, str) or not _BUNDLE_NAME_RE.match(value):
        raise ValueError(
            f"quickstart override bundle_name={value!r} is invalid; only "
            f"letters, digits, underscores and hyphens are allowed."
        )
    return value


_QUICKSTART_OVERRIDE_VALIDATORS: Dict[str, Any] = {
    "bundle_name": _bundle_name_override,
    "uc_catalog_name": lambda v: validate_uc_identifier(v, kind="uc_catalog_name"),
    "sdp_meta_schema": lambda v: validate_uc_identifier(v, kind="sdp_meta_schema"),
    "bronze_target_schema": lambda v: validate_uc_identifier(v, kind="bronze_target_schema"),
    "silver_target_schema": lambda v: validate_uc_identifier(v, kind="silver_target_schema"),
    "layer": _enum_override_validator("layer", ("bronze", "silver", "bronze_silver")),
    "pipeline_mode": _enum_override_validator("pipeline_mode", ("split", "combined")),
    "source_format": lambda v: validate_source_format(v, kind="source_format"),
    "onboarding_file_format": _enum_override_validator("onboarding_file_format", ("yaml", "json")),
    "dataflow_group": _nonempty_str_override("dataflow_group"),
    "author": _nonempty_str_override("author"),
    "sdp_meta_dependency": _nonempty_str_override("sdp_meta_dependency"),
}


def _validated_quickstart_overrides(overrides: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Validate caller-supplied quickstart overrides against the schema.

    Rejects unknown keys (so a typo like ``uc_catalog`` fails loudly instead
    of silently doing nothing) and validates each value with the same rules
    the template's ``databricks_template_schema.json`` enforces, so a bad
    catalog name is caught here rather than at deploy time. Returns the
    validated dict (empty when ``overrides`` is falsy).
    """
    if not overrides:
        return {}
    if not isinstance(overrides, dict):
        raise ValueError(
            f"quickstart overrides must be a mapping of "
            f"{{field: value}}, got {type(overrides).__name__}"
        )
    unknown = set(overrides) - set(QUICKSTART_BUNDLE_INIT_DEFAULTS)
    if unknown:
        raise ValueError(
            f"unknown quickstart override key(s): {sorted(unknown)}. "
            f"Overridable keys: {sorted(QUICKSTART_BUNDLE_INIT_DEFAULTS)}"
        )
    validated: Dict[str, Any] = {}
    for key, raw in overrides.items():
        validator = _QUICKSTART_OVERRIDE_VALIDATORS.get(key)
        validated[key] = validator(raw) if validator else raw
    return validated


def write_quickstart_config_file(
    dest_dir: Path, overrides: Optional[Dict[str, Any]] = None
) -> Path:
    """Write a `databricks bundle init --config-file` JSON to ``dest_dir``.

    The JSON pre-answers every prompt declared in
    ``databricks_template_schema.json`` with the developer-friendly defaults
    in :data:`QUICKSTART_BUNDLE_INIT_DEFAULTS`, so the user can scaffold a
    runnable-modulo-credentials bundle in one shot without wading through
    13 prompts. They still need to point ``sdp_meta_dependency`` at a real
    PyPI coordinate or wheel before deploy (the schema's default is the
    sentinel ``__SET_ME__`` and ``bundle-validate`` rejects it).

    ``overrides`` lets a caller change individual answers (e.g.
    ``{"uc_catalog_name": "acme_prod"}``) while keeping every other default,
    so quickstart stays one-shot even when the target catalog/schema isn't
    ``main``. Each override is validated against the schema's rules; unknown
    keys are rejected.

    Returns the path to the written file. Caller is responsible for cleanup
    if the file lives in a tmp dir.
    """
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    merged = dict(QUICKSTART_BUNDLE_INIT_DEFAULTS)
    merged.update(_validated_quickstart_overrides(overrides))
    cfg_path = dest_dir / "sdp_meta_quickstart.json"
    cfg_path.write_text(json.dumps(merged, indent=2))
    return cfg_path


def _load_bundle_prepare_wheel_config(wsi) -> BundlePrepareWheelCommand:
    uc_catalog = _ident_prompt(wsi, "Unity Catalog catalog name", kind="uc_catalog")
    uc_schema = _ident_prompt(
        wsi, "UC schema for the wheel volume",
        kind="uc_schema", default="sdp_meta_dataflowspecs",
    )
    uc_volume = _ident_prompt(
        wsi, "UC volume name", kind="uc_volume", default="sdp_meta_wheels",
    )
    # Defaults come from the standard pip env vars so users on networks that
    # require an internal mirror (e.g. PIP_INDEX_URL=https://pypi.internal...)
    # don't have to type the URL again here.
    pip_index_url = wsi._question(
        "pip --index-url (blank to use default / $PIP_INDEX_URL)",
        default=os.environ.get("PIP_INDEX_URL", ""),
    )
    pip_extra_index = wsi._question(
        "pip --extra-index-url (space-separated, blank for none)",
        default=os.environ.get("PIP_EXTRA_INDEX_URL", ""),
    )
    extras = [u for u in pip_extra_index.split() if u] if pip_extra_index else None
    create_choice = wsi._choice(
        "Auto-create the schema and volume if they don't exist?",
        ["True", "False"],
    )
    return BundlePrepareWheelCommand(
        uc_catalog=uc_catalog,
        uc_schema=uc_schema,
        uc_volume=uc_volume,
        pip_index_url=pip_index_url or None,
        pip_extra_index_urls=extras,
        create_if_missing=(create_choice == "True"),
    )


def _load_bundle_validate_config(wsi) -> BundleValidateCommand:
    bundle_dir = wsi._question("Bundle directory", default=".")
    target = wsi._question("Bundle target (blank for default)", default="")
    return BundleValidateCommand(
        bundle_dir=bundle_dir,
        target=target or None,
    )


def _load_bundle_add_pipeline_config(wsi) -> BundleAddPipelineCommand:
    """Interactive loader for `bundle-add-pipeline`."""
    bundle_dir = wsi._question("Bundle directory", default=".")
    name = wsi._question(
        "Pipeline name (letters, numbers, underscores)", default="additional"
    )
    layer = wsi._choice("Layer", ["bronze", "silver", "bronze_silver"])
    pipeline_mode = "split"
    if layer == "bronze_silver":
        pipeline_mode = wsi._choice("Pipeline mode", ["split", "combined"])
    dataflow_group = wsi._question("data_flow_group", default="additional_group")
    bronze_schema = None
    silver_schema = None
    if layer in ("bronze", "bronze_silver"):
        bronze_schema = wsi._question(
            "Bronze target schema (blank = bundle default)", default=""
        ) or None
    if layer in ("silver", "bronze_silver"):
        silver_schema = wsi._question(
            "Silver target schema (blank = bundle default)", default=""
        ) or None
    dry_run = (
        wsi._choice("Dry run (preview only, no file write)?", ["False", "True"])
        == "True"
    )
    return BundleAddPipelineCommand(
        bundle_dir=bundle_dir,
        pipeline=PipelineSpec(
            name=name,
            layer=layer,
            pipeline_mode=pipeline_mode,
            dataflow_group=dataflow_group,
            bronze_target_schema=bronze_schema,
            silver_target_schema=silver_schema,
        ),
        dry_run=dry_run,
    )


def _load_bundle_add_flow_config(wsi) -> BundleAddFlowCommand:
    """Interactive loader for `bundle-add-flow`.

    Two routes:
    - ``from-csv`` for batch additions (one prompt for the CSV path).
    - single-flow interactive prompts that depend on the chosen source format.
    """
    bundle_dir = wsi._question("Bundle directory", default=".")
    mode = wsi._choice("Add a single flow or batch from CSV?", ["single", "csv"])
    dry_run_choice = wsi._choice("Dry run (preview only, no file write)?", ["False", "True"])
    dry_run = dry_run_choice == "True"

    if mode == "csv":
        # Default to the shipped sample's real location so hitting Enter from
        # the bundle root just works (the file lives under conf/samples/, not
        # the bundle root).
        csv_path = wsi._question("Path to CSV file", default="conf/samples/flows.csv")
        return BundleAddFlowCommand(
            bundle_dir=bundle_dir,
            from_csv=csv_path,
            dry_run=dry_run,
        )

    source_format = wsi._choice("Source format", sorted(SUPPORTED_SOURCE_FORMATS))
    bronze_table = wsi._question("Bronze table name (leave blank if silver-only)", default="")
    silver_table = wsi._question("Silver table name (blank = same as bronze)", default="")
    data_flow_id = wsi._question("data_flow_id (use `auto` to auto-increment)", default="auto")
    data_flow_group = wsi._question("data_flow_group (blank = use bundle default)", default="")
    flow_layer = wsi._question(
        "Flow layer (blank = use bundle default)", default=""
    )
    if flow_layer and flow_layer not in ("bronze", "silver", "bronze_silver"):
        raise ValueError(
            "Flow layer must be blank or one of: bronze, silver, bronze_silver"
        )
    bronze_target_schema = wsi._question(
        "Bronze target schema (blank = use bundle default)", default=""
    )
    silver_target_schema = wsi._question(
        "Silver target schema (blank = use bundle default)", default=""
    )

    spec_kwargs: Dict[str, Any] = {
        "source_format": source_format,
        "bronze_table": bronze_table or None,
        "silver_table": silver_table or None,
        "data_flow_id": data_flow_id,
        "data_flow_group": data_flow_group or None,
        "layer": flow_layer or None,
        "bronze_target_schema": bronze_target_schema or None,
        "silver_target_schema": silver_target_schema or None,
    }

    if source_format == "cloudFiles":
        spec_kwargs["source_path"] = wsi._question(
            "Source path (e.g. /Volumes/raw/landing/orders/)", default=""
        ) or None
        spec_kwargs["source_schema_path"] = wsi._question(
            "Source schema DDL path (blank to skip)", default=""
        ) or None
    elif source_format == "delta":
        spec_kwargs["source_database"] = wsi._question("Source database", default="") or None
        spec_kwargs["source_table"] = wsi._question("Source table", default="") or None
    elif source_format == "kafka":
        spec_kwargs["kafka_bootstrap_servers"] = wsi._question(
            "kafka.bootstrap.servers", default="<your-kafka-host>:9092"
        )
        spec_kwargs["kafka_topic"] = wsi._question("subscribe (topic name)")
    elif source_format == "eventhub":
        spec_kwargs["kafka_bootstrap_servers"] = wsi._question(
            "eventhub.namespace", default="<your-eventhub-namespace>"
        )
        spec_kwargs["kafka_topic"] = wsi._question("eventhub.name")
    elif source_format == "snapshot":
        spec_kwargs["source_path"] = wsi._question(
            "Snapshot source path (e.g. /Volumes/raw/snapshots/orders/)", default=""
        ) or None

    return BundleAddFlowCommand(
        bundle_dir=bundle_dir,
        flows=[FlowSpec(**spec_kwargs)],
        dry_run=dry_run,
    )
