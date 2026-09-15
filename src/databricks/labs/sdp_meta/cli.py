"""Main entry point for the CLI."""

import io
import logging
import json
import os
import subprocess
import sys
import tempfile
import uuid
import webbrowser
import yaml
from dataclasses import dataclass
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, pipelines, compute
from databricks.sdk.service.pipelines import PipelineLibrary, NotebookLibrary
from databricks.sdk.core import DatabricksError
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import SchemasAPI, VolumeType
from databricks.labs.sdp_meta import __about__
from databricks.labs.sdp_meta.identifiers import (
    prompt_uc_identifier,
    validate_uc_identifier,
)
from databricks.labs.sdp_meta.install import WorkspaceInstaller

logger = logging.getLogger('databricks.labs.sdp_meta')


def _maybe_open_url(url: str) -> None:
    """Best-effort browser launch — safe in tests, CI, and headless contexts.

    Calling :func:`webbrowser.open` unconditionally from CLI code is a
    portability hazard:

    * Unit tests that mock :class:`WorkspaceClient` build the URL from
      MagicMock attributes and the bare ``webbrowser.open`` happily
      hands the resulting garbage string to the OS, popping a real
      browser tab on every test run (see ``tests/test_cli.py`` cases
      that exercise ``create_onnboarding_job`` /
      ``_create_sdp_meta_pipeline``).
    * Inside the Databricks Apps container (``databricks_app``)
      there's no display, so ``webbrowser.open`` either silently
      fails or — worse, on some platforms — falls back to printing
      to stdout, which corrupts the JSON the Flask route returns to
      the browser-side caller.
    * On CI / SSH / sandboxed Linux runners ``webbrowser.open`` can
      raise ``Error: could not locate runnable browser``.

    This helper makes the launch opt-out: callers that DO want a browser
    (interactive ``sdp-meta`` invocations on a developer's laptop) get
    one, everyone else stays quiet.

    Suppression triggers:
      * ``SDP_META_NO_BROWSER=1`` — explicit opt-out (set by
        ``tests/conftest.py`` for the whole pytest session, and
        recommended for CI).
      * ``DATABRICKS_APP_PORT`` — set by the Databricks Apps runtime
        whenever this code runs inside an App container. Implies
        "no display, definitely no browser".
      * Any exception from :func:`webbrowser.open` (no $DISPLAY on
        Linux, missing default browser, etc.) — swallowed and logged
        at DEBUG so a stray failure can't crash the CLI.
    """
    if os.environ.get("SDP_META_NO_BROWSER") == "1":
        logger.debug("Skipping webbrowser.open: SDP_META_NO_BROWSER=1")
        return
    if os.environ.get("DATABRICKS_APP_PORT"):
        logger.debug("Skipping webbrowser.open: running inside Databricks Apps")
        return
    try:
        webbrowser.open(url)
    except Exception as exc:  # pragma: no cover — platform-specific.
        logger.debug("webbrowser.open(%r) failed: %s", url, exc)


def _normalize_file_uri_to_path(file_uri: str) -> str:
    """Convert a file URI to a normalized local filesystem path.

    Handles both Unix and Windows paths correctly.
    Examples:
    - 'file:/path/to/dir' -> '/path/to/dir' (Unix)
    - 'file:/C:\\projects\\dir' -> 'C:\\projects\\dir' (Windows)
    - 'file:///C:/projects/dir' -> 'C:/projects/dir' (Windows)
    - '/path/to/dir' -> '/path/to/dir' (already a path)

    Args:
        file_uri: A file URI or local path

    Returns:
        A normalized local filesystem path
    """
    if not file_uri.startswith('file:'):
        return file_uri

    # Remove 'file:' prefix
    path = file_uri[5:]

    # Remove leading slashes for file:// or file:/// URIs
    while path.startswith('//'):
        path = path[1:]

    # Handle Windows paths: /C:\... or /C:/... -> C:\... or C:/...
    # After removing 'file:', we may have '/C:\path' or '/C:/path'
    if len(path) > 2 and path[0] == '/' and path[2] in (':', '|'):
        path = path[1:]
        # Normalize pipe to colon (file URI spec allows C| instead of C:)
        if path[1] == '|':
            path = path[0] + ':' + path[2:]

    return path


def _coerce_bool(v):
    """Coerce ``v`` to a real Python ``bool``.

    The App's JSON envelope sends HTML radio-button values as the STRINGS
    "1" / "0" — both are Python-truthy, so a naive ``if input_params["x"]:``
    accepts either, but the value is then passed through to SDK calls
    (e.g. ``ws.pipelines.create(serverless=...)``) whose request body is
    ``json.dumps``-ed. The literal string "1" lands in the wire body as
    ``"serverless": "1"`` (JSON string, not boolean true) and the
    control-plane silently treats the field as missing — manifests as
    "You must use serverless compute in this workspace." on serverless-
    only workspaces. Coerce here so callers always see a real ``bool``.

    Accepts: ``True``/``False`` (passthrough), the strings ``"1"``,
    ``"true"``, ``"yes"``, ``"on"`` (case-insensitive) as True; everything
    else (including ``"0"``, ``""``, ``None``) as False."""
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().lower() in ("1", "true", "yes", "on")
    return bool(v)


def _recover_swallowed_flag(flags: dict, bool_flag_name: str) -> None:
    """Undo the labs CLI string-flag parser eating a boolean flag's neighbor.

    Every flag declared in ``labs.yml`` is registered as a *string* flag by
    the labs (cobra) CLI. A bare boolean-style flag such as ``--quickstart``
    therefore consumes the NEXT token as its value, so
    ``--quickstart --output-dir=./x`` arrives here as
    ``{"quickstart": "--output-dir=./x"}`` with ``output-dir`` missing.

    Detect that case in place: when ``bool_flag_name``'s value looks like a
    swallowed ``--key=value`` token, split it back out, restore the real flag
    (unless already set explicitly), and normalize the boolean flag to
    ``"true"`` since the user clearly meant it as an on-switch. Only the
    ``--key=value`` form is recoverable; a swallowed bare ``--flag`` (no
    ``=``) is dropped because its value is unknowable here.

    Mutates ``flags`` and returns ``None``.
    """
    raw = flags.get(bool_flag_name)
    if not isinstance(raw, str):
        return
    stripped = raw.strip()
    if not stripped.startswith("-"):
        return
    token = stripped.lstrip("-")
    if "=" in token:
        key, _, value = token.partition("=")
        key = key.strip()
        # The labs CLI still emits the swallowed flag's own key, but with an
        # empty value (e.g. {"output-dir": "", "quickstart": "--output-dir=x"}).
        # Restore it whenever the caller hasn't provided a real value, so an
        # explicit later `--output-dir=y` is never clobbered.
        if key and not flags.get(key):
            flags[key] = value
    flags[bool_flag_name] = "true"


def _create_pipeline(ws, **kwargs):
    """Create a pipeline via ``ws.pipelines.create(...)``, tolerating both
    the modern ``schema=`` parameter and the legacy ``target=`` parameter.

    The SDK requires exactly one of ``schema`` or ``target``. ``schema`` is
    the direct-publishing-mode (DPM) API and is what Unity-Catalog
    workspaces with DPM enabled expect; ``target`` is the legacy fallback
    still needed on workspaces / pipelines where DPM is not enabled.

    Kept private to ``cli.py`` on purpose: this is production code that
    creates real customer pipelines. The demo/test infrastructure has its
    own copy in ``integration_tests/run_integration_tests.py`` so
    production doesn't depend on test infra.

    Behavior:
      * If the caller passes ``schema=``, try it first and retry with
        ``target=`` (same value) on any exception.
      * If the caller passes ``target=`` (or neither), pass through
        untouched -- no fallback needed.
      * If both attempts fail, raise ``RuntimeError`` chaining both
        underlying errors so operators can diagnose the real cause.

    Deprecation note: ``target=`` is documented as deprecated for pipeline
    creation in favor of ``schema=``. When it is fully removed from the
    SDK, the fallback branch becomes dead code (``schema=`` succeeds on
    every DPM-enabled workspace, so we return before reaching the
    fallback) and this helper can be replaced with a plain
    ``ws.pipelines.create(**kwargs)`` call.
    """
    if "schema" not in kwargs:
        return ws.pipelines.create(**kwargs)
    schema_val = kwargs.pop("schema")
    try:
        return ws.pipelines.create(schema=schema_val, **kwargs)
    except Exception as e_schema:
        logger.warning(
            "pipelines.create(schema=%r) failed with %s: %s. "
            "Retrying with legacy target=%r.",
            schema_val, type(e_schema).__name__, e_schema, schema_val,
        )
        try:
            return ws.pipelines.create(target=schema_val, **kwargs)
        except Exception as e_target:
            raise RuntimeError(
                "pipelines.create() failed with both parameter forms.\n"
                f"  schema={schema_val!r}: "
                f"{type(e_schema).__name__}: {e_schema}\n"
                f"  target={schema_val!r}: "
                f"{type(e_target).__name__}: {e_target}"
            ) from e_target


def _path_to_file_uri(local_path: str) -> str:
    """Convert a local filesystem path to a file URI.

    Handles both Unix and Windows paths correctly.
    Examples:
    - '/path/to/dir' -> 'file:/path/to/dir' (Unix)
    - 'C:\\projects\\dir' -> 'file:///C:/projects/dir' (Windows)
    - 'C:/projects/dir' -> 'file:///C:/projects/dir' (Windows)

    Args:
        local_path: A local filesystem path

    Returns:
        A properly formatted file URI
    """
    # Check if it's already a file URI
    if local_path.startswith('file:'):
        return local_path

    # Check for Windows absolute path (e.g., C:\... or C:/...)
    if len(local_path) > 1 and local_path[1] == ':':
        # Windows path - use file:/// format with forward slashes
        normalized = local_path.replace('\\', '/')
        return f"file:///{normalized}"

    # Unix path - use file: format
    return f"file:{local_path}"


# Runner notebook template for the SDP/DLT pipeline. The ``{dependency}``
# placeholder is replaced at deploy time with either:
#   * a PyPI spec, e.g. ``databricks-labs-sdp-meta==0.1.0``
#   * a UC Volumes wheel path, e.g.
#     ``/Volumes/<catalog>/<schema>/<volume>/databricks_labs_sdp_meta-<ver>-py3-none-any.whl``
# The latter is the recommended path for air-gapped workspaces / private
# preview rings without PyPI access.
SDP_META_RUNNER_NOTEBOOK = """
# Databricks notebook source
# MAGIC %pip install {dependency}

# COMMAND ----------
layer = spark.conf.get("layer", None)
from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline
DataflowPipeline.invoke_dlt_pipeline(spark, layer)
"""

# Backwards compatibility alias (deprecated)
DLT_META_RUNNER_NOTEBOOK = SDP_META_RUNNER_NOTEBOOK

cloud_node_type_id_dict = {"aws": "i3.xlarge", "azure": "Standard_D3_v2", "gcp": "n1-highmem-4"}


@dataclass
class OnboardCommand:
    """Class representing the onboarding command."""
    onboarding_file_path: str
    onboarding_files_dir_path: str
    onboard_layer: str
    env: str
    import_author: str
    version: str
    sdp_meta_schema: str
    dbfs_path: str = None
    cloud: str = None
    dbr_version: str = None
    serverless: bool = True
    bronze_schema: str = None
    silver_schema: str = None
    uc_enabled: bool = False
    uc_catalog_name: str = None
    uc_volume_path: str = None
    overwrite: bool = True
    bronze_dataflowspec_table: str = "bronze_dataflowspec"
    silver_dataflowspec_table: str = "silver_dataflowspec"
    bronze_dataflowspec_path: str = None
    silver_dataflowspec_path: str = None
    update_paths: bool = True
    sdp_meta_dependency: str = None

    def __post_init__(self):
        if not self.onboarding_file_path or self.onboarding_file_path == "":
            raise ValueError("onboarding_file_path is required")
        if not self.onboarding_files_dir_path or self.onboarding_files_dir_path == "":
            raise ValueError("onboarding_files_dir_path is required")
        if not self.onboard_layer or self.onboard_layer == "":
            raise ValueError("onboard_layer is required")
        if self.onboard_layer.lower() not in ["bronze", "silver", "bronze_silver"]:
            raise ValueError("onboard_layer must be one of bronze, silver, bronze_silver")
        # if self.uc_enabled == "":
        #     raise ValueError("uc_enabled is required, please set to True or False")
        if not self.uc_enabled and not self.dbfs_path:
            raise ValueError("dbfs_path is required")
        if not self.serverless:
            if not self.cloud:
                raise ValueError("cloud is required")
            if not self.dbr_version:
                raise ValueError("dbr_version is required")
        if self.onboard_layer and self.onboard_layer.lower() == "bronze_silver":
            if not self.uc_enabled:
                if not self.bronze_dataflowspec_path:
                    raise ValueError("bronze_dataflowspec_path is required")
                if not self.silver_dataflowspec_path:
                    raise ValueError("silver_dataflowspec_path is required")
        elif self.onboard_layer.lower() == "bronze":
            if not self.uc_enabled:
                if not self.bronze_dataflowspec_path:
                    raise ValueError("bronze_dataflowspec_path is required")
        elif self.onboard_layer.lower() == "silver":
            if not self.silver_dataflowspec_table:
                raise ValueError("silver_dataflowspec_table is required")
            if not self.uc_enabled:
                if not self.silver_dataflowspec_path:
                    raise ValueError("silver_dataflowspec_path is required")
        if not self.sdp_meta_schema:
            raise ValueError("sdp_meta_schema is required")
        if not self.import_author:
            raise ValueError("import_author is required")
        if not self.version:
            raise ValueError("version is required")
        if not self.env:
            raise ValueError("env is required")
        # Validate UC identifiers up front (issue #261). Even when UC is
        # disabled the *_schema and *_dataflowspec_table values are spliced
        # unquoted into the onboarding template (see
        # ``SDPMeta.update_ws_onboarding_paths``: the ``{bronze_schema}`` /
        # ``{silver_schema}`` placeholders land inside identifier positions
        # like ``bronze_database_<env>`` and from there into the deployed
        # pipeline's SQL), so they have to be valid regardless of
        # ``uc_enabled``. Only ``uc_catalog_name`` is gated because the
        # non-UC code path never references it.
        validate_uc_identifier(self.sdp_meta_schema, kind="sdp_meta_schema")
        if self.bronze_dataflowspec_table:
            validate_uc_identifier(
                self.bronze_dataflowspec_table, kind="bronze_dataflowspec_table"
            )
        if self.silver_dataflowspec_table:
            validate_uc_identifier(
                self.silver_dataflowspec_table, kind="silver_dataflowspec_table"
            )
        if self.bronze_schema:
            validate_uc_identifier(self.bronze_schema, kind="bronze_schema")
        if self.silver_schema:
            validate_uc_identifier(self.silver_schema, kind="silver_schema")
        if self.uc_enabled:
            validate_uc_identifier(self.uc_catalog_name, kind="uc_catalog_name")


@dataclass
class DeployCommand:
    """Class representing the deploy command."""
    layer: str
    pipeline_name: str
    dlt_target_schema: str
    onboard_bronze_group: str = None
    onboard_silver_group: str = None
    sdp_meta_bronze_schema: str = None
    sdp_meta_silver_schema: str = None
    dataflowspec_bronze_table: str = None
    dataflowspec_silver_table: str = None
    num_workers: int = None
    uc_catalog_name: str = None
    dataflowspec_bronze_path: str = None
    dataflowspec_silver_path: str = None
    uc_enabled: bool = False
    serverless: bool = False
    dbfs_path: str = None
    sdp_meta_dependency: str = None

    def __post_init__(self):
        if self.uc_enabled and not self.uc_catalog_name:
            raise ValueError("uc_catalog_name is required")
        if not self.serverless and not self.num_workers:
            raise ValueError("num_workers is required")
        if not self.layer:
            raise ValueError("layer is required")
        if self.layer in ["bronze", "bronze_silver"]:
            if not self.onboard_bronze_group:
                raise ValueError("onboard_bronze_group is required")
            if self.uc_enabled and not self.dataflowspec_bronze_table:
                raise ValueError("dataflowspec_bronze_table is required")
            if not self.uc_enabled and not self.dataflowspec_bronze_path:
                raise ValueError("dataflowspec_bronze_path is required")
        if self.layer in ["silver", "bronze_silver"]:
            if not self.onboard_silver_group:
                raise ValueError("onboard_silver_group is required")
            if self.uc_enabled and not self.dataflowspec_silver_table:
                raise ValueError("dataflowspec_silver_table is required")
            if not self.uc_enabled and not self.dataflowspec_silver_path:
                raise ValueError("dataflowspec_silver_path is required")
        if not self.pipeline_name:
            raise ValueError("pipeline_name is required")
        if not self.dlt_target_schema:
            raise ValueError("dlt_target_schema is required")
        # Issue #261: catalog / schema / table names are spliced into SQL
        # identifiers downstream (e.g. `spark.read.table(<dataflowspecTable>)`),
        # so reject anything that can't be safely emitted before the pipeline
        # is even created.
        validate_uc_identifier(self.dlt_target_schema, kind="dlt_target_schema")
        if self.uc_enabled:
            validate_uc_identifier(self.uc_catalog_name, kind="uc_catalog_name")
        if self.sdp_meta_bronze_schema:
            validate_uc_identifier(
                self.sdp_meta_bronze_schema, kind="sdp_meta_bronze_schema"
            )
        if self.sdp_meta_silver_schema:
            validate_uc_identifier(
                self.sdp_meta_silver_schema, kind="sdp_meta_silver_schema"
            )
        if self.dataflowspec_bronze_table:
            validate_uc_identifier(
                self.dataflowspec_bronze_table, kind="dataflowspec_bronze_table"
            )
        if self.dataflowspec_silver_table:
            validate_uc_identifier(
                self.dataflowspec_silver_table, kind="dataflowspec_silver_table"
            )


class SDPMeta:
    """Class representing the SDP-META CLI."""

    def __init__(self, ws: WorkspaceClient):
        self._ws = ws
        self._wsi = WorkspaceInstaller(ws)
        self.version = __about__.__version__

    def _ident_question(
        self,
        text: str,
        kind: str,
        *,
        default: str = None,
        max_attempts: int = 10,
    ) -> str:
        """Prompt for a UC identifier with validation + re-prompt on bad input.

        Thin wrapper over
        :func:`databricks.labs.sdp_meta.identifiers.prompt_uc_identifier`
        that supplies this CLI's :class:`WorkspaceInstaller` (``_wsi``)
        as the prompter. Kept so existing call sites remain readable
        (``self._ident_question(text, kind=...)``) while sharing the
        retry / TTY-aware error-print logic with ``bundle.py``
        (issue #261).
        """
        return prompt_uc_identifier(
            self._wsi,
            text,
            kind=kind,
            default=default,
            max_attempts=max_attempts,
        )

    @staticmethod
    def _get_schema_from_json(oc_json: dict) -> str:
        """Read the schema key from onboarding_job_details.json with backward compatibility.

        Supports both the new key ``sdp_meta_schema`` and the legacy key
        ``dlt_meta_schema`` so that JSON files produced by either version
        of the CLI are accepted.

        Args:
            oc_json: Parsed onboarding job details JSON dict.

        Returns:
            The schema name string.

        Raises:
            KeyError: If neither key is found in the JSON.
        """
        if "sdp_meta_schema" in oc_json:
            return oc_json["sdp_meta_schema"]
        if "dlt_meta_schema" in oc_json:
            logger.warning(
                "Found legacy key 'dlt_meta_schema' in onboarding_job_details.json. "
                "Please re-run onboarding with SDP-META to update the file."
            )
            return oc_json["dlt_meta_schema"]
        raise KeyError(
            "Neither 'sdp_meta_schema' nor 'dlt_meta_schema' found in "
            "onboarding_job_details.json. Please re-run the onboarding step."
        )

    def _my_username(self):
        if not hasattr(self._ws, "_me"):
            _me = self._ws.current_user.me()
        else:
            _me = self._ws._me
        return _me.user_name

    def copy_to_uc_volume(self, src, dst):
        """Recursive copy of a directory tree to a UC Volume location.

        Two source layouts are supported:

        * **Local filesystem path** (e.g. ``./demo/``) — the historical
          path. Files are enumerated with :func:`os.walk` and streamed
          via the SDK's ``files.upload``.
        * **UC Volume path** (e.g. ``/Volumes/cat/sch/vol/sub``) — needed
          because Apps containers (and most CLI hosts) don't mount
          ``/Volumes/`` as a local filesystem. ``os.walk`` would silently
          return zero files and the onboarding job would later produce
          empty tables because every ``{uc_volume_path}/...`` reference
          in the rendered template would resolve to a nonexistent path.
          For this branch we use the SDK Files API to list, download,
          and re-upload the tree.

        Both branches mirror the same destination layout:
        ``{dst}/{base_dir_name}/<path-relative-to-src>``.

        Zero-files-copied is treated as a hard error rather than a
        silent no-op — see issue surfaced from the App's onboarding form
        where the user pointed ``local_directory`` at a UC Volume path
        and got an empty pipeline back."""
        main_dir = _normalize_file_uri_to_path(src)
        base_dir_name = os.path.basename(os.path.normpath(main_dir))
        if main_dir.startswith('/Volumes/'):
            self._copy_uc_volume_tree_to_uc_volume(main_dir, dst, base_dir_name)
            return
        file_count = 0
        for root, dirs, files in os.walk(main_dir):
            for filename in files:
                target_dir = root[len(main_dir):]
                uc_volume_path = f"{dst}/{base_dir_name}/{target_dir}/{filename}".replace("//", "/")
                with open(os.path.join(root, filename), "rb") as contents:
                    self._ws.files.upload(file_path=uc_volume_path, contents=contents, overwrite=True)
                file_count += 1
        if file_count == 0:
            raise FileNotFoundError(
                f"copy_to_uc_volume: walked local directory {main_dir} but found "
                f"zero files. Supporting files (DQE rules, silver_transformations "
                f"JSON, sample data) must exist under this directory before "
                f"onboarding runs — otherwise the rendered template's "
                f"'{{uc_volume_path}}/...' references resolve to nothing and "
                f"the resulting tables are empty."
            )
        logger.info(
            f"copy_to_uc_volume: copied {file_count} file(s) from {main_dir} "
            f"to {dst}{base_dir_name}/"
        )

    def _copy_uc_volume_tree_to_uc_volume(self, src_dir, dst, base_dir_name):
        """SDK-driven recursive copy of a UC Volume directory tree to
        another UC Volume location. Called from :meth:`copy_to_uc_volume`
        when the source path starts with ``/Volumes/`` — see that
        method's docstring for why this branch exists at all."""
        src_dir_normalized = src_dir.rstrip('/')
        file_count = 0

        def _walk(current_dir):
            nonlocal file_count
            try:
                entries = list(self._ws.files.list_directory_contents(current_dir))
            except Exception as exc:
                raise FileNotFoundError(
                    f"Could not list UC Volume directory {current_dir}: {exc}. "
                    f"Verify the path exists and the calling identity has "
                    f"READ_VOLUME on the source volume."
                ) from exc
            for entry in entries:
                if entry.is_directory:
                    _walk(entry.path)
                    continue
                rel = entry.path[len(src_dir_normalized):].lstrip('/')
                target = f"{dst}/{base_dir_name}/{rel}".replace("//", "/")
                resp = self._ws.files.download(entry.path)
                self._ws.files.upload(
                    file_path=target,
                    contents=resp.contents,
                    overwrite=True,
                )
                file_count += 1

        _walk(src_dir_normalized)
        if file_count == 0:
            raise FileNotFoundError(
                f"copy_to_uc_volume: UC Volume directory {src_dir} is empty or "
                f"unreadable. Supporting files (DQE rules, silver_transformations "
                f"JSON, sample data) must exist under this path before onboarding "
                f"runs — otherwise the rendered template's '{{uc_volume_path}}/...' "
                f"references resolve to nothing and the resulting tables are empty."
            )
        logger.info(
            f"copy_to_uc_volume: copied {file_count} file(s) from UC Volume "
            f"{src_dir} to {dst}{base_dir_name}/"
        )

    def copy_to_dbfs(self, src, dst):
        dst = dst.replace('//', '/')
        main_dir = _normalize_file_uri_to_path(src)
        main_dir = main_dir.replace('//', '/')
        base_dir_name = None
        if main_dir.endswith('/'):
            base_dir_name = main_dir[:-1]
        if base_dir_name is None:
            base_dir_name = main_dir[main_dir.rfind('/') + 1:]
        else:
            base_dir_name = base_dir_name[base_dir_name.rfind('/') + 1:]
        for root, dirs, files in os.walk(main_dir):
            for filename in files:
                target_dir = root[len(main_dir):]
                dbfs_path = f"{dst}/{base_dir_name}/{target_dir}/{filename}"
                logger.info(
                    f"local_path={os.path.join(root, filename)} "
                    f"dbfs_path={dst}/{base_dir_name}/{target_dir}/{filename}"
                )
                with open(os.path.join(root, filename), "rb") as contents:
                    self._ws.dbfs.upload(dbfs_path, contents, overwrite=True)

    def create_uc_volume(self, uc_catalog_name, sdp_meta_schema):
        try:
            self._ws.volumes.create(
                catalog_name=uc_catalog_name,
                schema_name=sdp_meta_schema,
                name=sdp_meta_schema,
                volume_type=VolumeType.MANAGED,
            )
        except Exception:
            logger.info(f"Volume {sdp_meta_schema} already exists")
        return f"/Volumes/{uc_catalog_name}/{sdp_meta_schema}/{sdp_meta_schema}/"

    def onboard(self, cmd: OnboardCommand):
        """launch the onboarding job."""
        if cmd.uc_enabled:
            self.create_uc_schema(cmd.uc_catalog_name, cmd.sdp_meta_schema)
            cmd.uc_volume_path = self.create_uc_volume(cmd.uc_catalog_name, cmd.sdp_meta_schema)
            # ``update_ws_onboarding_paths`` renders the template AND
            # uploads it directly to UC Volume \u2014 there is no local
            # staging file to upload separately. ``cmd.onboarding_file_path``
            # is rewritten to the ``/Volumes/...`` destination on
            # return so downstream consumers (named_parameters,
            # ``copy_to_uc_volume``, ``create_onnboarding_job``) see
            # the canonical UC location.
            self.update_ws_onboarding_paths(cmd)
            self.copy_to_uc_volume(cmd.onboarding_files_dir_path, cmd.uc_volume_path + "/sdp_meta_conf/")
            logger.info(f"uploading to  {cmd.uc_volume_path}/sdp_meta_conf complete!!!")
        else:
            # DBFS flow keeps the historical local-then-upload shape:
            # render writes a local file, then ``dbfs.upload`` pushes
            # it to ``{dbfs_path}/sdp_meta_conf/``. Distinct SDK
            # surface from UC; intentionally untouched here.
            onboarding_filename = os.path.basename(cmd.onboarding_file_path)
            self._ws.dbfs.mkdirs(f"{cmd.dbfs_path}/sdp_meta_conf/")
            with open(cmd.onboarding_file_path, "rb") as ob_file:
                self._ws.dbfs.upload(
                    f"{cmd.dbfs_path}/sdp_meta_conf/{onboarding_filename}",
                    ob_file,
                    overwrite=True,
                )
            self.update_ws_onboarding_paths(cmd)
            self.copy_to_dbfs(cmd.onboarding_files_dir_path, cmd.dbfs_path + "/sdp_meta_conf/")
            logger.info(f"uploading to  {cmd.dbfs_path}/sdp_meta_conf complete!!!")
        created_job = self.create_onnboarding_job(cmd)
        logger.info(f"Waiting for job to complete. job_id={created_job.job_id}")
        run = self._ws.jobs.run_now(job_id=created_job.job_id)
        msg = (
            "SDP-META Onboarding Job(job_id={}) "
            "launched with run_id={}, Please check the job status in databricks workspace jobs tab"
        ).format(created_job.job_id, run.run_id)
        logger.info(msg)
        job_url = f"{self._ws.config.host}/jobs/{created_job.job_id}?o={self._ws.get_workspace_id()}"
        print(
            f"Job created successfully. job_id={created_job.job_id}, url={job_url}"
        )
        _maybe_open_url(f"{self._ws.config.host}/jobs/{created_job.job_id}?o={self._ws.get_workspace_id()}")

    def create_uc_schema(self, uc_catalog_name, sdp_meta_schema):
        try:
            SchemasAPI(self._ws.api_client).get(full_name=f"{uc_catalog_name}.{sdp_meta_schema}")
        except Exception:
            msg = (
                "Schema {catalog}.{schema} not found. "
                "Creating schema={schema}"
            ).format(catalog=uc_catalog_name, schema=sdp_meta_schema)
            logger.info(msg)
            SchemasAPI(self._ws.api_client).create(
                catalog_name=uc_catalog_name,
                name=sdp_meta_schema,
                comment="sdp_meta framework schema"
            )

    def create_onnboarding_job(self, cmd: OnboardCommand):
        """Create the onboarding job."""
        if cmd.serverless:
            cluster_spec = None
        else:
            cluster_spec = compute.ClusterSpec(
                spark_version=cmd.dbr_version,
                num_workers=1,
                driver_node_type_id=cloud_node_type_id_dict[cmd.cloud],
                node_type_id=cloud_node_type_id_dict[cmd.cloud],
                data_security_mode=compute.DataSecurityMode.SINGLE_USER
                if cmd.uc_enabled else compute.DataSecurityMode.LEGACY_SINGLE_USER,
                spark_conf={},
                spark_env_vars={
                    "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                }
            )
        named_parameters = self._get_onboarding_named_parameters(cmd)
        # Default to the PyPI *distribution* name (databricks-labs-sdp-meta),
        # not the labs project name (sdp-meta) — the latter does not exist on
        # PyPI and fails at job run time during environment install. Must
        # stay in sync with the deploy fallback in _create_sdp_meta_pipeline.
        sdp_meta_dependency = cmd.sdp_meta_dependency or f"databricks-labs-sdp-meta=={self.version}"
        sdp_meta_environments = [
            jobs.JobEnvironment(
                environment_key="sdp_meta_cli_env",
                spec=compute.Environment(client="1",
                                         dependencies=[sdp_meta_dependency]
                                         )
            )
        ]
        return self._ws.jobs.create(
            name="sdp_meta_onboarding_job",
            environments=None if not cmd.serverless else sdp_meta_environments,
            tasks=[
                jobs.Task(
                    task_key="sdp_meta_onbarding_task",
                    description="test",
                    new_cluster=cluster_spec if not cmd.serverless else None,
                    environment_key="sdp_meta_cli_env" if cmd.serverless else None,
                    timeout_seconds=0,
                    python_wheel_task=jobs.PythonWheelTask(
                        package_name="databricks_labs_sdp_meta",
                        entry_point="run",
                        named_parameters=named_parameters,
                    ),
                    libraries=self._onboarding_job_libraries(sdp_meta_dependency)
                    if not cmd.serverless else None,
                ),
            ]
        )

    def _onboarding_job_libraries(self, sdp_meta_dependency: str):
        if sdp_meta_dependency.startswith("/Volumes/") or sdp_meta_dependency.endswith(".whl"):
            return [jobs.compute.Library(whl=sdp_meta_dependency)]
        return [
            jobs.compute.Library(
                pypi=compute.PythonPyPiLibrary(package=sdp_meta_dependency)
            )
        ]

    def _get_onboarding_named_parameters(self, cmd: OnboardCommand):
        named_parameters = {
            "onboard_layer": cmd.onboard_layer,
            "database":
                f"{cmd.uc_catalog_name}.{cmd.sdp_meta_schema}"
                if cmd.uc_enabled else cmd.sdp_meta_schema,
            "import_author": cmd.import_author,
            "version": cmd.version,
            "overwrite": "True" if cmd.overwrite else "False",
            "env": cmd.env,
            "uc_enabled": "True" if cmd.uc_enabled else "False"
        }
        if cmd.uc_enabled:
            # Use basename only — cmd.onboarding_file_path is a full local path
            # after update_ws_onboarding_paths runs, and uc_volume_path has a
            # trailing slash, so naively joining them produces double slashes.
            named_parameters["onboarding_file_path"] = (
                f"{cmd.uc_volume_path.rstrip('/')}/sdp_meta_conf/tmp/"
                f"{os.path.basename(cmd.onboarding_file_path)}"
            )
        else:
            named_parameters["onboarding_file_path"] = (
                f"{cmd.dbfs_path}/sdp_meta_conf/"
                f"{os.path.basename(cmd.onboarding_file_path)}"
            )
        if cmd.onboard_layer == "bronze_silver":
            named_parameters["bronze_dataflowspec_table"] = cmd.bronze_dataflowspec_table
            named_parameters["silver_dataflowspec_table"] = cmd.silver_dataflowspec_table
            if not cmd.uc_enabled:
                named_parameters["bronze_dataflowspec_path"] = cmd.bronze_dataflowspec_path
                named_parameters["silver_dataflowspec_path"] = cmd.silver_dataflowspec_path
        elif cmd.onboard_layer == "bronze":
            named_parameters["bronze_dataflowspec_table"] = cmd.bronze_dataflowspec_table
            if not cmd.uc_enabled:
                named_parameters["bronze_dataflowspec_path"] = cmd.bronze_dataflowspec_path
        elif cmd.onboard_layer == "silver":
            named_parameters["silver_dataflowspec_table"] = cmd.silver_dataflowspec_table
            if not cmd.uc_enabled:
                named_parameters["silver_dataflowspec_path"] = cmd.silver_dataflowspec_path
        return named_parameters

    def _install_folder(self):
        return f"/Users/{self._my_username()}/sdp-meta"

    def _create_sdp_meta_pipeline(self, cmd: DeployCommand):
        """Create the SDP-META pipeline."""
        # ``sdp_meta_dependency`` lets users replace the default PyPI install
        # (``databricks-labs-sdp-meta==<version>``) with a UC-volume wheel
        # path or any other pip-installable spec — see ``deploy()`` for the
        # resolution order (CLI flag > onboarding_job_details.json > PyPI).
        dependency = cmd.sdp_meta_dependency or f"databricks-labs-sdp-meta=={self.version}"
        runner_notebook_py = SDP_META_RUNNER_NOTEBOOK.format(dependency=dependency).encode("utf8")
        runner_notebook_path = f"{self._install_folder()}/init_sdp_meta_pipeline.py"
        try:
            self._ws.workspace.mkdirs(self._install_folder())
        except DatabricksError as e:
            logger.error(e)
        self._ws.workspace.upload(runner_notebook_path, runner_notebook_py, overwrite=True)
        configuration = {
            "layer": cmd.layer,
        }
        if cmd.layer in ["bronze", "silver", "bronze_silver"]:
            if cmd.layer in ["bronze", "bronze_silver"]:
                configuration["bronze.group"] = cmd.onboard_bronze_group
                if cmd.uc_catalog_name:
                    configuration["bronze.dataflowspecTable"] = (
                        f"{cmd.uc_catalog_name}.{cmd.sdp_meta_bronze_schema}.{cmd.dataflowspec_bronze_table}"
                    )
                else:
                    configuration["bronze.dataflowspecTable"] = (
                        f"{cmd.sdp_meta_bronze_schema}.{cmd.dataflowspec_bronze_table}"
                    )
            if cmd.layer in ["silver", "bronze_silver"]:
                configuration["silver.group"] = cmd.onboard_silver_group
                if cmd.uc_catalog_name:
                    configuration["silver.dataflowspecTable"] = (
                        f"{cmd.uc_catalog_name}.{cmd.sdp_meta_silver_schema}.{cmd.dataflowspec_silver_table}"
                    )
                else:
                    configuration["silver.dataflowspecTable"] = (
                        f"{cmd.sdp_meta_silver_schema}.{cmd.dataflowspec_silver_table}"
                    )
        else:
            raise ValueError("layer must be one of bronze, silver, bronze_silver ")
        created = None
        configuration["version"] = self.version
        # Tag every pipeline created by SDP-META so the Databricks App monitor
        # can filter to only SDP-META pipelines via list_pipelines(filter=...).
        #
        # Tag value = SDP-META version (e.g. "0.1.0"). Two reasons over the
        # historical sentinel "true":
        #   1. Forensics — "which SDP-META version created this pipeline?"
        #      becomes a tag read instead of a full get() + config dive.
        #   2. Migration queries — find all pipelines created by a specific
        #      release with a single workspace-tag filter.
        # The consumer (_is_sdp_meta in databricks_app/routes/pipelines.py)
        # treats ANY non-empty sdp_meta tag value as a match, so legacy
        # pipelines tagged sdp_meta=true keep working.
        _sdp_meta_tags = {"sdp_meta": self.version}
        if cmd.uc_catalog_name:
            # DPM-enabled UC workspaces expect schema=. _create_pipeline
            # transparently falls back to target= on non-DPM workspaces.
            created = _create_pipeline(
                self._ws,
                catalog=cmd.uc_catalog_name,
                name=cmd.pipeline_name,
                configuration=configuration,
                libraries=[
                    PipelineLibrary(
                        notebook=NotebookLibrary(
                            path=runner_notebook_path
                        )
                    )
                ],
                schema=cmd.dlt_target_schema,
                clusters=[
                    pipelines.PipelineCluster(
                        label="default", num_workers=cmd.num_workers
                    )
                ] if not cmd.serverless else None,
                serverless=cmd.serverless if cmd.uc_enabled else None,
                channel="PREVIEW" if cmd.serverless else None,
                tags=_sdp_meta_tags,
            )
        else:
            # Non-UC (Hive metastore) pipelines only support the legacy
            # target= parameter, so the helper passes through unchanged.
            created = _create_pipeline(
                self._ws,
                name=cmd.pipeline_name,
                configuration=configuration,
                libraries=[
                    PipelineLibrary(
                        notebook=NotebookLibrary(
                            path=runner_notebook_path
                        )
                    )
                ],
                target=cmd.dlt_target_schema,
                clusters=[pipelines.PipelineCluster(label="default", num_workers=cmd.num_workers)],
                tags=_sdp_meta_tags,
            )
        if created is None:
            raise Exception("Pipeline creation failed")
        return created.pipeline_id

    def deploy(self, cmd: DeployCommand):
        pipeline_id = self._create_sdp_meta_pipeline(cmd)
        update_response = self._ws.pipelines.start_update(pipeline_id=pipeline_id)
        msg = (
            f"sdp-meta pipeline={pipeline_id} created and launched with "
            f"update_id={update_response.update_id}, Please check the pipeline status in "
            "databricks workspace under workflows -> Lakeflow Spark Declarative Pipelines tab"
        )
        logger.info(msg)
        print(
            f"sdp-meta pipeline={pipeline_id} created and launched with update_id={update_response.update_id}, "
            f"url={self._ws.config.host}/#joblist/pipelines/{pipeline_id}?o={self._ws.get_workspace_id()}/"
        )
        _maybe_open_url(f"{self._ws.config.host}/#joblist/pipelines/{pipeline_id}?o={self._ws.get_workspace_id()}/")

    def _load_onboard_config(self) -> OnboardCommand:
        onboard_cmd_dict = {}
        onboard_cmd_dict["uc_enabled"] = self._wsi._choice(
            "Run onboarding with unity catalog enabled?", ['True', 'False'])
        onboard_cmd_dict["uc_enabled"] = True if onboard_cmd_dict["uc_enabled"] == "True" else False
        if onboard_cmd_dict["uc_enabled"]:
            onboard_cmd_dict["dbfs_path"] = None
            onboard_cmd_dict["uc_catalog_name"] = self._ident_question(
                "Provide unity catalog name", kind="uc_catalog_name")
        else:
            onboard_cmd_dict["dbfs_path"] = self._wsi._question(
                "Provide dbfs path", default=f"dbfs:/sdp-meta_cli_demo_{uuid.uuid4().hex}")
        onboard_cmd_dict["serverless"] = self._wsi._choice(
            "Run onboarding with serverless?", ['True', 'False'])
        onboard_cmd_dict["serverless"] = True if onboard_cmd_dict["serverless"] == 'True' else False
        if onboard_cmd_dict["serverless"]:
            onboard_cmd_dict["cloud"] = None
            onboard_cmd_dict["dbr_version"] = None
        else:
            onboard_cmd_dict["cloud"] = self._wsi._choice(
                "Provide cloud provider name", ['aws', 'azure', 'gcp'])
            onboard_cmd_dict["dbr_version"] = self._wsi._question(
                "Provide databricks runtime version", default=self._ws.clusters.select_spark_version(latest=True))
        onboard_cmd_dict["onboarding_file_path"] = self._wsi._question(
            "Provide onboarding file path", default='demo/conf/json/onboarding.template')
        cwd = os.getcwd()
        onboarding_files_dir_path = self._wsi._question(
            "Provide onboarding files local directory", default=f'{cwd}/demo/')
        onboard_cmd_dict["onboarding_files_dir_path"] = _path_to_file_uri(onboarding_files_dir_path)
        onboard_cmd_dict["sdp_meta_schema"] = self._ident_question(
            "Provide sdp meta schema name",
            kind="sdp_meta_schema",
            default=f'sdp_meta_dataflowspecs_{uuid.uuid4().hex}')
        onboard_cmd_dict["bronze_schema"] = self._ident_question(
            "Provide sdp meta bronze layer schema name",
            kind="bronze_schema",
            default=f'sdp_meta_bronze_{uuid.uuid4().hex}')
        onboard_cmd_dict["silver_schema"] = self._ident_question(
            "Provide sdp meta silver layer schema name",
            kind="silver_schema",
            default=f'sdp_meta_silver_{uuid.uuid4().hex}')
        onboard_cmd_dict["onboard_layer"] = self._wsi._choice(
            "Provide sdp meta layer", ['bronze', 'silver', 'bronze_silver'])
        if onboard_cmd_dict["onboard_layer"] in ["bronze", "bronze_silver"]:
            onboard_cmd_dict["bronze_dataflowspec_table"] = self._ident_question(
                "Provide bronze dataflow spec table name",
                kind="bronze_dataflowspec_table",
                default='bronze_dataflowspec')
            if not onboard_cmd_dict["uc_enabled"]:
                onboard_cmd_dict["bronze_dataflowspec_path"] = self._wsi._question(
                    "Provide bronze dataflow spec path", default=f'{self._install_folder()}/bronze_dataflow_specs')
        if onboard_cmd_dict["onboard_layer"] in ["silver", "bronze_silver"]:
            onboard_cmd_dict["silver_dataflowspec_table"] = self._ident_question(
                "Provide silver dataflow spec table name",
                kind="silver_dataflowspec_table",
                default='silver_dataflowspec')
            if not onboard_cmd_dict["uc_enabled"]:
                onboard_cmd_dict["silver_dataflowspec_path"] = self._wsi._question(
                    "Provide silver dataflow spec path", default=f'{self._install_folder()}/silver_dataflow_specs')
        onboard_cmd_dict["overwrite"] = self._wsi._choice(
            "Overwrite dataflow spec?", ['True', 'False'])
        onboard_cmd_dict["overwrite"] = True if onboard_cmd_dict["overwrite"] == 'True' else False
        onboard_cmd_dict["version"] = self._wsi._question(
            "Provide dataflow spec version", default='v1')
        onboard_cmd_dict["env"] = self._wsi._question(
            "Provide environment name", default='prod')
        onboard_cmd_dict["import_author"] = self._wsi._question(
            "Provide import author name", default=self._wsi._short_name)
        onboard_cmd_dict["update_paths"] = self._wsi._choice(
            "Update workspace/dbfs uc volume paths, unity catalog name, bronze/silver schema names in onboarding file?",
            ['True', 'False'])
        with open("onboarding_job_details.json", "w") as oc_file:
            json.dump(onboard_cmd_dict, oc_file, indent=4)
        cmd = OnboardCommand(**onboard_cmd_dict)

        return cmd

    def _load_deploy_config(self) -> DeployCommand:
        oc_job_details_json = None
        if os.path.isfile("onboarding_job_details.json"):
            with open("onboarding_job_details.json") as f:
                oc_job_details_json = f.read()
        load_from_ojd_json = False
        if oc_job_details_json:
            load_from_ojd_json_opt = self._wsi._choice(
                "onboarding_job_details.json Found! Do you want to use it for deployment?",
                ['Yes', 'No']
            )
            load_from_ojd_json = True if load_from_ojd_json_opt == "Yes" else False
        deploy_cmd_dict = {}
        if load_from_ojd_json:
            oc_job_details_json = json.loads(oc_job_details_json)
            deploy_cmd_dict["uc_enabled"] = self._wsi._choice(
                "Deploy SDP-META with unity catalog enabled?", ["True", "False"])
            deploy_cmd_dict["uc_enabled"] = True if deploy_cmd_dict["uc_enabled"] == "True" else False
            if deploy_cmd_dict["uc_enabled"]:
                deploy_cmd_dict["uc_catalog_name"] = self._ident_question(
                    "Provide unity catalog name", kind="uc_catalog_name")
                deploy_cmd_dict["serverless"] = self._wsi._choice(
                    "Deploy SDP-META with serverless?", ["True", "False"])
                deploy_cmd_dict["serverless"] = True if deploy_cmd_dict["serverless"] == "True" else False
            else:
                deploy_cmd_dict["serverless"] = False
            deploy_cmd_dict["layer"] = self._wsi._choice(
                "Provide sdp meta layer", ['bronze', 'silver', 'bronze_silver'])
            if deploy_cmd_dict["layer"] == "bronze" or deploy_cmd_dict["layer"] == "bronze_silver":
                if deploy_cmd_dict["uc_enabled"]:
                    deploy_cmd_dict["sdp_meta_bronze_schema"] = self._get_schema_from_json(oc_job_details_json)
                    deploy_cmd_dict["dataflowspec_bronze_table"] = oc_job_details_json["bronze_dataflowspec_table"]
                else:
                    deploy_cmd_dict["dataflowspec_bronze_path"] = oc_job_details_json["bronze_dataflowspec_path"]
                deploy_cmd_dict["onboard_bronze_group"] = self._wsi._question(
                    "Provide sdp meta bronze onboard group")
            if deploy_cmd_dict["layer"] == "silver" or deploy_cmd_dict["layer"] == "bronze_silver":
                if deploy_cmd_dict["uc_enabled"]:
                    deploy_cmd_dict["sdp_meta_silver_schema"] = self._get_schema_from_json(oc_job_details_json)
                    deploy_cmd_dict["dataflowspec_silver_table"] = oc_job_details_json["silver_dataflowspec_table"]
                else:
                    deploy_cmd_dict["dataflowspec_silver_path"] = oc_job_details_json["silver_dataflowspec_path"]
                deploy_cmd_dict["onboard_silver_group"] = self._wsi._question(
                    "Provide sdp meta silver onboard group")
            if not deploy_cmd_dict["serverless"]:
                deploy_cmd_dict["num_workers"] = int(self._wsi._question(
                    "Provide number of workers", default=4))
        else:
            deploy_cmd_dict["uc_enabled"] = self._wsi._choice(
                "Deploy SDP-META with unity catalog enabled?", ["True", "False"])
            deploy_cmd_dict["uc_enabled"] = True if deploy_cmd_dict["uc_enabled"] == "True" else False
            if deploy_cmd_dict["uc_enabled"]:
                deploy_cmd_dict["uc_catalog_name"] = self._ident_question(
                    "Provide unity catalog name", kind="uc_catalog_name")
                deploy_cmd_dict["serverless"] = self._wsi._choice(
                    "Deploy SDP-META with serverless?", ["True", "False"])
                deploy_cmd_dict["serverless"] = True if deploy_cmd_dict["serverless"] == "True" else False
            else:
                deploy_cmd_dict["serverless"] = False
            deploy_cmd_dict["layer"] = self._wsi._choice(
                "Provide sdp meta layer", ['bronze', 'silver', 'bronze_silver'])
            if deploy_cmd_dict["layer"] in ["bronze", "bronze_silver"]:
                deploy_cmd_dict["onboard_bronze_group"] = self._wsi._question(
                    "Provide sdp meta onboard bronze group")
                deploy_cmd_dict["sdp_meta_bronze_schema"] = self._ident_question(
                    "Provide sdp_meta bronze dataflowspec schema name",
                    kind="sdp_meta_bronze_schema")
                deploy_cmd_dict["dataflowspec_bronze_table"] = self._ident_question(
                    "Provide bronze dataflowspec table name",
                    kind="dataflowspec_bronze_table",
                    default='bronze_dataflowspec')
                if not deploy_cmd_dict["uc_enabled"]:
                    deploy_cmd_dict["dataflowspec_bronze_path"] = self._wsi._question(
                        "Provide bronze dataflowspec path", default=f'{self._install_folder()}/bronze_dataflow_specs')
            if deploy_cmd_dict["layer"] in ["silver", "bronze_silver"]:
                deploy_cmd_dict["onboard_silver_group"] = self._wsi._question(
                    "Provide sdp meta silver onboard group")
                deploy_cmd_dict["sdp_meta_silver_schema"] = self._ident_question(
                    "Provide sdp_meta silver dataflowspec schema name",
                    kind="sdp_meta_silver_schema")
                deploy_cmd_dict["dataflowspec_silver_table"] = self._ident_question(
                    "Provide silver dataflowspec table name",
                    kind="dataflowspec_silver_table",
                    default='silver_dataflowspec')
                if not deploy_cmd_dict["uc_enabled"]:
                    deploy_cmd_dict["dataflowspec_silver_path"] = self._wsi._question(
                        "Provide silver dataflowspec path",
                        default=f'{self._install_folder()}/silver_dataflow_specs')
            if not deploy_cmd_dict["serverless"]:
                deploy_cmd_dict["num_workers"] = int(self._wsi._question(
                    "Provide number of workers", default=4))
        layer = deploy_cmd_dict["layer"]
        deploy_cmd_dict["pipeline_name"] = self._wsi._question(
            "Provide sdp meta pipeline name", default=f"sdp_meta_{layer}_pipeline_{uuid.uuid4().hex}")
        deploy_cmd_dict["dlt_target_schema"] = self._ident_question(
            "Provide dlt target schema name", kind="dlt_target_schema")
        return DeployCommand(**deploy_cmd_dict)

    def _load_onboard_config_ui(self, form_data) -> OnboardCommand:
        onboard_cmd_dict = {}

        # Get unity catalog settings
        onboard_cmd_dict["uc_enabled"] = True if form_data.get('unity_catalog_enabled') == "1" else False
        if onboard_cmd_dict["uc_enabled"]:
            onboard_cmd_dict["dbfs_path"] = None
            onboard_cmd_dict["uc_catalog_name"] = form_data.get('unity_catalog_name')
        else:
            onboard_cmd_dict["dbfs_path"] = f"dbfs:/sdp-meta_cli_demo_{uuid.uuid4().hex}"

        # Get serverless setting
        onboard_cmd_dict["serverless"] = True if form_data.get('serverless') == "1" else False
        if onboard_cmd_dict["serverless"]:
            onboard_cmd_dict["cloud"] = None
            onboard_cmd_dict["dbr_version"] = None
        else:
            # These fields are not in the form, so using defaults
            onboard_cmd_dict["cloud"] = "aws"  # Default value
            onboard_cmd_dict["dbr_version"] = self._ws.clusters.select_spark_version(latest=True)

        # Get file paths
        onboard_cmd_dict["onboarding_file_path"] = form_data.get(
            'onboarding_file_path', 'demo/conf/json/onboarding.template'
        )
        onboarding_files_dir_path = form_data.get('local_directory', f'{os.getcwd()}/demo/')
        onboard_cmd_dict["onboarding_files_dir_path"] = _path_to_file_uri(onboarding_files_dir_path)

        # Get schema names
        onboard_cmd_dict["sdp_meta_schema"] = form_data.get(
            'sdp_meta_schema', f'sdp_meta_dataflowspecs_{uuid.uuid4().hex}'
        )
        onboard_cmd_dict["bronze_schema"] = form_data.get('bronze_schema', f'sdp_meta_bronze_{uuid.uuid4().hex}')
        onboard_cmd_dict["silver_schema"] = form_data.get('silver_schema', f'sdp_meta_silver_{uuid.uuid4().hex}')

        # Map sdp_meta_layer value from form to expected values
        layer_map = {
            "0": "bronze",
            "1": "bronze_silver",
            "2": "silver"
        }
        onboard_cmd_dict["onboard_layer"] = layer_map.get(form_data.get('sdp_meta_layer'), 'bronze_silver')

        # Handle layer-specific settings
        if onboard_cmd_dict["onboard_layer"] == "bronze" or onboard_cmd_dict["onboard_layer"] == "bronze_silver":
            onboard_cmd_dict["bronze_dataflowspec_table"] = form_data.get('bronze_table', 'bronze_dataflowspec')
            if not onboard_cmd_dict["uc_enabled"]:
                onboard_cmd_dict["bronze_dataflowspec_path"] = f'{self._install_folder()}/bronze_dataflow_specs'

        if onboard_cmd_dict["onboard_layer"] == "silver" or onboard_cmd_dict["onboard_layer"] == "bronze_silver":
            onboard_cmd_dict["silver_dataflowspec_table"] = form_data.get('silver_table', 'silver_dataflowspec')
            if not onboard_cmd_dict["uc_enabled"]:
                onboard_cmd_dict["silver_dataflowspec_path"] = f'{self._install_folder()}/silver_dataflow_specs'

        # Get other settings
        onboard_cmd_dict["overwrite"] = True if form_data.get('overwrite') == "1" else False
        onboard_cmd_dict["version"] = form_data.get('version', 'v1')
        onboard_cmd_dict["env"] = form_data.get('environment', 'prod')
        onboard_cmd_dict["import_author"] = form_data.get('author', self._wsi._short_name)
        onboard_cmd_dict["update_paths"] = True if form_data.get('update_paths') == "1" else False

        # Save to file
        with open("onboarding_job_details.json", "w") as oc_file:
            json.dump(onboard_cmd_dict, oc_file, indent=4)

        cmd = OnboardCommand(**onboard_cmd_dict)
        return cmd

    def _load_deploy_config_ui(self, input_params) -> DeployCommand:
        oc_job_details_json = None
        if os.path.isfile("onboarding_job_details.json"):
            with open("onboarding_job_details.json") as f:
                oc_job_details_json = f.read()

        load_from_ojd_json = _coerce_bool(input_params.get("load_from_ojd_json", False))
        deploy_cmd_dict = {}

        if load_from_ojd_json and oc_job_details_json:
            oc_job_details_json = json.loads(oc_job_details_json)
            # The App envelope sends ``uc_enabled`` / ``serverless`` as the
            # STRINGS "1" / "0" (HTML radio button values). Python truthy-
            # checks accept both as True, but downstream ``self._ws.pipelines.
            # create(serverless=...)`` round-trips the value through json.dumps
            # — a literal string "1" then lands in the request body as
            # ``"serverless": "1"`` (a JSON string, not the boolean true),
            # and the control-plane silently treats the field as missing,
            # defaults to a classic cluster, and rejects the pipeline on
            # serverless-only workspaces with "You must use serverless
            # compute in this workspace." Coerce to bool here so the SDK
            # sees an actual boolean. Same reasoning for ``uc_enabled``.
            deploy_cmd_dict["uc_enabled"] = _coerce_bool(input_params.get("uc_enabled", False))
            if deploy_cmd_dict["uc_enabled"]:
                deploy_cmd_dict["uc_catalog_name"] = input_params.get("uc_catalog_name")
                deploy_cmd_dict["serverless"] = _coerce_bool(input_params.get("serverless", False))
            else:
                deploy_cmd_dict["serverless"] = False
            deploy_cmd_dict["layer"] = input_params.get("layer")
            if deploy_cmd_dict["layer"] in ["bronze", "bronze_silver"]:
                if deploy_cmd_dict["uc_enabled"]:
                    deploy_cmd_dict["sdp_meta_bronze_schema"] = self._get_schema_from_json(oc_job_details_json)
                    deploy_cmd_dict["dataflowspec_bronze_table"] = oc_job_details_json["bronze_dataflowspec_table"]
                else:
                    deploy_cmd_dict["dataflowspec_bronze_path"] = oc_job_details_json["bronze_dataflowspec_path"]
                deploy_cmd_dict["onboard_bronze_group"] = input_params.get("onboard_bronze_group")
            if deploy_cmd_dict["layer"] in ["silver", "bronze_silver"]:
                if deploy_cmd_dict["uc_enabled"]:
                    deploy_cmd_dict["sdp_meta_silver_schema"] = self._get_schema_from_json(oc_job_details_json)
                    deploy_cmd_dict["dataflowspec_silver_table"] = oc_job_details_json["silver_dataflowspec_table"]
                else:
                    deploy_cmd_dict["dataflowspec_silver_path"] = oc_job_details_json["silver_dataflowspec_path"]
                deploy_cmd_dict["onboard_silver_group"] = input_params.get("onboard_silver_group")
            if not deploy_cmd_dict["serverless"]:
                deploy_cmd_dict["num_workers"] = input_params.get("num_workers", 4)
        else:
            # See the matching block above for why coercion is needed.
            deploy_cmd_dict["uc_enabled"] = _coerce_bool(input_params.get("uc_enabled", False))
            if deploy_cmd_dict["uc_enabled"]:
                deploy_cmd_dict["uc_catalog_name"] = input_params.get("uc_catalog_name")
                deploy_cmd_dict["serverless"] = _coerce_bool(input_params.get("serverless", False))
            else:
                deploy_cmd_dict["serverless"] = False
            deploy_cmd_dict["layer"] = input_params.get("layer")
            if deploy_cmd_dict["layer"] in ["bronze", "bronze_silver"]:
                deploy_cmd_dict["onboard_bronze_group"] = input_params.get("onboard_bronze_group")
                deploy_cmd_dict["sdp_meta_bronze_schema"] = input_params.get("sdp_meta_bronze_schema")
                deploy_cmd_dict["dataflowspec_bronze_table"] = input_params.get("dataflowspec_bronze_table",
                                                                                "bronze_dataflowspec")
                if not deploy_cmd_dict["uc_enabled"]:
                    deploy_cmd_dict["dataflowspec_bronze_path"] = input_params.get(
                        "dataflowspec_bronze_path",
                        f'{self._install_folder()}/bronze_dataflow_specs'
                    )
            if deploy_cmd_dict["layer"] in ["silver", "bronze_silver"]:
                deploy_cmd_dict["onboard_silver_group"] = input_params.get("onboard_silver_group")
                deploy_cmd_dict["sdp_meta_silver_schema"] = input_params.get("sdp_meta_silver_schema")
                deploy_cmd_dict["dataflowspec_silver_table"] = input_params.get("dataflowspec_silver_table",
                                                                                "silver_dataflowspec")
                if not deploy_cmd_dict["uc_enabled"]:
                    deploy_cmd_dict["dataflowspec_silver_path"] = input_params.get(
                        "dataflowspec_silver_path",
                        f'{self._install_folder()}/silver_dataflow_specs'
                    )
            if not deploy_cmd_dict["serverless"]:
                deploy_cmd_dict["num_workers"] = input_params.get("num_workers", 4)

        layer = deploy_cmd_dict["layer"]
        deploy_cmd_dict["pipeline_name"] = input_params.get("pipeline_name",
                                                            f"sdp_meta_{layer}_pipeline_{uuid.uuid4().hex}")
        deploy_cmd_dict["dlt_target_schema"] = input_params.get("dlt_target_schema")

        return DeployCommand(**deploy_cmd_dict)

    def update_ws_onboarding_paths(self, cmd: OnboardCommand):
        """Substitute ``{placeholder}`` tokens in the onboarding file and
        publish the rendered result to a location the onboarding job
        can actually read.

        Delegates the in-memory substitution and parse-validate round-trip to
        :func:`render_onboarding_template` so the same logic powers the
        App's ``/onboarding/preview`` endpoint. We preserve the source
        extension so downstream consumers (``onboard_dataflowspec.py``) read
        the file with the matching parser \u2014 JSON sources stay
        ``onboarding.json``, YAML sources stay ``onboarding.yml`` (or
        ``.yaml``).

        Output-location strategy
        ------------------------
        UC-enabled flows (the supported path for the Databricks App):
            The rendered bytes are uploaded DIRECTLY to
            ``{cmd.uc_volume_path}/sdp_meta_conf/tmp/onboarding.{ext}``
            and ``cmd.onboarding_file_path`` is rewritten to point at
            that UC Volume path. No local file is written. This is
            the only place the cluster running the onboarding job
            can read from anyway \u2014 staging on the App
            container's local filesystem (in ``/tmp``, in the App
            wheel folder, or under the user's
            ``onboarding_files_dir_path``) would either leak files
            on every run, risk overwriting the user's own files, or
            put the spec somewhere the job can't open.

            Callers that previously did a follow-up local-to-UC
            upload (see :meth:`onboard`) MUST skip that step now
            \u2014 doing both is wasteful but harmless (same bytes,
            same destination, ``overwrite=True``); the local
            ``open()`` would simply fail because
            ``cmd.onboarding_file_path`` is now a ``/Volumes/...``
            path.

        Non-UC (DBFS) flows:
            Preserve the historical behaviour \u2014 render to
            ``<src_dir>/onboarding.{json|yml}`` and let
            :meth:`onboard` push it to DBFS with ``dbfs.upload``.
            The DBFS code path uses a different SDK surface and
            its own staging contract; we don't change it here.
        """
        string_subs = {
            "{uc_volume_path}": f"{cmd.uc_volume_path}/sdp_meta_conf/",
            "{uc_catalog_name}": cmd.uc_catalog_name,
            "{bronze_schema}": cmd.bronze_schema,
            "{silver_schema}": cmd.silver_schema,
        }
        with open(cmd.onboarding_file_path) as f:
            content = f.read()

        src_ext = os.path.splitext(cmd.onboarding_file_path)[1].lower()
        rendered, _ = render_onboarding_template(content, src_ext, string_subs)

        if src_ext in (".yml", ".yaml"):
            rendered_basename = f"onboarding{src_ext}"
        else:
            rendered_basename = "onboarding.json"

        if cmd.uc_enabled and cmd.uc_volume_path:
            uc_dest = (
                f"{cmd.uc_volume_path.rstrip('/')}/sdp_meta_conf/tmp/"
                f"{rendered_basename}"
            )
            self._ws.files.upload(
                file_path=uc_dest,
                contents=io.BytesIO(rendered.encode("utf-8")),
                overwrite=True,
            )
            logger.info(
                "Uploaded rendered onboarding file directly to UC Volume "
                "at %s (no local staging file written).",
                uc_dest,
            )
            cmd.onboarding_file_path = uc_dest
            return

        # Non-UC (DBFS) flow: keep historical write-next-to-source.
        src_dir = os.path.dirname(cmd.onboarding_file_path)
        updated_ob_file_path = os.path.join(src_dir, rendered_basename)
        with open(updated_ob_file_path, "w") as out:
            out.write(rendered)
        cmd.onboarding_file_path = updated_ob_file_path


def render_onboarding_template(content: str, source_ext: str, substitutions: dict):
    """Apply ``{token}`` → value substitutions to ``content`` and re-emit it
    canonically in the source's format. Returns ``(rendered_text, parsed_obj)``.

    Used by both :meth:`SDPMeta.update_ws_onboarding_paths` (which writes the
    rendered text to disk) and the App's ``/onboarding/preview`` endpoint
    (which returns it to the browser unmodified). Keeping the logic in one
    place avoids drift between what the preview shows and what onboarding
    actually emits.

    ``source_ext`` is the file's extension *as seen on disk* (``.yml`` /
    ``.yaml`` / ``.json`` / anything else). YAML sources round-trip through
    ``yaml.safe_load`` / ``yaml.safe_dump``; everything else round-trips
    through ``json.loads`` / ``json.dumps``. The round-trip is intentional
    — it catches malformed substitutions (e.g. a value containing an
    unescaped quote that breaks JSON) before the file leaves the caller.

    ``substitutions`` is a dict of ``{placeholder_token: value}``. ``None``
    values are normalised to empty string so the substitution doesn't write
    the literal text ``None`` into the output."""
    for key, val in substitutions.items():
        val = "" if val is None else val
        content = content.replace(key, val)

    src_ext = (source_ext or "").lower()
    if src_ext in (".yml", ".yaml"):
        parsed = yaml.safe_load(content)
        # ``sort_keys=False`` keeps the field order from the template so
        # diffs against the source remain readable for the user.
        rendered = yaml.safe_dump(parsed, sort_keys=False, indent=2)
    else:
        parsed = json.loads(content)
        rendered = json.dumps(parsed, indent=4)
    return rendered, parsed


# Backwards-compatibility alias for v0.0.10 customers.
#
# v0.0.10 published the entry-class as ``DLTMeta``; v0.1.0 renamed it to
# ``SDPMeta``. The ``compat/dlt_meta`` shim's ``src.*`` import alias maps
# ``src.cli`` to this module, so ``from src.cli import DLTMeta`` resolves
# only if ``DLTMeta`` is bound here (the shim aliases module objects, not
# individual symbols). Mirrors the ``DLT_META_RUNNER_NOTEBOOK`` rebind
# at the top of this file. Will be removed in v0.2.0 alongside the rest
# of the ``src.*`` shim.
DLTMeta = SDPMeta


def onboard(sdp_meta: SDPMeta, flags: dict = None):
    logger.info("Please answer a couple of questions to for launching SDP META onboarding job")
    flags = flags or {}
    # The `databricks labs` CLI registers every declared flag as a pflag
    # *string* flag (no boolean type). When users invoke a boolean-style
    # flag like `--build-and-upload-whl --profile profile_name`, pflag eats the
    # next token (`--profile`) as the value, so we receive
    # flags["build-and-upload-whl"] == "--profile". We detect that here,
    # treat it as truthy presence, and tell the user the canonical syntax.
    cmd = sdp_meta._load_onboard_config()
    whl_file_path = _flag_value(flags, "whl-file-path", "whl_file_path")
    build_raw = _flag_value(flags, "build-and-upload-whl", "build_and_upload_whl")
    build_and_upload = _is_truthy_flag(build_raw)
    if build_and_upload and isinstance(build_raw, str) and build_raw.startswith("-"):
        print(
            "Warning: --build-and-upload-whl appears to have consumed the next CLI token "
            f"({build_raw!r}) as its value, because the `databricks labs` CLI treats every "
            "flag as a string flag. Use the '=' syntax to avoid surprises, e.g.:\n"
            "  databricks labs sdp-meta onboard --build-and-upload-whl=true --profile=<name>"
        )
    if whl_file_path and build_and_upload:
        raise ValueError("--whl-file-path and --build-and-upload-whl are mutually exclusive")
    if whl_file_path:
        cmd.sdp_meta_dependency = whl_file_path
    elif build_and_upload:
        if not cmd.uc_enabled:
            raise ValueError("--build-and-upload-whl requires onboarding with unity catalog enabled")
        cmd.sdp_meta_dependency = _build_and_upload_onboard_wheel(sdp_meta, cmd, flags)
    # Only act on a real, non-empty string dependency. The isinstance() guard
    # keeps us safe in unit tests where ``cmd`` is a MagicMock (its attributes
    # auto-create truthy MagicMock values that aren't JSON-serializable).
    if isinstance(cmd.sdp_meta_dependency, str) and cmd.sdp_meta_dependency:
        print(f"Using sdp-meta dependency for onboarding job: {cmd.sdp_meta_dependency}")
        # Persist the resolved dependency back into onboarding_job_details.json
        # so a follow-up `databricks labs sdp-meta deploy` (run from the same
        # working directory) auto-discovers the wheel and bakes it into the
        # SDP runner notebook's `%pip install`. Without this, deploy would
        # silently fall back to `databricks-labs-sdp-meta==<version>` on PyPI
        # — exactly the failure mode that motivated this whole feature.
        _persist_dependency_to_onboarding_json(cmd.sdp_meta_dependency)
    sdp_meta.onboard(cmd)


def onboard_ui(sdp_meta: SDPMeta, form_data):
    logger.info("Please answer a couple of questions to for launching SDP META onboarding job")
    cmd = sdp_meta._load_onboard_config_ui(form_data)
    sdp_meta.onboard(cmd)


def deploy(sdp_meta: SDPMeta, flags: dict = None):
    logger.info("Please answer a couple of questions to for launching SDP META deployment job")
    flags = flags or {}
    cmd = sdp_meta._load_deploy_config()
    # Resolution order for the SDP runner notebook's `%pip install` target:
    #   1. --whl-file-path=...                         (explicit override)
    #   2. --build-and-upload-whl=true                  (build+upload now)
    #   3. sdp_meta_dependency from onboarding_job_details.json
    #      (auto-set by `onboard --build-and-upload-whl=true`)
    #   4. databricks-labs-sdp-meta==<self.version>     (PyPI default)
    whl_file_path = _flag_value(flags, "whl-file-path", "whl_file_path")
    build_raw = _flag_value(flags, "build-and-upload-whl", "build_and_upload_whl")
    build_and_upload = _is_truthy_flag(build_raw)
    if build_and_upload and isinstance(build_raw, str) and build_raw.startswith("-"):
        print(
            "Warning: --build-and-upload-whl appears to have consumed the next CLI token "
            f"({build_raw!r}) as its value, because the `databricks labs` CLI treats every "
            "flag as a string flag. Use the '=' syntax to avoid surprises, e.g.:\n"
            "  databricks labs sdp-meta deploy --build-and-upload-whl=true --profile=<name>"
        )
    if whl_file_path and build_and_upload:
        raise ValueError("--whl-file-path and --build-and-upload-whl are mutually exclusive")
    if whl_file_path:
        cmd.sdp_meta_dependency = whl_file_path
    elif build_and_upload:
        cmd.sdp_meta_dependency = _build_and_upload_deploy_wheel(sdp_meta, cmd, flags)
    elif not (isinstance(cmd.sdp_meta_dependency, str) and cmd.sdp_meta_dependency):
        # Auto-pickup from onboarding_job_details.json (written by onboard()
        # when it built/uploaded a wheel). Falls through silently if missing.
        # The isinstance() check keeps unit tests with MagicMock cmd objects
        # from accidentally short-circuiting the auto-pickup path.
        cmd.sdp_meta_dependency = _read_dependency_from_onboarding_json()
    if isinstance(cmd.sdp_meta_dependency, str) and cmd.sdp_meta_dependency:
        print(
            "Using sdp-meta dependency for SDP runner notebook "
            f"%pip install: {cmd.sdp_meta_dependency}"
        )
    sdp_meta.deploy(cmd)


def deploy_ui(sdp_meta: SDPMeta, form_data):
    logger.info("Please answer a couple of questions to for launching SDP META deployment job")
    cmd = sdp_meta._load_deploy_config_ui(form_data)
    sdp_meta.deploy(cmd)


def _persist_dependency_to_onboarding_json(dependency: str) -> None:
    """Best-effort write of ``sdp_meta_dependency`` into the local
    ``onboarding_job_details.json`` file so the subsequent ``deploy`` command
    inherits it. Silently ignored if the file is missing or unreadable —
    this is a convenience hook, not a hard contract.
    """
    path = "onboarding_job_details.json"
    if not os.path.isfile(path):
        return
    try:
        with open(path) as fh:
            data = json.load(fh)
        if not isinstance(data, dict):
            return
        if data.get("sdp_meta_dependency") == dependency:
            return
        data["sdp_meta_dependency"] = dependency
        with open(path, "w") as fh:
            json.dump(data, fh, indent=4)
    except (OSError, ValueError) as exc:
        logger.warning("Unable to update %s with sdp_meta_dependency: %s", path, exc)


def _read_dependency_from_onboarding_json() -> str:
    """Return ``sdp_meta_dependency`` from local ``onboarding_job_details.json``
    if present and non-empty, else ``None``."""
    path = "onboarding_job_details.json"
    if not os.path.isfile(path):
        return None
    try:
        with open(path) as fh:
            data = json.load(fh)
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    dep = data.get("sdp_meta_dependency")
    if isinstance(dep, str) and dep.strip():
        return dep
    return None


def _build_and_upload_wheel(sdp_meta: SDPMeta, *, uc_catalog: str,
                            default_schema: str, default_volume: str = None,
                            flags: dict) -> str:
    """Shared wheel-build / UC-volume-upload helper for `onboard` and `deploy`.

    Resolves ``uc_schema`` / ``uc_volume`` from CLI flags (with the demo
    launcher's hyphenated *and* underscored aliases) falling back to the
    caller-provided defaults. When ``--git-url`` / ``--git-branch`` is set
    the wheel is built from that Git source instead of the local checkout.
    """
    from databricks.labs.sdp_meta.bundle import (
        BundlePrepareWheelCommand,
        bundle_prepare_wheel as _run,
    )
    uc_schema = (
        _flag_value(flags, "uc-schema", "uc-schema-name", "uc_schema_name")
        or default_schema
    )
    uc_volume = (
        _flag_value(flags, "uc-volume", "uc-volume-name", "uc_volume_name")
        or default_volume
        or uc_schema
    )
    git_source = _git_wheel_source(flags)
    if git_source:
        return _build_and_upload_git_wheel(
            sdp_meta._ws,
            uc_catalog=uc_catalog,
            uc_schema=uc_schema,
            uc_volume=uc_volume,
            source=git_source,
            flags=flags,
        )
    return _run(BundlePrepareWheelCommand(
        uc_catalog=uc_catalog,
        uc_schema=uc_schema,
        uc_volume=uc_volume,
        profile=_flag_value(flags, "profile"),
        pip_index_url=_flag_value(flags, "pip-index-url", "pip_index_url") or None,
        pip_extra_index_urls=_split_flag_values(
            _flag_value(flags, "pip-extra-index-url", "pip_extra_index_url")
        ),
        create_if_missing=not _has_flag(flags, "no-create-missing-uc", "no_create_missing_uc"),
    ))


def _build_and_upload_onboard_wheel(sdp_meta: SDPMeta, cmd: OnboardCommand, flags: dict) -> str:
    """Build/upload a local wheel for regular `onboard` local-dev testing."""
    return _build_and_upload_wheel(
        sdp_meta,
        uc_catalog=cmd.uc_catalog_name,
        default_schema=cmd.sdp_meta_schema,
        flags=flags,
    )


def _build_and_upload_deploy_wheel(sdp_meta: SDPMeta, cmd: DeployCommand, flags: dict) -> str:
    """Build/upload a local wheel for `deploy` so the SDP runner notebook can
    ``%pip install`` it instead of pulling from PyPI. Uses the deploy's UC
    catalog and a sensible default schema (bronze > silver > target schema).
    """
    if not cmd.uc_catalog_name:
        raise ValueError(
            "--build-and-upload-whl requires deployment with unity catalog enabled"
        )
    default_schema = (
        cmd.sdp_meta_bronze_schema
        or cmd.sdp_meta_silver_schema
        or cmd.dlt_target_schema
    )
    if not default_schema:
        raise ValueError(
            "Cannot infer a UC schema for the wheel volume; pass --uc-schema=<name> "
            "or run `onboard --build-and-upload-whl=true ...` first so deploy can "
            "pick the wheel up from onboarding_job_details.json."
        )
    return _build_and_upload_wheel(
        sdp_meta,
        uc_catalog=cmd.uc_catalog_name,
        default_schema=default_schema,
        flags=flags,
    )


def _git_wheel_source(flags: dict) -> str:
    git_url = _flag_value(flags, "git-url", "git_url")
    git_branch = _flag_value(flags, "git-branch", "git_branch")
    if not git_url and not git_branch:
        return None
    if not git_url:
        git_url = "https://github.com/databrickslabs/sdp-meta.git"
    source = git_url if git_url.startswith("git+") else f"git+{git_url}"
    if git_branch:
        source = f"{source}@{git_branch}"
    return source


def _build_and_upload_git_wheel(ws: WorkspaceClient, *, uc_catalog: str,
                                uc_schema: str, uc_volume: str,
                                source: str, flags: dict) -> str:
    """Build a wheel from ``source`` (local path or git URL) and upload it to a UC volume.

    .. warning::
        Building from an arbitrary git URL or local path causes ``pip`` to
        execute that project's build backend (``pyproject.toml`` build hooks,
        ``setup.py``, etc.). Treat ``--git-url <url>`` and local-path sources
        as equivalent to running their build code: only point this at git
        repositories and paths you trust. The downstream UC volume upload
        also overwrites any wheel at the destination path (see the warning
        emitted below).
    """
    _ensure_uc_schema_and_volume(
        ws, uc_catalog, uc_schema, uc_volume,
        create_if_missing=not _has_flag(flags, "no-create-missing-uc", "no_create_missing_uc"),
    )
    with tempfile.TemporaryDirectory() as tmp_dir:
        pip_cmd = [
            sys.executable, "-m", "pip", "wheel", "--no-deps",
            "--wheel-dir", tmp_dir,
        ]
        pip_index_url = _flag_value(flags, "pip-index-url", "pip_index_url")
        if pip_index_url:
            pip_cmd.extend(["--index-url", pip_index_url])
        for extra in _split_flag_values(
            _flag_value(flags, "pip-extra-index-url", "pip_extra_index_url")
        ) or []:
            pip_cmd.extend(["--extra-index-url", extra])
        pip_cmd.append(source)
        subprocess.run(pip_cmd, check=True)
        wheels = sorted(Path(tmp_dir).glob("*.whl"))
        if not wheels:
            raise RuntimeError(f"pip wheel produced no wheel for {source!r}")
        wheel = wheels[-1]
        volume_path = f"/Volumes/{uc_catalog}/{uc_schema}/{uc_volume}/{wheel.name}"
        existed = _volume_path_exists(ws, volume_path)
        with wheel.open("rb") as fh:
            ws.files.upload(file_path=volume_path, contents=fh, overwrite=True)
        if existed:
            print(
                f"\n⚠️  Overwriting existing wheel at {volume_path}. "
                "Any in-flight pipeline pinned to this exact path will pick up "
                "the new build on its next run.\n"
            )
        print(f"\nUploaded wheel to:\n  {volume_path}\n")
        return volume_path


def _volume_path_exists(ws: WorkspaceClient, volume_path: str) -> bool:
    """Best-effort probe for whether a UC volume file exists.

    Returns ``False`` on any error (NotFound, transient SDK error, etc.) so
    overwrite warnings never block the upload — the warning is purely
    informational.
    """
    try:
        ws.files.get_metadata(file_path=volume_path)
        return True
    except NotFound:
        return False
    except DatabricksError:
        return False


def _ensure_uc_schema_and_volume(ws: WorkspaceClient, uc_catalog: str,
                                 uc_schema: str, uc_volume: str,
                                 *, create_if_missing: bool) -> None:
    try:
        SchemasAPI(ws.api_client).get(full_name=f"{uc_catalog}.{uc_schema}")
    except NotFound:
        if not create_if_missing:
            raise
        SchemasAPI(ws.api_client).create(
            catalog_name=uc_catalog,
            name=uc_schema,
            comment="sdp_meta wheel schema",
        )
    try:
        ws.volumes.read(name=f"{uc_catalog}.{uc_schema}.{uc_volume}")
    except NotFound:
        if not create_if_missing:
            raise
        ws.volumes.create(
            catalog_name=uc_catalog,
            schema_name=uc_schema,
            name=uc_volume,
            volume_type=VolumeType.MANAGED,
        )


def _is_truthy_flag(value) -> bool:
    """Return True when a labs-CLI flag should be treated as "set".

    The `databricks labs` CLI exposes every declared flag as a pflag *string*
    flag, so there is no native boolean type and an unsupplied flag arrives as
    ``""`` (the registered default). We therefore treat the *empty* / *None*
    case as false, the explicit falsy keywords as false, and any other
    non-empty value as truthy presence. That keeps the canonical
    ``--flag=true`` invocation working *and* recovers the common spillover
    case where pflag eats the next CLI token as the value (e.g. value ends up
    being ``"--profile"`` because the user typed
    ``--build-and-upload-whl --profile profile_name``).
    """
    if value is None or value is False:
        return False
    sv = str(value).strip()
    if sv == "":
        return False
    if sv.lower() in ("0", "false", "no", "off"):
        return False
    return True


# ---------------------------------------------------------------------------
# DAB (Declarative Automation Bundle) command wrappers
#
# Mirror the onboard/deploy pattern: each wrapper takes the shared SDPMeta
# instance, uses `sdp_meta._wsi` (a WorkspaceInstaller, set up in the SDPMeta
# constructor) as the interactive prompter, and delegates to the matching
# pure-function handler in `bundle.py`. The handlers themselves don't need
# a WorkspaceClient -- they shell out to the `databricks` CLI -- but routing
# them through SDPMeta keeps the dispatcher's signature uniform with the
# legacy commands.
#
# These four entries MUST stay in lock-step with `labs.yml commands:` and
# the `MAPPING` dict below. The `tests/test_cli.py::CliCommandWiringTests`
# regression test enforces both.
# ---------------------------------------------------------------------------


def bundle_init(sdp_meta: SDPMeta, flags: dict = None):
    """Scaffold a new sdp-meta DAB.

    With ``--quickstart`` (declared in labs.yml), all 13 template prompts are
    pre-answered with developer-friendly defaults via a generated
    ``--config-file`` (see ``QUICKSTART_BUNDLE_INIT_DEFAULTS`` in bundle.py).
    The user still has to fix ``sdp_meta_dependency`` afterwards, but the
    happy path is ``bundle-init --quickstart`` -> edit one file ->
    ``bundle-validate``. ``--output-dir`` is honored by both modes.
    Without ``--quickstart``, falls back to the interactive prompts.
    """
    from databricks.labs.sdp_meta.bundle import (
        BundleInitCommand,
        _load_bundle_init_config,
        bundle_init as _run,
        write_quickstart_config_file,
    )

    flags = flags or {}
    # The labs (cobra) CLI declares every flag as a *string* flag, so a bare
    # `--quickstart` greedily consumes the following CLI token as its value:
    # `--quickstart --output-dir=./x` parses as quickstart="--output-dir=./x"
    # with output-dir never set (the bundle then lands in the default dir).
    # Recover the swallowed `--key=value` and treat quickstart as the on-switch
    # the user obviously intended. `--quickstart=true` still works unchanged.
    _recover_swallowed_flag(flags, "quickstart")
    quickstart = _coerce_bool(flags.get("quickstart"))
    output_dir = flags.get("output-dir") or flags.get("output_dir") or "."

    if quickstart:
        logger.info(
            "Scaffolding a new sdp-meta DAB in --quickstart mode "
            "(no prompts; developer defaults)."
        )
        # Stash the generated config alongside the rendered bundle so users
        # have a record of which defaults they got. tempfile would also work
        # but leaving it next to the bundle aids debugging.
        cfg_dir = Path(output_dir).resolve()
        cfg_dir.mkdir(parents=True, exist_ok=True)
        cfg_path = write_quickstart_config_file(cfg_dir)
        cmd = BundleInitCommand(output_dir=output_dir, config_file=str(cfg_path))
    else:
        logger.info("Scaffolding a new sdp-meta DAB from the packaged template.")
        cmd = _load_bundle_init_config(sdp_meta._wsi)
        # CLI --output-dir wins over an interactive answer when both are given,
        # since the user explicitly typed it on the command line.
        if flags.get("output-dir") or flags.get("output_dir"):
            cmd.output_dir = output_dir
    _run(cmd)


def bundle_prepare_wheel(sdp_meta: SDPMeta, flags: dict = None):
    # `flags` is accepted for dispatcher uniformity (see main()) but unused.
    del flags
    logger.info("Building the sdp-meta wheel and uploading it to a UC volume.")
    from databricks.labs.sdp_meta.bundle import (
        _load_bundle_prepare_wheel_config,
        bundle_prepare_wheel as _run,
    )
    _run(_load_bundle_prepare_wheel_config(sdp_meta._wsi))


def _flag_value(flags: dict, *names: str):
    """Return the first non-empty value for any of the given flag spellings."""
    for name in names:
        value = flags.get(name)
        if value not in (None, ""):
            return value
    return None


def _has_flag(flags: dict, *names: str) -> bool:
    """Return True only if the flag was supplied with a *truthy* value.

    The `databricks labs` CLI registers every declared flag in the JSON
    payload with its registered default (an empty string), so plain key
    presence (`name in flags`) is *always* True for a declared flag and
    cannot be used to detect whether the user actually passed it. We
    therefore route through ``_is_truthy_flag``, which accepts the canonical
    ``--flag=true`` form, treats explicit falsy keywords as false, and
    recovers the pflag spillover case (value starts with ``-``).
    """
    return any(_is_truthy_flag(flags.get(name)) for name in names)


def _split_flag_values(value):
    if value in (None, ""):
        return None
    if isinstance(value, list):
        return [str(v) for v in value if str(v)]
    return [v for v in str(value).split() if v]


def bundle_validate(sdp_meta: SDPMeta, flags: dict = None):
    del flags
    logger.info("Validating the sdp-meta DAB (databricks bundle validate + sanity checks).")
    from databricks.labs.sdp_meta.bundle import (
        _load_bundle_validate_config,
        bundle_validate as _run,
    )
    rc = _run(_load_bundle_validate_config(sdp_meta._wsi))
    if rc != 0:
        sys.exit(rc)


def bundle_add_flow(sdp_meta: SDPMeta, flags: dict = None):
    del flags
    logger.info("Appending flow entries to the bundle's onboarding file.")
    from databricks.labs.sdp_meta.bundle import (
        _load_bundle_add_flow_config,
        bundle_add_flow as _run,
    )
    rc = _run(_load_bundle_add_flow_config(sdp_meta._wsi))
    if rc != 0:
        sys.exit(rc)


def mcp(sdp_meta: SDPMeta, flags: dict = None):
    """Run the sdp-meta MCP server over stdio.

    Requires the `mcp` extra. The server exposes a curated subset of
    sdp-meta operations as MCP tools so an MCP client (Claude Code,
    Cursor, Claude Desktop) can drive sdp-meta workflows.
    """
    del flags

    try:
        from databricks.labs.sdp_meta.mcp_server import run_stdio
    except ImportError as exc:
        import importlib.util

        sdk_present = importlib.util.find_spec("mcp") is not None
        if not sdk_present:
            raise ImportError(
                "The `mcp` extra is not installed. Install it with: "
                "pip install 'databricks-labs-sdp-meta[mcp]'"
            ) from exc
        raise ImportError(
            "Failed to import the SDP-META MCP server even though the "
            f"`mcp` SDK is installed. Original error: {exc}"
        ) from exc
    run_stdio(sdp_meta)


MAPPING = {
    "onboard": onboard,
    "deploy": deploy,
    "onboard_ui": onboard_ui,
    "deploy_ui": deploy_ui,
    "bundle-init": bundle_init,
    "bundle-prepare-wheel": bundle_prepare_wheel,
    "bundle-validate": bundle_validate,
    "bundle-add-flow": bundle_add_flow,
    "mcp": mcp,
}


# Backwards compatibility alias for class name (deprecated)
DLTMeta = SDPMeta


def main(raw):
    payload = json.loads(raw)
    command = payload["command"]

    if command not in MAPPING:
        msg = f"cannot find command: {command}. Available: {list(MAPPING.keys())}"
        raise KeyError(msg)
    flags = payload["flags"]
    log_level = flags.pop("log_level")
    if log_level != "disabled":
        databricks_logger = logging.getLogger("databricks")
        databricks_logger.setLevel(log_level.upper())
    version = __about__.__version__
    # Support both old and new product names
    ws_kwargs = {"product": "sdp-meta", "product_version": version}
    if flags.get("profile"):
        ws_kwargs["profile"] = flags["profile"]
    ws = WorkspaceClient(**ws_kwargs)
    sdp_meta = SDPMeta(ws)
    if command in ["onboard_ui", "deploy_ui"]:
        MAPPING[command](sdp_meta, payload)
    elif command in ("onboard", "deploy") or command.startswith("bundle-"):
        # Flag-aware wrappers receive `flags` so they can opt into
        # non-interactive behavior (e.g. `bundle-init --quickstart`, or
        # `onboard --build-and-upload-whl`, or `deploy --whl-file-path=...`).
        # Wrappers that ignore flags accept them as a `flags=None` keyword
        # arg, which keeps the wrapper callable in tests without constructing
        # a fake payload.
        MAPPING[command](sdp_meta, flags=flags)
    else:
        MAPPING[command](sdp_meta)


if __name__ == "__main__":
    main(*sys.argv[1:])
