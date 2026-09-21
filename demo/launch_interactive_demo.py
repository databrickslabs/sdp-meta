"""Launch the SDP-META interactive demo as a one-time job in a Databricks workspace.

Mirrors the pattern in ``integration_tests/run_integration_tests.py``:
authenticates against a workspace, uploads the demo notebook to a per-run
folder, submits a one-time **serverless** job with the demo notebook as a
single task, and blocks on completion.

Use this for:
  * CI smoke runs — pass ``--validate-counts true`` (the default) so the
    demo's final-validation cell asserts row counts and fails the job on
    any regression (broken append flow, broken sink, broken quarantine
    routing, etc.).
  * Headless demos — let an SA kick the demo against a fresh workspace
    without opening the notebook by hand.

The demo notebook handles everything else internally: UC schema/volume
creation, data generation/download, dataflowspec onboarding, and pipeline
creation (all 3 pipelines run on serverless). This launcher just wires up
the widgets and watches the run.

Three ways to install sdp-meta in the demo job
-----------------------------------------------

1. ``--install-source git_branch`` (default) -- the demo runs
   ``pip install git+https://github.com/databrickslabs/sdp-meta.git@<branch>``
   inside the job. Fastest path; matches the interactive demo experience.
2. ``--install-source whl_file --whl-file-path <UC volume path>`` -- you
   already uploaded a wheel; the demo just installs from it. Use this
   when the wheel is published by another job (e.g. release pipeline).
3. ``--build-and-upload-whl --uc-schema-name <s> --uc-volume-name <v>``
   -- the launcher builds the wheel from the local source tree, creates
   the UC schema/volume if needed, uploads the wheel, and points the
   demo at the resulting ``/Volumes/...`` path. Same machinery as
   ``databricks labs sdp-meta bundle prepare-wheel``.

Usage examples
--------------
::

    # Smoke run against a fresh per-run schema, default git branch.
    python demo/launch_interactive_demo.py \\
        --uc-catalog-name main \\
        --profile DEFAULT

    # Build the wheel locally, upload it to a UC volume, then run the
    # demo against it. One command, no manual `bundle prepare-wheel`.
    python demo/launch_interactive_demo.py \\
        --uc-catalog-name main \\
        --profile DEFAULT \\
        --build-and-upload-whl \\
        --uc-schema-name sdp_meta_demo \\
        --uc-volume-name sdp_meta_wheels

    # Validate a wheel that's ALREADY on a UC volume.
    python demo/launch_interactive_demo.py \\
        --uc-catalog-name main \\
        --profile DEFAULT \\
        --install-source whl_file \\
        --whl-file-path /Volumes/main/sdp_meta_demo/sdp_meta_wheels/\\
    databricks_labs_sdp_meta-0.1.1-py3-none-any.whl

    # Deterministic data path (recommended for CI):
    python demo/launch_interactive_demo.py \\
        --uc-catalog-name main \\
        --profile DEFAULT \\
        --data-source github \\
        --git-branch main
"""

from __future__ import annotations

import argparse
import sys
import uuid
import webbrowser
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Importing from src after path patching so the script runs without an
# editable install (mirrors integration_tests/run_integration_tests.py).
REPO_ROOT = Path(__file__).resolve().parents[1]
SRC = REPO_ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from databricks.sdk import WorkspaceClient  # noqa: E402
from databricks.sdk.service import jobs  # noqa: E402
from databricks.sdk.service.compute import Environment  # noqa: E402
from databricks.sdk.service.workspace import (  # noqa: E402
    ImportFormat,
    Language,
)

from databricks.labs.sdp_meta.bundle import (  # noqa: E402
    BundlePrepareWheelCommand,
    bundle_prepare_wheel,
)
from databricks.labs.sdp_meta.identifiers import (  # noqa: E402
    validate_uc_identifier,
)

DEMO_NOTEBOOK_SRC: Path = REPO_ROOT / "demo" / "SDP_META_INTERACTIVE_DEMO.py"
DEMO_CONF_DIR: Path = REPO_ROOT / "demo" / "conf"

# Files under ``demo/conf/`` excluded from the recursive co-upload.
# Hidden files (``.DS_Store``, ``.gitkeep``) and editor backups don't
# belong in the workspace; everything else under ``demo/conf/`` is
# fair game because the demo notebook (or a future contributor adding
# a new conf reference) might end up reading any of it via the
# ``open(/Workspace/.../demo/conf/...)`` path. Walking the tree --
# rather than maintaining an explicit allow-list -- means new conf
# files added to the repo automatically work in headless launcher
# runs without a follow-up commit here.
_DEMO_CONF_EXCLUDE_NAMES: frozenset[str] = frozenset({".DS_Store", ".gitkeep"})
_DEMO_CONF_EXCLUDE_SUFFIXES: tuple[str, ...] = (".pyc", ".swp", "~")


def _upload_demo_notebook(ws: WorkspaceClient, target_path: str) -> None:
    """Upload the demo notebook to ``target_path`` in the workspace.

    The file already starts with ``# Databricks notebook source`` so the
    workspace renders it as a notebook (not a plain .py file). Uses
    ``ImportFormat.SOURCE`` + ``Language.PYTHON`` to match the convention
    in ``integration_tests/run_integration_tests.py``'s
    ``upload_files_to_databricks``.
    """
    if not DEMO_NOTEBOOK_SRC.is_file():
        raise SystemExit(
            f"Demo notebook source not found at {DEMO_NOTEBOOK_SRC}. "
            "Did you delete demo/SDP_META_INTERACTIVE_DEMO.py?"
        )
    target_dir = "/".join(target_path.rstrip("/").split("/")[:-1])
    ws.workspace.mkdirs(target_dir)
    print(f"Uploading {DEMO_NOTEBOOK_SRC.name} to {target_path} ...")
    ws.workspace.upload(
        path=target_path,
        content=DEMO_NOTEBOOK_SRC.read_bytes(),
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        overwrite=True,
    )


def _upload_demo_conf(ws: WorkspaceClient, repo_root_ws: str) -> None:
    """Recursively co-upload every file under ``demo/conf/`` to the workspace.

    The demo notebook resolves conf files via:

        f"{repo_root_ws}/demo/conf/{onboarding_format}/sample_onboarding."
        f"{conf_ext}"

    where ``repo_root_ws`` is whatever is to the left of ``/demo/`` in
    the notebook's workspace path. By uploading the entire ``demo/conf/``
    subtree the launcher keeps the demo offline-friendly: no GitHub
    fallback is needed, even when the local branch isn't pushed (issue
    surfaced when running ``feature/sdp-meta`` locally — main doesn't
    yet have these files).

    We deliberately walk the tree rather than carry an allow-list: today
    the notebook only reads ``sample_onboarding.{json,yml}``, but the
    moment a contributor adds a new ``open(...demo/conf/<x>...)`` call
    in the notebook, the launcher would silently fall through to the
    GitHub raw-URL path (which fails for un-pushed branches and produces
    the confusing "HTTP 404" error we already debugged once). Uploading
    the whole subtree makes new conf files Just Work in headless runs.

    Files are uploaded as workspace files (``ImportFormat.RAW``), not
    notebooks, so the demo's ``open()`` call against ``/Workspace/...``
    reads back the exact bytes we wrote. Hidden files / editor backups
    are skipped via :data:`_DEMO_CONF_EXCLUDE_NAMES` /
    :data:`_DEMO_CONF_EXCLUDE_SUFFIXES`.
    """
    if not DEMO_CONF_DIR.is_dir():
        print(f"(no demo/conf dir at {DEMO_CONF_DIR}, skipping conf upload)")
        return

    uploaded = 0
    for src in sorted(DEMO_CONF_DIR.rglob("*")):
        if not src.is_file():
            continue
        if src.name in _DEMO_CONF_EXCLUDE_NAMES:
            continue
        if src.name.endswith(_DEMO_CONF_EXCLUDE_SUFFIXES):
            continue
        rel = src.relative_to(DEMO_CONF_DIR).as_posix()
        target = f"{repo_root_ws}/demo/conf/{rel}"
        target_dir = "/".join(target.rstrip("/").split("/")[:-1])
        ws.workspace.mkdirs(target_dir)
        print(f"Uploading {src.relative_to(REPO_ROOT)} to {target} ...")
        ws.workspace.upload(
            path=target,
            content=src.read_bytes(),
            format=ImportFormat.RAW,
            overwrite=True,
        )
        uploaded += 1
    print(f"Co-uploaded {uploaded} conf file(s) under {repo_root_ws}/demo/conf/")


def _submit_demo_job(
    ws: WorkspaceClient,
    *,
    run_id: str,
    run_name: str,
    target_notebook_path: str,
    base_parameters: dict[str, str],
    timeout_min: int,
):
    """Submit a one-time serverless job that runs the demo notebook.

    Uses the same ``Environment(client="2")`` serverless pattern as
    ``demo/launch_dab_template_demo.py::_run_delta_seed_notebook`` — the
    user doesn't have to plumb a cluster id in for the launcher to work.
    The demo creates its own pipelines internally (also on serverless),
    so the only compute this launcher provisions is the driver task.

    ``run_name`` is the user-facing label that shows up in the workspace
    Jobs > Job Runs UI; the caller composes it from launch context
    (timestamp + catalog + run_id) so concurrent runs don't visually
    collide in the run history.
    """
    print(
        f"Submitting one-time serverless job '{run_name}' "
        f"(run_id={run_id}) ..."
    )
    waiter = ws.jobs.submit(
        run_name=run_name,
        tasks=[
            jobs.SubmitTask(
                task_key="run_demo",
                notebook_task=jobs.NotebookTask(
                    notebook_path=target_notebook_path,
                    base_parameters=base_parameters,
                ),
                environment_key="demo_env",
            ),
        ],
        environments=[
            jobs.JobEnvironment(
                environment_key="demo_env",
                spec=Environment(client="2"),
            ),
        ],
    )

    # Resolve + open the run page URL in the browser BEFORE blocking on
    # completion -- mirrors `integration_tests/run_integration_tests.py
    # ::launch_workflow` so the user can watch the notebook run live
    # instead of staring at the terminal. The Wait[Run] returned by
    # `submit()` exposes the run_id immediately (without blocking) via
    # an internal attribute; we look it up defensively across SDK
    # versions and silently fall through if the SDK changes shape.
    submitted_run_id = (
        getattr(waiter, "run_id", None)
        or getattr(getattr(waiter, "response", None), "run_id", None)
    )
    if submitted_run_id is not None:
        try:
            meta = ws.jobs.get_run(run_id=submitted_run_id)
            page_url = getattr(meta, "run_page_url", None)
            job_id = getattr(meta, "job_id", None)

            # Emit a "Job created successfully. job_id=N, url=<path-routed>"
            # line so the databricks_app `/rundemo` response handler
            # (extract_command_output) lights up the same "Open in
            # Databricks ↗" success modal it does for every other demo.
            # The SDK's ``run_page_url`` returns a *hash-routed* legacy URL
            # (``<host>/?o=ID#job/N/run/M``) on serverless-stable shards,
            # which the workspace UI silently redirects to the modern
            # path-routed shape — but the App's URL filter only sees
            # ``/jobs/``/``/pipelines/`` substrings, so the legacy URL
            # gets dropped. We construct the path-routed URL ourselves
            # so both the script-mode and App-mode UX surface the same
            # link.
            if job_id is not None:
                try:
                    workspace_id = ws.get_workspace_id()
                except Exception:
                    workspace_id = None
                host = ws.config.host.rstrip("/")
                if submitted_run_id is not None:
                    path_url = f"{host}/jobs/{job_id}/runs/{submitted_run_id}"
                else:
                    path_url = f"{host}/jobs/{job_id}"
                if workspace_id is not None:
                    path_url = f"{path_url}?o={workspace_id}"
                print(
                    f"Job created successfully. job_id={job_id}, "
                    f"run_id={submitted_run_id}, url={path_url}"
                )

            if page_url:
                # Keep the original `Run page:` line for users who script
                # against the current stdout shape; it's also the URL the
                # browser auto-open below uses.
                print(f"Run page: {page_url}")
                # Open in the default browser. If we're headless / on CI,
                # `webbrowser.open` may print a warning but won't raise --
                # still tolerate that defensively so the launcher's main
                # job (waiting for the run) is never blocked by the open.
                try:
                    webbrowser.open(page_url)
                except Exception as exc:
                    print(f"(could not auto-open browser: {exc})")
        except Exception as exc:
            print(f"(could not fetch run_page_url early: {exc})")

    # Block on completion. We pick the timeout long enough to cover the
    # slowest stage (4 pipeline runs on a cold workspace) but short
    # enough that a hung run fails CI in reasonable time.
    #
    # When the run FAILS, ``waiter.result`` raises ``OperationFailed`` and
    # we'd otherwise unwind without printing the run URL. Fall back to
    # ``ws.jobs.get_run`` so main() can still report state + URL (which
    # we already opened in the browser above) and exit non-zero cleanly.
    print(
        f"Waiting up to {timeout_min} minute(s) for job to complete ..."
    )
    try:
        return waiter.result(timeout=timedelta(minutes=timeout_min))
    except Exception as exc:
        if submitted_run_id is None:
            raise
        print(f"(run finished with non-success state: {exc})")
        return ws.jobs.get_run(run_id=submitted_run_id)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__.split("\n", maxsplit=1)[0]
    )
    parser.add_argument(
        "--uc-catalog-name",
        required=True,
        help="Unity Catalog catalog the demo writes into. Substituted "
             "into the demo's uc_catalog_name widget. Must be a regular "
             "SQL identifier (issue #261).",
    )
    parser.add_argument(
        "--uc-schema-name",
        default=None,
        help="UC schema name passed to the demo's uc_schema_name widget. "
             "Defaults to ``sdp_meta_demo_<run_id>`` so concurrent runs "
             "and CI runs don't collide on bronze/silver tables. Pass "
             "this explicitly only if you want to reuse a previous run's "
             "schema (rare).",
    )
    parser.add_argument(
        "--profile",
        default=None,
        help="Databricks CLI profile name. When omitted, falls back to "
             "the SDK's default (env vars / DEFAULT profile).",
    )
    parser.add_argument(
        "--git-branch",
        default="main",
        help="Branch passed to the demo's git_branch widget. Drives BOTH "
             "(a) ``pip install git+https://github.com/databrickslabs/"
             "sdp-meta.git@<branch>`` when --install-source=git_branch, "
             "AND (b) where the demo fetches sample CSVs and onboarding "
             "templates from raw.githubusercontent.com (always — even "
             "when installing from a wheel).",
    )
    parser.add_argument(
        "--install-source",
        default="git_branch",
        choices=["git_branch", "pypi", "whl_file"],
        help="Where the demo installs sdp-meta from. Use 'whl_file' to "
             "validate a local build before merging.",
    )
    parser.add_argument(
        "--whl-file-path",
        default="",
        help="Wheel file path on a UC volume / Workspace. Required when "
             "--install-source=whl_file. Example: ``/Volumes/<cat>/"
             "<sch>/<vol>/sdp_meta-<version>-py3-none-any.whl``.",
    )
    parser.add_argument(
        "--pypi-version",
        default="",
        help="Optional version pin when --install-source=pypi (e.g. "
             "``0.1.1``). Leave blank to install the latest published "
             "``databricks-labs-sdp-meta`` release.",
    )
    parser.add_argument(
        "--build-and-upload-whl",
        action="store_true",
        help="Build the sdp-meta wheel from the local source tree, "
             "auto-create the UC schema/volume if needed, upload the "
             "wheel, and point the demo at the resulting /Volumes/... "
             "path. Implies --install-source=whl_file. Requires "
             "--uc-schema-name and --uc-volume-name. Reuses the same "
             "machinery as `databricks labs sdp-meta bundle "
             "prepare-wheel`, so a private pypi mirror is honored via "
             "$PIP_INDEX_URL / $PIP_EXTRA_INDEX_URL (or the explicit "
             "--pip-index-url / --pip-extra-index-url flags below).",
    )
    parser.add_argument(
        "--uc-volume-name",
        default=None,
        help="UC volume name used to upload the wheel when "
             "--build-and-upload-whl is set. Auto-created under "
             "<uc-catalog>.<uc-schema-name> if missing (catalogs are "
             "never auto-created).",
    )
    parser.add_argument(
        "--pip-index-url",
        default=None,
        help="Forwarded to `pip wheel` as --index-url when "
             "--build-and-upload-whl is set. Defaults to "
             "$PIP_INDEX_URL. Use this on networks where pypi.org is "
             "not reachable.",
    )
    parser.add_argument(
        "--pip-extra-index-url",
        action="append",
        default=None,
        help="Forwarded to `pip wheel` as --extra-index-url. Pass "
             "multiple times for multiple URLs. Defaults to "
             "$PIP_EXTRA_INDEX_URL (space-separated).",
    )
    parser.add_argument(
        "--no-create-missing-uc",
        dest="create_missing_uc",
        action="store_false",
        help="Do NOT auto-create the UC schema/volume during "
             "--build-and-upload-whl. Default: create them if missing "
             "(catalogs are never auto-created).",
    )
    parser.set_defaults(create_missing_uc=True)
    parser.add_argument(
        "--data-source",
        default="dbdatagen",
        choices=["dbdatagen", "github"],
        help="Demo data-source widget. 'github' downloads deterministic "
             "CSVs from the sdp-meta repo (recommended for CI smoke). "
             "'dbdatagen' generates random synthetic data (interactive "
             "demos only — counts vary run-to-run).",
    )
    parser.add_argument(
        "--onboarding-format",
        default="json",
        choices=["json", "yml"],
        help="Demo onboarding-format widget.",
    )
    parser.add_argument(
        "--validate-counts",
        default="true",
        choices=["true", "false"],
        help="Demo validate_counts widget. When 'true' (default), the "
             "demo's final-validation cell asserts row counts and the "
             "job FAILS on regression. Set to 'false' to disable "
             "assertions (interactive walkthrough mode).",
    )
    parser.add_argument(
        "--cleanup",
        default="false",
        choices=["true", "false"],
        help="Demo cleanup widget. When 'true', the demo's final cell "
             "drops every per-run resource it created -- pipelines, "
             "runner notebooks, and per-run schemas (bronze, silver, "
             "pipeline target, config volume). The user-supplied UC "
             "catalog is intentionally preserved (it's shared across "
             "runs). Default 'false' so interactive runs leave tables "
             "available for inspection. Set to 'true' for CI / smoke "
             "runs that need to leave the workspace clean.",
    )
    parser.add_argument(
        "--timeout-minutes",
        type=int,
        default=90,
        help="Job-completion timeout. The demo runs 3+ pipelines "
             "end-to-end on serverless; 90 minutes covers a cold "
             "workspace with margin. Bump for slower clouds/regions.",
    )
    args = parser.parse_args()

    # Validate UC identifiers up-front so the launcher fails fast with a
    # clear message instead of crashing later inside the job's CREATE
    # SCHEMA call (issue #261). The demo notebook itself re-validates
    # these on first read AND again post-restartPython, but we mirror
    # the check here so an obviously-broken --uc-catalog-name fails on
    # the laptop before we burn workspace minutes.
    validate_uc_identifier(args.uc_catalog_name, kind="--uc-catalog-name")
    if args.uc_schema_name:
        validate_uc_identifier(
            args.uc_schema_name, kind="--uc-schema-name"
        )
    if args.install_source == "whl_file" and not args.whl_file_path \
            and not args.build_and_upload_whl:
        raise SystemExit(
            "--install-source=whl_file requires --whl-file-path to be "
            "set, e.g. --whl-file-path /Volumes/<cat>/<sch>/<vol>/"
            "sdp_meta-<version>-py3-none-any.whl, OR re-run with "
            "--build-and-upload-whl so the launcher builds and uploads "
            "the wheel for you."
        )
    if args.build_and_upload_whl:
        # --build-and-upload-whl needs both a schema (for the volume to
        # live under) and a volume name; we don't fall back to the
        # per-run uc_schema_name default here because that schema gets
        # torn down with each demo run, which would orphan every wheel
        # uploaded under it.
        if not args.uc_schema_name:
            raise SystemExit(
                "--build-and-upload-whl requires --uc-schema-name "
                "(an existing or new UC schema for the wheel volume; "
                "do NOT reuse the per-run demo schema since it gets "
                "torn down with each run)."
            )
        if not args.uc_volume_name:
            raise SystemExit(
                "--build-and-upload-whl requires --uc-volume-name "
                "(the UC volume the wheel will be uploaded to)."
            )
        validate_uc_identifier(args.uc_volume_name, kind="--uc-volume-name")

    # Per-run isolation. Mirrors the run_integration_tests.py pattern of
    # baking a uuid into every workspace artifact so concurrent runs
    # (laptop x CI x another developer) never clobber each other's
    # bronze/silver tables, dataflowspecs, or pipelines.
    run_id = uuid.uuid4().hex[:12]
    # Human-scannable, globally-unique run name for the workspace's Job
    # Runs UI. Combines an ISO-8601 UTC timestamp (sortable, identifies
    # *when* the run was launched), the target catalog (identifies
    # *where* it ran), and the per-run uuid suffix (guarantees
    # uniqueness even if two launches share the same second + catalog).
    # Example: ``sdp-meta-demo-20260427T201112Z-ravi_dlt_meta_uc-a2b35bc8aa93``
    launch_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_name = (
        f"sdp-meta-demo-{launch_ts}-{args.uc_catalog_name}-{run_id}"
    )
    # The schema the demo writes to is intentionally distinct from the
    # schema the optional wheel volume lives under (see the schema/volume
    # check above). When --build-and-upload-whl is set,
    # ``args.uc_schema_name`` is the wheel-volume schema; the demo still
    # gets a fresh per-run schema so concurrent runs don't fight over
    # bronze/silver tables.
    if args.build_and_upload_whl:
        demo_uc_schema_name = f"sdp_meta_demo_{run_id}"
    else:
        demo_uc_schema_name = (
            args.uc_schema_name or f"sdp_meta_demo_{run_id}"
        )
    validate_uc_identifier(
        demo_uc_schema_name, kind="resolved uc_schema_name"
    )

    if args.profile:
        ws = WorkspaceClient(profile=args.profile)
    else:
        ws = WorkspaceClient()
    me = ws.current_user.me().user_name
    # Notebook lives under ``.../<run-id>/demo/`` so that the demo's
    # workspace-co-located lookup (``"/demo/" in nb_path``) succeeds and
    # finds the conf files we co-upload below at
    # ``.../<run-id>/demo/conf/{fmt}/sample_onboarding.{ext}``.
    repo_root_ws = f"/Users/{me}/sdp_meta_demo_runs/{run_id}"
    target_notebook_path = (
        f"{repo_root_ws}/demo/SDP_META_INTERACTIVE_DEMO"
    )

    # Optional STAGE 0: build + upload the wheel before submitting the
    # job. We resolve install_source / whl_file_path AFTER this so the
    # widgets the demo sees match what we actually want it to install.
    install_source = args.install_source
    whl_file_path = args.whl_file_path
    if args.build_and_upload_whl:
        import os as _os
        extras = args.pip_extra_index_url
        if not extras and _os.environ.get("PIP_EXTRA_INDEX_URL"):
            extras = [
                u for u in _os.environ["PIP_EXTRA_INDEX_URL"].split() if u
            ]
        print(
            "Building sdp-meta wheel locally and uploading to "
            f"/Volumes/{args.uc_catalog_name}/{args.uc_schema_name}/"
            f"{args.uc_volume_name} ..."
        )
        whl_file_path = bundle_prepare_wheel(BundlePrepareWheelCommand(
            uc_catalog=args.uc_catalog_name,
            uc_schema=args.uc_schema_name,
            uc_volume=args.uc_volume_name,
            profile=args.profile,
            pip_index_url=args.pip_index_url,
            pip_extra_index_urls=extras,
            create_if_missing=args.create_missing_uc,
        ))
        install_source = "whl_file"
        print(f"Wheel uploaded to: {whl_file_path}")

    _upload_demo_notebook(ws, target_notebook_path)
    _upload_demo_conf(ws, repo_root_ws)

    # ``base_parameters`` map 1:1 to the widgets defined at the top of
    # SDP_META_INTERACTIVE_DEMO.py. When a notebook task launches with
    # ``base_parameters``, the workspace pre-populates the matching
    # ``dbutils.widgets`` so the demo reads them straight through.
    # ``pypi_version`` only matters when install_source=pypi; harmless
    # to pass through for the other branches (the notebook ignores it).
    base_parameters = {
        "git_branch": args.git_branch,
        "uc_catalog_name": args.uc_catalog_name,
        "uc_schema_name": demo_uc_schema_name,
        "data_source": args.data_source,
        "onboarding_format": args.onboarding_format,
        "install_source": install_source,
        "whl_file_path": whl_file_path,
        "pypi_version": args.pypi_version,
        "validate_counts": args.validate_counts,
        "cleanup": args.cleanup,
    }
    print("Demo run parameters:")
    for k, v in base_parameters.items():
        print(f"  {k}: {v!r}")
    print(f"  run_id:    {run_id}")
    print(f"  run_name:  {run_name}")
    print(f"  notebook:  {target_notebook_path}")

    run = _submit_demo_job(
        ws,
        run_id=run_id,
        run_name=run_name,
        target_notebook_path=target_notebook_path,
        base_parameters=base_parameters,
        timeout_min=args.timeout_minutes,
    )

    # Surface the run page first — even on success it's the most useful
    # thing for the operator to bookmark, and on failure they need to
    # click straight through to the workspace UI.
    run_page_url = getattr(run, "run_page_url", None)
    state = getattr(run, "state", None)
    result_state = (
        state.result_state.value
        if state and state.result_state
        else "UNKNOWN"
    )
    state_message = (
        state.state_message
        if state and getattr(state, "state_message", None)
        else ""
    )

    print("")
    print("=" * 78)
    print(f"Demo run finished. result_state={result_state}")
    if state_message:
        print(f"  state_message: {state_message}")
    if run_page_url:
        print(f"  run_page_url:  {run_page_url}")
    print("=" * 78)

    if result_state != "SUCCESS":
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
