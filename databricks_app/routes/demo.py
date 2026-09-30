"""Demo launcher routes + UC-grant probe + background-job polling.

Three endpoints share a Blueprint because they're all part of the
single "run a demo" lifecycle:

  * ``GET  /check-uc-grants``         \u2014 verify App SP has USE_CATALOG +
                                        CREATE_SCHEMA on a UC catalog.
  * ``POST /rundemo``                 \u2014 launch a named demo via subprocess.
  * ``GET  /api/job/<token>/logs``    \u2014 poll buffered subprocess output
                                        from a previously-started job.
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import asdict

from flask import Blueprint, jsonify, request

import _jobs as _jobs_module
from _command_output import _parse_command_result
from _config import _repo_root
from _subprocess_runner import _run_command_in_background

# UC catalog pre-flight (Apps-SP grants). Lives next to app.py so the
# probe + GRANT SQL builder ship in the same source tree and can be
# imported without any PYTHONPATH gymnastics.
from uc_preflight import check_app_sp_grants_on_catalog

try:
    from databricks.labs.sdp_meta.identifiers import validate_uc_identifier
except ImportError:  # pragma: no cover
    def validate_uc_identifier(name, *, kind: str = "identifier") -> str:
        return name


logger = logging.getLogger(__name__)

bp = Blueprint('demo', __name__)


@bp.route('/check-uc-grants', methods=['GET'])
def check_uc_grants():
    """Probe whether the App SP has the privileges every demo needs on
    a UC catalog.

    Front-end "Test App access" button calls this to verify a catalog
    BEFORE the user clicks any demo. Returns 200 with the structured
    :class:`uc_preflight.PreflightResult` payload regardless of outcome \u2014
    the ``ok`` flag tells the UI whether to render the success indicator
    or the "Grant required" panel. The same probe runs as a 400-blocker
    inside ``/rundemo`` (see below).
    """
    uc_name = (request.args.get('uc_name') or '').strip()
    if not uc_name:
        return jsonify({'error': 'uc_name query parameter is required'}), 400
    try:
        validate_uc_identifier(uc_name, kind='uc_name')
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400

    result = check_app_sp_grants_on_catalog(uc_name)
    return jsonify(asdict(result))


# Allow-list of demos exposed by /rundemo. Each entry maps the UI
# ``data-command`` token to the launcher script + how to pass the UC
# catalog name on its CLI.
#
#   ``file``     \u2014 path to the launcher (relative to repo root).
#   ``uc_arg``   \u2014 flag the launcher's argparse expects for the UC
#                  catalog. The legacy ``launch_*_demo.py`` scripts use
#                  underscored ``--uc_catalog_name``;
#                  ``launch_interactive_demo.py`` uses hyphenated
#                  ``--uc-catalog-name`` (issue #261-era convention).
#   ``extra_args`` \u2014 fixed positional/flag args appended after the
#                  UC catalog. Used for the interactive demo so it
#                  returns promptly with the run URL instead of
#                  blocking the Flask request for the default 90-
#                  minute job timeout.
#
# Removed demos:
#   - ``demo_dabs`` (generate_dabs_resources.py) \u2014 shells out to
#     ``databricks bundle deploy/run`` which uses Terraform under the
#     hood. The Apps container ships only the SDK + CLI + sdp-meta
#     wheel; pulling in Terraform for one demo isn't worth the
#     runtime weight. Use the bundle CLI from a local terminal.
#   - ``demo_dlt_sink`` (launch_dlt_sink_demo.py) \u2014 requires Kafka /
#     Event Hubs source + secret-scope wiring that isn't available
#     to the App's service principal in this workspace. Run via the
#     CLI launcher with ``--profile`` instead.
_DEMO_REGISTRY = {
    "demo_at_scale_autoloader": {
        "file": "demo/launch_at_scale_autoloader_demo.py",
        "uc_arg": "--uc_catalog_name",
        # App users launch demos for inspection. Preserve the job, pipelines,
        # schemas, volume, and output tables instead of deleting them as soon
        # as validation succeeds. The standalone CLI keeps its cleanup default.
        "extra_args": ["--keep-resources"],
    },
    "demo_cloudfiles": {
        "file": "demo/launch_af_cloudfiles_demo.py",
        "uc_arg": "--uc_catalog_name",
    },
    "demo_acf": {
        "file": "demo/launch_acfs_demo.py",
        "uc_arg": "--uc_catalog_name",
    },
    "demo_silverfanout": {
        "file": "demo/launch_silver_fanout_demo.py",
        "uc_arg": "--uc_catalog_name",
    },
    "demo_dias": {
        "file": "demo/launch_dais_demo.py",
        "uc_arg": "--uc_catalog_name",
    },
    "demo_interactive": {
        "file": "demo/launch_interactive_demo.py",
        "uc_arg": "--uc-catalog-name",
        # Stream upload/submission output and the early run URL through the
        # same background progress UI as the 100-table featured demo. Keep
        # the launcher's normal completion wait so the UI reports the real
        # final state instead of turning a one-minute timeout into a false
        # failure while the remote run continues.
        #
        # ``--install-source pypi`` makes the spawned job
        # ``pip install databricks-labs-sdp-meta`` from PyPI on
        # every demo launch. Decouples the demo from whatever
        # branch the App container happens to have checked out, so
        # once a new release is published the App auto-picks it up
        # with zero redeploys.
        #
        # To pin a specific release instead of always-latest, add
        # e.g. ``"--pypi-version", "0.1.0"`` to the list below.
        "extra_args": [
            "--install-source", "pypi",
        ],
    },
}


@bp.route('/rundemo', methods=['POST'])
def run_demo():
    payload = request.get_json(silent=True) or {}
    code_to_run = payload.get('demo_name', '')
    logger.info("processing demo: %s", payload)
    current_directory = _repo_root()
    demo_entry = _DEMO_REGISTRY.get(code_to_run, None)
    uc_name = payload.get('uc_name', '')

    # Fix C3: reject unknown demo names (demo_entry already validated
    # by dict lookup).
    if demo_entry is None:
        return jsonify({'error': 'Unknown demo name'}), 400

    demo_file = demo_entry["file"]
    demo_uc_arg = demo_entry["uc_arg"]
    demo_extra_args = demo_entry.get("extra_args", [])

    # ── UC catalog pre-flight ──────────────────────────────────────
    # Validate the catalog name shape and confirm the App SP has the
    # privileges every demo launcher transitively needs (USE_CATALOG +
    # CREATE_SCHEMA \u2014 the demos all CREATE SCHEMA <cat>.<schema>).
    #
    # Without this check, a missing grant surfaces inside the demo
    # subprocess as a generic ``PermissionDenied`` traceback in
    # stderr \u2014 actionable but ugly. Returning 400 here lets the
    # front-end render the "Grant required" panel with the exact GRANT
    # SQL the catalog owner needs to run, plus a "Verify and retry"
    # button.
    try:
        validate_uc_identifier(uc_name, kind="uc_catalog_name")
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400

    preflight = check_app_sp_grants_on_catalog(uc_name)
    if not preflight.ok:
        # 400 \u2014 client-correctable: the catalog owner needs to run
        # SQL. Front-end pattern-matches on ``grant_required: True``
        # so the demo modal can render the panel instead of a generic
        # error.
        body = asdict(preflight)
        body['grant_required'] = True
        body['error'] = (
            f"App service principal '{preflight.sp_display_name}' "
            f"({preflight.sp_principal}) is missing "
            f"{', '.join(preflight.missing)} on catalog '{uc_name}'. "
            "Run the GRANT SQL below as the catalog owner and retry."
        )
        return jsonify(body), 400

    # Build subprocess environment.
    #
    # PYTHONPATH entries (order matters \u2014 earlier wins on import
    # resolution):
    #   1. databricks_app/ \u2014 Python's `site` module auto-imports a
    #      top-level module called `sitecustomize` from sys.path on
    #      every interpreter startup. databricks_app/sitecustomize.py
    #      installs the App-mode shim that strips trailing ".py" from
    #      notebook_path arguments on
    #      WorkspaceClient.pipelines.create / .jobs.create. The shim
    #      is a no-op outside the App container; see
    #      databricks_app/sitecustomize.py for the full rationale.
    #      Putting databricks_app/ on PYTHONPATH is what activates it
    #      in the demo subprocess, without any change to demo/ or
    #      integration_tests/.
    #   2. repo root \u2014 so demo scripts can import `integration_tests`
    #      (lives at the repo root, not inside the demo/ directory
    #      that Python adds automatically as sys.path[0]).
    demo_env = os.environ.copy()
    databricks_app_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    existing_pypath = demo_env.get('PYTHONPATH', '')
    pypath_entries = [databricks_app_dir, current_directory]
    if existing_pypath:
        pypath_entries.append(existing_pypath)
    demo_env['PYTHONPATH'] = ':'.join(pypath_entries)

    demo_command = [
        sys.executable,
        os.path.join(current_directory, demo_file),
        demo_uc_arg,
        uc_name,
        *demo_extra_args,
    ]
    # Deployed Apps already have DATABRICKS_APP_PORT, which makes launchers
    # use ambient service-principal auth and enables the notebook-path shim
    # needed for workspace-synced sources. A local Flask run must NOT fake
    # that marker: locally uploaded notebooks retain their ``.py`` suffix.
    # Instead, forward the selected CLI profile explicitly so the child stays
    # non-interactive while preserving local notebook paths.
    if not demo_env.get('DATABRICKS_APP_PORT'):
        local_profile = demo_env.get('DATABRICKS_CONFIG_PROFILE')
        if local_profile:
            demo_command.extend(['--profile', local_profile])

    # Every demo launcher can wait many minutes for remote jobs or pipelines.
    # Always return immediately and stream progress through the shared polling
    # UI instead of holding the browser request behind a blocking spinner.
    try:
        token = _jobs_module._new_job_token(
            kind='demo',
            max_active_for_kind=_jobs_module._MAX_ACTIVE_DEMO_JOBS,
        )
    except _jobs_module.JobCapacityError:
        logger.warning(
            "Demo launch rejected because the active-demo limit (%d) "
            "has been reached",
            _jobs_module._MAX_ACTIVE_DEMO_JOBS,
        )
        response = jsonify({
            'error': (
                "Active demo capacity is currently full. Wait for an "
                "existing demo to finish before "
                "launching another."
            ),
        })
        response.status_code = 429
        response.headers['Retry-After'] = '30'
        return response
    _run_command_in_background(
        token=token,
        command=demo_command,
        cwd=current_directory,
        env=demo_env,
        # Interactive waits up to 90 minutes and may be silent while the
        # remote job runs. Keep the orphan-protection timeout above that.
        idle_timeout_seconds=2 * 60 * 60,
    )
    return jsonify({'token': token}), 202


@bp.route('/api/job/<token>/logs', methods=['GET'])
def get_job_logs(token):
    """Polling endpoint: returns buffered log lines + done/returncode
    for the progress UI."""
    job = _jobs_module._get_job_snapshot(token)
    if job is None:
        return jsonify({'error': 'Job not found'}), 404

    # Validate `offset` up front: a non-numeric string or negative value
    # should produce a 400 with an actionable message, not bubble out of
    # ``int()`` as a 500 from the global ``handle_exception`` hook. Same
    # contract as the ``limit`` validation in routes/metadata_browse.py.
    raw_offset = request.args.get('offset', 0)
    try:
        offset = int(raw_offset)
    except (TypeError, ValueError):
        return jsonify({
            'error': f"offset must be an integer (got {raw_offset!r})"
        }), 400
    if offset < 0:
        # Negative slicing would silently return the tail of the log
        # buffer, which is never what the polling UI wants.
        return jsonify({
            'error': f"offset must be non-negative (got {offset})"
        }), 400
    base_offset = job.get('log_base_offset', 0)
    relative_offset = max(0, offset - base_offset)
    new_logs = job['logs'][relative_offset:]
    payload: dict = {
        'logs': new_logs,
        'next_offset': base_offset + len(job['logs']),
        'done': job['done'],
        'returncode': job.get('returncode'),
        'error': job.get('error'),
    }
    if job.get('done'):
        payload['result'] = _parse_command_result(
            job.get('stdout', ''),
            job.get('stderr', ''),
            job.get('returncode', -1),
        )
    return jsonify(payload)
