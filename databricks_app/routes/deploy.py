"""``POST /deploy`` \u2014 launch a Lakeflow Spark Declarative Pipeline.

Builds the JSON payload the CLI's ``deploy_ui`` command consumes and
spawns the CLI subprocess in a background thread (same machinery as
``/onboarding``). Returns a token the frontend polls.
"""

from __future__ import annotations

import json
import logging
import os

from flask import Blueprint, jsonify, request

import _jobs as _jobs_module
import _subprocess_runner as _runner_module
from _config import _repo_root

try:
    from databricks.labs.sdp_meta.identifiers import validate_uc_identifier
except ImportError:  # pragma: no cover
    def validate_uc_identifier(name, *, kind: str = "identifier") -> str:
        return name


logger = logging.getLogger(__name__)

bp = Blueprint('deploy', __name__)


@bp.route('/deploy', methods=['POST'])
def handle_deploy_form():
    logger.info("deploy details: %s", dict(request.form))
    current_directory = _repo_root()

    uc_enabled = request.form.get('uc_enabled') == "1"
    uc_name = (request.form.get('uc_catalog_name') or '').strip()

    # ── UC-only App contract ───────────────────────────────────────
    # The App ships with UC as the only supported mode (the legacy
    # enable/disable toggle was removed from the UI in favour of a
    # hidden input pinned to "1"). The underlying ``sdp-meta`` CLI
    # still supports HMS, but a UC=Disabled POST to the App is
    # unreachable via the UI and almost always indicates a hand-
    # crafted / stale client. Fail fast with a clear 400 instead of
    # letting the request fan out through the deploy pipeline.
    if not uc_enabled:
        return jsonify({
            'error': (
                "The SDP-META App supports Unity Catalog only. The "
                "form field ``uc_enabled`` must be '1'. If you need "
                "HMS support, drive sdp-meta via the ``databricks "
                "labs sdp-meta`` CLI directly."
            )
        }), 400
    layer = request.form.get('deploylayer', 'bronze')
    pipeline_name = (request.form.get('pipeline_name') or '').strip()
    spc_schema = (request.form.get('spc_schema_name') or '').strip()
    target_schema = (request.form.get('dlt_target_schema') or '').strip()
    bronze_group = (request.form.get('onboard_bronze_group') or '').strip()
    silver_group = (request.form.get('onboard_silver_group') or '').strip()
    bronze_spec_table = (request.form.get('bronze_dataflowspec_table') or '').strip() or 'bronze_dataflowspec'
    silver_spec_table = (request.form.get('silver_dataflowspec_table') or '').strip() or 'silver_dataflowspec'

    # Server-side mandatory-field check. The deploy form already wires
    # up client-side validation, but that lives in the user's browser
    # and can be bypassed by a hand-crafted POST \u2014 and silent
    # "missing field" failures downstream (None schema \u2192 ``None.
    # bronze_dataflowspec`` in the pipeline config) are exactly the
    # kind of failure we want to short-circuit at the App boundary
    # with an actionable error.
    missing = []
    if not pipeline_name:
        missing.append('pipeline_name')
    if not spc_schema:
        missing.append('spc_schema_name (DataFlow Spec Schema)')
    if not target_schema:
        missing.append('dlt_target_schema (Target Schema)')
    if uc_enabled and not uc_name:
        missing.append('uc_catalog_name (Unity Catalog Name)')
    if layer in ('bronze', 'bronze_silver') and not bronze_group:
        missing.append('onboard_bronze_group (Bronze Group)')
    if layer in ('silver', 'bronze_silver') and not silver_group:
        missing.append('onboard_silver_group (Silver Group)')
    if missing:
        return jsonify({
            'error': (
                'Missing required field(s): '
                + ', '.join(missing)
                + '. Fill them in on the Deployment form and try again.'
            )
        }), 400

    # Validate UC identifiers at the App boundary \u2014 same reasoning as
    # /onboarding. Not just the catalog: the spec schema, target schema,
    # and both dataflowspec table names all get spliced into the pipeline
    # config's fully-qualified ``catalog.schema.table`` references (read
    # later via ``spark.read.table`` without backtick-quoting), so a
    # malformed one must fail here with an actionable 400 instead of a
    # cryptic Spark name-resolution error at pipeline runtime (issue #261).
    # Catalog is only enforced when UC is enabled; schema/table names are
    # required regardless (checked above) and validated whenever present.
    uc_identifiers = [
        (spc_schema, "spc_schema_name"),
        (target_schema, "dlt_target_schema"),
        (bronze_spec_table, "bronze_dataflowspec_table"),
        (silver_spec_table, "silver_dataflowspec_table"),
    ]
    if uc_enabled and uc_name:
        uc_identifiers.insert(0, (uc_name, "uc_catalog_name"))
    for value, kind in uc_identifiers:
        if not value:
            continue
        try:
            validate_uc_identifier(value, kind=kind)
        except ValueError as exc:
            return jsonify({'error': str(exc)}), 400

    # NOTE on key naming: the CLI's ``_load_deploy_config_ui`` reads
    # the spec-schema as separate ``sdp_meta_bronze_schema`` /
    # ``sdp_meta_silver_schema`` keys and the bronze table as
    # ``dataflowspec_bronze_table``. The HTML form uses the friendlier
    # single ``spc_schema_name`` / ``bronze_dataflowspec_table``
    # fields. Translate at the App boundary \u2014 otherwise the CLI
    # silently sees ``None`` for the spec schema and produces a
    # pipeline config like ``catalog.None.bronze_dataflowspec`` which
    # fails at runtime. See test_deploy_payload_uses_cli_canonical_keys
    # for the regression guard.
    json_data = {
        "uc_enabled": "1" if uc_enabled else "0",
        "uc_catalog_name": uc_name,
        "serverless": "1" if request.form.get('serverless') == "1" else "0",
        "layer": layer,
        "pipeline_name": pipeline_name,
        "dlt_target_schema": target_schema,
        "command": "deploy_ui",
        "flags": {"log_level": "info"},
        "onboard_bronze_group": bronze_group,
        "onboard_silver_group": silver_group,
        "sdp_meta_bronze_schema": spc_schema,
        "sdp_meta_silver_schema": spc_schema,
        "dataflowspec_bronze_table": bronze_spec_table,
        "dataflowspec_silver_table": silver_spec_table,
    }
    quality_dependency = os.getenv("SDP_META_QUALITY_ENGINE_DEPENDENCY")
    if quality_dependency:
        json_data["quality_engine_dependency"] = quality_dependency
    json_string = json.dumps(json_data)

    # Background-thread + polling pattern shared with /onboarding so
    # the UI can stream live log output while the deploy runs.
    token = _jobs_module._new_job_token()
    _runner_module._run_cli_json_payload(
        token=token,
        json_string=json_string,
        cwd=current_directory,
    )
    return jsonify({'token': token, 'started': True})
