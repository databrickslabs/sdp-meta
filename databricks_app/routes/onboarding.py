"""Onboarding routes: landing page + onboard / preview / bundled-specs.

Four endpoints share a Blueprint because they all operate on the same
onboarding-template lifecycle:

  * ``GET  /``                            \u2014 landing page (the App's UI).
  * ``POST /onboarding``                  \u2014 submit a real onboarding run
                                            (UC volume create, schema create,
                                            CLI subprocess, etc.).
  * ``GET  /onboarding/bundled-specs``    \u2014 enumerate the curated demos.
  * ``POST /onboarding/preview``          \u2014 server-side dry-run that
                                            renders the spec with the
                                            user's form values and surfaces
                                            required-files preflight.
"""

from __future__ import annotations

import json
import logging
import os

from flask import Blueprint, jsonify, render_template, request

import _jobs as _jobs_module
import _subprocess_runner as _runner_module
from _config import _repo_root
from services.onboarding.bundled_specs import _list_bundled_specs
from services.onboarding.env_validation import (
    _detect_env_suffixes,
    _verify_env_matches_template,
)
from services.onboarding.path_resolver import (
    _OnboardingFileError,
    _preflight_parse_onboarding,
    _resolve_local_onboarding_path,
)
from services.onboarding.quality_preflight import (
    _verify_quality_configuration,
)
from services.onboarding.required_files import (
    _check_required_files_existence,
    _extract_required_files,
)

# UC identifier validation. The sdp-meta wheel is built and installed
# by databricks_app/start.sh in the App container, so this import
# succeeds at request-handling time. Local-dev runs that haven't
# installed the wheel (``pip install -e .`` skipped) get a graceful
# no-op fallback so /onboarding still responds instead of 500-ing on
# import.
try:
    from databricks.labs.sdp_meta.identifiers import validate_uc_identifier
except ImportError:  # pragma: no cover \u2014 only hit when the wheel is missing
    def validate_uc_identifier(name, *, kind: str = "identifier") -> str:
        return name


logger = logging.getLogger(__name__)

bp = Blueprint('onboarding', __name__)


@bp.route('/')
def index():
    return render_template('landingPage.html')


@bp.route('/onboarding', methods=['POST'])
def handle_onboard_form():
    logger.info("onboard details: %s", dict(request.form))
    current_directory = _repo_root()

    uc_enabled = request.form.get('unity_catalog_enabled') == "1"
    uc_name = request.form.get('unity_catalog_name', '')

    # ── UC-only App contract ───────────────────────────────────────
    # The App ships with UC as the only supported mode (the legacy
    # enable/disable toggle was removed from the UI in favour of a
    # hidden input pinned to "1"). The underlying ``sdp-meta`` CLI
    # still supports HMS, but a UC=Disabled POST to the App is
    # unreachable via the UI and almost always indicates a hand-
    # crafted / stale client. Fail fast with a clear 400 instead of
    # letting the request fan out through the onboarding pipeline.
    if not uc_enabled:
        return jsonify({
            'error': (
                "The SDP-META App supports Unity Catalog only. The "
                "form field ``unity_catalog_enabled`` must be '1'. "
                "If you need HMS support, drive sdp-meta via the "
                "``databricks labs sdp-meta`` CLI directly."
            )
        }), 400

    # Validate UC identifier at the App boundary so a malformed name
    # surfaces as an actionable 400 instead of a generic ``cli.py``
    # traceback. The underlying onboarding code splices catalog / schema
    # names into SQL strings unquoted (issue #261), so this is also a
    # defense-in-depth check against SQL-identifier injection via the
    # form. Only enforced when UC is enabled \u2014 the non-UC HMS path
    # doesn't take a catalog.
    if uc_enabled and uc_name:
        try:
            validate_uc_identifier(uc_name, kind="unity_catalog_name")
        except ValueError as exc:
            return jsonify({'error': str(exc)}), 400

    # ── Required-field validation ──────────────────────────────────
    missing = []
    if uc_enabled and not uc_name:
        missing.append("Unity Catalog Name")
    onboarding_file_path_raw = request.form.get('onboarding_file_path', '').strip()
    if not onboarding_file_path_raw:
        missing.append("Onboarding File Path")
    if not request.form.get('sdp_meta_schema', '').strip():
        missing.append("SDP Meta Schema")
    if not request.form.get('bronze_schema', '').strip():
        missing.append("Bronze Schema")
    if not request.form.get('silver_schema', '').strip():
        missing.append("Silver Schema")
    if missing:
        return jsonify({'error': 'Required fields missing: ' + ', '.join(missing)}), 400

    # ── UC-identifier validation (all names, not just the catalog) ──
    # The schema and dataflowspec-table names get spliced into
    # ``CREATE SCHEMA`` / ``saveAsTable`` / ``catalog.schema.table``
    # downstream without backtick-quoting, so a malformed one must fail
    # here with an actionable 400 rather than a cryptic Spark error
    # mid-run (issue #261). Schemas are required (checked above); table
    # names default and are validated whenever present.
    for value, kind in (
        (request.form.get('sdp_meta_schema', '').strip(), 'sdp_meta_schema'),
        (request.form.get('bronze_schema', '').strip(), 'bronze_schema'),
        (request.form.get('silver_schema', '').strip(), 'silver_schema'),
        (request.form.get('bronze_table', '').strip(), 'bronze_table'),
        (request.form.get('silver_table', '').strip(), 'silver_table'),
    ):
        if not value:
            continue
        try:
            validate_uc_identifier(value, kind=kind)
        except ValueError as exc:
            return jsonify({'error': str(exc)}), 400

    # ── Resolve onboarding_file_path + pre-flight parse ────────────
    # Resolve the user-supplied path (local / UC Volume / DBFS) onto
    # local disk, then parse it once to catch malformed YAML / JSON
    # BEFORE we shell out to the CLI or trigger any UC side-effects.
    try:
        onboarding_file_path, _tmp_onboarding = _resolve_local_onboarding_path(
            onboarding_file_path_raw, current_directory
        )
    except _OnboardingFileError as exc:
        return jsonify({'error': str(exc)}), 400

    try:
        _parsed_onboarding, _ = _preflight_parse_onboarding(onboarding_file_path)
    except _OnboardingFileError as exc:
        if _tmp_onboarding and os.path.exists(_tmp_onboarding):
            try:
                os.unlink(_tmp_onboarding)
            except OSError:
                pass
        return jsonify({'error': str(exc)}), 400

    # ── Env-suffix sanity check ────────────────────────────────────
    # The onboarding parser requires per-row fields suffixed with the
    # active env (``bronze_database_<env>``, ``source_path_<env>``, ...).
    # If the form's Environment value doesn't match the suffix present
    # on these fields, the parser silently ``continue``s past every row
    # and the dataflowspec tables come out empty WITH THE JOB STILL
    # REPORTING SUCCESS \u2014 the worst possible failure mode for a demo
    # onboarding. Fail-fast at the App boundary with a message that
    # names the actually-present suffix so the user can fix the form.
    _form_env = (request.form.get('environment') or 'demo').strip()
    try:
        _verify_env_matches_template(_parsed_onboarding, _form_env)
        _verify_quality_configuration(
            _parsed_onboarding, _form_env, uc_enabled=uc_enabled
        )
    except _OnboardingFileError as exc:
        if _tmp_onboarding and os.path.exists(_tmp_onboarding):
            try:
                os.unlink(_tmp_onboarding)
            except OSError:
                pass
        return jsonify({'error': str(exc)}), 400

    # ── Build CLI payload ──────────────────────────────────────────
    local_dir_raw = request.form.get('local_directory', '').strip()
    if not local_dir_raw:
        local_dir_raw = os.path.join(current_directory, 'demo') + os.sep

    # The HTML form field names match the CLI's ``sdp_meta_*`` keys
    # 1:1, so this dict is a straight pass-through. See
    # test_onboarding_payload_uses_sdp_meta_keys for the regression
    # guard against accidental key drift.
    json_data = {
        "unity_catalog_enabled": "1" if uc_enabled else "0",
        "unity_catalog_name": uc_name,
        "serverless": "1" if request.form.get('serverless') == "1" else "0",
        "onboarding_file_path": onboarding_file_path,
        "local_directory": local_dir_raw,
        "sdp_meta_schema": request.form.get('sdp_meta_schema', '').strip(),
        "bronze_schema": request.form.get('bronze_schema', '').strip(),
        "silver_schema": request.form.get('silver_schema', '').strip(),
        "sdp_meta_layer": request.form.get('sdp_meta_layer', '1'),
        "bronze_table": request.form.get('bronze_table', 'bronze_dataflowspec'),
        "silver_table": request.form.get('silver_table', 'silver_dataflowspec'),
        "overwrite": "1" if request.form.get('overwrite') == "1" else "0",
        "version": request.form.get('version') or 'v1',
        "environment": request.form.get('environment') or 'demo',
        "author": request.form.get('author') or 'sdp-meta-app',
        "update_paths": "1" if request.form.get('update_paths') == "1" else "0",
        "command": "onboard_ui",
        "flags": {"log_level": "info"},
    }
    quality_dependency = os.getenv("SDP_META_QUALITY_ENGINE_DEPENDENCY")
    if quality_dependency:
        json_data["quality_engine_dependency"] = quality_dependency
    json_string = json.dumps(json_data)

    # Spawn the CLI subprocess in a background thread so the frontend
    # can stream live log output while onboarding runs.
    token = _jobs_module._new_job_token()
    _runner_module._run_cli_json_payload(
        token=token,
        json_string=json_string,
        cwd=current_directory,
        cleanup_path=_tmp_onboarding,
    )
    return jsonify({'token': token, 'started': True})


@bp.route('/onboarding/bundled-specs', methods=['GET'])
def list_bundled_specs():
    """Return the curated list of onboarding specs the App container
    ships under ``demo/``. The UI uses this to render a "pick a demo"
    dropdown so first-time users don't have to know the exact relative
    path of ``demo/conf/json/cloudfiles-onboarding.template`` or which
    spec needs an A2 companion.

    No side-effects. Read-only filesystem scan of the App container.
    """
    return jsonify({"specs": _list_bundled_specs(_repo_root())})


@bp.route('/onboarding/preview', methods=['POST'])
def handle_onboarding_preview():
    """Server-side dry-run that renders the user's onboarding template
    with the same ``{token}`` \u2192 value substitution the real onboarding
    pipeline performs, and returns the rendered text to the browser.

    No side-effects. Does NOT create a UC volume, schema, run a job, or
    write anything to disk. The ``{uc_volume_path}`` substitution uses
    a deterministic preview value derived from form fields (since the
    path is deterministic from ``catalog`` + ``sdp_meta_schema``, even
    though the real volume is only created at onboarding time).

    Response (200) includes ``rendered``, ``source_extension``,
    ``uc_volume_path_used``, ``detected_envs``, ``env_warning``,
    ``required_files``, and ``supporting_files_directory_used``.
    Response (400): ``{"error": "<message>"}`` for malformed input.
    """
    from databricks.labs.sdp_meta.cli import render_onboarding_template

    logger.info("onboarding preview: %s",
                {k: v for k, v in request.form.items() if k != 'csrf_token'})
    current_directory = _repo_root()

    uc_enabled = request.form.get('unity_catalog_enabled') == "1"
    uc_name = request.form.get('unity_catalog_name', '').strip()
    sdp_meta_schema = request.form.get('sdp_meta_schema', '').strip()
    bronze_schema = request.form.get('bronze_schema', '').strip()
    silver_schema = request.form.get('silver_schema', '').strip()
    onboarding_file_path_raw = (
        request.form.get('onboarding_file_path', '').replace('\u00a0', '').strip()
    )

    if uc_enabled and uc_name:
        try:
            validate_uc_identifier(uc_name, kind="unity_catalog_name")
        except ValueError as exc:
            return jsonify({'error': str(exc)}), 400

    # Required-field validation \u2014 minimal set needed to render the
    # four substitution placeholders.
    missing = []
    if not onboarding_file_path_raw:
        missing.append("Onboarding File Path")
    if uc_enabled and not uc_name:
        missing.append("Unity Catalog Name")
    if not sdp_meta_schema:
        missing.append("SDP Meta Schema")
    if not bronze_schema:
        missing.append("Bronze Schema")
    if not silver_schema:
        missing.append("Silver Schema")
    if missing:
        return jsonify({'error': 'Required fields missing: ' + ', '.join(missing)}), 400

    # Validate the schema identifiers too (not just the catalog): they
    # are spliced into the ``{uc_volume_path}`` and the bronze/silver
    # schema substitutions below, and into SQL downstream on the real
    # /onboarding run, so a malformed one must fail here (issue #261).
    for value, kind in (
        (sdp_meta_schema, 'sdp_meta_schema'),
        (bronze_schema, 'bronze_schema'),
        (silver_schema, 'silver_schema'),
    ):
        try:
            validate_uc_identifier(value, kind=kind)
        except ValueError as exc:
            return jsonify({'error': str(exc)}), 400

    # Resolve + read + pre-flight parse the template. Same code path
    # as /onboarding so the preview and the real run can't disagree
    # about what counts as a valid template.
    tmp_to_cleanup = None
    try:
        local_path, tmp_to_cleanup = _resolve_local_onboarding_path(
            onboarding_file_path_raw, current_directory
        )
    except _OnboardingFileError as exc:
        return jsonify({'error': str(exc)}), 400

    try:
        try:
            parsed_pre, _ = _preflight_parse_onboarding(local_path)
        except _OnboardingFileError as exc:
            return jsonify({'error': str(exc)}), 400

        try:
            with open(local_path, 'r', encoding='utf-8') as fh:
                content = fh.read()
        except OSError as exc:
            return jsonify({'error': f'Could not read {local_path}: {exc}'}), 400

        # Compute the deterministic ``{uc_volume_path}`` exactly the
        # way ``SDPMeta.create_uc_volume`` does. We can't actually
        # create the volume here (no side-effects), but the path it
        # WOULD return is deterministic from catalog + schema.
        if uc_enabled and uc_name and sdp_meta_schema:
            uc_volume_path = f"/Volumes/{uc_name}/{sdp_meta_schema}/{sdp_meta_schema}/sdp_meta_conf/"
        else:
            uc_volume_path = "<not-applicable-without-uc>/sdp_meta_conf/"

        substitutions = {
            "{uc_volume_path}": uc_volume_path,
            "{uc_catalog_name}": uc_name or "",
            "{bronze_schema}": bronze_schema,
            "{silver_schema}": silver_schema,
        }
        source_ext = os.path.splitext(local_path)[1].lower()
        try:
            rendered, _parsed = render_onboarding_template(content, source_ext, substitutions)
        except Exception as exc:
            # ``render_onboarding_template`` raises on malformed
            # YAML/JSON *after substitution* \u2014 usually means a
            # substitution value contained an unescaped quote or
            # breaks YAML indentation.
            logger.exception("Onboarding preview render failed")
            return jsonify({
                'error': f'Could not render template after substitution: {exc}'
            }), 400

        # Surface the detected env suffix(es) so the UI can warn the
        # user when their form's Environment value won't match the
        # template. We don't reject here \u2014 the preview is a dry-run;
        # let the user see what would render and decide whether to fix
        # the form. The real /onboarding POST path enforces the match.
        detected_envs = _detect_env_suffixes(parsed_pre)
        form_env = (request.form.get('environment') or 'demo').strip()
        try:
            _verify_quality_configuration(
                parsed_pre, form_env, uc_enabled=uc_enabled
            )
        except _OnboardingFileError as exc:
            return jsonify({'error': str(exc)}), 400
        env_warning = None
        if detected_envs and form_env not in detected_envs:
            env_warning = (
                f"Environment '{form_env}' does not match the suffix(es) on "
                f"env-aware fields in this template: {detected_envs}. The "
                f"onboarding parser will skip every row and the dataflowspec "
                f"tables will come out empty. Change the Environment field to "
                f"{detected_envs[0] if len(detected_envs) == 1 else 'one of ' + str(detected_envs)} "
                f"before submitting."
            )

        # Extract the file paths the spec references and existence-
        # check each one against the user's supporting-files directory.
        local_dir_raw = (request.form.get('local_directory') or '').strip()
        if not local_dir_raw:
            local_dir_for_check = os.path.join(current_directory, "demo")
        elif local_dir_raw.startswith("/Volumes/") or os.path.isabs(local_dir_raw):
            local_dir_for_check = local_dir_raw
        else:
            local_dir_for_check = os.path.join(current_directory, local_dir_raw)
        required = _extract_required_files(parsed_pre, substitutions)
        required_files = _check_required_files_existence(
            required, uc_volume_path, local_dir_for_check
        )

        return jsonify({
            'rendered': rendered,
            'source_extension': source_ext,
            'uc_volume_path_used': uc_volume_path,
            'detected_envs': detected_envs,
            'env_warning': env_warning,
            'required_files': required_files,
            'supporting_files_directory_used': local_dir_for_check,
            'note': 'Preview only \u2014 onboarding has not been submitted. '
                    'Submit the form to actually create the UC volume and '
                    'launch the onboarding job.',
        })
    finally:
        if tmp_to_cleanup and os.path.exists(tmp_to_cleanup):
            try:
                os.unlink(tmp_to_cleanup)
            except OSError:
                pass
