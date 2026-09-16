"""``/api/metadata/workspace-*`` + ``/api/metadata/parse-spec`` \u2014 Spec Editor.

Workspace filesystem walk + read + write endpoints for the in-app
spec editor, plus a three-layer parse-and-validate endpoint that
catches syntax errors before the user clicks Save.

  * ``GET  /api/metadata/workspace-ls``
  * ``GET  /api/metadata/workspace-file``
  * ``POST /api/metadata/workspace-file``
  * ``POST /api/metadata/parse-spec``
"""

from __future__ import annotations

import io
import json
import logging
import os

from databricks.labs.sdp_meta.quality.onboarding_preflight import (
    collect_quality_configuration_errors,
)
from databricks.labs.sdp_meta.quality.registry import (
    deployable_quality_engines,
)
from flask import Blueprint, jsonify, request

logger = logging.getLogger(__name__)

bp = Blueprint('spec_editor', __name__)


@bp.route('/api/metadata/workspace-ls', methods=['GET'])
def workspace_ls():
    """List entries at a workspace path."""
    path = request.args.get('path', '/').strip()
    try:
        from databricks.sdk import WorkspaceClient
        ws = WorkspaceClient()
        entries = []
        for item in (ws.workspace.list(path) or []):
            obj_type = getattr(item, 'object_type', None)
            lang = getattr(item, 'language', None)
            entries.append({
                'path': item.path,
                'object_type': obj_type.value if hasattr(obj_type, 'value') else str(obj_type) if obj_type else None,
                'language': lang.value if hasattr(lang, 'value') else str(lang) if lang else None,
            })
        return jsonify(entries)
    except Exception as exc:
        logger.exception("workspace_ls failed for path=%s", path)
        return jsonify({'error': str(exc)}), 500


@bp.route('/api/metadata/workspace-file', methods=['GET'])
def get_workspace_file():
    """Download a workspace file and return its content as text."""
    path = request.args.get('path', '').strip()
    if not path:
        return jsonify({'error': 'path query parameter is required'}), 400
    try:
        from databricks.sdk import WorkspaceClient
        ws = WorkspaceClient()
        content_bytes = ws.workspace.download(path).read()
        fmt = 'yaml' if path.endswith(('.yml', '.yaml')) else 'json'
        return jsonify({'path': path, 'content': content_bytes.decode('utf-8'), 'format': fmt})
    except Exception as exc:
        logger.exception("get_workspace_file failed for path=%s", path)
        return jsonify({'error': str(exc)}), 500


@bp.route('/api/metadata/workspace-file', methods=['POST'])
def save_workspace_file():
    """Validate and write content to a workspace file path."""
    import yaml as _yaml
    body = request.get_json(silent=True) or {}
    path = body.get('path', '').strip()
    content = body.get('content', '')
    fmt = body.get('format', 'json')

    if not path:
        return jsonify({'error': 'path is required'}), 400

    # Validate before writing \u2014 prevents broken configs reaching
    # the workspace.
    try:
        if fmt == 'yaml':
            _yaml.safe_load(content)
        else:
            json.loads(content)
    except Exception as exc:
        return jsonify({'error': f'Parse error \u2014 content not written: {exc}'}), 400

    try:
        from databricks.sdk import WorkspaceClient
        ws = WorkspaceClient()
        parent = '/'.join(path.rstrip('/').split('/')[:-1])
        if parent:
            ws.workspace.mkdirs(parent)
        content_bytes = content.encode('utf-8')
        ws.workspace.upload(path, io.BytesIO(content_bytes), overwrite=True)
        return jsonify({'path': path, 'bytes_written': len(content_bytes)})
    except Exception as exc:
        logger.exception("save_workspace_file failed for path=%s", path)
        return jsonify({'error': str(exc)}), 500


@bp.route('/api/metadata/parse-spec', methods=['POST'])
def parse_spec():
    """Parse and validate a spec file in up to three layers.

    Layer 1 \u2014 syntax (always runs): JSON/YAML parse.
    Layer 2 \u2014 identifier semantics (runs when sdp-meta wheel is installed).
    Layer 3 \u2014 referenced file existence (cannot run without Spark;
              surfaced as warnings).
    """
    import yaml as _yaml

    body = request.get_json(silent=True) or {}
    content = body.get('content', '')
    fmt = body.get('format', 'json')
    spec_type = body.get('spec_type', 'onboarding')
    env = body.get('env', 'prod')

    # Layer 1 \u2014 syntax
    try:
        parsed = _yaml.safe_load(content) if fmt == 'yaml' else json.loads(content)
    except Exception as exc:
        return jsonify({'error': f'Syntax error: {exc}'}), 400

    errors = []
    warnings = []

    # Layer 2 \u2014 semantics (requires sdp-meta wheel)
    validators_available = False
    _vuc = None
    _vsf = None
    try:
        from databricks.labs.sdp_meta.identifiers import (
            validate_uc_identifier as _vuc_imported,
            validate_source_format as _vsf_imported,
        )
        _vuc = _vuc_imported
        _vsf = _vsf_imported
        validators_available = True
    except ImportError:
        pass

    if spec_type == 'onboarding' and isinstance(parsed, list):
        errors.extend(
            collect_quality_configuration_errors(
                parsed,
                env=env,
                uc_enabled=True,
                supported_engines=deployable_quality_engines(
                    os.getenv("SDP_META_QUALITY_ENGINE_DEPENDENCY")
                ),
            )
        )
        for i, row in enumerate(parsed):
            if not isinstance(row, dict):
                continue
            pfx = f"Row {i} ({row.get('data_flow_id', '?')}): "
            # source_format
            sf = row.get('source_format')
            if sf and _vsf:
                try:
                    _vsf(sf)
                except Exception as e:
                    errors.append(pfx + str(e))
            # table names
            for field in ('bronze_table', 'silver_table'):
                val = row.get(field)
                if val and _vuc:
                    try:
                        _vuc(val, kind=field)
                    except ValueError as e:
                        errors.append(pfx + str(e))
            # catalog/schema qualified names
            for field in (f'bronze_database_{env}', f'silver_database_{env}',
                          f'bronze_catalog_{env}', f'silver_catalog_{env}'):
                val = row.get(field)
                if val and _vuc:
                    for part in str(val).split('.'):
                        part = part.strip()
                        if part:
                            try:
                                _vuc(part, kind=field)
                            except ValueError as e:
                                errors.append(pfx + str(e))
            # append_flows inner source_format
            for flow in row.get('bronze_append_flows', []) or []:
                if isinstance(flow, dict):
                    inner_sf = flow.get('source_format')
                    if inner_sf and _vsf:
                        try:
                            _vsf(inner_sf)
                        except Exception as e:
                            errors.append(pfx + f"bronze_append_flows[{flow.get('name', '?')}].source_format: {e}")
            # CDC scd_type must be string "1" or "2"
            for cdc_key in ('bronze_cdc_apply_changes', 'silver_cdc_apply_changes'):
                cdc = row.get(cdc_key)
                if isinstance(cdc, dict):
                    scd = cdc.get('scd_type')
                    if scd is not None and str(scd) not in ('1', '2'):
                        errors.append(pfx + f"{cdc_key}.scd_type must be '1' or '2', got {scd!r}")
            # Layer 3 \u2014 file reference warnings
            for field in (f'bronze_data_quality_expectations_json_{env}',
                          f'bronze_quality_rules_path_{env}',
                          f'silver_quality_rules_path_{env}',
                          f'silver_transformation_json_{env}',
                          'source_schema_path'):
                if row.get(field):
                    warnings.append(
                        f"{pfx}'{field}' path '{row[field]}' cannot be verified without "
                        "a Spark session \u2014 validated at onboarding job runtime."
                    )

    elif spec_type == 'dqe':
        valid_actions = {'expect', 'expect_or_drop', 'expect_or_quarantine', 'expect_or_fail'}
        if not isinstance(parsed, dict):
            errors.append("DQE spec must be an object at the top level")
        else:
            for key, rules in parsed.items():
                if key not in valid_actions:
                    errors.append(f"Unknown DQE action '{key}'. Valid: {sorted(valid_actions)}")
                    continue
                if not isinstance(rules, dict):
                    errors.append(
                        f"'{key}' must be a dict of "
                        f"{{rule_name: sql_expression}}, got "
                        f"{type(rules).__name__}"
                    )
                    continue
                for rule_name, expr in rules.items():
                    if not isinstance(expr, str) or not expr.strip():
                        errors.append(f"'{key}.{rule_name}': expression must be a non-empty string")

    elif spec_type == 'silver_transform':
        if not isinstance(parsed, list):
            errors.append("Silver transformation spec must be an array")
        else:
            for i, entry in enumerate(parsed):
                pfx = f"Entry {i}: "
                if not isinstance(entry, dict):
                    errors.append(pfx + "must be an object")
                    continue
                if 'target_table' not in entry:
                    errors.append(pfx + "missing 'target_table'")
                elif _vuc:
                    try:
                        _vuc(entry['target_table'], kind='target_table')
                    except ValueError as e:
                        errors.append(pfx + str(e))
                if 'select_exp' not in entry:
                    errors.append(pfx + "missing 'select_exp'")
                elif not isinstance(entry['select_exp'], list) or not entry['select_exp']:
                    errors.append(pfx + "'select_exp' must be a non-empty list of strings")

    return jsonify({
        'parsed': parsed,
        'errors': errors,
        'warnings': warnings,
        'spec_type': spec_type,
        'validators_available': validators_available,
        'layer3_note': (
            "File-reference fields (DQE paths, schema DDL paths, "
            "silver transformation paths) require a Spark session to "
            "verify. They are surfaced as warnings only."
        ),
    })
