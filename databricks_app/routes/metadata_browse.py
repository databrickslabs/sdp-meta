"""``/api/metadata/{catalogs,schemas,tables,table-data}`` \u2014 UC browse.

Read-only catalog/schema/table tree-walking endpoints for the
Metadata Browse tab. ``table-data`` runs a SELECT * via the
Statement Execution API.
"""

from __future__ import annotations

import logging
import time

from flask import Blueprint, jsonify, request

from _config import _get_warehouse_id
from _errors import exception_response, friendly_message

try:
    from databricks.labs.sdp_meta.identifiers import (
        validate_sql_where_clause,
        validate_uc_identifier,
    )
except ImportError:  # pragma: no cover
    def validate_uc_identifier(name, *, kind: str = "identifier") -> str:
        return name

    def validate_sql_where_clause(value, *, kind: str = "where_clause") -> str:
        # Local-dev fallback: when the wheel isn't installed, refuse any
        # non-empty WHERE clause rather than allow unsafe passthrough.
        if value:
            raise ValueError(
                "where_clause validation is unavailable because the "
                "sdp-meta wheel is not installed; clear the filter to "
                "preview the table."
            )
        return ""


logger = logging.getLogger(__name__)

bp = Blueprint('metadata_browse', __name__)


@bp.route('/api/metadata/catalogs', methods=['GET'])
def list_catalogs():
    """Return all UC catalogs accessible to the App SP."""
    try:
        from databricks.sdk import WorkspaceClient
        ws = WorkspaceClient()
        catalogs = [c.name for c in ws.catalogs.list() if c.name]
        return jsonify(sorted(catalogs))
    except Exception as exc:
        logger.exception("list_catalogs failed")
        return exception_response(exc, 'list Unity Catalog catalogs')


@bp.route('/api/metadata/schemas', methods=['GET'])
def list_schemas():
    """Return all schemas in a UC catalog."""
    catalog = request.args.get('catalog', '').strip()
    if not catalog:
        return jsonify({'error': 'catalog query parameter is required'}), 400
    try:
        from databricks.sdk import WorkspaceClient
        ws = WorkspaceClient()
        schemas = [s.name for s in ws.schemas.list(catalog_name=catalog) if s.name]
        return jsonify(sorted(schemas))
    except Exception as exc:
        logger.exception("list_schemas failed for catalog=%s", catalog)
        return exception_response(exc, 'list Unity Catalog schemas')


@bp.route('/api/metadata/tables', methods=['GET'])
def list_tables():
    """Return tables with column info for a catalog.schema."""
    catalog = request.args.get('catalog', '').strip()
    schema = request.args.get('schema', '').strip()
    if not catalog or not schema:
        return jsonify({'error': 'catalog and schema query parameters are required'}), 400
    try:
        from databricks.sdk import WorkspaceClient
        ws = WorkspaceClient()
        tables = []
        for t in ws.tables.list(catalog_name=catalog, schema_name=schema):
            table_type = getattr(t, 'table_type', None)
            columns = []
            for c in (getattr(t, 'columns', None) or []):
                columns.append({'name': c.name, 'type_text': getattr(c, 'type_text', '')})
            if hasattr(table_type, 'value'):
                tt = table_type.value
            else:
                tt = str(table_type) if table_type else None
            tables.append({
                'name': t.name,
                'table_type': tt,
                'columns': columns,
            })
        return jsonify(tables)
    except Exception as exc:
        logger.exception("list_tables failed for %s.%s", catalog, schema)
        return exception_response(exc, 'list Unity Catalog tables')


@bp.route('/api/metadata/table-data', methods=['POST'])
def table_data():
    """Execute SELECT * FROM <catalog>.<schema>.<table> via Statement
    Execution API."""
    body = request.get_json(silent=True) or {}
    catalog = body.get('catalog', '').strip()
    schema = body.get('schema', '').strip()
    table = body.get('table', '').strip()
    where_clause = body.get('where_clause', '').strip()

    # Validate `limit` up front: a non-numeric string, ``null``, or a
    # non-positive value should produce a 400 with an actionable message,
    # not bubble out of ``int()`` as a 500 from the outer SQL try/except
    # (which doesn't wrap this line).
    raw_limit = body.get('limit', 100)
    try:
        limit = int(raw_limit)
    except (TypeError, ValueError):
        return jsonify({
            'error': f"limit must be an integer (got {raw_limit!r})"
        }), 400
    if limit <= 0:
        return jsonify({
            'error': f"limit must be a positive integer (got {limit})"
        }), 400
    limit = min(limit, 1000)

    # Validate every user-supplied component BEFORE any external
    # resource check (warehouse config etc.) so a malformed input
    # always surfaces the actionable validation error, not "no
    # warehouse" \u2014 the user's first task is to fix the input.
    for kind, val in [('catalog', catalog), ('schema', schema), ('table', table)]:
        if not val:
            return jsonify({'error': f'{kind} is required'}), 400
        try:
            validate_uc_identifier(val, kind=kind)
        except ValueError as exc:
            return jsonify({'error': str(exc)}), 400

    # Reject WHERE clauses that contain SQL statement separators,
    # comments, or DDL/DML/set-operation keywords. The Databricks
    # Statement Execution API can't parameterise a structural WHERE
    # expression, so denylist-validation at the App boundary is the
    # only place to stop ``'; DROP TABLE x --`` and
    # ``UNION SELECT * FROM system.\u2026`` style injection. See
    # ``validate_sql_where_clause`` for the full rule set.
    try:
        where_clause = validate_sql_where_clause(where_clause, kind='where_clause')
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400

    warehouse_id = _get_warehouse_id()
    if not warehouse_id:
        return jsonify({
            'error': 'No SQL warehouse configured. Use the Warehouse '
                     'button in the top bar to set one.'
        }), 400

    # Build a safe SELECT \u2014 table identifier components are validated
    # above and the WHERE clause (if any) is denylist-checked.
    sql = f"SELECT * FROM `{catalog}`.`{schema}`.`{table}`"
    if where_clause:
        sql += f" WHERE {where_clause}"
    sql += f" LIMIT {limit}"

    try:
        from databricks.sdk import WorkspaceClient
        ws = WorkspaceClient()
        result = ws.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=sql,
            wait_timeout='30s',
        )
        # Poll until terminal state (execute_statement with
        # wait_timeout may still return PENDING).
        terminal = {'SUCCEEDED', 'FAILED', 'CANCELED', 'CLOSED'}
        for _ in range(60):
            state = result.status.state
            state_val = state.value if hasattr(state, 'value') else str(state)
            if state_val in terminal:
                break
            time.sleep(1)
            result = ws.statement_execution.get_statement(result.statement_id)

        state = result.status.state
        state_val = state.value if hasattr(state, 'value') else str(state)
        if state_val != 'SUCCEEDED':
            err = getattr(result.status, 'error', None)
            msg = getattr(err, 'message', None) if err else 'Query failed'
            detail = msg or f'Query returned {state_val}'
            return jsonify({
                'error': friendly_message(RuntimeError(detail), 'preview table data'),
                'details': detail, 'state': state_val,
            }), 400

        schema_obj = result.manifest.schema if result.manifest else None
        columns = [c.name for c in (schema_obj.columns if schema_obj else [])]
        data_array = result.result.data_array if result.result else []
        rows = [list(r) for r in (data_array or [])]
        return jsonify({'columns': columns, 'rows': rows})
    except Exception as exc:
        logger.exception("table_data query failed: %s", sql)
        return exception_response(exc, 'preview table data')
