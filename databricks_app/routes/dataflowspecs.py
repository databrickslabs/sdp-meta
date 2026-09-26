"""``GET /api/dataflowspecs`` \u2014 DataflowSpec table browser.

Single-route blueprint. Queries the bronze + silver DataflowSpec
tables produced by onboarding and returns rows grouped by
``data_flow_group``.
"""

from __future__ import annotations

import logging
import time

from flask import Blueprint, jsonify, request

from _config import _get_warehouse_id
from _errors import friendly_message

try:
    from databricks.labs.sdp_meta.identifiers import validate_uc_identifier
except ImportError:  # pragma: no cover
    def validate_uc_identifier(name, *, kind: str = "identifier") -> str:
        return name


logger = logging.getLogger(__name__)

bp = Blueprint('dataflowspecs', __name__)


@bp.route('/api/dataflowspecs', methods=['GET'])
def get_dataflowspecs():
    """Query bronze and silver DataflowSpec tables and return rows
    grouped by ``data_flow_group``.

    Query params:
        catalog        \u2014 UC catalog name (validated)
        schema         \u2014 UC schema name  (validated)
        bronze_table   \u2014 bronze DataflowSpec table (default: bronze_dataflowspec)
        silver_table   \u2014 silver DataflowSpec table (default: silver_dataflowspec)

    Returns:
        {
          bronze: {columns, rows, groups, error},
          silver: {columns, rows, groups, error},
          catalog, schema
        }
    """
    catalog = request.args.get('catalog', '').strip()
    schema = request.args.get('schema', '').strip()
    bronze_table = request.args.get('bronze_table', 'bronze_dataflowspec').strip()
    silver_table = request.args.get('silver_table', 'silver_dataflowspec').strip()

    if not catalog or not schema:
        return jsonify({'error': 'catalog and schema query parameters are required'}), 400

    warehouse_id = _get_warehouse_id()
    if not warehouse_id:
        return jsonify({
            'error': 'No SQL warehouse configured. Use the Warehouse button in the top bar to set one.'
        }), 400

    for kind, val in [('catalog', catalog), ('schema', schema),
                      ('bronze_table', bronze_table), ('silver_table', silver_table)]:
        try:
            validate_uc_identifier(val, kind=kind)
        except ValueError as exc:
            return jsonify({'error': str(exc)}), 400

    def _run_query(table_name):
        """Execute SELECT * on a DataflowSpec table.

        Returns ``{columns, rows, groups, error}``.
        """
        sql = (
            f"SELECT * FROM `{catalog}`.`{schema}`.`{table_name}` "
            f"ORDER BY dataFlowGroup, dataFlowId"
        )
        try:
            from databricks.sdk import WorkspaceClient
            ws = WorkspaceClient()
            result = ws.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=sql,
                wait_timeout='30s',
            )
            terminal = {'SUCCEEDED', 'FAILED', 'CANCELED', 'CLOSED'}
            for _ in range(60):
                state_val = (result.status.state.value
                             if hasattr(result.status.state, 'value')
                             else str(result.status.state))
                if state_val in terminal:
                    break
                time.sleep(1)
                result = ws.statement_execution.get_statement(result.statement_id)

            state_val = (result.status.state.value
                         if hasattr(result.status.state, 'value')
                         else str(result.status.state))
            if state_val != 'SUCCEEDED':
                err = getattr(result.status, 'error', None)
                msg = getattr(err, 'message', str(err)) if err else f'Query returned {state_val}'
                # Translate common SQL errors into plain English
                if msg and 'TABLE_OR_VIEW_NOT_FOUND' in msg:
                    friendly = (
                        f"Table `{catalog}`.`{schema}`.`{table_name}` not found. "
                        f"Run onboarding first to create the DataflowSpec tables."
                    )
                    return {'columns': [], 'rows': [], 'groups': [], 'error': friendly, 'not_found': True}
                return {
                    'columns': [], 'rows': [], 'groups': [],
                    'error': friendly_message(RuntimeError(msg), 'load DataflowSpec rows'),
                    'details': msg,
                }

            schema_obj = result.manifest.schema if result.manifest else None
            columns = [c.name for c in (schema_obj.columns if schema_obj else [])]
            data_array = result.result.data_array if result.result else []
            rows = [list(r) for r in (data_array or [])]

            # Extract unique groups \u2014 case-insensitive lookup because
            # some SQL warehouses lowercase result schema column names
            # (dataFlowGroup \u2192 dataflowgroup).
            col_lower = [c.lower() for c in columns]
            group_idx = col_lower.index('dataflowgroup') if 'dataflowgroup' in col_lower else -1
            groups = []
            seen: set = set()
            for row in rows:
                g = row[group_idx] if group_idx >= 0 and group_idx < len(row) else None
                if g and g not in seen:
                    seen.add(g)
                    groups.append(g)

            return {'columns': columns, 'rows': rows, 'groups': groups, 'error': None}
        except Exception as exc:
            logger.exception("DataflowSpec query failed for %s.%s.%s", catalog, schema, table_name)
            return {
                'columns': [], 'rows': [], 'groups': [],
                'error': friendly_message(exc, 'load DataflowSpec rows'),
                'details': f'{type(exc).__name__}: {exc}',
            }

    bronze_result = _run_query(bronze_table)
    silver_result = _run_query(silver_table)

    return jsonify({
        'catalog': catalog,
        'schema': schema,
        'bronze_table': bronze_table,
        'silver_table': silver_table,
        'bronze': bronze_result,
        'silver': silver_result,
    })
