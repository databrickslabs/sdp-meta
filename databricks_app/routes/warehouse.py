"""``/api/warehouse/*`` \u2014 SQL warehouse configuration.

Three routes for the top-bar warehouse picker:

  * ``GET  /api/warehouse/status``    \u2014 current active warehouse + state.
  * ``GET  /api/warehouse/list``      \u2014 all warehouses visible to the App SP.
  * ``POST /api/warehouse/configure`` \u2014 set the runtime warehouse override
                                        (use existing by ID, or create a
                                        new serverless one).
"""

from __future__ import annotations

import logging

from flask import Blueprint, jsonify, request

from _config import _get_warehouse_id, _set_runtime_warehouse_id
from _errors import exception_response, friendly_message

logger = logging.getLogger(__name__)

bp = Blueprint('warehouse', __name__)


@bp.route('/api/warehouse/status', methods=['GET'])
def warehouse_status():
    """Return the currently active warehouse ID and its state."""
    wh_id = _get_warehouse_id()
    if not wh_id:
        return jsonify({'configured': False, 'warehouse_id': None, 'name': None, 'state': None})
    try:
        from databricks.sdk import WorkspaceClient
        ws = WorkspaceClient()
        wh = ws.warehouses.get(wh_id)
        state = getattr(wh, 'state', None)
        return jsonify({
            'configured': True,
            'warehouse_id': wh_id,
            'name': wh.name,
            'state': state.value if hasattr(state, 'value') else str(state) if state else None,
        })
    except Exception as exc:
        logger.exception("warehouse_status failed for id=%s", wh_id)
        return jsonify({
            'configured': True, 'warehouse_id': wh_id,
            'name': None, 'state': None,
            'error': friendly_message(exc, 'load the SQL warehouse'),
            'details': f'{type(exc).__name__}: {exc}',
        })


@bp.route('/api/warehouse/list', methods=['GET'])
def list_warehouses():
    """List all SQL warehouses available to the App SP."""
    try:
        from databricks.sdk import WorkspaceClient
        ws = WorkspaceClient()
        warehouses = []
        for w in ws.warehouses.list():
            state = getattr(w, 'state', None)
            wh_type = getattr(w, 'warehouse_type', None)
            warehouses.append({
                'id': w.id,
                'name': w.name,
                'state': state.value if hasattr(state, 'value') else str(state) if state else None,
                'cluster_size': getattr(w, 'cluster_size', None),
                'warehouse_type': wh_type.value if hasattr(wh_type, 'value') else str(wh_type) if wh_type else None,
            })
        return jsonify(warehouses)
    except Exception as exc:
        logger.exception("list_warehouses failed")
        return exception_response(exc, 'list SQL warehouses')


@bp.route('/api/warehouse/configure', methods=['POST'])
def configure_warehouse():
    """Set the active warehouse for this session.

    Body (JSON):
        mode: "existing" \u2014 use an existing warehouse by ID
            warehouse_id: str  (required)
        mode: "create" \u2014 spin up a new serverless warehouse
            name: str  (optional, default: "sdp-meta-app-warehouse")
    """
    body = request.get_json(silent=True) or {}
    mode = body.get('mode', 'existing')

    if mode == 'existing':
        wh_id = body.get('warehouse_id', '').strip()
        if not wh_id:
            return jsonify({'error': 'warehouse_id is required'}), 400
        try:
            from databricks.sdk import WorkspaceClient
            ws = WorkspaceClient()
            wh = ws.warehouses.get(wh_id)
            _set_runtime_warehouse_id(wh_id)
            state = getattr(wh, 'state', None)
            state_val = state.value if hasattr(state, 'value') else str(state) if state else None
            logger.info("Warehouse configured (existing): id=%s name=%s", wh_id, wh.name)
            return jsonify({
                'warehouse_id': wh_id,
                'name': wh.name,
                'state': state_val,
                'message': f"Warehouse \"{wh.name}\" configured successfully.",
            })
        except Exception as exc:
            logger.exception("configure_warehouse (existing) failed for id=%s", wh_id)
            return exception_response(exc, 'select the SQL warehouse', 400)

    elif mode == 'create':
        name = (body.get('name') or 'sdp-meta-app-warehouse').strip()
        try:
            from databricks.sdk import WorkspaceClient
            from databricks.sdk.service.sql import CreateWarehouseRequestWarehouseType
            ws = WorkspaceClient()
            wh = ws.warehouses.create(
                name=name,
                cluster_size='2X-Small',
                warehouse_type=CreateWarehouseRequestWarehouseType.PRO,
                enable_serverless_compute=True,
                auto_stop_mins=30,
                max_num_clusters=1,
            )
            _set_runtime_warehouse_id(wh.id)
            logger.info("Warehouse created: id=%s name=%s", wh.id, name)
            return jsonify({
                'warehouse_id': wh.id,
                'name': name,
                'state': 'STARTING',
                'message': (
                    f"Serverless warehouse \"{name}\" is starting. "
                    "It will be ready in ~30 seconds. "
                    f"To make this permanent, add DATABRICKS_SQL_WAREHOUSE_ID: {wh.id} to app.yaml."
                ),
            })
        except Exception as exc:
            logger.exception("configure_warehouse (create) failed")
            return exception_response(exc, 'create the SQL warehouse')

    else:
        return jsonify({'error': f'Unknown mode: {mode}. Use "existing" or "create".'}), 400
