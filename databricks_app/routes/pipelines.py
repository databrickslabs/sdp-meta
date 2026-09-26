"""``/api/pipelines/*`` \u2014 SDP-META pipeline monitor + control surface.

Four read-mostly routes for the Monitor tab:

  * ``GET  /api/pipelines``                        \u2014 list all SDP-META
                                                     pipelines in the workspace.
  * ``GET  /api/pipelines/<id>/events``            \u2014 last 50 events.
  * ``POST /api/pipelines/<id>/start``             \u2014 trigger an update.
  * ``POST /api/pipelines/<id>/stop``              \u2014 stop a running pipeline.
"""

from __future__ import annotations

import logging

from flask import Blueprint, jsonify

from _errors import exception_response

logger = logging.getLogger(__name__)

bp = Blueprint('pipelines', __name__)


_SDP_META_CONFIG_KEYS = frozenset({
    "bronze.dataflowspecTable",
    "silver.dataflowspecTable",
    "bronze.group",
    "silver.group",
})


def _spec_of(pipeline_detail):
    """Return the ``PipelineSpec``-shaped object for either SDK return type.

    Databricks SDK quirk:
      * ``ws.pipelines.get(id)`` returns ``GetPipelineResponse``; its
        ``tags`` and ``configuration`` live under a nested ``.spec``
        attribute (a ``PipelineSpec``).
      * ``ws.pipelines.list_pipelines()`` yields ``PipelineStateInfo`` \u2014
        a flat summary with no ``.spec`` and no tags / configuration at all.
      * Tests and some serialised envelopes flatten the spec directly onto
        the top-level object.

    This helper normalises all three by preferring the nested ``.spec``
    when present and falling back to the object itself.
    """
    spec = getattr(pipeline_detail, "spec", None)
    return spec if spec is not None else pipeline_detail


_LEGACY_SENTINEL_TAG_VALUE = "true"


def _sdp_meta_tag_value(pipeline_detail):
    """Return the raw ``sdp_meta`` tag value (string) or ``None``.

    Used both by the SDP-META filter and by the response builder so the
    Monitor UI can surface which version of SDP-META created each pipeline.
    """
    spec = _spec_of(pipeline_detail)
    tags = getattr(spec, "tags", None) or {}
    if not isinstance(tags, dict):
        return None
    value = tags.get("sdp_meta")
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def _is_sdp_meta(pipeline_detail) -> bool:
    """Return ``True`` if this pipeline was created by SDP-META.

    Two-stage check (priority order):
      1. Tag ``sdp_meta=<value>`` \u2014 written by ``_create_sdp_meta_pipeline``.
         Any non-empty value is treated as a match. Today the producer
         writes the SDP-META version (e.g. ``"0.1.0"``); the historical
         sentinel ``"true"`` is also accepted for back-compat with
         pipelines created by older releases.
      2. Config-key fallback \u2014 covers pipelines created before tagging
         was introduced at all.

    Both checks read from the nested ``PipelineSpec`` when the input is a
    ``GetPipelineResponse``; fall back to the top-level object for
    ``PipelineStateInfo`` summaries and spec-flattened JSON.
    """
    if _sdp_meta_tag_value(pipeline_detail):
        return True
    spec = _spec_of(pipeline_detail)
    config = getattr(spec, "configuration", None) or {}
    if not isinstance(config, dict):
        return False
    return bool(_SDP_META_CONFIG_KEYS & set(config.keys()))


def _sdp_meta_version(pipeline_detail):
    """Return the SDP-META version that created this pipeline, or ``None``.

    Resolution order:
      1. ``sdp_meta`` tag value when it looks like a version (anything
         other than the legacy ``"true"`` sentinel).
      2. ``configuration["version"]`` \u2014 SDP-META has written this since
         the very first release, so it's a reliable fallback for legacy
         pipelines tagged ``sdp_meta=true``.
      3. ``None`` \u2014 pipeline detected via the config-key fallback but
         no explicit version was recorded.
    """
    tag_value = _sdp_meta_tag_value(pipeline_detail)
    if tag_value and tag_value != _LEGACY_SENTINEL_TAG_VALUE:
        return tag_value
    spec = _spec_of(pipeline_detail)
    config = getattr(spec, "configuration", None) or {}
    if isinstance(config, dict):
        config_version = config.get("version")
        if config_version:
            return str(config_version).strip() or None
    return None


def _workspace_pipeline_url(host, pipeline_id):
    """Build the Databricks UI URL for a pipeline.

    Returns ``None`` when the workspace host can't be resolved (e.g. the
    ``WorkspaceClient`` was constructed against a config without a host),
    so the frontend can gracefully fall back to the in-app events drawer.

    URL shape: ``<host>/pipelines/<pipeline_id>``. The control plane
    redirects this to the latest update view, which is what users almost
    always want when they click through from the Monitor table.
    """
    if not host or not pipeline_id:
        return None
    host = str(host).rstrip('/')
    if not host:
        return None
    return f"{host}/pipelines/{pipeline_id}"


@bp.route('/api/pipelines', methods=['GET'])
def list_pipelines():
    """Return all SDP-META pipelines in the workspace."""
    try:
        from databricks.sdk import WorkspaceClient
        ws = WorkspaceClient()
        # Resolve the workspace host ONCE per request and re-use across all
        # pipeline rows. ws.config.host typically reads from the auth env;
        # if it isn't populated (rare \u2014 e.g. test-time mocks), every row
        # falls back to pipeline_url=None and the frontend renders the
        # old in-app-events click target instead of an external link.
        host = getattr(getattr(ws, 'config', None), 'host', None)
        pipelines = []
        for p in ws.pipelines.list_pipelines():
            try:
                detail = ws.pipelines.get(p.pipeline_id)
            except Exception:
                detail = p
            if not _is_sdp_meta(detail):
                continue
            state = getattr(p, 'state', None)
            spec = _spec_of(detail)
            pipelines.append({
                'id': p.pipeline_id,
                'name': p.name,
                'state': state.value if hasattr(state, 'value') else str(state) if state else None,
                'creator': getattr(detail, 'creator_user_name', None),
                'last_modified': getattr(detail, 'last_modified', None),
                'sdp_meta_config': getattr(spec, 'configuration', {}) or {},
                'sdp_meta_version': _sdp_meta_version(detail),
                'pipeline_url': _workspace_pipeline_url(host, p.pipeline_id),
            })
        return jsonify(pipelines)
    except Exception as exc:
        logger.exception("list_pipelines failed")
        return exception_response(exc, 'load pipelines')


@bp.route('/api/pipelines/<pipeline_id>/events', methods=['GET'])
def pipeline_events(pipeline_id):
    """Return the last 50 events for a pipeline (most recent first)."""
    try:
        from databricks.sdk import WorkspaceClient
        ws = WorkspaceClient()
        events = []
        for e in ws.pipelines.list_pipeline_events(pipeline_id):
            level = getattr(e, 'level', None)
            events.append({
                'timestamp': getattr(e, 'timestamp', None),
                'event_type': getattr(e, 'event_type', None),
                'message': getattr(e.message, 'msg', None) if getattr(e, 'message', None) else None,
                'level': level.value if hasattr(level, 'value') else str(level) if level else None,
            })
            if len(events) >= 50:
                break
        return jsonify(events)
    except Exception as exc:
        logger.exception("pipeline_events failed for %s", pipeline_id)
        return exception_response(exc, 'load pipeline events')


@bp.route('/api/pipelines/<pipeline_id>/start', methods=['POST'])
def start_pipeline(pipeline_id):
    """Trigger a pipeline update (start)."""
    try:
        from databricks.sdk import WorkspaceClient
        ws = WorkspaceClient()
        ws.pipelines.start_update(pipeline_id)
        return jsonify({'status': 'started', 'pipeline_id': pipeline_id})
    except Exception as exc:
        logger.exception("start_pipeline failed for %s", pipeline_id)
        return exception_response(exc, 'start the pipeline')


@bp.route('/api/pipelines/<pipeline_id>/stop', methods=['POST'])
def stop_pipeline(pipeline_id):
    """Stop a running pipeline."""
    try:
        from databricks.sdk import WorkspaceClient
        ws = WorkspaceClient()
        ws.pipelines.stop(pipeline_id)
        return jsonify({'status': 'stopped', 'pipeline_id': pipeline_id})
    except Exception as exc:
        logger.exception("stop_pipeline failed for %s", pipeline_id)
        return exception_response(exc, 'stop the pipeline')
