"""Required-files preflight for the onboarding preview UI.

Two pure helpers driven by ``/onboarding/preview``:

  * ``_extract_required_files`` \u2014 walks a parsed onboarding spec and
    returns every file path the spec references via a path-shape field
    (``source_path*``, ``source_schema_path``, ``*_data_quality_
    expectations_json_*``, ``silver_transformation_json_*``, plus same
    field names nested inside ``bronze_append_flows`` /
    ``silver_append_flows``), with ``{token}`` substitutions applied so
    paths match what the cluster will actually read.

  * ``_check_required_files_existence`` \u2014 for each extracted entry,
    decides whether the referenced file exists today (local FS, UC
    Volume, or unknown / can't classify).

Drives the "Required files" panel on the preview modal so users see
red-X / green-check status BEFORE clicking Run \u2014 the second-most-
common cause of "job SUCCESS, tables empty" is missing supporting
files (the first being env-suffix mismatch).
"""

from __future__ import annotations

import os


def _extract_required_files(parsed_spec, substitutions: dict):
    """Walk a parsed onboarding spec and return a list of files it
    references via path-shape fields, with the ``{token}`` substitutions
    applied so paths match what the cluster will actually read.

    Recognised path-shape fields:
      * Anything whose key starts with ``source_path`` (raw or env-suffixed).
      * Anything whose key is ``source_schema_path``.
      * Anything whose key starts with ``bronze_data_quality_expectations_json``,
        ``silver_data_quality_expectations_json``,
        ``bronze_quality_rules_path``, ``silver_quality_rules_path``, or
        ``silver_transformation_json`` (raw or env-suffixed).
      * Anything inside ``source_details`` for ``bronze_append_flows`` or
        ``silver_append_flows`` that follows the same naming.

    Returns a list of dicts:
      [
        {"entity": "data_flow_id=100",
         "field": "source_path_demo",
         "path": "/Volumes/.../sdp_meta_conf/demo/resources/data/customers"},
        ...
      ]

    The caller decides how to check existence \u2014 the resolver
    (``_check_required_files_existence`` below) handles both local and
    UC Volume paths.
    """
    if not isinstance(parsed_spec, list):
        return []

    path_field_markers = (
        "source_path",
        "source_schema_path",
        "bronze_data_quality_expectations_json",
        "silver_data_quality_expectations_json",
        "bronze_quality_rules_path",
        "silver_quality_rules_path",
        "silver_transformation_json",
    )

    def _apply_subs(raw: str) -> str:
        for token, value in substitutions.items():
            raw = raw.replace(token, value if value is not None else "")
        # Collapse accidental ``//`` introduced when ``{uc_volume_path}``
        # ends in ``/`` and the spec line starts with ``/`` \u2014
        # functionally equivalent on POSIX / UC Volumes but cosmetically
        # ugly in the preview UI. Don't touch URL-style ``://`` prefixes
        # (onboarding paths never use them, but be defensive).
        if "://" not in raw:
            while "//" in raw:
                raw = raw.replace("//", "/")
        return raw

    def _is_path_field(key: str) -> bool:
        return any(key == m or key.startswith(m + "_") for m in path_field_markers)

    def _collect_from_dict(d, entity_label, out):
        if not isinstance(d, dict):
            return
        for k, v in d.items():
            if isinstance(v, str) and _is_path_field(k):
                # Only keep substitution-resolved values \u2014 raw
                # ``{uc_volume_path}/...`` strings can't be existence-
                # checked. Skip silently if the value is empty after
                # subs (likely a placeholder we don't know how to fill).
                resolved = _apply_subs(v).strip()
                if resolved and "{" not in resolved:
                    out.append({
                        "entity": entity_label,
                        "field": k,
                        "path": resolved,
                    })
            elif isinstance(v, dict):
                _collect_from_dict(v, entity_label, out)
            elif isinstance(v, list):
                for item in v:
                    _collect_from_dict(item, entity_label, out)

    out = []
    for row in parsed_spec:
        if not isinstance(row, dict):
            continue
        entity_id = row.get("data_flow_id") or row.get("bronze_table") or "<unnamed>"
        entity_label = f"data_flow_id={entity_id}"
        _collect_from_dict(row, entity_label, out)
    return out


def _check_required_files_existence(required, uc_volume_path: str,
                                    local_supporting_dir: str,
                                    ws_factory=None):
    """For each entry in ``required`` (output of ``_extract_required_files``)
    decide whether the referenced file exists today.

    Strategy per entry:

    1. If the path starts with the resolved ``{uc_volume_path}`` value
       AND the user provided a UC Volume ``local_supporting_dir``, the
       file SHOULD exist under ``local_supporting_dir`` (post-copy).
       Check via SDK ``files.get_metadata``.

    2. If the path starts with ``uc_volume_path`` AND
       ``local_supporting_dir`` is a local path, strip the
       ``uc_volume_path`` prefix to get the relative-under-sdp_meta_conf,
       then check ``parent(local_supporting_dir)/<rel>``. The CLI's
       ``copy_to_uc_volume`` copies a directory into the volume rooted
       at ``basename(local_supporting_dir)``, so the local equivalent
       is ``parent(local_supporting_dir)/<rel>``.

    3. Otherwise the path is opaque (raw DBFS, raw absolute, etc.) \u2014
       mark as ``unknown`` and don't fail the preflight on its account.

    ``ws_factory`` is for test injection \u2014 callers in production let
    it default to ``WorkspaceClient`` from the Databricks SDK.
    """
    if not required:
        return []

    uc_volume_path_norm = (uc_volume_path or "").rstrip("/")
    local_is_volume = (local_supporting_dir or "").startswith("/Volumes/")
    local_dir_abs = local_supporting_dir or ""

    ws = None
    if local_is_volume and ws_factory is not None:
        ws = ws_factory()
    elif local_is_volume:
        # Lazy import \u2014 only construct WorkspaceClient when needed
        # so the helper is unit-testable without Databricks creds.
        try:
            from databricks.sdk import WorkspaceClient
            ws = WorkspaceClient()
        except Exception:  # pragma: no cover \u2014 SDK init failure
            ws = None

    results = []
    for item in required:
        path = item["path"]
        entry = dict(item)  # don't mutate input

        # Try to express the path relative to uc_volume_path so we can
        # locate the local equivalent. If it doesn't start with the
        # volume root, we can't preflight it (unknown).
        rel = None
        if uc_volume_path_norm and path.startswith(uc_volume_path_norm):
            rel = path[len(uc_volume_path_norm):].lstrip("/")

        if rel is None:
            entry["exists"] = None
            entry["check_path"] = path
            entry["reason"] = "Path is not under {uc_volume_path}; can't preflight."
            results.append(entry)
            continue

        if local_is_volume and ws is not None:
            check_path = f"{local_dir_abs.rstrip('/')}/{rel}"
            try:
                ws.files.get_metadata(check_path)
                entry["exists"] = True
            except Exception:
                entry["exists"] = False
            entry["check_path"] = check_path
        else:
            # Local supporting dir. ``copy_to_uc_volume`` makes
            # ``basename(local_dir)`` the first segment under
            # ``sdp_meta_conf/``, so the local file at REL with that
            # first segment stripped lives under ``parent(local_dir)``.
            parts = rel.split("/", 1)
            first_seg = parts[0] if parts else rel
            parent = os.path.dirname(os.path.normpath(local_dir_abs)) or local_dir_abs
            local_basename = os.path.basename(os.path.normpath(local_dir_abs))
            if first_seg == local_basename:
                check_path = os.path.join(parent, rel)
            else:
                # User's directory name doesn't match the template's
                # first-segment expectation. Check as-is under their
                # local dir AND under parent, prefer the one that exists.
                cand_a = os.path.join(local_dir_abs, rel)
                cand_b = os.path.join(parent, rel)
                check_path = cand_a if os.path.exists(cand_a) else cand_b
            entry["check_path"] = check_path
            entry["exists"] = os.path.exists(check_path)
        results.append(entry)
    return results
