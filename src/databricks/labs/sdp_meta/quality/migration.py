"""Managed selective full-refresh execution for quality-engine migrations."""

from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass

from pyspark.sql import functions as f


TERMINAL_SUCCESS_STATES = {"COMPLETED"}
TERMINAL_FAILURE_STATES = {"FAILED", "CANCELED"}
logger = logging.getLogger("databricks.labs.sdp_meta")


def migration_fingerprint(config):
    """Return a stable identity for one diagnostic-schema migration."""
    payload = {
        "engine": config.get("engine"),
        "diagnostic_schema_fingerprint": config.get(
            "diagnostic_schema_fingerprint"
        ),
        "quarantine": (
            config.get("output_targets", {}).get("quarantine", {})
        ),
    }
    canonical = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), default=str
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def reconcile_migration_request(current, previous):
    """Avoid rearming an unchanged migration already completed."""
    if current.get("migration") != "full_refresh" or not previous:
        return current
    fingerprint = (
        current.get("migration_fingerprint")
        or migration_fingerprint(current)
    )
    if previous.get("completed_migration_fingerprint") != fingerprint:
        return current
    reconciled = dict(current)
    reconciled["migration"] = None
    reconciled["migration_fingerprint"] = None
    reconciled["completed_migration_fingerprint"] = fingerprint
    return reconciled


@dataclass(frozen=True)
class PendingQualityMigration:
    """A pending metadata row and its selective-refresh dataset."""

    spec_table: object
    dataflow_id: str
    original_config_json: str
    completed_config_json: str
    fingerprint: str
    quarantine_target: dict


def _state_value(update):
    state = getattr(update, "state", None)
    return getattr(state, "value", state)


def _quarantine_dataset(target, direct_publishing):
    if not direct_publishing:
        return target["table"]
    parts = [
        target.get("catalog"),
        target.get("database"),
        target.get("table"),
    ]
    return ".".join(str(part) for part in parts if part)


def _target_identity(target):
    return tuple(
        (target or {}).get(field) or None
        for field in ("catalog", "database", "table", "path")
    )


def _read_spec_frame(spark, reference):
    """Read a spec table by metastore name or explicit Delta path."""
    if isinstance(reference, dict):
        path = reference.get("path")
        if not path:
            raise ValueError("Delta spec path reference requires 'path'")
        return spark.read.format("delta").load(path)
    return spark.table(reference)


def _delta_spec_table(spark, reference):
    """Resolve a mutable Delta table from a name or path reference."""
    from delta.tables import DeltaTable

    if isinstance(reference, dict):
        path = reference.get("path")
        if not path:
            raise ValueError("Delta spec path reference requires 'path'")
        return DeltaTable.forPath(spark, path)
    return DeltaTable.forName(spark, reference)


def find_pending_migrations(spark, spec_tables, groups=None):
    """Read pending migrations from layer-to-table mappings."""
    groups = groups or {}
    pending = []
    for layer, table_name in spec_tables.items():
        if not table_name:
            continue
        frame = _read_spec_frame(spark, table_name)
        # Spec tables created before quality engines were introduced do not
        # have this optional column. They cannot contain a pending quality
        # migration, so preserve the legacy deployment path by skipping them.
        if "qualityConfig" not in frame.columns:
            continue
        group = groups.get(layer)
        if group:
            frame = frame.filter(f.col("dataFlowGroup") == group)
        for row in frame.select(
            "dataFlowId", "qualityConfig", "quarantineTargetDetails"
        ).where(f.col("qualityConfig").isNotNull()).collect():
            raw = row["qualityConfig"]
            config = json.loads(raw)
            if config.get("migration") != "full_refresh":
                continue
            snapshot_target = config["output_targets"]["quarantine"]
            configured_target = dict(
                row["quarantineTargetDetails"] or {}
            )
            if _target_identity(configured_target) != _target_identity(
                snapshot_target
            ):
                raise RuntimeError(
                    f"Flow {row['dataFlowId']} quarantineTargetDetails "
                    "differs from its immutable qualityConfig snapshot; "
                    "refusing to run a selective full refresh"
                )
            fingerprint = (
                config.get("migration_fingerprint")
                or migration_fingerprint(config)
            )
            completed = dict(config)
            completed["migration"] = None
            completed["migration_fingerprint"] = None
            completed["completed_migration_fingerprint"] = fingerprint
            pending.append(
                PendingQualityMigration(
                    spec_table=table_name,
                    dataflow_id=row["dataFlowId"],
                    original_config_json=raw,
                    completed_config_json=json.dumps(completed),
                    fingerprint=fingerprint,
                    quarantine_target=dict(snapshot_target),
                )
            )
    return pending


def complete_migrations(spark, pending):
    """Clear completed migrations only if metadata is unchanged."""
    for migration in pending:
        target = _delta_spec_table(spark, migration.spec_table)
        target.update(
            condition=(
                (f.col("dataFlowId") == migration.dataflow_id)
                & (
                    f.col("qualityConfig")
                    == migration.original_config_json
                )
            ),
            set={
                "qualityConfig": f.lit(
                    migration.completed_config_json
                )
            },
        )
        completed_count = (
            _read_spec_frame(spark, migration.spec_table)
            .where(
                (f.col("dataFlowId") == migration.dataflow_id)
                & (
                    f.col("qualityConfig")
                    == migration.completed_config_json
                )
            )
            .count()
        )
        if completed_count != 1:
            raise RuntimeError(
                f"Quality migration metadata compare-and-swap failed for "
                f"flow {migration.dataflow_id} in {migration.spec_table}; "
                "refusing to start the normal pipeline update"
            )


def run_managed_quality_update(
    ws,
    spark,
    pipeline_id,
    spec_tables,
    groups=None,
    *,
    poll_interval_seconds=10,
    timeout_seconds=7200,
):
    """Apply selective quarantine migrations, then run the full graph."""
    pending = find_pending_migrations(
        spark, spec_tables=spec_tables, groups=groups
    )
    pipeline = ws.pipelines.get(pipeline_id=pipeline_id)
    pipeline_spec = getattr(pipeline, "spec", pipeline)
    direct_publishing = bool(
        getattr(pipeline_spec, "catalog", None)
        or getattr(pipeline_spec, "schema", None)
    ) and not getattr(pipeline_spec, "target", None)

    def run_update(**selection):
        response = ws.pipelines.start_update(
            pipeline_id=pipeline_id, **selection
        )
        update_id = response.update_id
        deadline = time.monotonic() + timeout_seconds
        while True:
            update = ws.pipelines.get_update(
                pipeline_id=pipeline_id, update_id=update_id
            ).update
            state = _state_value(update)
            if state in TERMINAL_SUCCESS_STATES:
                return response
            if state in TERMINAL_FAILURE_STATES:
                raise RuntimeError(
                    f"Pipeline {pipeline_id} update {update_id} ended in "
                    f"{state}"
                )
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Timed out waiting for pipeline {pipeline_id} update "
                    f"{update_id}"
                )
            time.sleep(poll_interval_seconds)

    if pending:
        quarantine_datasets = sorted({
            _quarantine_dataset(
                migration.quarantine_target, direct_publishing
            )
            for migration in pending
        })
        try:
            run_update(full_refresh_selection=quarantine_datasets)
        except (RuntimeError, TimeoutError) as err:
            raise type(err)(
                f"{err}; quality migration remains pending"
            ) from err
        try:
            complete_migrations(spark, pending)
        except Exception:
            logger.critical(
                "Quarantine full refresh succeeded for pipeline %s, but "
                "quality migration metadata could not be completed. Do not "
                "rerun the managed migration until the metadata update is "
                "repaired, or quarantine may be reset again.",
                pipeline_id,
                exc_info=True,
            )
            raise

    try:
        return run_update()
    except (RuntimeError, TimeoutError) as err:
        if pending:
            raise type(err)(
                f"{err}; quarantine migration completed, but the normal "
                "full-graph update failed"
            ) from err
        raise
