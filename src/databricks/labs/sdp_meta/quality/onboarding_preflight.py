"""Spark-free structural validation for quality-engine onboarding fields."""

from typing import Any, Iterable, List, Mapping, Optional, Sequence

from databricks.labs.sdp_meta.quality.fields import (
    quality_engine_field,
    quality_migration_field,
    quality_rules_path_field,
)


def _value(row: Mapping[str, Any], field: str):
    value = row.get(field)
    return value if value not in ("", None) else None


def _matching_value(
    row: Mapping[str, Any], prefix: str, env: Optional[str]
):
    if env is not None:
        return _value(row, f"{prefix}_{env}")
    return next(
        (
            value
            for key, value in row.items()
            if key.startswith(f"{prefix}_") and value not in ("", None)
        ),
        None,
    )


def collect_quality_configuration_errors(
    rows: Iterable[Mapping[str, Any]],
    *,
    env: Optional[str],
    uc_enabled: bool,
    supported_engines: Optional[Sequence[str]] = None,
) -> List[str]:
    """Return all static quality configuration errors in onboarding rows."""
    errors = []
    supported = set(supported_engines) if supported_engines is not None else None
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            continue
        flow_id = row.get("data_flow_id", index)
        prefix = f"Flow {flow_id}: "
        for layer in ("bronze", "silver"):
            engine_field = quality_engine_field(layer)
            migration_field = quality_migration_field(layer)
            rules_prefix = quality_rules_path_field(layer, "").rstrip("_")
            legacy_prefix = f"{layer}_data_quality_expectations_json"
            engine = _value(row, engine_field)
            rules_path = _matching_value(row, rules_prefix, env)
            legacy_rules = _matching_value(row, legacy_prefix, env)
            migration = _value(row, migration_field)

            if not engine:
                if rules_path or migration:
                    errors.append(
                        prefix
                        + f"{engine_field} is required when quality rules "
                        "or migration are configured"
                    )
                continue

            if supported is not None and engine not in supported:
                errors.append(
                    prefix
                    + f"{engine_field} must be one of "
                    f"{sorted(supported)}, got {engine!r}"
                )
            if not rules_path:
                suffix = env if env is not None else "<env>"
                errors.append(
                    prefix
                    + f"{quality_rules_path_field(layer, suffix)} is "
                    f"required when {engine_field} is set"
                )
            if legacy_rules:
                errors.append(
                    prefix
                    + f"{engine_field} cannot combine with legacy "
                    "quality expectations"
                )
            if migration not in (None, "full_refresh"):
                errors.append(
                    prefix + f"{migration_field} must be 'full_refresh'"
                )

            unsupported = []
            if (
                layer == "bronze"
                and str(row.get("source_format", "")).lower() == "snapshot"
            ):
                errors.append(
                    prefix
                    + f"{engine_field} cannot be used with snapshot sources"
                )
            for field in (
                f"{layer}_cdc_apply_changes",
                f"{layer}_cdc_apply_changes_flows",
                f"{layer}_apply_changes_from_snapshot",
                f"{layer}_append_flows",
            ):
                if _value(row, field):
                    unsupported.append(field)
            if unsupported:
                errors.append(
                    prefix
                    + "quality engine does not support: "
                    + ", ".join(unsupported)
                )

            suffix = env if env is not None else "<env>"
            required_targets = [
                f"{layer}_database_quarantine_{suffix}",
                f"{layer}_quarantine_table",
            ]
            missing_targets = []
            if not _matching_value(
                row, f"{layer}_database_quarantine", env
            ):
                missing_targets.append(required_targets[0])
            if not _value(row, required_targets[1]):
                missing_targets.append(required_targets[1])
            if not uc_enabled and not _matching_value(
                row, f"{layer}_quarantine_table_path", env
            ):
                missing_targets.append(
                    f"{layer}_quarantine_table_path_{suffix}"
                )
            if missing_targets:
                errors.append(
                    prefix
                    + "quality engine requires complete quarantine target "
                    "fields; missing: "
                    + ", ".join(missing_targets)
                )

            if layer == "silver":
                canonical = _value(
                    row, "silver_quarantine_table_cluster_by"
                )
                legacy_alias = _value(
                    row, "silver_quarantine_cluster_by"
                )
                if (
                    canonical
                    and legacy_alias
                    and canonical != legacy_alias
                ):
                    errors.append(
                        prefix
                        + "Conflicting silver_quarantine_table_cluster_by "
                        "and silver_quarantine_cluster_by values"
                    )
    return errors
