"""Configuration builder for the at-scale Auto Loader demo.

The checked-in code is the canonical model.  The launcher serializes the same
payload as JSON or YAML so the demo can exercise both onboarding formats
without maintaining two 100-row files by hand.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml


GROUP = "AT_SCALE_AUTOLOADER"
DEFAULT_TABLE_COUNT = 100
MIN_TABLE_COUNT = 3
SCHEMA_HINTS = (
    "event_id BIGINT, customer_id BIGINT, event_ts TIMESTAMP, "
    "amount DECIMAL(12,2)"
)


def cohort_counts(table_count: int) -> dict[str, int]:
    """Return an 80/10/10 split with every strategy represented."""
    if table_count < MIN_TABLE_COUNT:
        raise ValueError(
            f"table_count must be at least {MIN_TABLE_COUNT}, got {table_count}"
        )
    explicit = max(1, table_count // 10)
    hinted = max(1, table_count // 10)
    inferred = table_count - hinted - explicit
    return {"inferred": inferred, "hinted": hinted, "explicit": explicit}


def _table_identity(index: int, counts: dict[str, int]) -> tuple[str, str]:
    if index == 1:
        return "orders", "inferred"
    if index <= counts["inferred"]:
        return f"inferred_{index:03d}", "inferred"
    if index <= counts["inferred"] + counts["hinted"]:
        return f"hinted_{index:03d}", "hinted"
    return f"explicit_{index:03d}", "explicit"


def build_onboarding(
    *,
    table_count: int,
    uc_volume_path: str,
    uc_catalog_name: str,
    bronze_schema: str,
    silver_schema: str,
) -> list[dict[str, Any]]:
    """Build all Bronze/Silver onboarding rows for the scale workload."""
    counts = cohort_counts(table_count)
    volume = uc_volume_path.rstrip("/")
    flows: list[dict[str, Any]] = []

    for index in range(1, table_count + 1):
        table_name, strategy = _table_identity(index, counts)
        source_details: dict[str, Any] = {
            "source_database": "AT_SCALE",
            "source_table": table_name.upper(),
            "source_path_demo": (
                f"{volume}/data/at_scale_autoloader/landing/{table_name}"
            ),
        }
        reader_options = {
            "cloudFiles.format": "json",
            "cloudFiles.rescuedDataColumn": "_rescued_data",
        }
        if strategy == "explicit":
            source_details["source_schema_path"] = (
                f"{volume}/data/at_scale_autoloader/ddl/{table_name}.ddl"
            )
        else:
            reader_options.update(
                {
                    "cloudFiles.inferColumnTypes": "true",
                    "cloudFiles.schemaEvolutionMode": "addNewColumns",
                }
            )
            if strategy == "hinted":
                reader_options["cloudFiles.schemaHints"] = SCHEMA_HINTS

        flow: dict[str, Any] = {
            "data_flow_id": f"at-scale-{index:03d}",
            "data_flow_group": GROUP,
            "source_system": "DAILY_DUMPS",
            "source_format": "cloudFiles",
            "source_details": source_details,
            "bronze_catalog_demo": uc_catalog_name,
            "bronze_database_demo": bronze_schema,
            "bronze_table": table_name,
            "bronze_reader_options": reader_options,
            "bronze_table_properties": {"pipelines.reset.allowed": "true"},
            "bronze_cluster_by_auto": True,
            "silver_catalog_demo": uc_catalog_name,
            "silver_database_demo": silver_schema,
            "silver_table": table_name,
            "silver_transformation_json_demo": (
                f"{volume}/conf/at_scale_autoloader/"
                "silver_transformations.json"
            ),
            "silver_table_properties": {"pipelines.reset.allowed": "true"},
            "silver_cluster_by_auto": True,
        }
        if table_name == "orders":
            flow.update(
                {
                    "bronze_data_quality_expectations_json_demo": (
                        f"{volume}/conf/at_scale_autoloader/dqe.json"
                    ),
                    "bronze_catalog_quarantine_demo": uc_catalog_name,
                    "bronze_database_quarantine_demo": bronze_schema,
                    "bronze_quarantine_table": "orders_quarantine",
                }
            )
        flows.append(flow)

    return flows


def generated_onboarding_path(file_format: str, run_id: str) -> Path:
    """Return the ignored local output used by the integration harness."""
    if file_format == "yaml":
        return Path(
            f"demo/conf/yml/onboarding_at_scale_autoloader_{run_id}.yml"
        )
    return Path(
        f"demo/conf/json/onboarding_at_scale_autoloader_{run_id}.json"
    )


def write_onboarding(
    payload: list[dict[str, Any]], path: Path, file_format: str
) -> None:
    """Serialize an onboarding payload deterministically."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if file_format == "yaml":
        path.write_text(
            yaml.safe_dump(payload, sort_keys=False),
            encoding="utf-8",
        )
    else:
        path.write_text(
            json.dumps(payload, indent=2) + "\n",
            encoding="utf-8",
        )
