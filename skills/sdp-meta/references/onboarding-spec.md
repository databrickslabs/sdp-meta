# Dataflowspec / onboarding file reference

The onboarding file is a **list** of dataflowspec records (JSON or YAML). Each
record describes one source table and its bronze (and optional silver) targets.
`onboard` reads this file and writes the records into the bronze/silver
**dataflowspec Delta tables**; `deploy` reads those tables to build the pipeline.

Copy a real example instead of inventing keys:
- `tests/resources/onboarding.json` — cloudFiles bronze+silver
- `tests/resources/onboarding_bronze_cdc_flows.json`, `onboarding_silver_cdc_flows.json` — CDC
- `tests/resources/onboarding_silverfanout.json` — silver fan-out
- `examples/json/`, `examples/yml/` — packaged templates

## Identity & grouping

| Field | Meaning |
|-------|---------|
| `data_flow_id` | Unique id for the flow (string). |
| `data_flow_group` | Groups flows into one pipeline (e.g. `A1`). Flows sharing a group deploy together. |
| `source_system` | Free-text label for the source (e.g. `MYSQL`). |
| `source_format` | `cloudFiles` (Auto Loader), `delta`, `eventhub`, `kafka`, or `snapshot`. |

## Environment suffixes

Catalog / database / path fields are suffixed per environment so one spec serves
all: `_dev`, `_staging`, `_prd`. Example: `bronze_catalog_dev`,
`bronze_catalog_staging`, `bronze_catalog_prd`. The active environment is chosen
at onboard/deploy time. Non-environment fields (`bronze_table`,
`bronze_reader_options`, …) are shared across environments.

## source_details

Describes where bronze reads from. Common keys:

| Key | Meaning |
|-----|---------|
| `source_database`, `source_table` | Logical source names. |
| `source_path_dev` (`_staging`/`_prd`) | Path Auto Loader/cloudFiles reads from. |
| `source_schema_path` | Optional DDL schema file for the source. |
| `source_metadata` | Optional: include Auto Loader metadata columns (`include_autoloader_metadata_column`, `autoloader_metadata_col_name`, `select_metadata_cols`). |

For `eventhub` / `kafka`, `source_details` carries the connection/topic config
instead of a path (see `tests/resources/silver_transformation_eventhub.json` and
kafka examples).

## Bronze layer fields

| Field | Meaning |
|-------|---------|
| `bronze_catalog_*`, `bronze_database_*` | Target UC catalog/schema per env. |
| `bronze_table` | Bronze table name. |
| `bronze_table_comment` | Table comment. |
| `bronze_reader_options` | Reader options map, e.g. `cloudFiles.format`, `cloudFiles.inferColumnTypes`, `cloudFiles.rescuedDataColumn`. |
| `bronze_table_path_dev` | Optional explicit storage path. |
| `bronze_table_properties` | Delta/pipeline table properties (e.g. `pipelines.reset.allowed`). |
| `bronze_cluster_by` | Liquid clustering columns. |
| `bronze_data_quality_expectations_json_*` | Path to the bronze DQ expectations file. |
| `bronze_quarantine_table` (+ optional `bronze_catalog_quarantine_*` / `bronze_database_quarantine_*`) | Quarantine target for `expect_or_quarantine`. Catalog/database default to the main bronze target when omitted. |

## Silver layer fields

Present only when the flow has a silver layer. Mirror the bronze fields with a
`silver_` prefix (`silver_catalog_*`, `silver_database_*`, `silver_table`,
`silver_table_properties`, `silver_cluster_by`, `silver_quarantine_table`, …),
plus:

| Field | Meaning |
|-------|---------|
| `silver_transformation_json_*` | Path to the silver transformation spec. |
| `silver_data_quality_expectations_json_*` | Silver DQ expectations file. |
| `silver_cdc_apply_changes` | CDC / SCD apply-changes config for silver. |

## Data quality expectations file

A JSON object keyed by action. Each action maps constraint-name → SQL predicate:

```json
{
  "expect_or_drop": {
    "no_rescued_data": "_rescued_data IS NULL",
    "valid_id": "id IS NOT NULL",
    "valid_operation": "operation IN ('APPEND','DELETE','UPDATE')"
  }
}
```

| Action | Behaviour |
|--------|-----------|
| `expect` | Record violations; keep the row (warn). |
| `expect_or_drop` | Drop violating rows (routed to the quarantine table if configured). |
| `expect_or_fail` | Fail the pipeline update on any violation. |

## Silver transformation file

A **list** of per-target transformations:

```json
[
  {
    "target_table": "customers",
    "select_exp": ["id", "email", "firstname", "lastname", "operation_date", "operation", "_rescued_data"],
    "where_clause": ["id IS NOT NULL", "email is not NULL"]
  }
]
```

| Key | Meaning |
|-----|---------|
| `target_table` | Silver table this transform produces. |
| `select_exp` | Projection — column names or SQL expressions. |
| `where_clause` | List of filter predicates (ANDed). |

Fan-out (one bronze → many silver targets) uses multiple entries with distinct
`target_table` values (see `silver_transformations_fanout.json`).

## CDC / SCD Type 2

For change-data-capture sources, bronze uses CDC reader/apply options and silver
uses `silver_cdc_apply_changes` (keys, sequence-by, SCD type, columns to
track/except). See `tests/resources/onboarding_silver_cdc_flows.json` and
`silver_transformations_ac_type2.json` for the exact shape, and
`onboarding_applychanges_from_snapshot_silver.json` for snapshot-based CDC.
