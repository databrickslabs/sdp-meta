---
id: dataflowspec
title: Dataflowspec
sidebar_position: 2
---

# Dataflowspec

The onboarding file (YAML or JSON) is the configuration you write to define your pipelines. The Onboard Job reads it and writes the derived metadata into `bronze_dataflowspec` and `silver_dataflowspec` Delta tables, which the Generic Declarative Pipeline reads at runtime.

Each entry in the onboarding file represents one flow. A flow has a shared identity section, a Bronze section, and an optional Silver section.

## Architecture

SDP-META operates in two phases:

1. **Onboard Job** — reads your onboarding file and writes metadata into `bronze_dataflowspec` and `silver_dataflowspec` Delta tables. Re-run this job whenever you change the onboarding file.
2. **Generic Declarative Pipeline** — at startup, reads the DataflowSpec tables and dynamically constructs the pipeline graph.

## Common fields

These fields are required on every flow entry.

| Field | Type | Required | Description |
|---|---|---|---|
| `data_flow_id` | string | Yes | Unique identifier for the flow within the file. |
| `data_flow_group` | string | Yes | Group name. A pipeline processes only flows matching its configured group. |
| `source_format` | string | Yes | Source format: `cloudFiles`, `delta`, `eventhub`, `kafka`, or `snapshot`. |
| `source_details` | map | Yes | Source-specific connection properties. Keys vary by `source_format` (e.g. `source_path_<env>`, `source_schema_path`). |
| `source_system` | string | No | Informational label for the source system. |

## Bronze fields

| Field | Type | Required | Description |
|---|---|---|---|
| `bronze_catalog_<env>` | string | UC only | Unity Catalog catalog name for the Bronze target. |
| `bronze_database_<env>` | string | Yes | Schema for the Bronze target table. |
| `bronze_table` | string | Yes | Bronze table name. |
| `bronze_table_comment` | string | No | Table comment. |
| `bronze_table_path_<env>` | string | Non-UC | External path for the Bronze table (required without Unity Catalog). |
| `bronze_reader_options` | map | No | Spark reader options (e.g. `cloudFiles.format`, `header`). |
| `bronze_table_properties` | map | No | Delta table properties. |
| `bronze_partition_columns` | string | No | Comma-separated partition column names. |
| `bronze_cluster_by` | list | No | Liquid clustering columns. |
| `bronze_cluster_by_auto` | boolean | No | Enable auto liquid clustering. |
| `bronze_data_quality_expectations_json_<env>` | string | No | Path to a DQE file (JSON or YAML). |
| `bronze_catalog_quarantine_<env>` | string | No | Catalog for the quarantine table (defaults to bronze catalog). |
| `bronze_database_quarantine_<env>` | string | No | Schema for the quarantine table. Required when DQE has `drop` expectations. |
| `bronze_quarantine_table` | string | No | Quarantine table name. |
| `bronze_quarantine_table_comment` | string | No | Quarantine table comment. |
| `bronze_quarantine_table_path_<env>` | string | No | External path for the quarantine table (non-UC). |
| `bronze_quarantine_table_cluster_by` | list | No | Liquid clustering columns for the quarantine table. |
| `bronze_quarantine_table_cluster_by_auto` | boolean | No | Auto liquid clustering for the quarantine table. |
| `bronze_cdc_apply_changes` | map | No | Single-source CDC config. See [CDC](../guides/cdc.md). |
| `bronze_apply_changes_from_snapshot` | map | No | Snapshot-based CDC config. See [Snapshot CDC](../guides/snapshot.md). |
| `bronze_append_flows` | list | No | Additional append flows. See [Autoloader](../guides/autoloader.md). |
| `bronze_sinks` | list | No | Sink configs (Delta, Kafka, Event Hubs). See [DLT Sink](../guides/dlt-sink.md). |
| `bronze_row_filter` | string | No | `ROW FILTER` clause for the Bronze table (Unity Catalog only). |
| `bronze_column_comments` | map | No | `{column: comment}` map. Comments annotate the Bronze table columns. See [Column policies](../guides/column-policies.md). |
| `bronze_column_masks` | map | No | `{column: mask_clause}` map (e.g. `cat.schema.fn USING COLUMNS (other)`), Unity Catalog only. See [Column policies](../guides/column-policies.md). |

## Silver fields

| Field | Type | Required | Description |
|---|---|---|---|
| `silver_catalog_<env>` | string | UC only | Unity Catalog catalog name for the Silver target. |
| `silver_database_<env>` | string | Yes | Schema for the Silver target table. |
| `silver_table` | string | Yes | Silver table name. |
| `silver_table_comment` | string | No | Table comment. |
| `silver_table_path_<env>` | string | Non-UC | External path for the Silver table (required without Unity Catalog). |
| `silver_transformation_json_<env>` | string | Yes* | Path to a transformations file defining `select_exp` and `where_clause`. *Not required when using `silver_cdc_apply_changes_flows`. |
| `silver_data_quality_expectations_json_<env>` | string | No | Path to a DQE file. |
| `silver_cdc_apply_changes` | map | No | Single-source CDC config. See [CDC](../guides/cdc.md). |
| `silver_cdc_apply_changes_flows` | map | No | Multi-source CDC flow group. See [Multi-source CDC](../guides/multi-source-cdc.md). |
| `silver_reader_options` | map | No | Additional Spark reader options for the Silver source. |
| `silver_cluster_by` | list | No | Liquid clustering columns. |
| `silver_cluster_by_auto` | boolean | No | Enable auto liquid clustering. |
| `silver_row_filter` | string | No | `ROW FILTER` clause for the Silver table (Unity Catalog only). |
| `silver_column_comments` | map | No | `{column: comment}` map. Comments annotate the Silver table columns. See [Column policies](../guides/column-policies.md). |
| `silver_column_masks` | map | No | `{column: mask_clause}` map (e.g. `cat.schema.fn USING COLUMNS (other)`), Unity Catalog only. See [Column policies](../guides/column-policies.md). |

## Environment suffixes

Fields ending in `_<env>` accept environment-specific values. The Onboard Job is called with an `env` parameter (typically `dev` or `prod`), and it reads the matching field — for example `bronze_database_prod` when `env=prod`. This lets one onboarding file serve multiple environments.

## Example

```yaml
- data_flow_id: '1'
  data_flow_group: retail
  source_format: cloudFiles
  source_details:
    source_path_prod: /Volumes/my_catalog/my_schema/landing/customers
    source_schema_path: /Volumes/my_catalog/my_schema/ddl/customers.ddl
  bronze_catalog_prod: my_catalog
  bronze_database_prod: retail_bronze
  bronze_table: customers
  bronze_reader_options:
    cloudFiles.format: csv
    header: 'true'
  bronze_cluster_by_auto: true
  bronze_data_quality_expectations_json_prod: /Volumes/my_catalog/my_schema/dqe/customers.yml
  bronze_database_quarantine_prod: retail_bronze
  bronze_quarantine_table: customers_quarantine
  silver_catalog_prod: my_catalog
  silver_database_prod: retail_silver
  silver_table: customers
  silver_cdc_apply_changes:
    keys: [customer_id]
    sequence_by: updated_at
    scd_type: '1'
  silver_transformation_json_prod: /Volumes/my_catalog/my_schema/transformations/silver.yml
```

## Source of truth

The `bronze_dataflowspec` and `silver_dataflowspec` Delta tables are derived artifacts. Always edit the onboarding file, then re-run the Onboard Job. Do not edit DataflowSpec rows directly — they are overwritten on the next run when `overwrite=True`.
