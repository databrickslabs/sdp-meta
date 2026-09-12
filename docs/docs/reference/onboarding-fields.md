---
id: onboarding-fields
title: Onboarding File Fields
sidebar_position: 1
---

# Onboarding File Fields

The onboarding file is a JSON or YAML array of flow definitions. Each element describes one data flow — its source, bronze target, and optionally a silver target.

:::note
`{env}` is your environment placeholder, for example `dev`, `prod`, or `stag`. You supply the value at onboarding time via the `env` parameter.
:::

Full example files:

- JSON: [`demo/conf/json/onboarding.template`](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/json/onboarding.template)
- YAML: [`demo/conf/yml/onboarding.template.yml`](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/yml/onboarding.template.yml)

---

## Top-Level Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `data_flow_id` | string | Yes | Unique identifier for this pipeline flow |
| `data_flow_group` | string | Yes | Group identifier — flows in the same group share one Declarative Pipeline |
| `source_format` | string | Yes | Source type: `cloudFiles`, `eventhub`, `kafka`, `delta`, or `snapshot` |
| `source_details` | object | Yes | Source-specific connection details — see per-format tables below |

---

## `source_details` — cloudFiles

| Field | Type | Description |
|---|---|---|
| `source_schema_path` | string | Path to the Spark DDL schema file for the source. If omitted, schema inference is used. For a custom parser, implement `bronze_schema_mapper(schema_file_path, spark): Schema`. |
| `source_path_{env}` | string | Cloud storage path where source files land (e.g. `s3://bucket/path/`) |
| `source_catalog` | string | Unity Catalog name of a Delta table used as the source (Delta format only) |
| `source_database` | string | Schema/database of the Delta source table |
| `source_metadata` | object | Autoloader file metadata configuration — see nested fields below |

**`source_metadata` nested fields:**

| Field | Type | Description |
|---|---|---|
| `include_autoloader_metadata_column` | boolean | When `true`, adds the `_metadata` struct column to the bronze table |
| `autoloader_metadata_col_name` | string | Rename `_metadata` to this column name (default: `source_metadata`) |
| `select_metadata_cols` | object | Map of `{target_col_name: _metadata expression}` to extract individual fields from `_metadata` |

---

## `source_details` — eventhub

| Field | Type | Description |
|---|---|---|
| `source_schema_path` | string | Path to the Spark DDL schema file |
| `eventhub.accessKeyName` | string | Name of the SAS policy key |
| `eventhub.accessKeySecretName` | string | Databricks Secrets key name containing the access key value |
| `eventhub.name` | string | Event Hub topic name |
| `eventhub.secretsScopeName` | string | Databricks Secrets scope name |
| `kafka.sasl.mechanism` | string | SASL mechanism, e.g. `PLAIN` |
| `kafka.security.protocol` | string | Security protocol, e.g. `SASL_SSL` |
| `eventhub.namespace` | string | Event Hubs namespace |
| `eventhub.port` | string | Port, typically `9093` |

---

## `source_details` — kafka

| Field | Type | Description |
|---|---|---|
| `source_schema_path` | string | Path to the Spark DDL schema file |
| `kafka.bootstrap.servers` | string | Kafka broker address(es), e.g. `host:9092` |
| `subscribe` | string | Kafka topic name |
| `kafka.sasl.mechanism` | string | SASL mechanism if using authenticated Kafka |
| `kafka.security.protocol` | string | Security protocol |

---

## `source_details` — snapshot

| Field | Type | Description |
|---|---|---|
| `snapshot_format` | string | File format of the snapshot files, e.g. `parquet`, `csv` |
| `source_path_{env}` | string | Cloud storage path where snapshot files are located |

---

## `source_details` — delta

Used when reading from an existing Delta table — typically a bronze table feeding a silver flow.

| Field | Type | Description |
|---|---|---|
| `source_database` | string | `catalog.schema` (fully qualified) or schema name of the source Delta table |
| `source_table` | string | Name of the source Delta table |

Example:

```json
{
  "source_format": "delta",
  "source_details": {
    "source_database": "my_catalog.retail_bronze",
    "source_table": "orders_raw"
  }
}
```

---

## Bronze Layer Fields

| Field | Type | Description |
|---|---|---|
| `bronze_catalog_{env}` | string | Unity Catalog name for the bronze table |
| `bronze_database_{env}` | string | Schema (database) name for the bronze table |
| `bronze_table` | string | Bronze table name |
| `bronze_table_comment` | string | Comment applied to the bronze table |
| `bronze_reader_options` | object | Options passed to the Spark reader (e.g. `{"multiline": "true", "header": "true"}`) |
| `bronze_partition_columns` | array | List of column names to use as partition columns |
| `bronze_cluster_by` | array | List of column names for liquid clustering |
| `bronze_cluster_by_auto` | boolean | Enable automatic liquid clustering. Can be combined with `bronze_cluster_by`. See [Automatic liquid clustering](https://docs.databricks.com/aws/en/delta/clustering#auto-liquid) |
| `bronze_cdc_apply_changes` | object | Configuration for `create_auto_cdc_flow` on the bronze table — see [CDC guide](../guides/cdc) |
| `bronze_apply_changes_from_snapshot` | object | Snapshot CDC configuration. Mandatory fields: `keys` (array), `scd_type` (`1` or `2`). Optional: `track_history_column_list`, `track_history_except_column_list` |
| `bronze_table_path_{env}` | string | External storage path for the bronze table (optional, uses managed table if omitted) |
| `bronze_table_properties` | object | Declarative Pipeline table properties, e.g. `{"pipelines.autoOptimize.managed": "false", "pipelines.reset.allowed": "false"}` |
| `bronze_sink` | object | Declarative Pipeline Sink API configuration for writing to an external Delta table or Kafka topic — see [DLT Sink guide](../guides/dlt-sink) |
| `bronze_data_quality_expectations_json_{env}` | string | Path to the DQE rules JSON/YAML file for bronze — see [DQ Rules](./dq-rules) |
| `bronze_quality_engine` | string | Opt-in quality engine: `lakeflow` or `dqx`. Omit it to use the legacy path. |
| `bronze_quality_rules_path_{env}` | string | Path to the selected engine's JSON/YAML rules document |
| `bronze_quality_engine_migration` | string | Set to `full_refresh` only when changing engine or diagnostic schema while reusing the quarantine table |
| `bronze_catalog_quarantine_{env}` | string | Unity Catalog name for the quarantine table |
| `bronze_database_quarantine_{env}` | string | Schema name for the quarantine table |
| `bronze_quarantine_table` | string | Explicit quarantine table name for `expect_or_quarantine` routing |
| `bronze_quarantine_table_comment` | string | Comment applied to the quarantine table |
| `bronze_quarantine_table_path_{env}` | string | External storage path for the quarantine table |
| `bronze_quarantine_table_partitions` | array | Partition columns for the quarantine table |
| `bronze_quarantine_table_cluster_by` | array | Liquid clustering columns for the quarantine table |
| `bronze_quarantine_table_properties` | object | Table properties for the quarantine table |
| `bronze_append_flows` | array | Additional `append_flow` definitions — each element specifies an extra source that appends to the same bronze target. See [Multi-Source CDC guide](../guides/multi-source-cdc) |

:::warning
If legacy or opt-in rules contain `expect_or_quarantine`,
`bronze_quarantine_table` is required. Non-UC pipelines also require
`bronze_quarantine_table_path_{env}`. SDP-META does not derive a quarantine
table name.
:::

---

## Silver Layer Fields

| Field | Type | Description |
|---|---|---|
| `silver_catalog_{env}` | string | Unity Catalog name for the silver table |
| `silver_database_{env}` | string | Schema (database) name for the silver table |
| `silver_table` | string | Silver table name |
| `silver_cdc_apply_changes` | object | Configuration for `create_auto_cdc_flow` on the silver table — see [CDC guide](../guides/cdc) |
| `silver_table_path_{env}` | string | External storage path for the silver table |
| `silver_table_properties` | object | Declarative Pipeline table properties for the silver table |
| `silver_cluster_by` | array | List of column names for liquid clustering |
| `silver_cluster_by_auto` | boolean | Enable automatic liquid clustering on the silver table |
| `silver_data_quality_expectations_json_{env}` | string | Path to the DQE rules file for silver — see [DQ Rules](./dq-rules) |
| `silver_quality_engine` | string | Opt-in quality engine: `lakeflow` or `dqx`. Omit it to use the legacy path. |
| `silver_quality_rules_path_{env}` | string | Path to the selected engine's JSON/YAML rules document |
| `silver_quality_engine_migration` | string | Set to `full_refresh` only when changing engine or diagnostic schema while reusing the quarantine table |
| `silver_catalog_quarantine_{env}` | string | Unity Catalog name for the quarantine table |
| `silver_database_quarantine_{env}` | string | Schema name for the quarantine table |
| `silver_quarantine_table` | string | Explicit quarantine table name for `expect_or_quarantine` routing |
| `silver_quarantine_table_comment` | string | Comment applied to the quarantine table |
| `silver_quarantine_table_path_{env}` | string | External storage path for the quarantine table |
| `silver_quarantine_table_partitions` | array | Partition columns for the quarantine table |
| `silver_quarantine_table_cluster_by` | array | Liquid clustering columns for the quarantine table |
| `silver_quarantine_table_properties` | object | Table properties for the quarantine table |
| `silver_append_flows` | array | Additional `append_flow` definitions for the silver layer |
| `silver_sink` | object | Declarative Pipeline Sink API configuration for silver output — see [DLT Sink guide](../guides/dlt-sink) |
| `silver_transformation_json_{env}` | string | Path to the silver transformations JSON/YAML file — see [Silver Transformations](./silver-transformations) |

:::warning
If legacy or opt-in rules contain `expect_or_quarantine`,
`silver_quarantine_table` is required. Non-UC pipelines also require
`silver_quarantine_table_path_{env}`. SDP-META does not derive a quarantine
table name.
:::
