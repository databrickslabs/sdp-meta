---
id: autoloader
title: Autoloader / Cloud Files
sidebar_position: 1
---

# Autoloader / Cloud Files

SDP-META uses Databricks Autoloader (`cloudFiles`) to incrementally ingest files from cloud object storage. Autoloader tracks which files have been processed, making it suitable for continuously arriving data.

Supported platforms: AWS S3, Azure Data Lake Storage Gen2 (ADLS), Google Cloud Storage (GCS)

Supported file formats: JSON, CSV, Parquet, Avro, ORC, text, binary

## Onboarding configuration

Set `source_format` to `cloudFiles` and populate `source_details`:

```json
{
  "data_flow_id": "1",
  "data_flow_group": "retail_group",
  "source_format": "cloudFiles",
  "source_details": {
    "source_schema_path": "/Volumes/my_catalog/my_schema/my_volume/schema/customers.ddl",
    "source_path_dev": "s3://my-bucket/landing/customers/",
    "source_path_prod": "s3://my-prod-bucket/landing/customers/"
  },
  "bronze_catalog_dev": "my_catalog",
  "bronze_database_dev": "retail_bronze",
  "bronze_table": "customers_bronze",
  "bronze_reader_options": {
    "cloudFiles.format": "json",
    "cloudFiles.inferColumnTypes": "true",
    "cloudFiles.rescuedDataColumn": "_rescued_data"
  }
}
```

:::tip
Provide an explicit `source_schema_path` to avoid schema inference instability in production. See [Troubleshooting — Autoloader schema inference issues](../operations/troubleshooting#autoloader-schema-inference-issues).
:::

## Inference without a DDL

`source_schema_path` is optional. When it is omitted, SDP-META persists no
explicit Bronze schema and lets Auto Loader infer it:

```yaml
source_format: cloudFiles
source_details:
  source_path_prod: /Volumes/main/landing/events
bronze_reader_options:
  cloudFiles.format: json
  cloudFiles.inferColumnTypes: "true"
  cloudFiles.schemaHints: >-
    event_id BIGINT, event_ts TIMESTAMP, amount DECIMAL(10,2)
  cloudFiles.schemaEvolutionMode: addNewColumns
  cloudFiles.rescuedDataColumn: _rescued_data
```

Schema hints constrain selected inferred columns without defining the complete
schema. With `addNewColumns`, Auto Loader adds newly discovered columns to its
tracked schema; discovery can request a stream restart, so production jobs
should allow the pipeline update to retry. Values that cannot be parsed using
the inferred or hinted type are retained in `_rescued_data`.

For production feeds, prefer explicit schemas when contracts must remain
stable. Inference and evolution are most useful for exploratory or
intentionally flexible feeds.

## File metadata columns

Attach file-level metadata (file name, path, modification time) via `source_metadata` in `source_details`:

```json
{
  "source_details": {
    "source_schema_path": "/Volumes/my_catalog/my_schema/my_volume/schema/customers.ddl",
    "source_path_dev": "s3://my-bucket/landing/customers/",
    "source_metadata": {
      "include_autoloader_metadata_column": "True",
      "autoloader_metadata_col_name": "source_metadata",
      "select_metadata_cols": {
        "input_file_name": "_metadata.file_name",
        "input_file_path": "_metadata.file_path"
      }
    }
  }
}
```

- `include_autoloader_metadata_column` — adds the raw `_metadata` struct column
- `autoloader_metadata_col_name` — renames `_metadata` to this value (default: `source_metadata`)
- `select_metadata_cols` — map of `{target_column: _metadata_expression}` to extract specific fields into top-level columns

## Reader options

| Option | Description |
|---|---|
| `cloudFiles.format` | File format: `json`, `csv`, `parquet`, `avro`, `orc`, `text` |
| `cloudFiles.inferColumnTypes` | Infer column types. Set to `false` in production for schema stability. |
| `cloudFiles.rescuedDataColumn` | Column name for rescued (malformed) data |
| `cloudFiles.schemaHints` | Override inferred types for specific columns |
| `cloudFiles.schemaEvolutionMode` | Evolution behavior such as `addNewColumns` or `rescue` |
| `header` | For CSV: whether the first row is a header |
| `multiLine` | For JSON: whether records span multiple lines |

## Demo output

![Autoloader demo result](/img/af_am_demo.png)

## Running the demo

### At-Scale Auto Loader demo

The featured scale demo generates 100 metadata-driven Bronze and Silver
flows. Its 80/10/10 cohorts compare schema inference, schema hints, and
explicit DDLs in the same workload. The workflow then validates additive
evolution, rescued data, DQ quarantine, a table-specific Silver window/join
callback, and a separate Gold aggregation.

```bash
python demo/launch_at_scale_autoloader_demo.py \
  --uc_catalog_name=<your_catalog> \
  --profile=<your_profile>
```

The launcher validates the result and removes its per-run resources by
default. Use `--keep-resources` when you want to inspect the generated
pipelines and tables. Use `--table_count=12` for a lower-cost smoke run.

See the [complete demo walkthrough](https://github.com/databrickslabs/sdp-meta/tree/main/demo#at-scale-auto-loader-demo)
for the workflow stages, assertions, JSON/YAML mode, and cost guidance.

### Focused schema-evolution demo

Focused inference, hints, and evolution demo:

```bash
python demo/launch_autoloader_schema_demo.py \
  --uc_catalog_name=<your_catalog> \
  --profile=<your_profile>
```

### Append-flow and file-metadata demo

Append-flow and file-metadata demo:

```bash
python demo/launch_af_cloudfiles_demo.py \
  --cloud_provider_name=aws \
  --dbr_version=15.3.x-scala2.12 \
  --uc_catalog_name=<your_catalog>
```

For Azure, use `--cloud_provider_name=azure`.

## Related

- [Onboarding File Fields — cloudFiles source_details](../reference/onboarding-fields#source_details--cloudfiles)
- [Snapshot Ingestion](./snapshot) — for full-replace file ingestion
- [Multi-Source CDC](./multi-source-cdc) — for multiple cloudFiles paths writing to the same bronze table
