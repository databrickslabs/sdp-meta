---
id: troubleshooting
title: Troubleshooting
sidebar_position: 4
---

# Troubleshooting

## Python version issues

**Symptom:** `cloudpickle` errors, recursion depth errors, or import failures with `pyspark==3.5.5`.

**Cause:** Python 3.13+ is incompatible with `cloudpickle` bundled with `pyspark==3.5.5`.

**Fix:** Use Python 3.10, 3.11, or 3.12.

```bash
python3.11 -m venv .venv
source .venv/bin/activate
```

:::warning
Python 3.13+ is not supported with `pyspark==3.5.5`. This is a PySpark constraint.
:::

## "Materializing tables in custom schemas is not supported"

**Symptom:**

```
com.databricks.pipelines.common.errors.DLTAnalysisException: Materializing tables in custom schemas
is not supported. Please remove the database qualifier from table 'catalog_name.schema_name.table_name'
```

**Cause:** The pipeline was created in Legacy Publishing Mode.

**Fix:** Migrate to the default publishing mode: [Migrate to the default publishing mode](https://docs.databricks.com/aws/en/ldp/migrate-to-dpm#migrate-to-the-default-publishing-mode).

:::note
This migration is one-way. Verify in a non-production environment first.
:::

## Bundle validate fails with `__SET_ME__` sentinel

**Symptom:** `bundle-validate` fails with a message about `sdp_meta_dependency`.

**Fix:** Set `sdp_meta_dependency` to a real value in `resources/variables.yml`:

```bash
# Option 1: Use a published PyPI version
sed -i 's/__SET_ME__/databricks-labs-sdp-meta==0.1.1/' resources/variables.yml

# Option 2: Upload a local wheel, then set the /Volumes/... path
databricks labs sdp-meta bundle-prepare-wheel
```

## Onboarding job fails — DataflowSpec table not found

**Symptom:** `Table not found: my_catalog.my_schema.bronze_dataflowspec_table`.

**Fix:** Create the schema before running the onboarding job:

```sql
CREATE CATALOG IF NOT EXISTS my_catalog;
CREATE SCHEMA IF NOT EXISTS my_catalog.my_schema;
```

## Pipeline fails with "No flows found for group X"

**Symptom:** Pipeline starts and immediately fails with `No flows found for group retail_group`.

**Cause:** The `data_flow_group` in the pipeline configuration doesn't match the onboarding file.

**Fix:** Ensure the pipeline's `bronze.group` (or `silver.group`) matches `data_flow_group` in the onboarding file exactly, including case.

```json
{
  "configuration": {
    "layer": "bronze",
    "bronze.group": "retail_group"
  }
}
```

## Autoloader schema inference issues {#autoloader-schema-inference-issues}

**Symptom:** Bronze table schema changes unexpectedly between pipeline runs.

**Cause:** Autoloader's schema inference samples files and can produce different results as new files arrive.

**Fix:** Provide an explicit Spark DDL schema file via `source_schema_path`:

```json
{
  "source_details": {
    "source_schema_path": "/Volumes/my_catalog/my_schema/my_volume/schema/customers.ddl",
    "source_path_dev": "s3://my-bucket/landing/customers/"
  }
}
```

The DDL file uses standard Spark SQL schema syntax:

```sql
customer_id STRING,
name STRING,
email STRING,
created_at TIMESTAMP,
is_active BOOLEAN
```

Remove `cloudFiles.inferColumnTypes` from `bronze_reader_options` when using an explicit schema.

## CDC apply_changes fails with duplicate keys

**Symptom:** `create_auto_cdc_flow` produces incorrect results when multiple events share the same key and `sequence_by` value.

**Fix:** Add a tiebreaker column to `sequence_by`:

```json
{
  "bronze_cdc_apply_changes": {
    "keys": ["customer_id"],
    "sequence_by": "dmsTimestamp,sequenceId",
    "scd_type": "1"
  }
}
```

For common questions about features, DAB, installation, and the App, see the [FAQ](../faq).
