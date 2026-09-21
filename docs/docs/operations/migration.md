---
id: migration
title: "Migration: DLT-META to SDP-META"
sidebar_position: 3
---

# Migration: DLT-META to SDP-META

The project was renamed from **DLT-META** to **SDP-META** to align with current Databricks product terminology (Lakeflow Spark Declarative Pipelines). The rename took effect in v0.1.0.

## What changed

| Component | Before (DLT-META) | After (SDP-META) |
|---|---|---|
| PyPI package | `dlt-meta` | `databricks-labs-sdp-meta` |
| CLI command | `databricks labs dlt-meta` | `databricks labs sdp-meta` |
| Labs install | `databricks labs install dlt-meta` | `databricks labs install sdp-meta` |
| Python import | `from dlt_meta import ...` | `from databricks.labs.sdp_meta import ...` |
| Source layout | `src/dataflow_pipeline.py` (flat) | `src/databricks/labs/sdp_meta/dataflow_pipeline.py` (namespace) |
| Main class | `DLTMeta` | `SDPMeta` |
| Constants | `DLT_META_RUNNER_NOTEBOOK` | `SDP_META_RUNNER_NOTEBOOK` |
| Schemas | `dlt_meta_dataflowspecs` | `sdp_meta_dataflowspecs` |
| Workspace config keys | `dlt_meta_operation`, `dlt_meta_schema`, `dlt_meta_layer`, `dlt_meta_onboard_group` | `sdp_meta_operation`, `sdp_meta_schema`, `sdp_meta_layer`, `sdp_meta_onboard_group` |
| PythonWheelTask `package_name` | `dlt_meta` | `databricks_labs_sdp_meta` |
| Runner notebook | `init_dlt_meta_pipeline.py` | `init_sdp_meta_pipeline.py` |

## What did not change

- Onboarding file format — existing JSON/YAML files work without modification.
- Dataflowspec field names — all fields (`bronze_table`, `silver_cdc_apply_changes`, etc.) are unchanged.
- Public pipeline API method signatures. Review the
  [v0.1.1 semantic changes](#v011-compatibility-boundaries) before upgrading.
- Data in existing pipeline output tables — no data migration required.

## Backward compatibility

The `dlt-meta` PyPI package continues to work as a compatibility wrapper:

- `pip install dlt-meta` installs `databricks-labs-sdp-meta` as a dependency.
- `from dlt_meta import ...` re-exports all symbols with a `DeprecationWarning`.
- `databricks labs dlt-meta` CLI commands are forwarded to `sdp-meta` with a deprecation banner.
- `DLTMeta` is aliased to `SDPMeta`; the legacy workspace config keys `dlt_meta_operation`, `dlt_meta_schema`, `dlt_meta_layer`, and `dlt_meta_onboard_group` are still read (migrated to their `sdp_meta_*` equivalents) with a logged warning.

Legacy `src.*` imports (from v0.0.10) work via a `sys.modules` shim but **will be removed in v0.2.0**.

## Step-by-step migration

### 1. Update installation

```bash
pip uninstall dlt-meta
databricks labs install sdp-meta
# or
pip install databricks-labs-sdp-meta
```

### 2. Update CLI commands

```bash
# Before
databricks labs dlt-meta onboard
databricks labs dlt-meta deploy

# After
databricks labs sdp-meta onboard
databricks labs sdp-meta deploy
```

### 3. Update Python imports

```python
# Before (deprecated)
from dlt_meta.cli import DLTMeta
from dlt_meta import DataflowPipeline

# After
from databricks.labs.sdp_meta.cli import SDPMeta
from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline
from databricks.labs.sdp_meta.dataflow_spec import BronzeDataflowSpec, SilverDataflowSpec
from databricks.labs.sdp_meta.onboard_dataflowspec import OnboardDataflowspec
```

### 4. Update pipeline runner notebooks

```python
# Before
%pip install dlt-meta==0.0.10

# After
%pip install databricks-labs-sdp-meta==0.1.1
```

The pipeline invocation code is unchanged:

```python
layer = spark.conf.get("layer", None)
from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline
DataflowPipeline.invoke_dlt_pipeline(spark, layer)
```

### 5. Update workspace config keys (optional)

```json
// Before
{
  "dlt_meta_operation": "onboard",
  "dlt_meta_schema": "my_schema",
  "dlt_meta_layer": "bronze_silver",
  "dlt_meta_onboard_group": "A1"
}

// After
{
  "sdp_meta_operation": "onboard",
  "sdp_meta_schema": "my_schema",
  "sdp_meta_layer": "bronze_silver",
  "sdp_meta_onboard_group": "A1"
}
```

SDP-META v0.1.1 loads the four legacy names with a deprecation warning, so
existing installations can still be upgraded or uninstalled. If a config file
contains both forms of a key, the current `sdp_meta_*` value takes precedence.
Update the file before the legacy aliases are removed in v0.2.0.

## v0.1.1 compatibility boundaries

### Python runtime support

SDP-META v0.1.1 supports Python 3.10, 3.11, and 3.12. Python 3.8 and
3.9 are no longer supported because the required
`databricks-sdk>=0.138.0` supports Python 3.10 and newer. Python 3.13+
remains unsupported by the pinned PySpark 3.5.5 development and test stack.

Before upgrading, confirm the Python version used by local environments,
Databricks jobs, and build automation:

```bash
python --version
```

Recreate environments on Python 3.10–3.12 before installing either
`databricks-labs-sdp-meta==0.1.1` or the `dlt-meta==0.1.1` compatibility
package.

### Custom transformations and append flows

Before v0.1.1, `bronze_custom_transform_func` and
`silver_custom_transform_func` ran only for the primary source. Starting in
v0.1.1, the same layer transform runs separately for the primary source and
for every append-flow source. This is the intended behavior from
[Issue #445](https://github.com/databrickslabs/sdp-meta/issues/445).

This correction is also a semantic change. A transform can fail after the
upgrade when it references a column that exists in the primary source but not
in an append source. It can also change append output if different inputs take
different branches in the transform. Before upgrading:

1. Compare the schema of the primary source with every entry in
   `bronze_append_flows` and `silver_append_flows`.
2. Decide which columns are required and fail with a clear error when they are
   missing.
3. Add or rename optional columns explicitly.
4. Return the same target-compatible column names and types for every input.
5. Test each primary and append source independently in a non-production
   pipeline.

The following transform accepts either `order_id` or the legacy `orderId`,
adds an optional `source_system`, validates required columns, and returns a
stable schema for both primary and append inputs:

```python
from pyspark.sql import functions as F
from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline


def normalize_orders(df, dataflow_spec):
    columns = set(df.columns)

    if "order_id" not in columns and "orderId" in columns:
        df = df.withColumnRenamed("orderId", "order_id")
        columns = set(df.columns)

    required = {"order_id", "event_ts"}
    missing = sorted(required - columns)
    if missing:
        raise ValueError(
            f"Orders input is missing required columns: {missing}"
        )

    if "source_system" not in columns:
        df = df.withColumn("source_system", F.lit("unknown"))

    return df.select(
        F.col("order_id").cast("string"),
        F.col("event_ts").cast("timestamp"),
        F.col("source_system").cast("string"),
    )


layer = spark.conf.get("layer")
DataflowPipeline.invoke_dlt_pipeline(
    spark,
    layer,
    bronze_custom_transform_func=normalize_orders,
)
```

SDP-META invokes `normalize_orders(df, dataflow_spec)` once for the primary
DataFrame and once for each bronze append-flow DataFrame. The second argument
is the shared parent `BronzeDataflowSpec` or `SilverDataflowSpec`, not
append-flow metadata; this example does not need to inspect it. Use the
corresponding `silver_custom_transform_func` argument for a silver-layer
transform.

### Loading persisted dataflow specifications

Persisted v0.0.10 and v0.1.0 dataflow-spec rows can lack fields introduced in
v0.1.1. The supported migration boundary is the official reader API:

```python
from databricks.labs.sdp_meta.dataflow_spec import DataflowSpecUtils

bronze_specs = DataflowSpecUtils.get_bronze_dataflow_spec(spark)
silver_specs = DataflowSpecUtils.get_silver_dataflow_spec(spark)
```

These readers load the latest persisted row for each dataflow and backfill the
registered compatibility fields added since the legacy schema before
constructing `BronzeDataflowSpec` and `SilverDataflowSpec` objects. Append
onboarding also evolves legacy Delta spec tables additively so new fields can
be persisted without replacing existing rows.

Do not treat direct construction from a legacy Spark row as equivalent:

```python
# Unsupported for legacy persisted rows: missing fields are not backfilled.
BronzeDataflowSpec(**legacy_row.asDict())
SilverDataflowSpec(**legacy_row.asDict())
```

Code that reads spec tables directly must either switch to the official
readers or explicitly provide every field required by the installed
dataclasses. Direct dataclass construction has no legacy-row compatibility
guarantee.

## v0.1.1 upgrade checklist

### Upgrading from v0.0.10

- Move local and automated environments to Python 3.10–3.12 before installing
  v0.1.1.
- Test the upgrade against copies of the bronze and silver dataflow-spec Delta
  tables and pipeline outputs.
- Update package names, CLI commands, imports, runner notebooks, and
  layer-specific callback argument names using the steps above.
- Migrate pipelines from legacy DPM publishing mode before upgrading.
- Prefer the `sdp_meta_*` workspace config keys. v0.1.1 accepts the four
  `dlt_meta_*` aliases with warnings, but the aliases are temporary.
- Run append onboarding once in a non-production environment and verify that
  the legacy spec tables gain current fields without losing rows or audit
  values.
- Load persisted specifications through `get_bronze_dataflow_spec()` and
  `get_silver_dataflow_spec()`; remove direct legacy-row dataclass
  construction.
- Validate every primary and append source against each custom transform, then
  compare target schemas and representative output.

### Upgrading from v0.1.0

- Move local and automated environments to Python 3.10–3.12 before installing
  v0.1.1.
- Test the upgrade against copies of the dataflow-spec tables and pipeline
  outputs.
- Verify legacy Python wheel tasks and workspace configuration before changing
  them; migrate to current package and key names when practical.
- Run append onboarding in a non-production environment and confirm additive
  schema evolution is idempotent.
- Load persisted specifications through the official reader methods rather
  than constructing dataclasses directly from Spark rows.
- Validate every primary and append source against bronze and silver custom
  transforms, paying particular attention to primary-only columns.
- Run mixed-flow, append-flow, and snapshot pipelines and compare schemas,
  row counts, and data-quality results before promoting v0.1.1.

## v0.0.10 breaking changes

### DPM Mode removal

Pipelines using Legacy (DPM) publishing mode must be migrated before upgrading. Follow the Databricks guide: [Migrate to the default publishing mode](https://docs.databricks.com/aws/en/dlt/migrate-to-dpm#migrate-to-the-default-publishing-mode).

:::warning
This migration is irreversible. Test in a non-production environment first.
:::

### `invoke_dlt_pipeline` argument changes

```python
# v0.0.9 and earlier (no longer supported)
DataflowPipeline.invoke_dlt_pipeline(
    spark, layer,
    custom_transform_func=my_func,
    next_snapshot_and_version=my_snapshot_func
)

# v0.0.10 and later (current)
DataflowPipeline.invoke_dlt_pipeline(
    spark, layer,
    bronze_custom_transform_func=my_bronze_func,
    silver_custom_transform_func=my_silver_func,
    bronze_next_snapshot_and_version=my_bronze_snapshot_func,
    silver_next_snapshot_and_version=my_silver_snapshot_func
)
```

## Deprecation timeline

| Phase | Status | Description |
|---|---|---|
| v0.1.1 (current) | Active | Both packages work. Old package shows deprecation warnings. `src.*` imports work via shim. |
| Later v0.1.x | Planned | `dlt-meta` compat package maintained with no new features. |
| v0.2.0 | Planned | `src.*` shim removed — `from src.X import ...` raises `ModuleNotFoundError`. |
| Future | Planned | `dlt-meta` compatibility package removed. |

For help, see [Troubleshooting](./troubleshooting) or [GitHub Issues](https://github.com/databrickslabs/sdp-meta/issues).
