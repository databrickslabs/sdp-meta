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
- Pipeline behavior and API method signatures.
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
%pip install databricks-labs-sdp-meta==0.1.0
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
| v0.1.0 (current) | Active | Both packages work. Old package shows deprecation warnings. `src.*` imports work via shim. |
| v0.1.x | Planned | `dlt-meta` compat package maintained with no new features. |
| v0.2.0 | Planned | `src.*` shim removed — `from src.X import ...` raises `ModuleNotFoundError`. |
| Future | Planned | `dlt-meta` compatibility package removed. |

For help, see [Troubleshooting](./troubleshooting) or [GitHub Issues](https://github.com/databrickslabs/sdp-meta/issues).
