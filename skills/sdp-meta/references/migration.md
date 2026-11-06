# Migration: DLT-META to SDP-META

Reference for agents helping a user migrate from **DLT-META (v0.0.10)** to
**SDP-META (v0.1.1)**. Follow this when the user mentions upgrading, renaming,
or getting deprecation warnings after upgrading.

SDP-META v0.1.1 requires Python 3.10–3.12. Move Python 3.8/3.9 environments
to a supported interpreter before installing either distribution.

---

## Existing customers — minimal two-step migration

Most customers only need two changes. Everything else (onboarding file, dataflowspec fields, pipeline behavior) is unchanged.

### Step 1 — Onboarding: update the pip install

Wherever `dlt-meta` is installed for the onboarding job (cluster init script, job task `%pip install`, or whl library), change it to:

```bash
# Before
pip install dlt-meta==0.0.10

# After
pip install databricks-labs-sdp-meta==0.1.1
```

### Step 2 — SDP Pipeline: swap the runner notebook

Replace the old pipeline runner notebook with the new one. The only differences are the `%pip install` package name, the pipeline config key, and the import path.

**Old notebook (`init_dlt_meta_pipeline.py`):**

```python
dlt_meta_whl = spark.conf.get("dlt_meta_whl")
%pip install $dlt_meta_whl

layer = spark.conf.get("layer", None)
from src.dataflow_pipeline import DataflowPipeline
DataflowPipeline.invoke_dlt_pipeline(spark, layer)
```

**New notebook (`init_sdp_meta_pipeline.py`):**

```python
sdp_meta_whl = spark.conf.get("sdp_meta_whl")
%pip install $sdp_meta_whl

layer = spark.conf.get("layer", None)
from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline
DataflowPipeline.invoke_dlt_pipeline(spark, layer)
```

Copy the new notebook from [`demo/notebooks/afam_cloudfiles_runners/init_sdp_meta_pipeline.py`](../../../../demo/notebooks/afam_cloudfiles_runners/init_sdp_meta_pipeline.py) or any runner under `demo/notebooks/`.

Also update the pipeline configuration key from `dlt_meta_whl` → `sdp_meta_whl` and point it at the v0.1.1 wheel path or PyPI coordinate.

> **Note:** If you cannot update the notebook immediately, just swapping the `dlt_meta_whl` config to point at the v0.1.1 wheel still works — the v0.1.1 wheel bundles a `src` compat package that makes `from src.dataflow_pipeline import DataflowPipeline` keep resolving. This shim is **removed in v0.2.0**.

---

---

## What changed at a glance

| Component | Before (v0.0.10) | After (v0.1.1) |
|---|---|---|
| PyPI package | `dlt-meta` | `databricks-labs-sdp-meta` |
| CLI install | `databricks labs install dlt-meta` | `databricks labs install sdp-meta` |
| CLI command | `databricks labs dlt-meta` | `databricks labs sdp-meta` |
| Python import | `from dlt_meta import ...` | `from databricks.labs.sdp_meta import ...` |
| Source layout | `src/dataflow_pipeline.py` (flat) | `src/databricks/labs/sdp_meta/dataflow_pipeline.py` |
| Main class | `DLTMeta` | `SDPMeta` |
| Constants | `DLT_META_RUNNER_NOTEBOOK` | `SDP_META_RUNNER_NOTEBOOK` |
| Dataflowspec schema | `dlt_meta_dataflowspecs` | `sdp_meta_dataflowspecs` |
| Config key | `dlt_meta_schema` | `sdp_meta_schema` |
| PythonWheelTask `package_name` | `dlt_meta` | `databricks_labs_sdp_meta` |
| Runner notebook | `init_dlt_meta_pipeline.py` | `init_sdp_meta_pipeline.py` |
| Quarantine field | `bronze_quarantine_table_name` | `bronze_quarantine_table` |

## What did NOT change

- **Onboarding file format** — existing JSON/YAML files work without modification.
- **Dataflowspec field names** — all bronze/silver fields (`bronze_table`, `silver_cdc_apply_changes`, partition columns, cluster_by, DQE paths, etc.) are unchanged.
- **Pipeline behavior and `invoke_dlt_pipeline` signature** (as of v0.0.10).
- **Data in existing pipeline output tables** — no data migration required.

---

## Step-by-step migration

### 1. Update installation

```bash
pip uninstall dlt-meta
databricks labs install sdp-meta
# or directly:
pip install databricks-labs-sdp-meta==0.1.1
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
# Before (deprecated — raises DeprecationWarning in v0.1.0, breaks in v0.2.0)
from dlt_meta.cli import DLTMeta
from dlt_meta import DataflowPipeline
from src.dataflow_pipeline import DataflowPipeline   # v0.0.10 flat layout

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

Pipeline invocation code is **unchanged**:

```python
layer = spark.conf.get("layer", None)
from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline
DataflowPipeline.invoke_dlt_pipeline(spark, layer)
```

### 5. Update config keys (optional — old key still works with a warning)

```json
// Before
{ "dlt_meta_schema": "my_schema" }

// After
{ "sdp_meta_schema": "my_schema" }
```

### 6. Fix quarantine field name (breaking — silent if missed)

If any onboarding spec uses `bronze_quarantine_table_name` or
`silver_quarantine_table_name`, rename to `bronze_quarantine_table` /
`silver_quarantine_table`. The old key is silently ignored — no error is raised,
but quarantine tables will not be created.

```json
// Before (silently ignored in v0.1.0)
{ "bronze_quarantine_table_name": "my_quarantine" }

// After
{ "bronze_quarantine_table": "my_quarantine" }
```

---

## Backward compatibility shim

The `dlt-meta` PyPI package is preserved as a thin redirect — no code change
required for customers who cannot update immediately:

- `pip install dlt-meta==0.1.1` installs `databricks-labs-sdp-meta==0.1.1` as a dependency.
- `from dlt_meta import ...` re-exports all public symbols with a `DeprecationWarning`.
- `from src.X import ...` resolves via a `sys.modules` shim (removed in v0.2.0).
- `DLTMeta` is aliased to `SDPMeta`.
- `databricks labs dlt-meta` CLI commands forward to `sdp-meta` with a banner.

**Do not rely on the shim permanently** — `src.*` imports break in v0.2.0.

---

## Deprecation timeline

| Version | Status | What changes |
|---|---|---|
| v0.1.0 | Previous | Initial renamed release; upgrade to v0.1.1 for compatibility fixes. |
| v0.1.1 | Current | Both packages work. Old names show deprecation warnings. `src.*` shim active. |
| v0.1.x | Supported series | `dlt-meta` compat maintained; no new features added to it. |
| v0.2.0 | Planned | `src.*` shim removed — `from src.X import ...` raises `ModuleNotFoundError`. |
| Future | Planned | `dlt-meta` compat package removed from PyPI. |

---

## v0.0.10 → v0.0.9 breaking changes (if migrating from earlier)

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

### DPM mode removal

Pipelines using Legacy (DPM) publishing mode must be migrated before upgrading.
Follow: [Migrate to the default publishing mode](https://docs.databricks.com/aws/en/dlt/migrate-to-dpm).
This migration is **irreversible** — test in a non-production environment first.

---

## Common migration issues

| Symptom | Cause | Fix |
|---|---|---|
| `DeprecationWarning: dlt_meta is deprecated` | Still using `from dlt_meta import ...` | Update imports to `from databricks.labs.sdp_meta import ...` |
| `ModuleNotFoundError: No module named 'src'` | `src.*` shim removed (v0.2.0+) or not yet installed | Install `databricks-labs-sdp-meta`; update to namespace imports |
| Quarantine table not created after upgrade | Old `*_quarantine_table_name` key silently ignored | Rename to `*_quarantine_table` in onboarding spec and re-run onboard |
| Pipeline still uses old runner notebook | `init_dlt_meta_pipeline.py` references stale imports | Replace with `init_sdp_meta_pipeline.py` from the repo |
| `databricks labs dlt-meta` shows deprecation banner | CLI forwarding shim active | Switch to `databricks labs sdp-meta` |
