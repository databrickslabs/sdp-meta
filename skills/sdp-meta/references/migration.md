# Migration: DLT-META to SDP-META

Reference for agents helping a user migrate from **DLT-META** or SDP-META
v0.1.x to **SDP-META v0.2.0**. Version 0.2.0 removes the legacy package,
import, command, runner, and task-package shims. The four persisted
`dlt_meta_*` workspace keys remain readable with deprecation warnings.

Move Python 3.8/3.9 environments to a supported interpreter before upgrading.
Always uninstall the old Labs project and `dlt-meta` Python distribution, when
present, before installing v0.2.0; an old Python installation can leave a stale
`dlt_meta.pth` startup hook behind.

The `dlt-meta` distribution is frozen at v0.1.1. Installing it from an old
requirements file pins `databricks-labs-sdp-meta` to the 0.1.x line and can
downgrade a newer installation. Remove it from requirements and install
`databricks-labs-sdp-meta>=0.2.0` directly.

---

## Core runtime migration

The onboarding file, dataflowspec fields, pipeline IDs, data, and checkpoints
can remain unchanged. The installed package and pipeline runner must change;
Labs CLI installations and Python wheel task metadata must also use canonical
names where present.

### Step 1 — Onboarding: update the pip install

Wherever `dlt-meta` is installed for the onboarding job (cluster init script, job task `%pip install`, or whl library), change it to:

```bash
# Before
pip install dlt-meta==0.0.10

# After
python -m pip uninstall -y dlt-meta
python -m pip install databricks-labs-sdp-meta==0.2.0
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

Also update the pipeline configuration key from `dlt_meta_whl` →
`sdp_meta_whl` and point it at the v0.2.0 wheel path or PyPI coordinate. The
old runner and imports do not work with v0.2.0.

---

## What changed at a glance

| Component | Before | After (v0.2.0) |
|---|---|---|
| PyPI package | `dlt-meta` | `databricks-labs-sdp-meta` |
| CLI install | `databricks labs install dlt-meta` | `databricks labs install sdp-meta` |
| CLI command | `databricks labs dlt-meta` | `databricks labs sdp-meta` |
| Python import | `from dlt_meta import ...` | `from databricks.labs.sdp_meta import ...` |
| Source layout | `src/dataflow_pipeline.py` (flat) | `src/databricks/labs/sdp_meta/dataflow_pipeline.py` |
| Main class | `DLTMeta` | `SDPMeta` |
| Constants | `DLT_META_RUNNER_NOTEBOOK` | `SDP_META_RUNNER_NOTEBOOK` |
| Default dataflowspec schema for new installs | `dlt_meta_dataflowspecs` | `sdp_meta_dataflowspecs` |
| Config key | `dlt_meta_schema` | `sdp_meta_schema` |
| PythonWheelTask `package_name` | `dlt_meta` | `databricks_labs_sdp_meta` |
| Runner notebook | `init_dlt_meta_pipeline.py` | `init_sdp_meta_pipeline.py` |
| Quarantine field | `bronze_quarantine_table_name` | `bronze_quarantine_table` |

## What did NOT change

- **Onboarding file format** — existing JSON/YAML files work without modification.
- **Dataflowspec field names** — all bronze/silver fields (`bronze_table`, `silver_cdc_apply_changes`, partition columns, cluster_by, DQE paths, etc.) are unchanged.
- **Pipeline behavior and `invoke_dlt_pipeline` signature** (as of v0.0.10).
- **Data in existing pipeline output tables** — no data migration required.
- **Existing physical schema and table names** — keep them in place and point
  the canonical `sdp_meta_schema` key at the existing schema.

---

## Step-by-step migration

### 1. Update installation

```bash
databricks labs uninstall dlt-meta
python -m pip uninstall -y dlt-meta
databricks labs install sdp-meta
# or directly:
python -m pip install databricks-labs-sdp-meta==0.2.0
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
%pip install databricks-labs-sdp-meta==0.2.0
```

Pipeline invocation code is **unchanged**:

```python
layer = spark.conf.get("layer", None)
from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline
DataflowPipeline.invoke_dlt_pipeline(spark, layer)
```

### 5. Update config keys (recommended)

```json
// Before
{ "dlt_meta_schema": "my_schema" }

// After
{ "sdp_meta_schema": "my_schema" }
```

Version 0.2.0 still reads the four persisted `dlt_meta_*` workspace keys with
deprecation warnings. Canonical keys take precedence when both forms exist,
and saved configuration uses only `sdp_meta_*` names.

The CLI also reads `dlt_meta_schema` from an existing
`onboarding_job_details.json` with a warning. `sdp_meta_schema` takes
precedence when both keys are present.

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

## Preserve state during migration

Keep the onboarding JSON/YAML, dataflow-spec tables, output tables, pipeline
IDs, storage locations, and checkpoints. Update the installed package, runner,
imports, wheel configuration key, workspace config keys, and
`PythonWheelTask.package_name` in place. Test against copied metadata and
representative data before promoting the change.

Do not tell users that `dlt-meta`, `dlt_meta`, `src.*`, `DLTMeta`, or the old
CLI still work. The persisted configuration exceptions are the four
`dlt_meta_*` workspace keys and `dlt_meta_schema` in an existing
`onboarding_job_details.json`; v0.2.0 reads them with deprecation warnings,
but new and saved configuration should use canonical `sdp_meta_*` names.

---

## Deprecation timeline

| Version | Status | What changes |
|---|---|---|
| v0.1.0 | Previous | Initial renamed release; upgrade to v0.1.1 for compatibility fixes. |
| v0.1.1 | Historical | Legacy naming shims were available with warnings. |
| v0.2.0 | Current | Package, import, command, runner, and task-package shims are removed. Persisted `dlt_meta_*` workspace keys remain readable with warnings. |

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

### Legacy publishing mode

SDP-META v0.2.0 supports upgrading an existing legacy-publishing-mode pipeline
in place while preserving its pipeline ID, target schema, tables, and
checkpoints. Migrating separately to
[default publishing mode](https://docs.databricks.com/aws/en/dlt/migrate-to-dpm)
is optional and irreversible. Test either path in a non-production environment.

---

## Common migration issues

| Symptom | Cause | Fix |
|---|---|---|
| `DeprecationWarning: dlt_meta is deprecated` | Still using `from dlt_meta import ...` | Update imports to `from databricks.labs.sdp_meta import ...` |
| `ModuleNotFoundError: No module named 'src'` | `src.*` shim removed (v0.2.0+) or not yet installed | Install `databricks-labs-sdp-meta`; update to namespace imports |
| Quarantine table not created after upgrade | Old `*_quarantine_table_name` key silently ignored | Rename to `*_quarantine_table` in onboarding spec and re-run onboard |
| Pipeline still uses old runner notebook | `init_dlt_meta_pipeline.py` references stale imports | Replace with `init_sdp_meta_pipeline.py` from the repo |
| `databricks labs dlt-meta` fails | CLI forwarding was removed in v0.2.0 | Use `databricks labs sdp-meta` |
