---
id: dab-parameters
title: DAB Bundle Parameters
sidebar_position: 5
---

# DAB Bundle Parameters

This page covers all parameters exposed when scaffolding a bundle with `databricks labs sdp-meta bundle-init`, and all keys in the generated `resources/variables.yml`.

---

## Bundle Init Prompts

When you run `databricks labs sdp-meta bundle-init`, the template walks you through 13 prompts. Pass `--quickstart` to skip all prompts and accept developer defaults.

| # | Prompt Key | Default | Description |
|---|---|---|---|
| 1 | `bundle_name` | `my_sdp_meta_pipeline` | Folder name and prefix for every generated job and pipeline resource name |
| 2 | `uc_catalog_name` | `main` | Unity Catalog holding the SDP-META schema and target schemas |
| 3 | `sdp_meta_schema` | `sdp_meta_dataflowspecs` | Schema that holds the `bronze_dataflowspec` and `silver_dataflowspec` tables |
| 4 | `bronze_target_schema` | `sdp_meta_bronze` | Schema where the bronze pipeline writes its tables |
| 5 | `silver_target_schema` | `sdp_meta_silver` | Schema where the silver pipeline writes its tables |
| 6 | `layer` | `bronze_silver` | Layer(s) to deploy: `bronze`, `silver`, or `bronze_silver` |
| 7 | `pipeline_mode` | `split` | Only relevant when `layer=bronze_silver`. `split` deploys two pipelines (silver depends on bronze); `combined` deploys a single pipeline with both layers in one DAG |
| 8 | `source_format` | `cloudFiles` | Seed format for the example flow: `cloudFiles`, `delta`, `kafka`, `eventhub`, or `snapshot` |
| 9 | `onboarding_file_format` | `yaml` | File format for generated onboarding and transformation files: `yaml` or `json` |
| 10 | `dataflow_group` | `my_group` | The `data_flow_group` value used in the seeded onboarding file — must match the pipeline's `*.group` configuration |
| 11 | `wheel_source` | `pypi` | Where sdp-meta is installed from: `pypi` or `volume_path` |
| 12 | `sdp_meta_dependency` | `__SET_ME__` | Concrete install specification: a PyPI coordinate (e.g. `databricks-labs-sdp-meta==0.1.1`) or a `/Volumes/...` wheel path |
| 13 | `author` | `sdp-meta-user` | Written to the `import_author` column on dataflowspec rows |

:::warning
The `__SET_ME__` sentinel in `sdp_meta_dependency` is intentional. `bundle-validate` and the runner notebook both reject it, so deployment is blocked until you set a real value.
:::

---

## `resources/variables.yml` Keys

After scaffolding, every prompt answer is stored as a bundle variable in `resources/variables.yml`. You can override individual variables per target without editing other files.

| Key | Type | Default | Description |
|---|---|---|---|
| `uc_catalog_name` | string | `main` | Unity Catalog name used across all resources |
| `sdp_meta_schema` | string | `sdp_meta_dataflowspecs` | Schema holding the dataflowspec control tables |
| `bronze_target_schema` | string | `sdp_meta_bronze` | Target schema for bronze tables |
| `silver_target_schema` | string | `sdp_meta_silver` | Target schema for silver tables |
| `layer` | string | `bronze_silver` | Pipeline layer(s): `bronze`, `silver`, or `bronze_silver` |
| `pipeline_mode` | string | `split` | Pipeline topology when `layer=bronze_silver`: `split` or `combined` |
| `dataflow_group` | string | `my_group` | Group key that ties the onboarding file to the pipeline configuration |
| `sdp_meta_dependency` | string | `__SET_ME__` | Install spec for sdp-meta in jobs and pipeline notebooks |
| `author` | string | `sdp-meta-user` | Import author label on dataflowspec rows |
| `env` | string | `dev` | Environment suffix used in `{env}`-parameterized onboarding fields |

---

## Choosing `pipeline_mode`

| Mode | When to use |
|---|---|
| `split` (default) | Bronze and silver have different SLAs, update frequencies, or ownership. Each pipeline has independent update history. Silver pipeline depends on bronze completing first. |
| `combined` | Bronze and silver are a tight unit. One pipeline, one schedule, one set of metrics. Lower overhead but you must recompute both layers together. |

You can change `pipeline_mode` in `resources/variables.yml` at any time, then redeploy and re-run.

---

## Choosing `wheel_source`

| Source | When to use |
|---|---|
| `pypi` | Standard case — installs `databricks-labs-sdp-meta` from PyPI at job and pipeline runtime |
| `volume_path` | Air-gapped environments or pinning a locally built wheel. Run `bundle-prepare-wheel` to build and upload the wheel, then set `sdp_meta_dependency` to the resulting `/Volumes/...` path |

---

## Generated Bundle Files

| File | Purpose |
|---|---|
| `databricks.yml` | Bundle definition with `dev` (development mode) and `prod` (production mode) targets |
| `resources/variables.yml` | All configurable parameters with per-target overrides |
| `resources/sdp_meta_onboarding_job.yml` | Python wheel task that calls `databricks_labs_sdp_meta:run` to write or update dataflow specs |
| `resources/sdp_meta_pipelines.yml` | Lakeflow Spark Declarative Pipeline(s) plus a job that runs them end-to-end |
| `notebooks/init_sdp_meta_pipeline.py` | Pipeline runner notebook — pip-installs sdp-meta from `${var.sdp_meta_dependency}` and calls `DataflowPipeline.invoke_dlt_pipeline(spark, layer)` |
| `conf/onboarding.{yml,json}` | Seeded flow definition, branched by `source_format` at scaffold time |
| `conf/silver_transformations.{yml,json}` | Per-target SELECT projections for silver layers |
| `conf/dqe/example_table/bronze_expectations.{yml,json}` | Example DQE expectations for the bronze table |
| `.gitignore` | Ignores `.databricks/`, `.venv/`, `__pycache__/` |

---

## Quickstart Flow

```bash
# Scaffold with zero prompts
databricks labs sdp-meta bundle-init --quickstart
cd my_sdp_meta_pipeline

# Set a real sdp_meta_dependency value
sed -i 's/__SET_ME__/databricks-labs-sdp-meta==0.1.1/' resources/variables.yml

# Validate before deploying
databricks labs sdp-meta bundle-validate

# Deploy and run
databricks bundle deploy --target dev
databricks bundle run onboarding --target dev
databricks bundle run pipelines --target dev
```
