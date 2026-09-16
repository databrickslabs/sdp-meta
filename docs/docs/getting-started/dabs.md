---
id: dabs
title: Declarative Automation Bundles
sidebar_position: 2
---

# Declarative Automation Bundles

A [Databricks Declarative Automation Bundle](https://docs.databricks.com/aws/en/dev-tools/bundles/) (DAB) declares jobs, pipelines, and configuration as code. SDP-META ships a DAB template for deploying the onboarding job, metadata-driven Bronze/Silver pipelines, and an optional native SDP SQL Gold pipeline from git.

## Prerequisites

- Python 3.10+
- Databricks CLI v0.213 or later on `PATH`
- `databricks labs install sdp-meta`

## Scaffold a new bundle

```bash
# Fast path: zero prompts, developer-friendly defaults.
databricks labs sdp-meta bundle-init --quickstart

# Interactive (recommended the first time):
databricks labs sdp-meta bundle-init
```

### Prompt reference

| Prompt | Description |
|---|---|
| `bundle_name` | Folder name and job/pipeline prefix |
| `uc_catalog_name` | Unity Catalog catalog holding the SDP-META schema and target schemas |
| `sdp_meta_schema` | Schema for `bronze_dataflowspec` / `silver_dataflowspec` tables |
| `bronze_target_schema` / `silver_target_schema` | Schemas for Bronze/Silver pipeline outputs |
| `layer` | `bronze`, `silver`, or `bronze_silver` |
| `pipeline_mode` | `split` (default) or `combined`. Only used when `layer=bronze_silver`. |
| `source_format` | `cloudFiles`, `delta`, `kafka`, `eventhub`, or `snapshot` |
| `onboarding_file_format` | `yaml` or `json` |
| `gold_enabled` | Immutable scaffold choice that adds a native SDP SQL Gold pipeline after Silver |
| `gold_target_schema` | Schema where Gold materialized views are published |
| `gold_models_path` | Bundle-relative directory containing Gold `.sql` models |
| `dataflow_group` | Group name that ties flows in the onboarding file to the pipeline |
| `wheel_source` | `pypi` or `volume_path` |
| `sdp_meta_dependency` | PyPI coordinate or `/Volumes/...` wheel path. Default `__SET_ME__` is rejected by `bundle-validate`. |
| `author` | Written to the `import_author` column on DataflowSpec rows |

## Bundle directory structure

```
<bundle_name>/
├── databricks.yml
├── README.md
├── resources/
│   ├── variables.yml
│   ├── sdp_meta_onboarding_job.yml
│   └── sdp_meta_pipelines.yml
├── notebooks/
│   └── init_sdp_meta_pipeline.py
├── gold/models/                  default gold_models_path; create when enabled
├── conf/
│   ├── onboarding.yml  (or .json)
│   ├── silver_transformations.yml  (or .json)
│   └── dqe/
│       └── example_table/
│           └── bronze_expectations.yml  (or .json)
└── recipes/
    ├── from_uc.py
    ├── from_volume.py
    ├── from_inventory.py
    └── from_topics.py
```

`recipes/` contains helper scripts for bulk-adding flows via `bundle-add-flow`. See [CLI Commands](../reference/cli-commands) for usage.

## How Gold connects to Bronze and Silver

Gold does not add a third DataflowSpec table. Bronze and Silver remain the
metadata-driven contract; Gold consumes the Unity Catalog tables published by
Silver through native SDP SQL.

![How native SQL Gold connects to metadata-driven Bronze and Silver](/img/gold-layer-architecture.svg)

The Gold pipeline receives two configuration values:

```yaml
configuration:
  silver_catalog: ${var.uc_catalog_name}
  silver_schema: ${var.silver_target_schema}
```

Models use those values for Silver inputs:

```sql
CREATE OR REFRESH MATERIALIZED VIEW customer_360 AS
SELECT customer_id, email
FROM ${silver_catalog}.${silver_schema}.customers;
```

Gold-to-Gold references should be unqualified, allowing SDP to derive model
ordering from the SQL graph:

```sql
CREATE OR REFRESH MATERIALIZED VIEW high_value_customers AS
SELECT * FROM customer_360 WHERE transaction_count >= 5;
```

### Gold best practices

- Treat published Silver tables as a stable, documented contract for Gold.
- Keep Gold in its own schema and pipeline so business-model changes do not
  alter ingestion metadata or force Bronze/Silver redeployment.
- Use fully qualified, configuration-driven names for Silver inputs and
  unqualified names for dependencies within the Gold pipeline.
- Put one published materialized view or streaming table declaration in each
  model file; temporary views may support that declaration.
- Run Gold after Silver through the generated workflow dependency rather than
  relying on schedules that can overlap.
- Keep Gold serverless. Incremental materialized-view refresh also requires
  compatible Silver Delta change tracking; otherwise SDP can recompute safely.
- Run `bundle-validate` before deployment to catch missing paths, duplicate
  dataset names, unsupported template syntax, and deprecated `LIVE TABLE` SQL.

:::note
Gold support is orchestration of native SDP SQL, not metadata-driven Gold
onboarding. There is no `gold_dataflowspec` table or Python Gold runtime.
:::

## Installing SDP-META on pipelines

### Option A — PyPI

```yaml
# resources/variables.yml
wheel_source:
  default: pypi
sdp_meta_dependency:
  default: databricks-labs-sdp-meta==0.1.0
```

### Option B — UC Volume wheel

```bash
cd <bundle_name>
databricks labs sdp-meta bundle-prepare-wheel
```

Then paste the printed path into `resources/variables.yml`:

```yaml
wheel_source:
  default: volume_path
sdp_meta_dependency:
  default: /Volumes/<catalog>/<schema>/<volume>/databricks_labs_sdp_meta-0.1.0-py3-none-any.whl
```

## Validate → deploy → run

```bash
cd <bundle_name>
databricks labs sdp-meta bundle-validate
databricks bundle deploy --target dev
databricks bundle run onboarding --target dev
databricks bundle run pipelines --target dev
```

After the onboarding job runs:

![DAB onboarding job](/img/dab_onboarding_job.png)

![DAB Declarative pipelines](/img/dab_dlt_pipelines.png)

### What bundle-validate checks

- Onboarding file exists under `conf/`
- `dataflow_group` variable is referenced by at least one flow in the onboarding file
- `layer` variable matches the pipelines declared
- `sdp_meta_dependency` is not `__SET_ME__`
- `sdp_meta_dependency` shape matches `wheel_source`
- No `<your-...>` placeholders in `conf/onboarding.*` or `databricks.yml`
- All YAML/JSON files parse cleanly
- Gold model paths stay inside the bundle and contain valid SQL model files
- Gold dataset names are unique and deprecated `LIVE TABLE` syntax is absent

## Promote to prod

```bash
databricks bundle deploy --target prod
```

Per-target variable overrides go under `targets.<name>.variables` in `databricks.yml`.

### CI/CD: run_as for prod

Uncomment the `run_as` block in the prod target and set the service principal application ID:

```yaml
targets:
  prod:
    mode: production
    # run_as:
    #   service_principal_name: <your-prod-service-principal-application-id>
```

## Split vs combined pipelines

When `layer=bronze_silver`:

- **`pipeline_mode=split` (default)** — two separate pipelines. Silver waits for Bronze. Independent rollback and lifecycle management.
- **`pipeline_mode=combined`** — one pipeline, one update cycle. Lower overhead; best when Bronze and Silver always run together.

To switch: change `pipeline_mode` in `resources/variables.yml` and redeploy.

## Bundle CLI commands

| Command | What it does |
|---|---|
| `bundle-init` | Scaffold a new SDP-META DAB from the packaged template |
| `bundle-prepare-wheel` | Build the local wheel and upload it to a UC Volume |
| `bundle-add-flow` | Append one or more flow entries to the bundle's onboarding file |
| `bundle-add-gold` | Idempotently add native SQL Gold resources to an existing Silver-bearing bundle |
| `bundle-validate` | Run `databricks bundle validate` plus SDP-META-specific consistency checks |

### Adding Gold to an existing bundle

```bash
cd <bundle_name>
databricks labs sdp-meta bundle-add-gold
databricks labs sdp-meta bundle-validate
databricks bundle deploy --target dev
databricks bundle run pipelines --target dev
```

The command rejects Bronze-only bundles because Gold requires published Silver
inputs. It prevalidates the planned edits before writing and is safe to run
again. It does not invent business models: add at least one Silver-compatible
`.sql` model under `gold_models_path` before validation or deployment.

### Adding flows with bundle-add-flow

```bash
# Single flow (interactive prompts)
databricks labs sdp-meta bundle-add-flow

# Bulk from CSV
databricks labs sdp-meta bundle-add-flow
# pick "csv" mode and point at the file
```

`bundle-add-flow` pulls bundle defaults from `resources/variables.yml`, auto-increments `data_flow_id`, and refuses to write on ID collisions.

:::tip
After editing the onboarding file, re-run only the **onboarding job** — not `databricks bundle deploy` — unless you also changed `resources/variables.yml` or the bundle YAML files.
:::
