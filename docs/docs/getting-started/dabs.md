---
id: dabs
title: Declarative Automation Bundles
sidebar_position: 2
---

# Declarative Automation Bundles

A [Databricks Declarative Automation Bundle](https://docs.databricks.com/aws/en/dev-tools/bundles/) (DAB) declares jobs, pipelines, and configuration as code. SDP-META ships a DAB template for deploying the onboarding job and Lakeflow Spark Declarative Pipelines from git — no interactive wizards, and promoting from `dev` to `prod` is a single command.

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
| `bundle-add-pipeline` | Add another independently configured pipeline topology |
| `bundle-validate` | Run `databricks bundle validate` plus SDP-META-specific consistency checks |

### Adding flows with bundle-add-flow

```bash
# Single flow (interactive prompts)
databricks labs sdp-meta bundle-add-flow

# Bulk from CSV
databricks labs sdp-meta bundle-add-flow
# pick "csv" mode and point at the file
```

`bundle-add-flow` pulls bundle defaults from `resources/variables.yml`, auto-increments `data_flow_id`, and refuses to write on ID collisions.

### Adding independent pipelines

One bundle can contain multiple pipeline topologies with different layers,
data-flow groups, and target schemas:

```bash
databricks labs sdp-meta bundle-add-pipeline
```

Choose a resource name, layer (`bronze`, `silver`, or `bronze_silver`), split
or combined mode where applicable, and the `data_flow_group`. The command
updates both `resources.pipelines` and the `pipelines` job. Add matching
onboarding rows with `bundle-add-flow`, then run `bundle-validate`.

:::tip
After editing the onboarding file, re-run only the **onboarding job** — not `databricks bundle deploy` — unless you also changed `resources/variables.yml` or the bundle YAML files.
:::
