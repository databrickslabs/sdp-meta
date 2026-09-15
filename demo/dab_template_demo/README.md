# DAB Template Demo

End-to-end showcase of the **new** sdp-meta DAB-template features
(`bundle-init`, `bundle-prepare-wheel`, `bundle-add-flow`, the four
`recipes/*.py`, `bundle-validate`, and `pipeline_mode` split vs combined)
against the source shapes the existing `demo/` directory already covers in
template form: **cloudFiles**, **Kafka**, **Event Hubs**, and **Delta**.

The single launcher [`demo/launch_dab_template_demo.py`](../launch_dab_template_demo.py)
walks every feature in six stages. Stages 1-5 run **fully offline** (no
workspace required), so you can iterate locally; stages 2 (apply mode) and 6
talk to Databricks.

## What it covers

| Feature added in the DAB work | Where it shows up in the demo |
|---|---|
| `bundle-init` (12-prompt template) | STAGE 1, one bundle per scenario, pre-answered via `answers/<scenario>.json` |
| `pipeline_mode = split` | `cloudfiles` and `delta` scenarios -> TWO SDP Pipelines (`bronze` + `silver`, silver depends_on bronze) |
| `pipeline_mode = combined` | `cloudfiles_combined` and `eventhub` scenarios -> ONE SDP Pipeline (`bronze_silver`) materializing both layers in a single DAG |
| `layer = bronze` only | `kafka` scenario |
| `source_format = cloudFiles` | `cloudfiles` (split) and `cloudfiles_combined` (combined) — same demo data, different topology |
| `source_format = kafka` | `kafka` scenario |
| `source_format = eventhub` | `eventhub` scenario |
| `source_format = delta` | `delta` scenario |
| `bundle-add-flow` auto-seeding `silver_transformations.{yml,json}` | every silver-bearing scenario; one default `target_table: <silver_table>, select_exp: ["*"]` row is appended per new flow so the silver dataflowspec join isn't empty |
| `wheel_source = volume_path` + `__SET_ME__` sentinel | every scenario; STAGE 2 either pins a placeholder (offline) or runs `bundle-prepare-wheel` (with `--apply-prepare-wheel`) |
| `bundle-prepare-wheel` | STAGE 2 with `--apply-prepare-wheel` |
| `bundle-add-flow --from-csv` | STAGE 3, CSV per scenario in `flows/` |
| `recipes/from_volume.py` | STAGE 4 of the `cloudfiles` scenario (against a seeded fake landing tree) |
| `recipes/from_topics.py` (kafka) | STAGE 4 of the `kafka` scenario, topics from `topics/kafka_topics.txt` |
| `recipes/from_topics.py` (eventhub) | STAGE 4 of the `eventhub` scenario, topics from `topics/eventhub_topics.txt` |
| `recipes/from_uc.py` | STAGE 4 of the `delta` scenario (only with `--apply-recipe` + `--profile` since it lists real UC tables) |
| `bundle-validate` (incl. `__SET_ME__` / wheel_source / pipeline_mode checks) | STAGE 5 — runs sdp-meta sanity checks AND `databricks bundle validate` |
| YAML / JSON onboarding format | cloudfiles + eventhub + delta use YAML, kafka uses JSON — exercises both round-trips |
| Onboarding summary print | STAGE 5 prints per-flow `data_flow_id`, `source_format`, `bronze_table` |

## File layout

```
demo/
├── launch_dab_template_demo.py     # the orchestrator (run this)
└── dab_template_demo/
    ├── README.md                   # (this file)
    ├── answers/
    │   ├── cloudfiles_split.json    # bronze_silver, SPLIT,    cloudFiles, yaml
    │   ├── cloudfiles_combined.json # bronze_silver, COMBINED, cloudFiles, yaml (same data as split)
    │   ├── kafka_bronze.json        # bronze,        n/a,      kafka,      json
    │   ├── eventhub_combined.json   # bronze_silver, COMBINED, eventhub,   yaml
    │   └── delta_split.json         # bronze_silver, SPLIT,    delta,      yaml
    ├── flows/
    │   ├── cloudfiles_extra.csv    # 4 cloudFiles flows reusing demo/resources/data/{customers,transactions,products,stores}
    │   ├── kafka_extra.csv         # 3 kafka flows (iot_sensors, payments, audit_log)
    │   ├── eventhub_extra.csv      # 3 eventhub flows (iot_telemetry, clicks, events)
    │   └── delta_extra.csv         # 5 delta flows (mirroring <catalog>.staging.*)
    └── topics/
        ├── kafka_topics.txt        # 10 topics for from_topics.py
        └── eventhub_topics.txt     # 5 names for from_topics.py
```

### Pipeline mode: `split` vs `combined`

The DAB template's `pipeline_mode` answer controls how many Lakeflow
Declarative Pipelines (SDP) are rendered when `layer=bronze_silver`:

| `pipeline_mode` | SDP Pipelines rendered in `resources/sdp_meta_pipelines.yml` | Runner notebook call |
|---|---|---|
| `split` | TWO pipelines: `bronze` and `silver`. The `pipelines` job runs `silver` after `bronze` (`depends_on`). | per-pipeline: `invoke_dlt_pipeline(spark, "bronze")` and `invoke_dlt_pipeline(spark, "silver")` |
| `combined` | ONE pipeline: `bronze_silver`, with both `bronze.dataflowspecTable` and `silver.dataflowspecTable` in `configuration`. | `invoke_dlt_pipeline(spark, "bronze_silver")` — registers both layers' `@dp.table` decorators in a single graph |

`pipeline_mode` is **only** consulted when `layer=bronze_silver`; with
`layer=bronze` or `layer=silver` there's only ever one pipeline by
definition.

The demo ships paired scenarios so you can compare side-by-side without
hand-editing answers:

| Scenario | `layer` | `pipeline_mode` | Source | Demo data |
|---|---|---|---|---|
| `cloudfiles` | `bronze_silver` | **`split`** | cloudFiles | Real CSVs uploaded to UC volume |
| `cloudfiles_combined` | `bronze_silver` | **`combined`** | cloudFiles | Same as above |
| `kafka` | `bronze` | n/a | Kafka | Placeholder topics |
| `eventhub` | `bronze_silver` | **`combined`** | Event Hubs | Placeholder topics |
| `delta` | `bronze_silver` | **`split`** | Delta tables | Mirrors `<catalog>.staging.*` |

To run the same data through a combined SDP instead of the split topology,
just swap `--scenario cloudfiles` for `--scenario cloudfiles_combined`:

```bash
python demo/launch_dab_template_demo.py \
  --scenario cloudfiles_combined \
  --uc-catalog-name <cat> \
  --uc-schema   <sch> \
  --uc-volume   <vol> \
  --apply-prepare-wheel --apply-recipe --apply-deploy \
  --profile <profile>
```

After `--apply-deploy`, in the workspace UI you'll see ONE pipeline named
`dab_demo_cloudfiles_combined - bronze+silver` with all bronze + silver
tables in a single graph, instead of two separate pipelines.

### Why the silver pipeline used to be empty (auto-seeded transformations)

The onboarding job builds the silver dataflowspec table by **inner-joining**
each onboarding row's `silver_transformation_json_dev` file against the
per-row silver target details on `target_table`. If the transformations
file has no row for a `silver_table`, that silver flow is silently dropped,
and if NONE of the silver_tables match, the silver dataflowspec gets
overwritten with zero rows — at which point the silver SDP Pipeline fails
at startup with `[NO_TABLES_IN_PIPELINE] Pipelines are expected to have at
least one table defined`.

The seeded `conf/silver_transformations.yml` only ships with
`target_table: example_table`, so historically a demo that pruned the
example flow would end up with an empty silver pipeline. `bundle-add-flow`
now keeps this file in sync automatically: every newly-appended onboarding
row that has a `silver_table` triggers a default
`{target_table: <silver_table>, select_exp: ["*"]}` row in
`silver_transformations.{yml,json}` (skipping any `target_table` already
present so user-customizations are preserved). This applies uniformly to
flows added via CSV (`--from-csv`), recipes (`from_volume.py`,
`from_uc.py`, `from_topics.py`, `from_inventory.py`), and the interactive
`bundle-add-flow` CLI. The launcher's `[STAGE 3]` line will tell you how
many rows it auto-seeded; replace the `["*"]` with real projections /
`where_clause` / `partition_columns` whenever you're ready.

### Reusing the existing demo's CSV datasets (cloudfiles scenarios)

To keep the cloudFiles flows pointed at **real, working data** instead of
fabricated `/Volumes/.../landing/...` paths, the launcher re-uses the four
CSV datasets that ship under
`demo/resources/data/{customers,transactions,products,stores}` (the same
files the original `demo/launch_*` scripts rely on).

When you pass `--apply-prepare-wheel`, STAGE 2:

1. Builds + uploads the sdp-meta wheel into `/Volumes/<cat>/<sch>/<vol>/`
   (existing behavior).
2. **Also** uploads each shipped dataset into
   `/Volumes/<cat>/<sch>/<vol>/demo_data/<table>/` (idempotent overwrite).
3. STAGE 3 then substitutes `{demo_data_volume_path}` in
   `flows/cloudfiles_extra.csv` with the real volume base, so each row's
   `source_path` resolves on the workspace.

Because those datasets are CSV (not JSON), each CSV row in
`cloudfiles_extra.csv` declares `cloudfiles_format=csv`, which the new
`cloudFiles.format` per-row override forwards into the flow's
`bronze_reader_options.cloudFiles.format`.

If you skip `--apply-prepare-wheel` (offline mode), the launcher writes a
clearly-fake `/Volumes/<cat>/__placeholder__/__placeholder__/demo_data`
path so the CSV still parses for `bundle-validate`; deploying that bundle
would fail loudly when the SDP Pipeline can't find the source files.

### Other placeholder substitutions

The `{uc_catalog_name}` placeholder in answer files and CSVs is substituted
at runtime by the launcher using the `--uc-catalog-name` argument.

## Quick start (no workspace needed — runs all 5 offline stages)

```bash
# From the repo root (the launcher resolves paths relative to it)
python demo/launch_dab_template_demo.py \
    --scenario all \
    --uc-catalog-name main \
    --out-dir demo_runs
```

This:

1. Scaffolds five bundles under `demo_runs/`:
   - `dab_demo_cloudfiles_split/`        — `pipeline_mode=split`, cloudFiles
   - `dab_demo_cloudfiles_combined/`     — `pipeline_mode=combined`, cloudFiles (same data)
   - `dab_demo_kafka_bronze/`            — `layer=bronze`, kafka
   - `dab_demo_eventhub_combined/`       — `pipeline_mode=combined`, eventhub
   - `dab_demo_delta_split/`             — `pipeline_mode=split`, delta
2. For each bundle, pins a placeholder `sdp_meta_dependency` (so the
   `__SET_ME__` sentinel guard rails are exercised but accepted by the
   sanity checks).
3. Bulk-appends 3-5 flows from the matching CSV via `bundle-add-flow`.
4. Runs the matching recipe in dry-run mode and prints the proposed plan
   (the `delta` scenario skips this stage in offline mode — `from_uc.py`
   needs a real workspace; re-run with `--apply-recipe --profile <p>`).
5. Runs `bundle-validate` and prints a summary table of every flow that
   landed in the rendered onboarding file.

Pick one scenario with:

```bash
python demo/launch_dab_template_demo.py --scenario cloudfiles          --uc-catalog-name main
python demo/launch_dab_template_demo.py --scenario cloudfiles_combined --uc-catalog-name main
python demo/launch_dab_template_demo.py --scenario kafka               --uc-catalog-name main
python demo/launch_dab_template_demo.py --scenario eventhub            --uc-catalog-name main
python demo/launch_dab_template_demo.py --scenario delta               --uc-catalog-name main
```

## Apply mode — actually upload the wheel and deploy

```bash
# Build + upload the local sdp-meta wheel to a UC volume, validate, then
# deploy and run onboarding + pipelines against the workspace.
python demo/launch_dab_template_demo.py \
    --scenario kafka \
    --uc-catalog-name main \
    --uc-schema sdp_meta_dab_demo_kafka \
    --uc-volume sdp_meta_wheels \
    --apply-prepare-wheel \
    --apply-recipe \
    --apply-deploy \
    --profile DEFAULT
```

| Flag | What it turns on |
|---|---|
| `--apply-prepare-wheel` | STAGE 2 actually runs `bundle-prepare-wheel` (builds the wheel, ensures the volume, uploads, pastes the `/Volumes/...` path into `variables.yml`). Requires `--uc-schema` + `--uc-volume`. |
| `--apply-recipe` | STAGE 4 runs the recipe with `--apply` so the recipe-discovered flows are actually appended to `conf/onboarding.*`. |
| `--apply-deploy` | STAGE 6 runs `databricks bundle deploy --target dev && bundle run onboarding && bundle run pipelines`. |

Each flag is independent; you can flip them on one at a time as you grow
confidence.

### Side-by-side: split vs combined for cloudFiles

Run both back-to-back with the same UC catalog/schema/volume to see the
exact same data (`customers`, `transactions`, `products`, `stores`, plus the
recipe-discovered streaming tables) materialized two different ways:

```bash
# 1) Split: two SDP Pipelines (bronze + silver)
python demo/launch_dab_template_demo.py \
  --scenario cloudfiles \
  --uc-catalog-name <<your_catalog>> \
  --uc-schema sdp_meta_dab_demo \
  --uc-volume sdp_meta_wheels \
  --apply-prepare-wheel --apply-recipe --apply-deploy \
  --profile <<profile name>> \
  --pip-index-url https://pypi.internal.example.com/simple

# 2) Combined: one SDP Pipeline materializing both layers in a single DAG
python demo/launch_dab_template_demo.py \
  --scenario cloudfiles_combined \
  --uc-catalog-name <<your_catalog>> \
  --uc-schema sdp_meta_dab_demo \
  --uc-volume sdp_meta_wheels \
  --apply-prepare-wheel --apply-recipe --apply-deploy \
  --profile  <<profile name>> \
  --pip-index-url https://pypi.internal.example.com/simple
```

Bundle directories (`demo_runs/dab_demo_cloudfiles_split/` vs
`demo_runs/dab_demo_cloudfiles_combined/`), pipeline names, and `bundle run
pipelines` job topology will differ; the demo data, dataflow_group,
schemas, and onboarding rows are identical so the output tables land in
the same UC location.

## How this re-uses the existing demo assets

The four scenarios mirror the source shapes already covered by
`demo/conf/{json,yml}/`, plus a Delta upstream variant:

| Existing template (`demo/conf/...`) | New CSV in this demo (`flows/...`) |
|---|---|
| `cloudfiles-onboarding.template.yml` | `cloudfiles_extra.csv` (orders, customers, transactions, products, stores) |
| `kafka-sink-onboarding.template.yml` | `kafka_extra.csv` (iot_sensors, payments, audit_log) |
| `eventhub-onboarding.template.yml` | `eventhub_extra.csv` (iot_telemetry, clicks, events) |
| (no legacy template — new) | `delta_extra.csv` (5 upstream tables in `<catalog>.staging.*`) |

The CSVs intentionally omit the deeply customized fields from the legacy
templates (DDL paths, secret-scope refs, append-flows, sinks, quarantine
tables, cluster_by, etc.). Those are advanced features you keep adding by
hand-editing the rendered `conf/onboarding.*`. **The point of this demo is
the new CLI surface**: scaffold + bulk-extend + recipe-discover + validate
with zero hand-edits, then layer the advanced bits on top.

For the legacy hand-curated cloudFiles bundle (the `people` flow with custom
job + var schema), use the original [`generate_dabs_resources.py`](../generate_dabs_resources.py)
as documented in [`../README.md` "DAB Demo"](../README.md#dab-demo). The two
demos are complementary and intentionally untangled.

## Outputs you will see

After running `--scenario all` offline, each bundle in `demo_runs/<name>/`
contains:

```
<bundle_name>/
├── databricks.yml                 # rendered targets, includes resources/
├── README.md                      # variable reference + quick-start
├── resources/
│   ├── variables.yml              # sdp_meta_dependency now pinned (no __SET_ME__)
│   ├── sdp_meta_onboarding_job.yml
│   └── sdp_meta_pipelines.yml     # split or combined per scenario
├── notebooks/init_sdp_meta_pipeline.py
├── recipes/                       # the four runnable recipes
│   ├── README.md
│   ├── from_uc.py
│   ├── from_volume.py
│   ├── from_topics.py
│   └── from_inventory.py
└── conf/
    ├── onboarding.yml             # 1 seeded + 3-5 from CSV (+ recipes when --apply-recipe)
    ├── samples/{flows.csv,topics.txt}
    ├── silver_transformations.yml (when layer includes silver)
    └── dqe/example_table/bronze_expectations.yml
```

The terminal will end with a summary like:

```
[STAGE 5 summary] conf/onboarding.yml -> 6 flow(s)
  - data_flow_id=  100  source_format=cloudFiles   bronze_table=example_table
  - data_flow_id=  101  source_format=cloudFiles   bronze_table=customers
  - data_flow_id=  102  source_format=cloudFiles   bronze_table=transactions
  - data_flow_id=  103  source_format=cloudFiles   bronze_table=orders
  - data_flow_id=  104  source_format=cloudFiles   bronze_table=products
  - data_flow_id=  105  source_format=cloudFiles   bronze_table=stores
```

## Running just the SDP Pipeline (debugging `INTERNAL_ERROR`)

When `bundle run pipelines` fails with the generic
`INTERNAL_ERROR: Workload failed, see run output for details.`, the **wrapper
job** is hiding the real error — the actual stack trace lives in the SDP
pipeline's event log, not the job log. Two ways to bypass the wrapper and
trigger the underlying pipeline directly:

### Option 1 (preferred): `bundle run <pipeline_resource_name>`

DAB knows the pipeline names from `resources/sdp_meta_pipelines.yml` and will
start an SDP update inline, streaming the event log so the failure surfaces
in your terminal:

```bash
cd demo_runs/dab_demo_cloudfiles_combined

# Combined topology -> ONE pipeline resource named `bronze_silver`
databricks bundle run bronze_silver --target dev --profile <profile>
```

For the split scenarios (`cloudfiles`, `kafka`, `eventhub`, `delta`) the
resources are named `bronze` and `silver`:

```bash
cd demo_runs/dab_demo_cloudfiles_split
databricks bundle run bronze --target dev --profile <profile>
databricks bundle run silver --target dev --profile <profile>
```

The mapping `scenario -> pipeline resources` matches the table in
[Pipeline mode: split vs combined](#pipeline-mode-split-vs-combined):

| Scenario | `bundle run` targets you can use |
|---|---|
| `cloudfiles` | `bronze`, `silver`, `pipelines` (wrapper job) |
| `cloudfiles_combined` | `bronze_silver`, `pipelines` (wrapper job) |
| `kafka` | `bronze`, `pipelines` (wrapper job) |
| `eventhub` | `bronze_silver`, `pipelines` (wrapper job) |
| `delta` | `bronze`, `silver`, `pipelines` (wrapper job) |

Useful flags:

- `--refresh-all` — full refresh (wipes + reloads all tables)
- `--no-wait` — fire-and-return; watch in the UI

### Option 2: direct SDP API (no DAB)

If you've already deployed and just want to retrigger an update:

```bash
# Find the pipeline id once
databricks pipelines list-pipelines --profile <profile> \
  | jq -r '.[] | select(.name | startswith("[dev <user>] dab_demo_cloudfiles_combined")) | "\(.pipeline_id)\t\(.name)"'

# Then start an update (CLI streams events)
databricks pipelines start-update <pipeline_id> --profile <profile>
```

### Reading the failure

Open the pipeline UI:

```
https://<workspace-host>/pipelines?o=<workspace-id>
```

Filter by your bundle name, click the failed update, and check **Update
details -> Errors** plus the event log. Common causes (all already covered
in [Troubleshooting](#troubleshooting)):

- `[NO_TABLES_IN_PIPELINE]` — no `silver_transformations` row matches any
  `silver_table` (or `dataflow_group` filter is wrong).
- `cannot resolve '*'` / silver SELECT fails — auto-seeded `select_exp:
  ["*"]` doesn't work for that source; replace with explicit columns.
- `PATH_NOT_FOUND` on a UC volume CSV path — re-run with
  `--apply-prepare-wheel` to re-seed `/Volumes/<cat>/<sch>/<vol>/demo_data/`.
- `SCHEMA_NOT_FOUND` — schema auto-create skipped because `--profile` was
  missing.

## Tearing it down

The bundles live entirely under `--out-dir` (default: `demo_runs/`). Delete
the folder to remove all local artifacts. If you ran `--apply-deploy`, also
clean the workspace:

```bash
cd demo_runs/dab_demo_kafka_bronze
databricks bundle destroy --target dev --profile DEFAULT
```

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `databricks CLI not on PATH` | Install the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html). |
| `bundle init failed ... one or more files already exist: dab_demo_*` | A previous run of the same scenario left a scaffold behind. The launcher now wipes the target directory before STAGE 1 by default; if you passed `--no-clean`, delete `--out-dir/<bundle_name>/` manually or drop the flag. |
| `bundle init failed with exit code 1` | The catalog you passed doesn't exist or isn't visible from the profile. Check `--uc-catalog-name` and `--profile`. |
| `pip subprocess-exited-with-error` / `Connection refused` during `bundle prepare-wheel` | The build host can't reach pypi.org. Pass `--pip-index-url https://pypi.internal.example.com/simple` (or set `PIP_INDEX_URL` in the env) to point at your internal mirror. |
| `RuntimeError: Schema cat.schema not found ... does not exist` | Should be self-healing now: `bundle-prepare-wheel` auto-creates the schema and volume by default. If you re-ran with `--no-create-missing-uc`, drop that flag. If schema creation fails with `PERMISSION_DENIED`, your principal needs `CREATE SCHEMA` on the catalog (or pick a `--uc-schema` you already own). |
| `RuntimeError: Catalog 'X' not found ... Catalogs are not auto-created` | Catalogs require metastore-admin perms; pick a `--uc-catalog-name` you already have access to or have one created for you. |
| `STAGE 5 sdp-meta sanity checks reported issues` and one is about `__SET_ME__` | You ran without `--apply-prepare-wheel` and the placeholder pinning didn't run for some reason; check that STAGE 2 ran for the failing scenario. |
| Recipe (STAGE 4) returns 1 with `data_flow_id collision` | The CSV in STAGE 3 already used the IDs the recipe is now trying to claim. Either skip the CSV or run with `--scenario <one-only>` and a clean `--out-dir`. |
| `bundle prepare-wheel` fails with permission errors | Your profile needs `WRITE_VOLUME` on the target schema. Either change `--uc-schema` to one you own or pre-create the volume manually. |
| Silver SDP Pipeline starts then fails with `[NO_TABLES_IN_PIPELINE] Pipelines are expected to have at least one table defined` | The silver dataflowspec table was overwritten with zero rows because no `target_table` in `silver_transformations.{yml,json}` matched any `silver_table` in `onboarding.{yml,json}`. Fixed automatically by `bundle-add-flow` (auto-seeds a `target_table: <silver_table>, select_exp: ["*"]` row per new flow). If you hit this on an old / hand-edited bundle: open `conf/silver_transformations.{yml,json}` and add one row per `silver_table` listed in `onboarding.{yml,json}`, then re-run `bundle run onboarding` followed by `bundle run pipelines`. |
| Pipeline deploy fails with `cannot update pipeline: Whl libraries are not supported. Please remove them from the pipeline settings.` | You're on serverless SDP, which rejects both `whl:` and `pypi:` library entries on the pipeline. The template ships the `sdp-meta` install inside the runner notebook (`%pip install --force-reinstall $sdp_meta_dependency`) for exactly this reason — make sure your `resources/sdp_meta_pipelines.yml` `libraries:` block ONLY contains the `notebook:` entry. |
| `NOTEBOOK_PIP_INSTALL_ERROR` / `SyntaxError: incomplete input` from the runner notebook | Cell 1 of `notebooks/init_sdp_meta_pipeline.py` must be exactly `<var = spark.conf.get(...)>` followed immediately by `%pip install --force-reinstall $var`, then a `# COMMAND ----------` cell break — any multi-line Python before the `%pip` line breaks Databricks's `.py`-notebook parser. Mirror the layout used by the existing `demo/notebooks/*_runners/init_sdp_meta_pipeline.py`. |
| `bundle run pipelines` fails with `INTERNAL_ERROR: Workload failed, see run output for details.` | The wrapper job is masking the real error — it lives in the SDP Pipeline event log, not the job log. Run the SDP Pipeline directly with `databricks bundle run bronze_silver --target dev --profile <profile>` (combined) or `... bronze` / `... silver` (split). See [Running just the SDP Pipeline](#running-just-the-sdp-pipeline-debugging-internal_error). |
