---
id: faq
title: FAQ
sidebar_position: 10
---

# Frequently Asked Questions

## General

**Q: What reader types are supported?**

Databricks Autoloader (`cloudFiles`), Delta, Kafka, Event Hubs, and snapshot. Any Spark Structured Streaming reader can also be added by overriding `read_bronze()` in `DataflowPipeline`.

**Q: How many pipelines will SDP-META launch?**

One pipeline per `data_flow_group` value. Rows sharing the same `data_flow_group` run inside the same pipeline.

**Q: Can I run onboarding for bronze only?**

Yes. Set `"onboard_layer": "bronze"`. Both `bronze_dataflowspec_table` and `silver_dataflowspec_table` are required in the config, but only the bronze table gets populated.

**Q: Can I run onboarding for silver only?**

Yes. Set `"onboard_layer": "silver"`. Both table names are still required in the config. The bronze dataflowspec table must already exist and contain rows.

**Q: Do I have to use JSON? Can I use YAML?**

Both are supported. Pass a `.yml` file to `onboard` or reference it in your DAB bundle. All fields are identical between formats.

**Q: Can I write to the same target table from multiple sources?**

Yes — two patterns are supported:

- **Append + merge** — use `bronze_append_flows` to land additional raw sources into the same bronze streaming table, then `bronze_cdc_apply_changes` (or `silver_cdc_apply_changes`) to run a single CDC merge over the combined bronze. Best when sources are heterogeneous and you want them durably landed in bronze before the merge step. This is the pattern shown in the [Multi-Source CDC guide](./guides/multi-source-cdc).
- **Native multi-source CDC** — use `bronze_cdc_apply_changes_flows` (a single CDC group with a `flows: [...]` list and per-flow `source_details` / `source_schema_path`) when sources share a CDC schema and should merge directly through one `create_auto_cdc_flow_from_*` call. The same shape exists at silver as `silver_cdc_apply_changes_flows` for fan-in into a silver table.

The two patterns are mutually exclusive on the same flow row (the framework rejects an onboarding row that sets both `bronze_cdc_apply_changes` and `bronze_cdc_apply_changes_flows`).

**Q: How do I chain multiple silver tables from one bronze table?**

Use [Silver Fanout](./guides/silver-fanout). Run a second onboarding job in append mode (`"overwrite": "False"`).

**Q: How do I add Autoloader file metadata columns (filename, size, etc.) to the bronze table?**

Configure `source_metadata` inside `source_details`. See [Autoloader — File metadata columns](./guides/autoloader#file-metadata-columns).

---

## Migrating from DLT-META

**Q: What is the new CLI command? I was using `databricks labs dlt-meta`.**

The command is now `databricks labs sdp-meta`. The old `dlt-meta` commands are forwarded automatically with a deprecation banner, so existing scripts keep working — but update them before v0.2.0 when the forwarding is removed.

```bash
# Before
databricks labs dlt-meta onboard
databricks labs dlt-meta deploy

# After
databricks labs sdp-meta onboard
databricks labs sdp-meta deploy
```

The install command also changes: `databricks labs install sdp-meta`.

**Q: My onboarding file was written for DLT-META. Is it compatible?**

Yes — the onboarding file format (JSON or YAML) is fully backward-compatible. Field names are unchanged. The only things that need updating are config keys: `dlt_meta_schema` → `sdp_meta_schema` (the old key is still accepted with a logged warning until v0.2.0).

**Q: My runner notebook uses `init_dlt_meta_pipeline.py`. Do I need to update it?**

Yes. The runner notebook was renamed to `init_sdp_meta_pipeline.py`. Re-run `databricks labs sdp-meta onboard` to upload the new notebook. If you're on the DAB path, update the notebook reference in your bundle.

**Q: My existing DLT-META pipelines are running in production. Will they break?**

They keep running as-is — the pipeline graph is unchanged. However, if you upgrade the wheel to `databricks-labs-sdp-meta` you must also migrate away from Legacy Publishing Mode (see below) and update any `PythonWheelTask` `package_name` from `dlt_meta` to `databricks_labs_sdp_meta`. The safest upgrade path is: migrate to default publishing mode first → test in non-prod → upgrade the wheel → update the runner notebook.

**Q: My pipeline runs in Legacy Publishing Mode (DPM). Can I upgrade to v0.1.0?**

No — not yet. You must migrate to the default publishing mode first, then upgrade. Legacy DPM pipelines with custom schemas will fail at runtime in v0.1.0 with:
```
DLTAnalysisException: Materializing tables in custom schemas is not supported.
```
Follow the Databricks guide to [migrate to the default publishing mode](https://docs.databricks.com/aws/en/ldp/migrate-to-dpm#migrate-to-the-default-publishing-mode) before upgrading. Test in a non-production environment — the migration is one-way.

**Q: My notebooks use `from src.dataflow_pipeline import DataflowPipeline`. Will they break?**

No. A `sys.modules` shim maps all `src.*` imports to `databricks.labs.sdp_meta.*` automatically. Your notebooks keep working without changes. This shim will be removed in v0.2.0 — update imports before then.

**Q: My code uses `import dlt`. Does v0.1.0 still support this?**

The underlying Databricks API is now `pyspark.pipelines` (imported as `dp`). SDP-META handles this internally — you do not write `import dlt` or `import pyspark.pipelines` in your own code. `DataflowPipeline.invoke_dlt_pipeline()` is unchanged from the user's perspective.

**Q: I used `custom_transform_func` in `invoke_dlt_pipeline`. Do I need to change it?**

Yes. The single `custom_transform_func` argument was split into layer-specific arguments in v0.0.10:

```python
# Before (v0.0.9 and earlier — no longer supported)
DataflowPipeline.invoke_dlt_pipeline(spark, layer, custom_transform_func=my_func)

# After (v0.0.10+)
DataflowPipeline.invoke_dlt_pipeline(
    spark, layer,
    bronze_custom_transform_func=my_bronze_func,
    silver_custom_transform_func=my_silver_func,
)
```

**Q: My onboarding file uses `apply_changes`. Do I need to rename it to `create_auto_cdc_flow`?**

No. Field names in the onboarding file (`bronze_cdc_apply_changes`, `silver_cdc_apply_changes`, etc.) are unchanged. The rename from `apply_changes` to `create_auto_cdc_flow` happened in the underlying Databricks API — SDP-META translates automatically.

**Q: I used DBFS paths in my onboarding file (`dbfs:/...`). Do I need to migrate to UC Volumes?**

DBFS paths still work, but the recommended approach for Unity Catalog workspaces is UC Volumes (`/Volumes/catalog/schema/volume/...`). The CLI and DAB workflows default to UC Volumes for all file uploads. Migrate at your own pace — there is no forced cutover.

**Q: What happened to the `dlt_meta_schema` config key?**

It was renamed to `sdp_meta_schema` in v0.1.0. The old key is still accepted with a logged warning. Update it before v0.2.0.

**Q: My pipeline uses `database` as a single schema name (not `catalog.schema`). Does UC mode still work?**

Yes. The `database` field accepts `schema` (one part) or `catalog.schema` (two parts). Three-part names (`catalog.schema.table`) are not valid here — table names go in `bronze_dataflowspec_table` and `silver_dataflowspec_table`.

---

## New Features in v0.1.0

**Q: How do I enable liquid clustering on my tables?**

Add `bronze_cluster_by` (or `silver_cluster_by`) to your onboarding row with a comma-separated list of columns:
```json
{ "bronze_cluster_by": "event_date,customer_id" }
```
To let Databricks choose the clustering columns automatically, set the layer-specific `*_cluster_by_auto` key to `true`:
```json
{ "bronze_cluster_by_auto": true }
```
Both layers have their own field — use `bronze_cluster_by_auto` for the bronze table and `silver_cluster_by_auto` for the silver table. The bare `cluster_by_auto` key is **not** recognized and will be silently ignored. The quarantine-table variant `bronze_quarantine_table_cluster_by_auto` is also supported. See the [Onboarding fields reference](reference/onboarding-fields) for the full list.

**Q: What is `bronze_row_filter` / `silver_row_filter`?**

Row-level security filters applied when reading from the streaming table. Useful for masking sensitive data per-pipeline. Set in the onboarding file:
```json
{ "bronze_row_filter": "region = 'US'" }
```
Quarantine table variants (`bronze_quarantine_row_filter`) are also supported.

**Q: Can I deploy bronze and silver in a single pipeline run?**

Yes. Set `"layer": "bronze_silver"` in the pipeline configuration. This chains bronze into silver in one Lakeflow Spark Declarative Pipeline execution. Alternatively, use `pipeline_mode=combined` in a DAB bundle.

**Q: What is `create_sink` and what formats does it support?**

`create_sink` writes pipeline output to an external destination alongside the managed Delta table. Supported sinks: external Delta table and Kafka. Configure in the onboarding file via `bronze_sinks` or `silver_sinks`.

**Q: Does the silver layer now support quarantine tables?**

Yes. Silver quarantine tables were added in v0.1.0. Set `silver_quarantine_table` and related fields in the onboarding file — rows failing silver expectations are written there instead of being dropped.

---

## Declarative Automation Bundles (DAB)

**Q: What is the recommended way to deploy SDP-META?**

Use the DAB interface (`bundle-init`, `bundle-add-flow`, `bundle-validate`, `bundle deploy`). It gives you git-tracked pipeline state, `dev`/`prod` targets, and CI/CD support. The interactive `onboard`/`deploy` CLI is for first-touch exploration only.

**Q: What does `bundle-init --quickstart` do?**

Scaffolds a bundle with developer-friendly defaults (Autoloader + bronze_silver + split pipeline mode + PyPI dependency) so you can get a working bundle in one command. Edit `resources/variables.yml` afterwards to point at your real catalog/schema.

**Q: What is `pipeline_mode` and when should I use each?**

- `split` (default) — bronze and silver deploy as two separate Lakeflow Spark Declarative Pipelines. Recommended for most cases: independent scheduling and failure isolation.
- `combined` — bronze and silver run in a single pipeline. Use when you need atomic bronze-to-silver promotion or want a single pipeline UI view.

**Q: What is `bundle-validate` checking beyond `databricks bundle validate`?**

It catches SDP-META-specific authoring mistakes: unedited `<your-...>` or `__SET_ME__` placeholders in `databricks.yml` and onboarding files, per-pipeline `data_flow_group` and layer configuration errors, missing or duplicate pipeline-job wiring, invalid split dependencies, and `wheel_source` vs `sdp_meta_dependency` drift.

**Q: How do I generate onboarding entries in bulk?**

Use the recipes inside the scaffolded bundle (`recipes/`):
- `from_uc.py` — from existing Unity Catalog tables
- `from_volume.py` — from CSVs in a UC volume
- `from_topics.py` — from Kafka / Event Hub topic lists
- `from_inventory.py` — from an inventory CSV

**Q: I edited my onboarding file. Do I need to re-run `databricks bundle deploy`?**

No — only re-run the onboarding job:
```bash
databricks bundle run onboarding --target dev
```
`databricks bundle deploy` is only needed when you change `resources/variables.yml`, the bundle YAML files, or the pipeline/job definitions themselves. Deploying unnecessarily recreates pipeline resources and resets their state.

**Q: What is the difference between the `onboarding` and `pipelines` bundle jobs?**

- `onboarding` — reads your `conf/onboarding.*` file and writes rows into the `bronze_dataflowspec` / `silver_dataflowspec` Delta tables. Run this whenever the onboarding file changes.
- `pipelines` — runs the Lakeflow Spark Declarative Pipeline(s) that read the DataflowSpec tables at runtime and build the processing graph. In `split` mode this runs bronze then silver in sequence; in `combined` mode it runs a single pipeline.

Run onboarding first, then the pipeline. In `split` mode there are two separate pipeline resources (bronze, silver); in `combined` mode there is one.

**Q: How do I add a new source flow to an existing bundle?**

Use `bundle-add-flow`:
```bash
databricks labs sdp-meta bundle-add-flow
```
It reads bundle defaults from `resources/variables.yml`, auto-increments `data_flow_id`, and prevents ID collisions. After adding, re-run only the onboarding job — no redeploy needed.

**Q: Can a single bundle have multiple `data_flow_group` values?**

Yes. The bundle template scaffolds one group by default. Run `databricks labs sdp-meta bundle-add-pipeline` to add a topology with its own `bronze.group` / `silver.group`, layer, mode, and optional target schemas. Add matching flows with `bundle-add-flow`, validate, redeploy, and re-run onboarding.

**Q: How do I switch from PyPI to a local wheel (or vice versa)?**

Change `wheel_source` and `sdp_meta_dependency` in `resources/variables.yml`, then redeploy:
```yaml
# Switch to local wheel
wheel_source:
  default: volume_path
sdp_meta_dependency:
  default: /Volumes/my_catalog/my_schema/my_volume/databricks_labs_sdp_meta-0.1.0-py3-none-any.whl
```
Run `bundle-prepare-wheel` first to build and upload the wheel if you don't have the path yet.

**Q: How do I promote a bundle from dev to prod?**

```bash
databricks bundle deploy --target prod
databricks bundle run onboarding --target prod
databricks bundle run pipelines --target prod
```
Per-target catalog, schema, and variable overrides live under `targets.prod.variables` in `databricks.yml`. For CI/CD, uncomment the `run_as` block in the prod target and set your service principal application ID.

---

## Installation & PyPI

**Q: What is the difference between `databricks-labs-sdp-meta` and `dlt-meta` on PyPI?**

`databricks-labs-sdp-meta` is the primary package with all code. `dlt-meta` is an empty compatibility wrapper that declares `databricks-labs-sdp-meta` as a dependency — so `pip install dlt-meta` keeps working for existing users without any notebook changes.

**Q: I was using `pip install dlt-meta`. Do I need to change anything?**

No. `pip install dlt-meta==0.1.0` installs `databricks-labs-sdp-meta` as a dependency automatically. Your existing `from dlt_meta import ...` imports continue to work with a deprecation warning.

**Q: When will the `dlt-meta` compatibility package be removed?**

The compat package will be maintained through v0.1.x with no new features. `src.*` imports (v0.0.10 style) will be removed in v0.2.0. The `dlt-meta` package itself will be removed in a future major version. See the full [deprecation timeline](./operations/migration#deprecation-timeline).

---

## MCP Agent

**Q: What is the SDP-META MCP Agent?**

An MCP (Model Context Protocol) server that lets MCP-capable AI assistants
(Claude Code, Cursor, Claude Desktop, and others) scaffold, inspect, modify, and
validate local SDP-META bundles through natural language. Live onboarding and
deployment remain explicit CLI operations.

**Q: How do I install the MCP Agent?**

```bash
pip install databricks-labs-sdp-meta[mcp]
```

See the [MCP Getting Started guide](./getting-started/mcp) for configuration details.

---

## App

**Q: Do I need to run an initial setup before using the SDP-META App?**

Yes. Click the **Setup** button when the app first loads. This initializes the SDP-META project environment (creates required schemas, uploads notebooks, etc.) before any other feature is usable.

**Q: Who can access the SDP-META App?**

Authenticated Databricks workspace users with `CAN_USE` permission on the app. `CAN_MANAGE` is required for administration.

**Q: How does catalog and schema access work in the App?**

The app uses a dedicated Service Principal with `USE CATALOG`, `USE SCHEMA`, and `SELECT` permissions on all Unity Catalog resources used by SDP-META. An optional On-Behalf-Of (OBO) mode uses individual user credentials instead.

**Q: Can I run the SDP-META App locally without deploying to Databricks?**

Yes:
```bash
cd databricks_app
pip install -r requirements.txt
databricks configure --host YOUR_WORKSPACE_URL --token YOUR_TOKEN
python App.py
```
The app is then available at `http://127.0.0.1:5000`. It connects to your real workspace, so onboarding and pipeline operations work exactly as in the deployed version.

**Q: What is the difference between the UI tab and the CLI tab in the App?**

- **UI tab** — form-based interface for onboarding (fill in catalog, schema, onboarding file path, layer, group) and deploying pipelines. Designed for non-engineers.
- **CLI tab** — exposes `databricks labs sdp-meta` commands directly in the browser for users who prefer command-line operations but want a browser interface.

**Q: My pipeline doesn't appear in the App's Monitor view. Why?**

The Monitor view identifies SDP-META pipelines by a tag (`sdp_meta=true`) set during deployment. Pipelines created before tagging was introduced (DLT-META era) fall back to a config-key check (`sdp_meta_schema`). If neither is present, the pipeline won't be listed. Re-run the onboarding job via the App or CLI to re-register the pipeline with the correct tag.

**Q: Can I use the App to edit the onboarding spec after the pipeline is deployed?**

Yes — use the **Spec Editor** tab (available in v0.1.1+). It lets you parse, validate, and modify your onboarding YAML/JSON in the browser. Changes are validated in three layers (syntax → field semantics → file reference warnings) before you re-run the onboarding job. File-reference checks (DQE JSONs, silver transformation files) are surfaced as warnings since they require Spark to resolve at runtime.
