---
name: sdp-meta
description: "Use this skill for metadata-driven Databricks pipelines built with SDP-META (formerly DLT-META; package databricks-labs-sdp-meta; CLI `databricks labs sdp-meta`). Trigger when users mention SDP-META/DLT-META, onboarding or dataflowspec JSON/YAML, config-driven Bronze/Silver Lakeflow pipelines, SDP-META CLI or MCP tools, Asset Bundle scaffolding, or Git-based CI/CD automation for SDP-META. This framework is a config-driven layer on top of Lakeflow Spark Declarative Pipelines; hand-authored streaming tables or materialized views are plain SDP."
---

# SDP-META — Metadata-Driven Lakeflow Pipelines

## Overview

SDP-META (the framework in this repository) is a metadata-driven layer that
builds Databricks **Lakeflow Spark Declarative Pipelines (SDP)** from
configuration instead of per-table pipeline code. You describe each
source→bronze→silver flow once as a **dataflowspec** (an onboarding JSON/YAML
record); the framework generates and runs the bronze and silver SDP pipelines.
Add a table by adding a metadata entry — not by writing another pipeline.

- **Package:** `databricks-labs-sdp-meta` (formerly `dlt-meta`).
- **CLI:** `databricks labs sdp-meta <command>`.
- **Canonical imports:** `from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline`.
  Version 0.2.0 does not support `dlt_meta` or `src.*` imports.
- **Relationship to plain SDP:** SDP-META *generates* SDP pipelines. Use this
  skill when the user wants config-driven fan-out across many tables/layers;
  hand-authored streaming tables / materialized views are plain SDP.

**When to use:** the user has (or wants) an onboarding file describing many
tables, mentions sdp-meta/dlt-meta, or wants to onboard/deploy/scaffold a
metadata-driven pipeline. **When NOT to use:** a one-off pipeline with a couple
of hand-written tables.

## The mental model (3 steps)

1. **Author** an onboarding file: a list of dataflowspec records, one per source
   table, describing bronze (and optionally silver) targets, reader options,
   data-quality expectations, and transformations.
2. **Onboard** (`sdp-meta onboard`): reads the onboarding file and writes the
   dataflowspec into **Delta tables** (the bronze/silver dataflowspec tables).
   Metadata only — no data pipeline runs yet.
3. **Deploy** (`sdp-meta deploy`): creates a Lakeflow SDP pipeline whose runner
   notebook reads the dataflowspec tables and materializes bronze/silver tables.

For infrastructure-as-code, `bundle-init` scaffolds a Databricks Asset Bundle
(DAB) wiring the onboarding job + the SDP pipeline + variables together.

## Quick start

```bash
# Author conf/onboarding.json (see references/onboarding-spec.md), then:
databricks labs sdp-meta onboard --profile <profile>   # write dataflowspec Delta tables
databricks labs sdp-meta deploy  --profile <profile>   # create + run the SDP pipeline
```

Prefer a bundle for anything beyond a one-off:

```bash
databricks labs sdp-meta bundle-init --quickstart=true --output-dir=.
# edit resources/variables.yml (catalog / schema / sdp_meta_dependency), then:
databricks labs sdp-meta bundle-validate
databricks bundle deploy --target dev
databricks bundle run onboarding --target dev
databricks bundle run pipelines --target dev
```

> Use the `=` syntax on boolean flags (`--quickstart=true`,
> `--build-and-upload-whl=true`) — the Labs CLI string-flag parser otherwise
> consumes the next token as the value.

Real, working configs live in this repo under [`examples/`](../../examples)
(`json/`, `yml/`, and the `sdp_meta_pipeline*.ipynb` notebooks) and in
[`tests/resources/`](../../tests/resources) (`onboarding*.json`,
`silver_transformations*.json`, `dqe/`). Copy from these rather than inventing
field names.

## How agents drive SDP-META

Two complementary surfaces:

- **MCP tools** (scaffolding + inspection, no live workspace needed): the
  framework ships its own stdio MCP server (`databricks labs sdp-meta mcp`,
  requires the `mcp` extra) with 5 tools — `sdp_meta_bundle_init`,
  `sdp_meta_bundle_add_flow`, `sdp_meta_bundle_validate`,
  `sdp_meta_list_templates`, `sdp_meta_get_onboarding_template`. See
  [references/mcp-tools.md](references/mcp-tools.md).
- **CLI** (`onboard`/`deploy`/`bundle-*`): the operational path that touches a
  live workspace. See [references/cli-and-bundles.md](references/cli-and-bundles.md).
- **CI/CD** (GitHub Actions, Azure DevOps, or another unattended runner): use
  service-principal OAuth and the non-interactive bundle workflow. See
  [references/ci-cd-automation.md](references/ci-cd-automation.md).

Recommended flow: scaffold + add flows + validate locally with the MCP tools (or
`bundle-init`), then hand off to the CLI locally or in CI/CD for deployment.

## Onboarding file essentials

A dataflowspec record keys targets by layer and environment. Environment-suffixed
fields (`_dev` / `_staging` / `_prd`) let one spec serve all environments.

```jsonc
{
  "data_flow_id": "100",
  "data_flow_group": "A1",              // groups flows into one pipeline
  "source_format": "cloudFiles",        // cloudFiles | delta | eventhub | kafka | snapshot
  "source_details": { "source_database": "APP", "source_table": "CUSTOMERS",
                       "source_path_dev": "/Volumes/.../customers" },
  "bronze_catalog_dev": "bronze_cl", "bronze_database_dev": "bronze",
  "bronze_table": "customers",
  "bronze_reader_options": { "cloudFiles.format": "json", "cloudFiles.inferColumnTypes": "true" },
  "bronze_data_quality_expectations_json_dev": "/path/bronze_dqe.json",
  "silver_catalog_dev": "silver_cl", "silver_database_dev": "silver",
  "silver_table": "customers",
  "silver_transformation_json_dev": "/path/silver_transformations.json"
}
```

- **Data quality** (`*_data_quality_expectations_json_*`): `expect_or_drop`
  (quarantine bad rows), `expect_or_fail` (halt), `expect` (warn).
- **Silver transformations** (`silver_transformation_json_*`): a list of
  `{ target_table, select_exp[], where_clause[] }`.
- **CDC / SCD Type 2**: `silver_cdc_apply_changes` (and bronze CDC options).

Full field reference: [references/onboarding-spec.md](references/onboarding-spec.md).

## Reference files

| Use case | Reference |
|----------|-----------|
| **New to sdp-meta? Zero-to-running pipeline against your own data** | [references/getting-started-walkthrough.md](references/getting-started-walkthrough.md) |
| Every dataflowspec field, layers, env suffixes, DQ, CDC, source formats | [references/onboarding-spec.md](references/onboarding-spec.md) |
| CLI commands (`onboard`/`deploy`), the DAB workflow, wheel delivery flags | [references/cli-and-bundles.md](references/cli-and-bundles.md) |
| GitHub Actions/Azure DevOps, OAuth, targets, permissions, promotion, rollback | [references/ci-cd-automation.md](references/ci-cd-automation.md) |
| MCP tools + registering the sdp-meta MCP server for agents | [references/mcp-tools.md](references/mcp-tools.md) |
| Migrating DLT-META or SDP-META v0.1.x to v0.2.0 | [references/migration.md](references/migration.md) |

## Common issues

| Issue | Solution |
|-------|----------|
| Boolean flag "consumed" the next argument | Use `=` syntax: `--quickstart=true`, `--build-and-upload-whl=true`. |
| `deploy` fails: no dataflowspec found | Run `onboard` first — `deploy` reads the dataflowspec Delta tables `onboard` writes. |
| Pipeline cluster can't reach PyPI to install the wheel | Use `--build-and-upload-whl=true` (or `--whl-file-path=/Volumes/...`) so the wheel is staged on a UC Volume and baked into the runner's `%pip install`. |
| Serverless pipeline rejects a `whl` library | Serverless DLT does not support whl-typed pipeline libraries; the wheel is delivered via the runner notebook's `%pip install`, not a pipeline library. |
| `bundle-validate` flags layer/topology or placeholder errors | Fix `resources/variables.yml` (catalog/schema/`sdp_meta_dependency`) and unresolved onboarding placeholders before `databricks bundle deploy`. |
