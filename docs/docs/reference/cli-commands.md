---
id: cli-commands
title: CLI Commands
sidebar_position: 4
---

# CLI Commands

All SDP-META operations are available through the Databricks Labs CLI extension.

**Prerequisites:**

- [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/tutorial.html) installed and authenticated
- `databricks labs install sdp-meta`

## Command summary

| Command | Description |
|---|---|
| `onboard` | Interactive onboarding wizard — prompts for config, pushes code, creates onboarding job |
| `deploy` | Deploy bronze/silver Declarative Pipeline interactively |
| `bundle-init` | Scaffold a new DAB bundle (`--quickstart` for zero-prompt fast path) |
| `bundle-prepare-wheel` | Build and upload the sdp-meta wheel to a UC Volume |
| `bundle-add-flow` | Add a new flow to an existing bundle from UC, Volumes, Kafka topics, or CSV inventory |
| `bundle-add-pipeline` | Add an independently configured pipeline topology and job wiring |
| `bundle-validate` | Validate bundle configuration (enforces `sdp_meta_dependency` is set) |
| `mcp` | Start the MCP server (stdio transport) |

## `onboard`

Collects onboarding parameters, uploads configuration files to your workspace, and creates the onboarding Databricks Job.

```bash
databricks labs sdp-meta onboard
```

Prompts for: Databricks profile, onboarding file path, Bronze/silver dataflowspec table names, environment name, catalog, and schema. After completion, prints and opens the onboarding job URL.

:::note
If you have cloned the sdp-meta repository, pressing Enter at each prompt accepts the default demo values from the `demo/` directory.
:::

## `deploy`

Deploys a Lakeflow Spark Declarative Pipeline for a given layer and group.

```bash
databricks labs sdp-meta deploy
```

Prompts for: layer, pipeline group, dataflowspec table names, catalog/schema, and cluster configuration. After completion, prints and opens the pipeline URL.

## `bundle-init`

Scaffolds a new Databricks Asset Bundle (DAB) configured for SDP-META.

```bash
databricks labs sdp-meta bundle-init
databricks labs sdp-meta bundle-init --quickstart
```

The generated bundle includes `databricks.yml`, `resources/variables.yml`, `resources/sdp_meta_pipelines.yml`, `resources/sdp_meta_onboarding_job.yml`, and `notebooks/init_sdp_meta_pipeline.py`.

## `bundle-prepare-wheel`

Builds the `databricks-labs-sdp-meta` wheel from source and uploads it to a Unity Catalog Volume.

```bash
databricks labs sdp-meta bundle-prepare-wheel
```

| Flag | Description |
|---|---|
| `--volume-path` | Target Volume path, e.g. `/Volumes/my_catalog/my_schema/my_volume/` |
| `--profile` | Databricks CLI profile to use |

After running, update `sdp_meta_dependency` in `resources/variables.yml` to the uploaded wheel path.

## `bundle-add-flow`

Adds a new data flow entry to an existing bundle's onboarding configuration from UC tables, UC Volumes, Kafka topics, or CSV inventory files.

```bash
databricks labs sdp-meta bundle-add-flow
```

## `bundle-add-pipeline`

Adds another bronze-only, silver-only, split bronze/silver, or combined
bronze/silver topology to `resources/sdp_meta_pipelines.yml`. Each topology
has its own `data_flow_group` and may override its bronze and silver target
schemas. Existing single-topology bundles remain valid.

```bash
databricks labs sdp-meta bundle-add-pipeline
```

The command adds the pipeline resources and corresponding tasks to the
`pipelines` job. In split mode, it also wires the silver task to depend on
the matching bronze task. It widens the onboarding job when another layer is
introduced; later `bundle-add-flow` calls for that group inherit the pipeline's
layer and target schemas.

## `bundle-validate`

Validates every configured pipeline independently, checks its layer-specific
dataflowspec table and group settings, and verifies that every pipeline is
referenced exactly once by the `pipelines` job. Split silver pipelines must
depend on a bronze pipeline for the same group. Target-specific variable
overrides are resolved when `--target` is selected.

```bash
databricks labs sdp-meta bundle-validate
```

Returns a non-zero exit code on failure, suitable for CI pipelines.

:::tip
Run `bundle-validate` in CI before every `databricks bundle deploy`.
:::

## `mcp`

Starts the SDP-META MCP server using stdio transport.

```bash
databricks labs sdp-meta mcp
```

See the [MCP Server guide](../getting-started/mcp.md) for setup instructions.
