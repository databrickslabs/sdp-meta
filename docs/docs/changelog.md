---
id: changelog
title: Changelog
sidebar_position: 99
---

# Changelog

---

## v0.1.1

### New Features

- **Multiple pipeline topologies per DAB bundle** — `bundle-add-pipeline` adds independently configured bronze, silver, split, or combined pipelines with their own data-flow groups and target schemas. `bundle-validate` validates each pipeline and its job wiring independently. ([Issue #446](https://github.com/databrickslabs/sdp-meta/issues/446))

### Fixes

- **Databricks SDK compatibility floor** — runtime and App dependencies now require `databricks-sdk>=0.138.0,<1`, and CI tests serverless onboarding environment models against exactly that minimum. ([Issue #457](https://github.com/databrickslabs/sdp-meta/issues/457))
- **Databricks App deployment packaging** — deployment staging now includes the `examples/` wheel-build inputs on both macOS/Linux and Windows, preventing remote App startup failures while building the SDP-META wheel.
- **DAB pipeline group ownership** — `bundle-add-pipeline` and `bundle-validate` reject duplicate ownership of the same data-flow group and layer, including conflicts introduced through target overrides. ([Issue #458](https://github.com/databrickslabs/sdp-meta/issues/458))
- **Customized DAB validation** — `bundle-validate` now honors default and environment-selected targets, accepts transitive bronze-to-silver task dependencies, and reports malformed targets or dependency cycles cleanly. ([Issue #459](https://github.com/databrickslabs/sdp-meta/issues/459))
- **Mixed snapshot and non-snapshot pipelines** — a layer-level snapshot callback no longer suppresses input views for CloudFiles and other non-snapshot specs. ([Issue #443](https://github.com/databrickslabs/sdp-meta/issues/443))
- **Append-flow source metadata** — CloudFiles append flows now serialize and select source metadata correctly. ([Issue #444](https://github.com/databrickslabs/sdp-meta/issues/444))
- **Append-flow custom transformations** — custom transformation functions now run for append-flow inputs as well as primary inputs. ([Issue #445](https://github.com/databrickslabs/sdp-meta/issues/445))
- **Legacy dataflow-spec schema evolution** — append onboarding adds current bronze and silver fields to v0.0.10 Delta spec tables before merging, preserving existing rows and audit fields. ([Issue #460](https://github.com/databrickslabs/sdp-meta/issues/460))
- **Legacy Python wheel task entry point** — the `dlt-meta` compatibility package restores the `group_1:run` entry point used by v0.0.10 Databricks Python wheel tasks. ([Issue #455](https://github.com/databrickslabs/sdp-meta/issues/455))
- **Legacy workspace configuration keys** — v0.0.10 `dlt_meta_*` workspace keys migrate to their `sdp_meta_*` equivalents with deprecation warnings and deterministic precedence. ([Issue #456](https://github.com/databrickslabs/sdp-meta/issues/456))

### Compatibility warning

- **Python 3.10 is now the minimum supported interpreter** — the required
  `databricks-sdk>=0.138.0` supports Python 3.10 and newer. Upgrade Python 3.8
  and 3.9 environments before installing either v0.1.1 distribution.
- **Append-flow transforms now run on every input** — v0.1.1 applies
  `bronze_custom_transform_func` and `silver_custom_transform_func` separately
  to primary and append-flow DataFrames. This is the intended fix for
  [Issue #445](https://github.com/databrickslabs/sdp-meta/issues/445), but
  transforms that assume primary-only columns can fail or change append-flow
  output. Validate every append source and normalize all inputs to one
  target-compatible schema before upgrading.
- **Use the official persisted-spec readers during upgrades** —
  `DataflowSpecUtils.get_bronze_dataflow_spec()` and
  `DataflowSpecUtils.get_silver_dataflow_spec()` backfill the registered
  compatibility fields added since legacy schema versions. Direct dataclass
  construction from legacy Spark rows does not. See the
  [v0.1.1 migration guidance](./operations/migration#v011-compatibility-boundaries)
  and [upgrade checklist](./operations/migration#v011-upgrade-checklist).

---

## v0.1.0

### New Features

- **Windows PowerShell deploy script** — `scripts/deploy_app.ps1` is a native port of `scripts/deploy_app.sh` using `robocopy` + the `databricks` CLI. No Git Bash / WSL / Python required. Mirrors the bash flow (stage → sync → deploy) and adds a CRLF → LF normalization pass so `start.sh` reaches the Linux App container with LF endings. Uses `-DatabricksProfile` to avoid shadowing PowerShell's built-in `$PROFILE` variable. See [databricks_app/WINDOWS_DEPLOY.md](https://github.com/databrickslabs/sdp-meta/blob/main/databricks_app/WINDOWS_DEPLOY.md).
- **Apps UI + Git folder deploy path** — click-only alternative documented in [databricks_app/UI_GIT_DEPLOY.md](https://github.com/databrickslabs/sdp-meta/blob/main/databricks_app/UI_GIT_DEPLOY.md). Create a Databricks Git folder pointing at the repo, aim the App at `databricks_app/` only, and `start.sh`'s Mode B clones the full `sdp-meta` repo into `/tmp/sdp-meta` at container start.
- **Repo-wide line-ending policy** — `.gitattributes` pins LF for shell/Python/YAML/JSON/template files, CRLF for `.bat`/`.cmd`, binary for images/archives. Prevents the `bad interpreter: /bin/bash\r` App-container crash from recurring via any tooling path.
- **Automatic liquid clustering** (`cluster_by_auto`) for bronze and silver tables. When set to `true`, Databricks automatically determines the optimal clustering columns. Works alongside explicit `cluster_by` to define initial keys followed by automatic optimization. Supported for `bronze_cluster_by_auto`, `bronze_quarantine_table_cluster_by_auto`, and `silver_cluster_by_auto`. ([Issue #238](https://github.com/databrickslabs/sdp-meta/issues/238))
- **MCP Server support** — opt-in `mcp` CLI command (`databricks labs sdp-meta mcp`) exposes sdp-meta over stdio so MCP-capable clients (Claude Code, Cursor, Claude Desktop) can drive scaffolding and inspection. Install with `pip install databricks-labs-sdp-meta[mcp]`.
- **Declarative Automation Bundle (DAB) template** with `bundle-init --quickstart` zero-prompt fast path for instant bundle scaffolding. New CLI commands: `bundle-init`, `bundle-add-flow`, `bundle-prepare-wheel`, `bundle-validate`. Packaged template includes onboarding job, Lakeflow Spark Declarative Pipelines, runner notebook, and four flow-generation recipes.
- **Row filter support** — `where_clause` in silver transformations files for pipeline-time row filtering, with coverage for multi-source CDC flows.
- **Multi-source AUTO CDC** — multiple CDC sources can now feed into a single target via `create_auto_cdc_flow`.
- **End-to-end YAML support** — onboarding, DQE rules, silver transformations, and packaged demos all accept YAML in addition to JSON.
- **`build-and-upload-whl` flag** for `onboard` and `deploy` — builds the local sdp-meta wheel, uploads it to a UC volume, and bakes the path into the runner notebook's `%pip install` (avoids needing PyPI access on the pipeline cluster).
- **Databricks App refactor** — monolithic `app.py` split into `routes/` (8 blueprints) + `services/onboarding/` helpers, input validation hardened, renamed `lakehouse_app` → `databricks_app`, PyPI install option added, UC preflight probe surfaces required `GRANT` SQL before demos.
- **Docs site migrated to Docusaurus 3** — 34 pages across Getting Started, Concepts, Reference, Guides, Operations, and Contributing sections.
- **DAB conf staging for serverless onboarding jobs** — new `stage_conf` wheel entry point stages bundle `conf/` files to a Unity Catalog Volume and rewrites `${workspace.file_path}/conf` references before onboarding, so DAB-deployed onboarding jobs work on serverless compute. ([PR #350](https://github.com/databrickslabs/sdp-meta/pull/350))
- **Agent Skill for SDP-META** — added `skills/sdp-meta/` with workflow guidance and references for onboarding specs, CLI/DAB usage, MCP tools, and a zero-to-running walkthrough. ([PR #356](https://github.com/databrickslabs/sdp-meta/pull/356))
- **Databricks App documentation refresh** — added app screenshots, a documentation map, clearer service-principal permission steps, post-deploy guidance, and matching Docusaurus App guide updates. ([PR #352](https://github.com/databrickslabs/sdp-meta/pull/352))

### Breaking Changes

- **PyPI package renamed**: `dlt-meta` → `databricks-labs-sdp-meta`
- **CLI commands renamed**: `databricks labs dlt-meta` → `databricks labs sdp-meta`
- **Python imports changed**: `from dlt_meta import ...` → `from databricks.labs.sdp_meta import ...`
- **Main class renamed**: `DLTMeta` → `SDPMeta`
- **Source layout changed**: flat `src/` → `src/databricks/labs/sdp_meta/` namespace package
- **Lakeflow Spark Declarative Pipelines API**: DLT decorators/APIs migrated to `pyspark.pipelines`. Update references that import from `dlt` to use the new `pyspark.pipelines` module. ([Issue #274](https://github.com/databrickslabs/sdp-meta/issues/274))
- **`quarantine_table` field**: Renamed `quarantine_table_name` to `quarantine_table` in dataflow specs for naming consistency. ([Issue #243](https://github.com/databrickslabs/sdp-meta/issues/243))

### Bug Fixes & Improvements

- **Git portability for DAB template filenames** — renamed three template files under `src/databricks/labs/sdp_meta/templates/dab/template/{{.bundle_name}}/conf/` (`onboarding.*.tmpl`, `silver_transformations.*.tmpl`, `dqe/example_table/bronze_expectations.*.tmpl`) to use Go `text/template` backtick string literals instead of double-quoted ones. The literal `"` in the previous filenames clashed with Git's `core.protectNTFS=true` (default on all platforms since Git 2.22, 2019) and caused `error: invalid path` on checkout. Functionally identical to `databricks bundle init`.
- **Security**: Replaced unsafe `eval()` on `uc_enabled` widget with a strict parser. ([Issue #260](https://github.com/databrickslabs/sdp-meta/issues/260))
- **Performance**: O(N+M) schema modification for wide tables in CDC flows (was previously O(N×M)). ([Issue #284](https://github.com/databrickslabs/sdp-meta/issues/284))
- Fixed cross-platform file URI handling in CLI; updated cloudFiles demo clustering metadata.
- Fixed SCD Type 2 processing; renamed demo tables to `sdp_meta`.
- Switched demo paths from DBFS to UC Volumes.
- Renamed references from "Lakeflow Declarative Pipelines" to "Lakeflow Spark Declarative Pipelines".
- DAB quickstart is more robust: validated overrides, clearer `output-dir` semantics, recovered bare `--quickstart` parsing, and scaffold version stamping. ([PR #350](https://github.com/databrickslabs/sdp-meta/pull/350))
- Demo launchers and integration tests now tolerate both `schema=` and legacy `target=` forms for `pipelines.create`. ([PR #354](https://github.com/databrickslabs/sdp-meta/pull/354))
- MCP server path/profile handling was hardened with root confinement, symlink escape checks, profile validation, and stricter `bundle_add_flow` identifiers. ([PR #350](https://github.com/databrickslabs/sdp-meta/pull/350))
- Databricks App terminal output now escapes CLI stdout/stderr before ANSI colorization to prevent injected HTML. ([PR #350](https://github.com/databrickslabs/sdp-meta/pull/350))
- Onboarding fixes cover quarantine table comments, row-filter validation, snapshot source path validation, non-UC silver/multi-source CDC errors, typed `dataflowIds` filtering, and SCD Type 2 explicit-schema fields. ([PR #350](https://github.com/databrickslabs/sdp-meta/pull/350))

### Backward Compatibility

The `dlt-meta` compatibility wrapper package re-exports all symbols and forwards CLI commands. Existing code continues to work with deprecation warnings. `src.*` imports are supported via a `sys.modules` shim (removed in v0.2.0).

See the [Migration guide](./operations/migration) for full details.

---

## v0.0.10

### Breaking Changes

- **DPM mode removed**: Pipelines using the Legacy Publishing Mode (DPM) flag must be migrated to the default publishing mode before upgrading. Follow [Migrate to the default publishing mode](https://docs.databricks.com/aws/en/dlt/migrate-to-dpm#migrate-to-the-default-publishing-mode). This migration is irreversible.
- **Multi-level namespace qualifiers**: Custom schema qualification in table names is no longer supported. Tables must be created without database qualifiers in the pipeline context.
- **`invoke_dlt_pipeline` argument names**: Method arguments now require layer-specific prefixes (`bronze_` or `silver_`). Replace `custom_transform_func` with `bronze_custom_transform_func`, and `next_snapshot_and_version` with `bronze_next_snapshot_and_version`.

### Enhancements

- `apply_changes_from_snapshot` support in the silver layer
- Databricks App UI for onboarding and deploy commands
- Non-Delta sink support (Delta and Kafka sinks) via `bronze_sink` / `silver_sink`
- Quarantine support in the silver layer for data quality rules
- Table comments, column comments, and `cluster_by` support
- Catalog support for `sourceDetails` and `targetDetails`
- DBDemos integration
- YAML support for onboarding files
- Multiple column support for `create_auto_cdc_flow`
- Custom transformations support for Kafka and Delta sources

### Migration

See the [Migration guide](./operations/migration#v0010-breaking-changes).

---

## v0.0.9

### Enhancements

- `apply_changes_from_snapshot` API support in the bronze layer
- `append_flow` API support for the silver layer
- File metadata column support for Autoloader
- Bring-your-own custom transformation support
- UC Volume and serverless support for CLI, integration tests, and demos
- Bronze/silver pipeline chaining into a single Declarative Pipeline
- Liquid clustering support
- Silver fanout demo

---

## v0.0.8

### Enhancements

- `append_flow` API support for bronze and silver layers
- File metadata columns for Autoloader
- Custom transformation support
- Silver fanout demo and unit tests

---

## v0.0.7

### Enhancements

- Fixed mismatched key: `read_dlt_delta()` updated to use `source_database` instead of `database`

---

## v0.0.6

### Enhancements

- Migrated to `create_streaming_table` API from deprecated `create_streaming_live_table`
- Data quality support for the silver layer
- Unity Catalog integration test framework

---

## v0.0.5

### New Features

- Unity Catalog support
- Databricks Labs CLI support with `onboard` and `deploy` commands

---

## v0.0.4

### Bug Fixes

- New `eventhub.accessKeySecretName` option for Event Hubs source

---

## v0.0.3

### Bug Fixes

- Infer datatypes from `sequence_by` for `__START_AT` / `__END_AT` in the `apply_changes` API

---

## v0.0.2

### New Features

- Table properties support for bronze, quarantine, and silver tables
- `track_history_column` support in `apply_changes`
- Delta source support
- Bronze/silver onboarding validation

---

## v0.0.1

Initial release.
