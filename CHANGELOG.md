# Changelog

## [v0.1.1]
### Added
- **Multiple pipeline topologies per DAB bundle**: `bundle-add-pipeline` adds independently configured bronze, silver, split, or combined pipelines with their own data-flow groups and target schemas. `bundle-validate` validates each pipeline and its job wiring independently. [Issue #446](https://github.com/databrickslabs/sdp-meta/issues/446)

### Fixed
- **Mixed snapshot and non-snapshot pipelines**: a layer-level `next_snapshot_and_version` callback now suppresses input-view creation only for snapshot specs, allowing CloudFiles and other flows to share the pipeline group. [Issue #443](https://github.com/databrickslabs/sdp-meta/issues/443)
- **Append-flow source metadata**: nested Spark rows are normalized before serialization, so CloudFiles append flows can select file metadata columns correctly. [Issue #444](https://github.com/databrickslabs/sdp-meta/issues/444)
- **Append-flow custom transformations**: bronze and silver custom transformation functions now run for append-flow inputs as well as the primary input. [Issue #445](https://github.com/databrickslabs/sdp-meta/issues/445)
- **Legacy dataflow-spec schema evolution**: append onboarding now adds current bronze and silver fields to v0.0.10 Delta spec tables before merging, preserving existing rows and audit fields while allowing new features without destructive overwrite. [Issue #460](https://github.com/databrickslabs/sdp-meta/issues/460)
- **Compat shim: no more startup tracebacks on machines without pyspark** (ships as `databricks-labs-sdp-meta` 0.1.1 + `dlt-meta` 0.1.1 — the fixed `dlt_meta` package files live in the primary wheel, so BOTH distributions are re-released and the compat wrapper now requires `databricks-labs-sdp-meta>=0.1.1`). The `dlt_meta.pth` startup hook printed a `ModuleNotFoundError: No module named 'pyspark'` traceback on every `python3` launch — and `import dlt_meta` failed outright — in any environment without pyspark (laptops/CI that `pip install dlt-meta`). Root cause: `_optional_runtime_import_error` did not recognize the `ModuleNotFoundError(name='pyspark')` shape CPython raises when the *top-level* pyspark package is absent. The predicate now recognizes it, and `import dlt_meta` never raises at import time: any failure — including missing `yaml`/`databricks-sdk` on stripped images — degrades to a stub that raises an actionable `ImportError` at first attribute access (SDP-runtime-missing message when the predicate matches, a generic missing-dependency message otherwise). Predicate tests now derive the exception shape from a real `from pyspark import pipelines` in a pyspark-free subprocess instead of hand-setting `exc.name`, new `tests/test_compat_startup.py` pins the never-raise invariant, and a new `compat-startup-smoke` CI job installs both wheels into a bare venv and fails on any interpreter-startup stderr.
- **Onboarding PyPI dependency**: `databricks labs sdp-meta onboard` now defaults to the published `databricks-labs-sdp-meta` distribution instead of the nonexistent `sdp-meta` package, preventing serverless and classic job environment setup failures. [Issue #408](https://github.com/databrickslabs/sdp-meta/issues/408)
- **DAB wheel packaging**: the primary wheel now includes the complete Declarative Automation Bundle template, so `bundle-init` works from an installed release instead of failing because `templates/dab` is absent.
- **DAB quickstart onboarding**: generated bronze flows now include the quarantine defaults required when DQE is configured, preventing `PySparkValueError: bronze_quarantine_table`; serverless runner installs also force a clean dependency reinstall to avoid stale environment metadata.

## [v0.1.0]
### ⚠️ Breaking Changes
- **Project rename `dlt-meta` → `sdp-meta`** to align with the Lakeflow Spark Declarative Pipelines product naming. This affects the PyPI package, CLI command, Python import path, source layout, and main class name. A backward-compatibility wrapper is published so existing installations keep working with a deprecation warning. [PR](https://github.com/databrickslabs/sdp-meta/pull/289)
  - PyPI package: `dlt-meta` → `databricks-labs-sdp-meta`
  - CLI command: `databricks labs dlt-meta` → `databricks labs sdp-meta`
  - Python import: `from dlt_meta import ...` → `from databricks.labs.sdp_meta import ...`
  - Main class: `DLTMeta` → `SDPMeta`
  - Source layout: flat `src/` → `src/databricks/labs/sdp_meta/` namespace package
- **Lakeflow Spark Declarative Pipelines API migration**: DLT decorators/APIs migrated to `pyspark.pipelines`. Update references that import from `dlt` to use the new `pyspark.pipelines` module. [Issue #274](https://github.com/databrickslabs/sdp-meta/issues/274)
- **`quarantine_table` field**: Renamed `quarantine_table_name` to `quarantine_table` in dataflow specs for naming consistency. [Issue #243](https://github.com/databrickslabs/sdp-meta/issues/243)

### Migration Guide
The legacy `dlt-meta` PyPI package is preserved as a thin compatibility shim that pulls in `databricks-labs-sdp-meta` and re-exports every public symbol with a `DeprecationWarning`. Legacy `src.*` imports from v0.0.10 also work via a `sys.modules` shim, but **both shims will be removed in v0.2.0**.

See [docs/operations/migration](https://databrickslabs.github.io/sdp-meta/docs/operations/migration) for the step-by-step migration walkthrough.

### Added
- **Windows PowerShell deploy script** (`scripts/deploy_app.ps1`): Native PowerShell port of `scripts/deploy_app.sh` using `robocopy` and the `databricks` CLI only — no Git Bash / WSL / Python install required on the developer machine. Same stage → sync → deploy flow as the bash script, plus an explicit CRLF → LF normalization pass on every staged text file so `start.sh` reaches the Linux App container with LF endings and boots cleanly. Uses `-DatabricksProfile` (alias `-Profile`) to avoid shadowing PowerShell's built-in `$PROFILE` automatic variable. See [databricks_app/WINDOWS_DEPLOY.md](https://github.com/databrickslabs/sdp-meta/blob/main/databricks_app/WINDOWS_DEPLOY.md).
- **Repo-wide line-ending policy** (`.gitattributes`): Pins `.sh`/`.py`/`.yml`/`.json`/`.tmpl`/`.ps1` to LF, `.bat`/`.cmd` to CRLF, and marks images/archives as binary. Prevents the `bad interpreter: /bin/bash\r` App-container crash from recurring through any tooling path (CI, alternative deploy scripts, DAB template rendering, GitHub web editor).
- **Apps UI + Git folder deploy guide** (`databricks_app/UI_GIT_DEPLOY.md`): Click-only deploy path — create a Databricks Git folder pointing at this repo, aim the App at `databricks_app/` only, and `start.sh`'s Mode B clones the full repo into `/tmp/sdp-meta` at container start. No local CLI required.
- **Automatic liquid clustering** (`cluster_by_auto`): Databricks automatically determines the optimal clustering columns for bronze and silver tables. Works alongside explicit `cluster_by` to define initial keys followed by automatic optimization. Supported via `bronze_cluster_by_auto`, `bronze_quarantine_table_cluster_by_auto`, and `silver_cluster_by_auto`. [Issue #238](https://github.com/databrickslabs/sdp-meta/issues/238)
- **MCP Server support**: Optional MCP (Model Context Protocol) stdio server (`databricks labs sdp-meta mcp`) so MCP-capable clients (Claude Code, Cursor, Claude Desktop) can drive sdp-meta scaffolding and inspection. Install with `pip install databricks-labs-sdp-meta[mcp]`. [PR #299](https://github.com/databrickslabs/sdp-meta/pull/299)
- **Declarative Automation Bundle (DAB) template**: End-to-end DAB-based deployment path for CI/CD, multi-target environments, and agent-driven flows. New CLI commands: `bundle-init` (with `--quickstart` zero-prompt fast path), `bundle-add-flow`, `bundle-prepare-wheel`, `bundle-validate`. Packaged template includes `databricks.yml`, `variables.yml`, onboarding job, Lakeflow Spark Declarative Pipelines, runner notebook, and four flow-generation recipes. [Issue #248](https://github.com/databrickslabs/sdp-meta/issues/248)
- **Row filter support**: New `where_clause` field in silver transformations files for pipeline-time row filtering, including coverage for multi-source CDC flows. [PR #294](https://github.com/databrickslabs/sdp-meta/pull/294), [PR #306](https://github.com/databrickslabs/sdp-meta/pull/306)
- **Multi-source AUTO CDC pipeline support**: Multiple CDC sources can now feed into a single target via `create_auto_cdc_flow`. [PR #303](https://github.com/databrickslabs/sdp-meta/pull/303)
- **End-to-end YAML support**: YAML format now supported for onboarding files, DQE rules, silver transformations, and packaged demos (in addition to JSON). [PR #251](https://github.com/databrickslabs/sdp-meta/pull/251)
- **`build-and-upload-whl` for `onboard` / `deploy`**: New CLI flag builds the local sdp-meta wheel, uploads it to a UC volume, and uses that wheel for the onboarding/deployment job (avoids needing PyPI access on the pipeline cluster). [PR #299](https://github.com/databrickslabs/sdp-meta/pull/299)
- **Databricks App refactor**: Monolithic `app.py` split into `routes/` (8 blueprints) + `services/onboarding/` helpers; input validation hardened; renamed `lakehouse_app` → `databricks_app`; PyPI install option added; new UC preflight probe surfaces required `GRANT` SQL before demos. [PR #295](https://github.com/databrickslabs/sdp-meta/pull/295), [PR #325](https://github.com/databrickslabs/sdp-meta/issues/325)
- **Docs site migrated from Hugo to Docusaurus 3**: 34 pages across 6 sections (Getting Started, Concepts, Reference, Guides, Operations, Contributing) plus a landing page and Databricks-branded CSS. [PR #315](https://github.com/databrickslabs/sdp-meta/pull/315)
- **DAB conf staging for serverless onboarding jobs**: New `stage_conf` wheel entry point copies bundle `conf/` assets to a Unity Catalog Volume before onboarding and rewrites `${workspace.file_path}/conf` references to the staged volume path. This makes DAB-deployed onboarding jobs work reliably on serverless compute. [PR #350](https://github.com/databrickslabs/sdp-meta/pull/350)
- **Agent Skill for SDP-META**: Added `skills/sdp-meta/` with an Agent Skill and references for getting started, onboarding specs, CLI/DAB workflows, and MCP tools, plus docs that explain how to use the skill with skill-aware agents. [PR #356](https://github.com/databrickslabs/sdp-meta/pull/356)
- **Databricks App documentation refresh**: Added app screenshots, a documentation map, post-deploy guidance, clearer service-principal permission steps, and matching updates to the Docusaurus App guide. [PR #352](https://github.com/databrickslabs/sdp-meta/pull/352)
- Added `dlt-meta-dab.md` documentation for Lakeflow Connect and synthetic data generation. [Issue #254](https://github.com/databrickslabs/sdp-meta/issues/254)
- Added interactive notebook-based demo and LFC (Lakeflow Connect) Python demo. [Issue #172](https://github.com/databrickslabs/sdp-meta/issues/172)

### Changed
- Renamed references from "Lakeflow Declarative Pipelines" to "Lakeflow Spark Declarative Pipelines". [Issue #285](https://github.com/databrickslabs/sdp-meta/issues/285)
- Updated Databricks Asset Bundle to Declarative Automation Bundle terminology throughout demos and docs.
- Switched demo paths from DBFS to UC Volumes. [Issue #254](https://github.com/databrickslabs/sdp-meta/issues/254)
- Standardized repository and documentation links for the renamed `databrickslabs/sdp-meta` GitHub repo and `/sdp-meta/` GitHub Pages site.

### Fixed
- **Git portability**: Renamed three DAB template filenames containing literal `"` characters (Go `text/template` `eq` comparisons with double-quoted string literals inside the filename itself) to use backtick literals instead. Fixes `error: invalid path` on every system with Git's `core.protectNTFS=true`, which has been the default on all platforms since Git 2.22 (2019). Affected files under `src/databricks/labs/sdp_meta/templates/dab/template/{{.bundle_name}}/conf/`: `onboarding.*.tmpl`, `silver_transformations.*.tmpl`, `dqe/example_table/bronze_expectations.*.tmpl`. Functionally identical to `databricks bundle init`.
- **DAB quickstart robustness**: Validated quickstart overrides, clarified `output-dir` as the parent directory, recovered bare `--quickstart` values consumed by the Labs CLI parser, and stamped scaffolded bundles with the generating SDP-META version. [PR #350](https://github.com/databrickslabs/sdp-meta/pull/350)
- **Pipeline creation compatibility**: Demo launchers and integration tests now tolerate both modern `schema=` and legacy `target=` forms for `pipelines.create`, improving compatibility across workspace/pipeline publishing modes. [PR #354](https://github.com/databrickslabs/sdp-meta/pull/354)
- **MCP server hardening**: Confined caller-supplied MCP paths to a filesystem root, blocked escaping symlinks, validated Databricks CLI profile names, and tightened `bundle_add_flow` identifier validation. [PR #350](https://github.com/databrickslabs/sdp-meta/pull/350)
- **Databricks App XSS hardening**: Escaped CLI stdout/stderr before ANSI colorization so terminal output renders safely in the browser. [PR #350](https://github.com/databrickslabs/sdp-meta/pull/350)
- **Onboarding correctness fixes**: Restored quarantine table comment handling, added row-filter SQL validation, fixed snapshot path validation, improved non-UC silver/multi-source CDC errors, and made `dataflowIds` filtering use typed Spark column predicates instead of raw SQL string splicing. [PR #350](https://github.com/databrickslabs/sdp-meta/pull/350)
- **SCD Type 2 schema handling**: Ensured `__START_AT` / `__END_AT` fields are appended for SCD2 tables with explicit schemas even when no `except_column_list` is configured. [PR #350](https://github.com/databrickslabs/sdp-meta/pull/350)
- **Security**: Replaced unsafe `eval()` on `uc_enabled` widget with a strict parser. [Issue #260](https://github.com/databrickslabs/sdp-meta/issues/260)
- **Performance**: O(N+M) schema modification for wide tables in CDC flows (was previously O(N×M)). [Issue #284](https://github.com/databrickslabs/sdp-meta/issues/284)
- Fixed cross-platform file URI handling in CLI; updated cloudFiles demo clustering metadata. [Issue #251](https://github.com/databrickslabs/sdp-meta/issues/251)
- Fixed integration tests UC volume path construction. [Issue #284](https://github.com/databrickslabs/sdp-meta/issues/284)
- Fixed SCD Type 2 processing; renamed demo tables to `sdp_meta`. [Issue #266](https://github.com/databrickslabs/sdp-meta/issues/266)
- Removed orphaned enhanced-CLI subsystem and fixed flake8 lint errors.

### Backward Compatibility
- The `dlt-meta` compatibility wrapper package re-exports all public symbols and forwards CLI commands to `sdp-meta` with a deprecation banner.
- `from dlt_meta import ...` and `import src.*` continue to work with `DeprecationWarning`; both shims are scheduled for removal in v0.2.0.
- Legacy config key `dlt_meta_schema` is still read with a logged warning; prefer `sdp_meta_schema`.

## [v0.0.10]
### ⚠️ Breaking Changes
- **DPM Mode Flag Removal from v0.0.9**: DLT-META v0.0.9 pipelines using DPM mode flag must be migrated to the default publishing mode before upgrading. This change is metadata-only and doesn't impact existing datasets, but is irreversible.
- **invoke_dlt_pipeline Argument Changes**: Method arguments now require layer-specific prefixes (bronze_ or silver_) to support apply_changes_from_snapshot in both layers. This affects existing pipeline configurations using the previous argument naming.

### Migration Guide
1. **DPM Mode Migration**:
   - Before upgrading to v0.0.10, update pipeline JSON settings as per Databricks documentation [Migrate to the default publishing mode](https://docs.databricks.com/aws/en/dlt/migrate-to-dpm#migrate-to-the-default-publishing-mode)
   - This is a one-way migration - ensure all stakeholders are informed
   - Verify pipeline functionality in test environment first

2. **invoke_dlt_pipeline Updates**:
   - Method signature changed to support layer-specific functions:
     ```python
     invoke_dlt_pipeline(
         spark,
         layer,
         bronze_custom_transform_func=None,    # Previously: custom_transform_func
         silver_custom_transform_func=None,    # New in v0.0.10
         bronze_next_snapshot_and_version=None,  # Previously: next_snapshot_and_version
         silver_next_snapshot_and_version=None   # New in v0.0.10
     )
     ```
   - Layer-specific functions allow different transformations for bronze and silver layers
   - Existing code using single custom_transform_func should move to bronze_custom_transform_func
   - Existing code using next_snapshot_and_version should move to bronze_next_snapshot_and_version
   - Review and update all pipeline configurations using this method

### Added
- Added apply_changes_from_snapshot support in silver layer [PR](https://github.com/databrickslabs/sdp-meta/pull/187)
- Added UI using Databricks App for onboarding/deploy commands [PR](https://github.com/databrickslabs/sdp-meta/pull/168)
- Added support for non-Delta as sinks(delta, kafka) [PR](https://github.com/databrickslabs/sdp-meta/pull/157)
- Added quarantine support in silver layer for data quality rules [PR](https://github.com/databrickslabs/sdp-meta/pull/191)
- Added support for table comments, column comments, and cluster_by [PR](https://github.com/databrickslabs/sdp-meta/pull/91)
- Added catalog support for sourceDetails and targetDetails [PR](https://github.com/databrickslabs/sdp-meta/issues/173)
- Added DBDemos for dlt-meta [PR](https://github.com/databrickslabs/sdp-meta/issues/183)
- Added YAML support for onboarding [PR](https://github.com/databrickslabs/sdp-meta/issues/184)
- Fixed issue cluster by not working with bronze append only table [PR](https://github.com/databrickslabs/sdp-meta/issues/197)
- Fixed issue view name containing period when using DPM [PR](https://github.com/databrickslabs/sdp-meta/issues/169)
- Fixed issue CLI onboarding overwrite option always set to True [PR](https://github.com/databrickslabs/sdp-meta/issues/163)
- Fixed issue Silver DLT not creating based on passed database [PR](https://github.com/databrickslabs/sdp-meta/issues/160)
- Fixed issue PyPI download stats display [PR](https://github.com/databrickslabs/sdp-meta/issues/200)
- Fixed issue Silver Data Quality not working [PR](https://github.com/databrickslabs/sdp-meta/issues/156)
- Fixed issue Removed DPM flag check inside dataflowpipeline [PR](https://github.com/databrickslabs/sdp-meta/issues/177)
- Fixed issue Updated dlt-meta demos into Delta Live Tables Notebook github [PR](https://github.com/databrickslabs/sdp-meta/issues/158)
- Fixed issue Adding multiple col support for auto_cdc api [PR](https://github.com/databrickslabs/sdp-meta/pull/224)
- Fixed issue Added support for custom transformations for Kafka/Delta [PR](https://github.com/databrickslabs/sdp-meta/pull/228)


## [v.0.0.9] 
- Added  apply_changes_from_snapshot api support in bronze layer: [PR](https://github.com/databrickslabs/sdp-meta/pull/124)
- Added dlt append_flow api support for silver layer: [PR](https://github.com/databrickslabs/sdp-meta/pull/63)
- Added support for file metadata columns for autoloader: [PR](https://github.com/databrickslabs/sdp-meta/pull/56)
- Added support for Bring your own custom transformation: [Issue](https://github.com/databrickslabs/sdp-meta/issues/68)
- Added support to Unify PyPI releases with GitHub OIDC: [PR](https://github.com/databrickslabs/sdp-meta/pull/62)
- Added demo for append_flow and file_metadata options: [PR](https://github.com/databrickslabs/sdp-meta/issues/74)
- Added Demo for silver fanout architecture: [PR](https://github.com/databrickslabs/sdp-meta/pull/83)
- Added  hugo-theme-relearn themee: [PR](https://github.com/databrickslabs/sdp-meta/pull/132)
- Added unit tests to showcase silver layer fanout examples: [PR](https://github.com/databrickslabs/sdp-meta/pull/67)
- Added liquid cluster support: [PR](https://github.com/databrickslabs/sdp-meta/pull/136)
- Added support for UC Volume + Serverless support for CLI, Integration tests and Demos: [PR](https://github.com/databrickslabs/sdp-meta/pull/105)
- Added Chaining bronze/silver pipelines into single DLT: [PR](https://github.com/databrickslabs/sdp-meta/pull/130)
- Fixed issue for No such file or directory: '/demo' :[PR](https://github.com/databrickslabs/sdp-meta/issues/59)
- Fixed issue DLT-META CLI onboard command issue for Azure: databricks.sdk.errors.platform.ResourceAlreadyExists :[PR](https://github.com/databrickslabs/sdp-meta/issues/51)
- Fixed issue Changed dbfs.create to mkdirs for CLI: [PR](https://github.com/databrickslabs/sdp-meta/pull/53)
- Fixed issue DLT-META CLI should use pypi lib instead of whl : [PR](https://github.com/databrickslabs/sdp-meta/pull/79)
- Fixed issue Onboarding with multiple partition columns errors out: [PR](https://github.com/databrickslabs/sdp-meta/pull/134)

## [v.0.0.7] 
- Added dlt-meta cli documentation and readme with browser support: [PR](https://github.com/databrickslabs/sdp-meta/pull/45)

## [v.0.0.6] 
- migrate to create streaming table api from create streaming live table: [PR](https://github.com/databrickslabs/sdp-meta/pull/39)

## [v.0.0.5] 
- Enabled Unity Catalog support: [PR](https://github.com/databrickslabs/sdp-meta/pull/28)
- Added databricks labs cli: [PR](https://github.com/databrickslabs/sdp-meta/pull/28)

## [v0.0.4] - 2023-10-09
### Added
- Functionality to introduce an new option for event hub configuration. Namely a source_details option 'eventhub.accessKeySecretName' to properly construct the eh_shared_key_value properly. Without this option, there were errors while connecting to the event hub service (linked to [issue-13 - java.lang.RuntimeException: non-nullable field authBytes was serialized as null #13](https://github.com/databrickslabs/sdp-meta/issues/13))

## [v0.0.3] - 2023-06-07
### Fixed
-  infer datatypes from sequence_by to __START_AT, __END_AT for apply changes API
### Changed
-   setup.py for version
### Removed
-   Git release tag from github actions

## [v0.0.2] - 2023-05-11
### Added
- Table properties support for bronze, quarantine and silver tables using create_streaming_live_table api call
- Support for track history column using apply_changes api
- Support for delta as source
- Validation for bronze/silver onboarding
### Fixed
- Input schema parsing issue in onboarding
### Modified
-  Readme and docs to include above features

## [v0.0.1] - 2023-03-22
### Added

- Initial public release version.