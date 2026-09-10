# Implementation Plan: Metadata-Driven Quality Engines

Companion to `QUALITY_ENGINE_PROPOSAL.md`. Assumes one full-time engineer,
serial delivery, existing CI, and serverless workspace access.

## Estimate summary

| Release | Working days | Calendar (1 FTE) |
| --- | --- | --- |
| A: native quality foundation | 45.5 | ~9.5 weeks |
| B: plugin SPI and reference DQX adapter | 34 | ~7 weeks |
| Total | 79.5 | ~16 weeks |

Risk-adjusted range: 15 to 19 weeks. Variance drivers: serverless
integration iteration (A9, B6), the onboarding and validator work (A5) on a
2,600-line module with 2,700 lines of tests, the managed migration path (A10),
and the week-3 temporary-view spike (A6a), which can stop Release A.

The itemized tasks total 43.5 days for A and 32 days for B. The summary
includes an explicit schedule reserve of 2 days for A and 2 days for B,
producing the 45.5-day and 34-day release estimates. The reserve absorbs
estimation error only; scope additions require a re-estimate, not reserve
consumption.

Estimates include unit tests and review turnaround; they exclude waiting on
external decisions (adapter repository, package naming).

## Codebase touchpoints

<!-- markdownlint-disable MD013 -->

| Area | Files | Size today |
| --- | --- | --- |
| Runtime | `dataflow_pipeline.py`, `pipeline_writers.py` | 1,200 + 95 lines |
| Spec model | `dataflow_spec.py` | 890 lines |
| Onboarding | `onboard_dataflowspec.py` | 2,587 lines |
| DAB | `bundle.py`, `templates/`, `demo/dab_template_demo` | 1,514 lines |
| App | `databricks_app/routes/spec_editor.py`, `services/onboarding/*` | n/a |
| Tests | `tests/test_dataflow_pipeline.py`, `tests/test_onboard_dataflowspec.py` | 3,517 + 2,700 lines |
| Integration | `integration_tests/run_integration_tests.py` | ~1,300 lines |
| Docs | `docs/docs/concepts/data-quality.md`, `reference/dq-rules.md`, `concepts/architecture.md`, `demo/README.md` | n/a |

<!-- markdownlint-enable MD013 -->

## Routing and metrics contract (binding for A6, A7, B2, B5)

Native metrics come from `dp.expect*` decorators, which only count rows they
evaluate. The main target must therefore receive unfiltered checked rows and
let `expect_or_drop` bindings perform quarantine-rule filtering. Pre-filtering
in `get_main_input()` would make every quarantine rule report zero failures.

Writer contract:

1. `checked = plugin.apply(input_df, rules, options)` becomes a
   `dp.temporary_view`; `checked`-target bindings are applied to that view.
2. Main `dp.table` reads `plugin.get_main_input(checked).drop(*reserved)` and
   the writer applies `main`-target bindings to it.
3. Quarantine `dp.table` reads `plugin.get_invalid(checked)`.
4. An engine that wants per-rule native metrics returns the checked rows
   unfiltered from `get_main_input()` and supplies `expect_or_drop` bindings
   that perform the filtering. The built-in Lakeflow engine does exactly this.
5. An engine that routes by its own filters (DQX) filters in
   `get_main_input()` and returns no `main` drop bindings.
6. Every engine declares `allows_overlap: bool` on the SPI. `False` means the
   main rows (after `main` drop bindings) and `get_invalid()` rows are
   disjoint; `True` means the engine documents an overlap (DQX warning-only
   rows). Lakeflow declares `False`; the DQX adapter declares `True`.

The contract kit (B2) reads `allows_overlap` and asserts disjointness when it
is `False`; when `True` it asserts that every overlapping row carries a
non-empty warning-type diagnostic and an empty error-type diagnostic as named
by the plugin. Lakeflow-mode metrics parity is tested in A8/A9.

## Release A: native quality foundation (45.5 days)

### A1. Golden tests for legacy paths (3 d)

Files: `tests/test_dataflow_pipeline.py`, `tests/test_onboard_dataflowspec.py`,
new `tests/resources/dqe/golden/`.

- Pin `write_layer_with_dqe()` decorator calls and table declarations for
  standard (expect/drop/fail), quarantine-only, drop+quarantine, CDC with
  quarantine, Bronze, Silver, and empty quarantine name.
- Pin onboarding output (`dataQualityExpectations`, `quarantineTargetDetails`)
  for every fixture under `tests/resources/dqe/`.
- Local-Spark routing test that runs legacy inverse predicates and pins
  row-level results.

Exit: golden tests green on `main` before any refactor commit.

### A2. Quarantine-only main-target correction (1 d)

Files: `dataflow_pipeline.py` (lines ~639-688).

- Declare the main `dp.table` when only `expect_or_quarantine` is present.
- Separate, revertible commit; `CHANGELOG.md` entry flagged user-visible.
- Update the A1 quarantine-only golden test to the corrected expectation.

Exit: quarantine-only spec declares both targets; golden suite green.

### A3. Missing quarantine target rejection (1.5 d)

Files: `onboard_dataflowspec.py` (`__get_quarantine_details`, Bronze ~1509,
Silver ~2518), `dataflow_pipeline.py` (~719).

- Onboarding: raise when `expect_or_quarantine` exists and
  `<layer>_quarantine_table` is missing or blank; identical rule for both
  layers including empty-string handling.
- Enforce the check at both Bronze and Silver call sites, not only inside
  `__get_quarantine_details`; Bronze currently skips the helper for a falsey
  table while Silver enters it whenever DQE is configured.
- Runtime: replace `logger.warning(...); return` with a raised error naming
  flow, layer, and field.

Exit: no silent-skip path remains; tests for both layers and both paths.

### A4. `qualityConfig` field and runtime dispatcher (2.5 d)

Files: `dataflow_spec.py`, `dataflow_pipeline.py`, `onboard_dataflowspec.py`
(Bronze schema builder ~1290-1357, Silver ~2262-2314).

- Add nullable `qualityConfig: str` (JSON) to Bronze and Silver specs,
  both onboarding schema builders, `additional_bronze_df_columns`,
  `additional_silver_df_columns`, and both backward-compat
  `NEW_*_FIELDS_AT_READ` test sets.
- Persisted snapshot fields: engine registry name, SPI version, plugin
  package and version, underlying engine version, diagnostic-schema
  fingerprint, original rule document, normalized Lakeflow forms, source path,
  content hash, validation-policy version, output targets, engine options,
  migration choice.
- Backward compat: tables without the column deserialize with `None`; extend
  `test_backward_compat_*`.
- Add a runtime quality preflight hook at the start of `write()`, before the
  sink loop and before `write_layer_table()` branches on snapshot, DQE, CDC,
  multi-source CDC, or append flows. In A4 the hook performs only the
  mixed-configuration check; A5 registers the remaining validators (source
  modes, append flows, quarantine target completeness, deferred SQL analysis).
- After preflight, dispatch `qualityConfig` → new writer,
  `dataQualityExpectations` → legacy, else standard. The new-writer branch is
  inserted in `write_layer_table()` ahead of the existing snapshot check so no
  topology precedence bypasses it.

Exit: old spec rows load; dispatcher tests cover all four branches.

### A5. Onboarding and runtime validators for the Lakeflow engine (7.5 d)

Files: `onboard_dataflowspec.py`, new `quality/rules_loader.py`,
`quality/validation.py`, `quality/fields.py` (shared field-name constants for
onboarding, DAB, and App).

- Read `<layer>_quality_engine`, `<layer>_quality_rules_path_{env}`,
  `<layer>_quality_engine_migration`.
- Accept only `lakeflow` in Release A; reject `dqx` and unknown values with a
  message pointing to Release B. Engine dispatch is structured so that plugin
  engines bypass four-group parsing and pass the opaque document to
  `plugin.validate()` (wired in B1).
- Load JSON/YAML; validate four groups, rule-name syntax, duplicates, 1 MiB
  limit; random-function denylist (reject) and time-function list (warn).
- Parse every Lakeflow expression with Spark SQL at onboarding; reject
  malformed SQL. Column and result-type analysis depends on schema
  availability: Silver (Delta source table) and Bronze flows with an explicit
  schema file analyze at onboarding and reject unknown columns and
  non-boolean predicates; Bronze cloudFiles/Kafka/eventhub flows without a
  schema file persist an `analysis_required` marker and are analyzed by the
  runtime preflight against the transformed input DataFrame before
  `plugin.apply()`.
- Reject mixed legacy and new fields; reject unsupported source modes (CDC,
  snapshot, append flows, multi-source CDC).
- Require complete quarantine target details whenever any new engine is
  selected.
- Register the same validators (source modes, append flows, quarantine target
  completeness, deferred SQL analysis) with the A4 runtime preflight hook so
  persisted rows that bypass onboarding are rejected identically.
- Reserved-column check when the resolved schema is available.
- Engine-switch check: read the existing spec row for the same flow before
  MERGE/overwrite; if no prior row exists, skip the comparison (first
  onboarding). Otherwise compare engine and diagnostic-schema fingerprint and
  require a new quarantine table or explicit `full_refresh`.
- Silver `silver_quarantine_cluster_by` alias: canonicalize and reject
  conflicts on the new path only.
- Build the `qualityConfig` snapshot with every field listed in A4.

Exit: onboarding unit tests for each validation rule, first-onboarding path,
and engine-switch rejection; legacy onboarding tests unchanged.

### A6a. Spike: expectations on `dp.temporary_view` (1.5 d, start of week 3)

- Confirm on the target runtime that `dp.expect_all_or_fail` bound to a
  temporary view prevents each consuming flow from committing a batch that
  contains a failing row.
- Confirm main and quarantine consumers both evaluate checked-view bindings
  and resume independently from their checkpoints.
- Spike the exact dynamic decorator pattern used by the writer,
  `dp.expect_all*(...)(dp.table(...))`, and confirm event-log metrics remain
  visible after checked-view fan-out.
- If unsupported: stop Release A work on A6/A7, amend the proposal's
  `expect_or_fail` contract, and re-review. No main-only fallback is
  permitted because it lets quarantine commit the failing batch.

Exit: written spike result attached to the tracking issue.

### A6. Private SPI and common writer (5 d)

Files: new `quality/spi.py`, `quality/bindings.py`, `quality/writer.py`;
`dataflow_pipeline.py` wiring.

- `QualityEnginePlugin` base: `validate`, `apply`, `get_main_input`,
  `get_invalid`, `native_expectations`, `reserved_columns`, and the
  `allows_overlap` attribute (with `overlap_diagnostics` naming the
  warning-type and error-type columns when `True`); `ExpectationBinding`
  dataclass (target `checked|main`, action, name→expression).
- Writer implements the routing and metrics contract above; reuses existing
  target-option resolution (catalog, schema, path, properties, partitions,
  cluster_by, cluster_by_auto, comment, row filter) for both tables; runtime
  reserved-column check before `apply`.
- No engine-specific branches in the writer.

Exit: writer unit tests with a stub plugin verify graph shape, options, and
that main-target bindings are applied to unfiltered rows for a metrics-style
plugin.

### A7. Lakeflow engine implementation (2 d)

Files: new `quality/lakeflow_engine.py`.

- `apply`: add `_errors` array of `{name, expression}` structs for failed
  normalized quarantine predicates; empties filtered.
- `get_main_input`: returns checked rows unfiltered (routing is done by
  bindings); `get_invalid`: `size(_errors) > 0`; `allows_overlap = False`.
- `native_expectations`: `expect` and `expect_or_drop` → main; normalized
  `expect_or_quarantine` → main as `expect_or_drop`; `expect_or_fail` →
  checked.
- `reserved_columns`: `["_errors"]`.

Exit: pure-function tests for normalization and bindings.

### A8. Local Spark behavior tests (3 d)

Files: new `tests/test_quality_lakeflow_spark.py`.

- Real DataFrames through `apply`, main drop bindings, and `get_invalid`:
  pass-only rows in main; any single failure quarantined; multi-failure rows
  list every rule in stable order; `NULL` fails; main omits `_errors`;
  drop+quarantine row lands in quarantine; drop-only row absent from both.
- Metrics parity: for each quarantine rule, count of rows failing the main
  drop binding equals count of rows whose `_errors` contain that rule,
  including when `expect_or_drop` rules are present.
- Bindings carry the identical normalized strings used for `_errors`.

Exit: routing and parity semantics proven without a pipeline.

### A9. Serverless integration tests (7.5 d)

Files: `integration_tests/run_integration_tests.py`, new
`integration_tests/conf/{json,yml}/quality/`, and a new event-log assertion
helper.

- Deterministic 8-row dataset (5 valid, 3 quarantine, at least one
  multi-rule failure, at least one `NULL`).
- Matrix: Bronze and Silver; JSON and YAML; UC and path-based non-UC
  targets; legacy and Declarative Pipeline publishing modes; main and
  quarantine row filters.
- Assert table contents, `_errors` values, per-rule event-log metrics
  (including drop+quarantine parity), second-update idempotency.
- Build reusable event-log parsing and golden metric assertions before running
  the complete matrix.
- `expect_or_fail` scenario: assert failed update/event-log state, absence of
  the failing row and every row from its rejected micro-batch in both targets,
  allowance for different previously committed prefixes, and checkpoint
  recovery after the rule or data is fixed.
- Engine-switch mechanism: seed a prior spec row with a different engine and
  diagnostic-schema fingerprint, then re-onboard; verify rejection without a
  new quarantine table or `full_refresh`, and success with either. A real
  cross-engine switch is only testable in B6, since Release A ships one engine.
  For the acknowledged path, execute a real full-refresh update, verify the
  quarantine table is recreated with the expected schema, verify the migration
  flag is cleared afterwards, and verify the next update is incremental.
- Legacy fixtures re-run: contents and metrics identical to golden.

Exit: suite green on serverless; results attached to PR.

### A10. Examples, DAB, App, migration execution, docs (6 d)

Files: `examples/{json,yml}/quality/`, `demo/conf/`, `bundle.py`, `cli.py`,
`src/.../templates/`, `demo/dab_template_demo`,
`templates/dab/databricks_template_schema.json`,
`databricks_app/routes/spec_editor.py`,
`databricks_app/templates/landingPage.html`,
`databricks_app/services/onboarding/*`, four docs pages, and `CHANGELOG.md`.

- New Lakeflow examples with valid-rule semantics; legacy fixtures untouched.
- DAB template and App spec editor expose engine and rules-path fields using
  `quality/fields.py` constants. The engine-switch comparison itself lives in
  onboarding (A5); DAB and App surface the resulting onboarding error together
  with the two remediations (new quarantine table or explicit `full_refresh`)
  before deployment.
- Add a managed migration path for acknowledged `full_refresh` (1.5 d of this
  task). Managed launchers (DAB, CLI, App) perform three steps after
  onboarding: (1) read `qualityConfig.migration` for flows in the pipeline;
  (2) start one pipeline update with `full_refresh_selection` limited to the
  affected quarantine tables; (3) on successful completion, clear the
  migration flag in the dataflow-spec row and record the completed migration
  fingerprint, so subsequent updates are incremental. A sticky flag would
  truncate quarantine on every run; step (3) is mandatory and tested. If the
  update fails, the flag remains and the launcher reports that migration is
  pending. Unmanaged launches must use a new quarantine target or receive
  explicit operator instructions; storing the flag alone never completes a
  migration.
- Docs: valid-vs-invalid semantics, disjoint outputs, any-rule routing,
  `NULL` handling, exact `_errors` schema, table naming from
  `*_quarantine_table`, `expect_or_drop` alone does not route to quarantine,
  supported and unsupported modes, opt-in procedure; remove the `_error`
  claim.
- Release notes: both legacy corrections with migration guidance.

Exit: docs examples exercised by tests (`test_dab_template_demo`, app tests).

### A11. Stabilization and release (3 d)

- CI triage, review feedback, `run_backward_compat_tests.py` against prior
  version profiles, markdown lint clean.
- Tag and publish Release A.

## Release B: plugin SPI and reference DQX adapter (34 days)

### B1. Experimental SPI publication and discovery (4.5 d)

Files: `quality/spi.py`, new `quality/registry.py`,
`onboard_dataflowspec.py`, `dataflow_pipeline.py`, and `quality/writer.py`.

- Expose the SPI under an `experimental` namespace with
  `api_version = "1-beta"`.
- Entry-point group `databricks.labs.sdp_meta.quality_engines`; resolve only
  the selected engine via `importlib.metadata`.
- Onboarding: accept registered plugin names; for plugin engines skip
  four-group parsing and call `plugin.validate(document, options)`; record
  plugin package/version, engine version, and diagnostic-schema fingerprint in
  `qualityConfig`.
- Compatibility checks per the proposal's plugin-version-drift section: fail
  on incompatible SPI major or engine out of range; warn on compatible
  adapter drift; strict opt-in for exact match; record actual versions in
  update diagnostics; actionable errors naming engine, flow, layer, package.
- Runtime resolves the selected plugin and compares installed adapter, SPI,
  underlying engine, and diagnostic-schema fingerprints with `qualityConfig`
  immediately before `plugin.apply()`. Runtime tests cover missing plugins,
  compatible warnings, strict mismatches, and out-of-range engines.

Exit: native-only startup imports no plugin; onboarding and runtime error paths
are unit-tested.

### B2. Contract test kit (2.5 d)

Files: new `quality/testing/contract.py` (shipped in the wheel).

- Parametrized pytest suite any plugin can run: reserved columns declared and
  honored; routing contract above (disjoint or documented overlap); bindings
  well-formed; `validate` rejects malformed documents; streaming-safe `apply`.

Exit: kit documented and importable from an external package.

### B3. Synthetic plugin (1 d)

Files: `tests/fixtures/synthetic_quality_plugin/`, `Makefile`, CI workflow
(adds a wheel build-and-install step before the test job).

- Build a minimal non-DQX fixture distribution under
  `tests/fixtures/synthetic_quality_plugin/`. CI builds and installs its wheel,
  which registers a real quality-engine entry point.
- Use the installed fixture for discovery, drift, onboarding delegation, and
  contract-kit tests. Do not satisfy discovery by monkeypatching the registry.

Exit: an independently installed non-DQX distribution passes the contract kit.

### B4. DQX adapter repository bootstrap (2 d)

- New repository and package (name provisional): layout, packaging metadata,
  SPI and DQX version ranges, CI, entry-point registration for `dqx`.
- Named SDP-META owners recorded in the repository `CODEOWNERS`. Ship gate:
  Release B does not tag without them.

Exit: `pip install` of the adapter registers the `dqx` entry point.

### B5. DQX adapter implementation (4 d)

- `validate`: delegate to DQX metadata validation.
- `apply`: `DQEngine.apply_checks_by_metadata` on the streaming DataFrame.
- `get_main_input` and `get_invalid`: DQX-native routing (warning overlap
  preserved); no `main` drop bindings; `allows_overlap = True` with
  `overlap_diagnostics = ("_warnings", "_errors")`.
- `reserved_columns`: `_errors`, `_warnings`; publish a diagnostic-schema
  fingerprint per supported DQX version.
- `native_expectations`: returns none in the first release.
- Adapter docs: warning-only filter example against the quarantine table.

Exit: adapter passes the contract kit.

### B6. Adapter tests (6 d)

- Unit tests with DQX installed locally.
- Serverless matrix: minimum, a representative middle, and maximum supported
  DQX versions; error rows, warning rows, valid rows, idempotency;
  missing-adapter and drift errors.
- Pass-through coverage: a DQX check not referenced anywhere in the adapter
  runs unchanged through the metadata path.
- Cross-engine migration: Lakeflow to DQX and DQX to Lakeflow reject reuse of
  an incompatible quarantine target; a new target or acknowledged full refresh
  succeeds. The acknowledged path executes a real full-refresh update and
  verifies the new diagnostic schema.

Exit: matrix green; compatibility range published in adapter README.

### B7. DAB, App, and onboarding environments (4 d)

Files: `bundle.py`, DAB templates and schema, pipeline init notebook template,
App onboarding services and runtime packaging, onboarding job definition, and
`docs/`.

- `dqx` engine value accepted end to end.
- Adapter installed wherever `plugin.validate()` or `plugin.apply()` runs:
  the pipeline environment (extend the init notebook `%pip` line in
  `init_sdp_meta_pipeline.py.tmpl`; do not use unsupported pipeline
  `libraries` entries), the onboarding job environment (job library or
  `%pip`), and the App runtime package requirements when the App performs
  onboarding. Fail onboarding with an install instruction when the adapter is
  missing; document all three requirements.
- DAB and App flows surface the engine-switch migration choice before
  deployment.

Exit: onboarding a `dqx` flow from DAB and from the App succeeds with the
adapter installed and fails actionably without it.

### B8. Same-pipeline demo and docs (2.5 d)

Files: `demo/`, `DQX_DEMO_README.md`, `docs/`.

- Replace the separate-task DQX demo as the recommended path; keep the old
  demo as a compatibility example.
- Document UI limitations, warning semantics, engine-switch migration, and
  environment installation.

### B9. Stabilization (4 d)

- Review, CI, external-repository coordination, markdown lint.

### B10. SPI promotion and release (1.5 d)

- Promote `1-beta` to `1` after the DQX adapter and synthetic plugin pass the
  kit; publish the compatibility statement.
- Verify the ship gate (named owners) and tag Release B and adapter v0.1.

## Milestones

| Week | Milestone |
| --- | --- |
| 1 | A1, A2: legacy paths pinned; quarantine-only correction |
| 2 | A3, A4: target validation, `qualityConfig`, dispatcher, preflight hook |
| 3 to 4 | A6a spike decided; A5 onboarding and runtime validators |
| 5 to 6 | A6 to A8: writer, Lakeflow engine, local Spark semantics green |
| 7 to 8 | A9: serverless and migration suites green |
| 9 to 10 | A10, A11: managed migration path; Release A tagged |
| 11 | B1, B2: experimental SPI, contract kit |
| 12 | B3 to B5: synthetic plugin, adapter bootstrap and implementation |
| 13 to 14 | B6: adapter matrix green |
| 15 | B7: environments, DAB, App |
| 16 | B8 to B10: demo, stabilization, Release B and adapter tagged |

## Dependencies and blockers

- Serverless workspace with permissions for pipeline creation and event-log
  queries (A9, B6).
- A6a spike result gates A6 and A7. An unsupported result stops Release A
  pending a proposal amendment.
- Adapter repository location and package name before B4.
- Named SDP-META adapter owners before B10 (ship gate). DQX maintainer
  ownership is not a blocker.

## Risk register

<!-- markdownlint-disable MD013 -->

| Risk | Impact | Mitigation |
| --- | --- | --- |
| Onboarding refactor breaks legacy tests | High | A1 golden suite first; A5 adds code paths, never edits legacy branches |
| Runtime topology bypasses quality dispatch | High | A4 preflight runs at the top of `write()`; new-writer branch precedes the snapshot/DQE/CDC/append branches in `write_layer_table()` |
| Migration acknowledgment does not execute a full refresh, or flag stays sticky | High | A10 managed launchers execute the refresh and clear the flag; A9/B6 verify schema replacement, flag cleared, next update incremental |
| Temporary-view expectations unsupported | High | A6a spike in week 2; stop and amend, no silent fallback |
| Metrics report zero failures because rows were pre-filtered | High | Routing contract above; A6 stub test and A8 parity test |
| Malformed or unresolved SQL reaches graph construction | High | A5 parses and analyzes expressions; runtime rechecks deferred analysis |
| Serverless iteration slow | Medium | Batch scenarios per layer; local Spark covers semantics |
| DQX diagnostic schema changes mid-project | Medium | Adapter pins a range; fingerprint check in B1 |
| Adapter missing in onboarding or App environment | Medium | B7 installs and verifies in every environment that calls the plugin |
| App and DAB fields drift from onboarding | Low | Shared `quality/fields.py` constants from A5 |

<!-- markdownlint-enable MD013 -->

## Out of scope

- New-engine CDC, snapshot, append-flow, and multi-source CDC support.
- DQX summary metrics.
- Per-rule DQX UI bridge.
- Legacy path removal.
