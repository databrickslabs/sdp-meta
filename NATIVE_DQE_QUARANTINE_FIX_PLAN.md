# Native DQE Quarantine Correction Plan

## Purpose

Correct SDP-META's native `expect_or_quarantine` behavior so one set of
valid-data expectations reliably:

- keeps passing rows in the main target;
- routes any failing row to the quarantine target;
- records which rules failed on each quarantined row; and
- produces consistent results for multiple rules and SQL `NULL` values.

This work is independent of optional DQX integration. Existing users of native
Lakeflow expectations need correct, documented quarantine semantics even if
SDP-META never adds DQX support.

## Current behavior

`DataflowPipeline.write_layer_with_dqe()` reads four rule groups from
`dataQualityExpectations`. The main target applies `expect`, `expect_or_drop`,
and `expect_or_fail`, but it does not apply `expect_or_quarantine`.

The quarantine target reads the same input view and decorates it with
`dp.expect_all_or_drop(expect_or_quarantine_dict)`. Consequently:

1. `expect_or_quarantine` expressions currently have to describe invalid
   records, unlike normal expectations that describe valid records.
2. Invalid rows remain in the main target unless customers separately add the
   inverse condition under `expect_or_drop`.
3. Multiple independent invalid predicates are combined with all-of behavior.
   A row is retained in quarantine only when every predicate is true. Existing
   examples avoid this by combining failures into one large `OR` expression.
4. The runtime does not add the documented `_error` column.
5. Documentation examples that use valid predicates under
   `expect_or_quarantine` do not match runtime behavior.
6. If a spec contains only `expect_or_quarantine`, none of the main-table
   branches declares `dlt_table_with_expectation`; the quarantine table may be
   declared, but the main target is missing.
7. If quarantine rules exist while the quarantine table name is empty,
   `write_layer_with_dqe()` logs a warning and returns early. This silently
   skips quarantine instead of rejecting an invalid configuration.
8. CDC currently calls `cdc_apply_changes()` and then executes the legacy
   quarantine block against the raw input view. That is existing behavior and
   must be pinned before any refactor.

For example, current repository fixtures use this legacy convention:

```yaml
expect_or_drop:
  valid_id: id IS NOT NULL

expect_or_quarantine:
  quarantine_rule: id IS NULL
```

## Target semantics

All quality rules should describe valid data:

```yaml
expect_or_quarantine:
  customer_id_required: customer_id IS NOT NULL
  positive_amount: amount > 0
```

SDP-META should derive the routing behavior:

```text
main target:
  retain rows where every quarantine expectation passes

quarantine target:
  retain rows where any quarantine expectation fails
```

SQL `NULL` results must count as failures:

```text
rule_passes = COALESCE(rule_expression, FALSE)
quarantine = NOT(rule_1_passes AND rule_2_passes ...)
```

The quarantine target should retain source columns and add structured failure
details. Recommended schema:

```text
_errors ARRAY<STRUCT<
  name: STRING,
  expression: STRING
>>
```

Use `_errors`, rather than the currently documented singular `_error`, to
represent multiple failures without losing information.

## Compatibility strategy

Changing the meaning of existing `expect_or_quarantine` expressions silently
would break customers whose files use legacy invalid predicates. Do not
reinterpret, migrate, deprecate, or eventually change that existing field.
Keep the legacy path as a permanent compatibility mode.

Introduce a separate additive quality-engine configuration:

```yaml
bronze_quality_engine: lakeflow
bronze_quality_rules_path_prod: /Volumes/catalog/config/quality/customers.yml
bronze_quarantine_table: customers_quarantine
```

or:

```yaml
bronze_quality_engine: dqx
bronze_quality_rules_path_prod: /Volumes/catalog/config/dqx/customers.yml
bronze_quarantine_table: customers_quarantine
bronze_quality_metrics_table: customers_quality_metrics
```

Runtime dispatch must be explicit:

```python
if dataflow_spec.qualityConfig:
    write_with_quality_engine(dataflow_spec.qualityConfig)
elif dataflow_spec.dataQualityExpectations:
    write_layer_with_dqe()  # Existing behavior remains unchanged.
else:
    write_standard_table()
```

The three supported modes are:

| Configuration | Runtime behavior |
|---|---|
| Existing `*_data_quality_expectations_json_*` | Legacy implementation, unchanged |
| `bronze_quality_engine: lakeflow` / `silver_quality_engine: lakeflow` | New valid-rule routing with structured errors |
| `bronze_quality_engine: dqx` / `silver_quality_engine: dqx` | DQX-native checks, routing, diagnostics, and optional metrics |

Reject onboarding rows that mix legacy DQE fields with the new prefixed
`bronze_quality_engine`/`silver_quality_engine` configuration. Never infer
semantics from rule names or SQL expressions.

Avoid heuristic detection based on rule names such as `quarantine_rule` or
expressions containing `IS NULL`; those approaches are unreliable.

## Scope and release boundaries

Native quarantine correctness and optional DQX integration are separate
deliverables.

### Deliverable A: legacy safety and native correctness

This can ship without DQX:

- golden tests for every existing legacy path;
- declaration of the main target when only `expect_or_quarantine` exists;
- fail-fast validation for missing quarantine targets;
- accurate documentation of legacy names and routing;
- nullable `qualityConfig` schema support and explicit runtime dispatch;
- `bronze_quality_engine`/`silver_quality_engine` plus
  `*_quality_rules_path_{env}` onboarding support for `lakeflow`;
- shared quality writer contract and the Lakeflow adapter;
- additive Lakeflow valid-predicate mode; and
- Spark and serverless integration coverage.

### Deliverable B: optional DQX integration

This begins only after Deliverable A is complete:

- DQX adapter implementation behind the shared interface;
- `dqx` as an accepted engine value and DQX-specific validation;
- optional package extra and runtime dependency guard;
- DQX valid/quarantine/metrics outputs; and
- DQX-specific examples and tests.

The native fixes must not be blocked on DQX packaging, metrics, or API design.

## Shared quality-engine contract

Implement the new path behind an internal adapter contract:

```python
class QualityEngine:
    def apply(self, input_df, rules):
        """Return a checked DataFrame with engine-specific diagnostics."""

    def get_valid(self, checked_df):
        """Return rows accepted by the configured engine."""

    def get_invalid(self, checked_df):
        """Return rows routed to quarantine."""
```

The common writer owns pipeline table declaration and target options. Engine
adapters own rule evaluation:

- `LakeflowQualityEngine` evaluates valid SQL predicates, constructs
  `_errors`, and preserves native expectation metrics.
- `DQXQualityEngine` delegates to `DQEngine.apply_checks_by_metadata`,
  `get_valid`, and `get_invalid`.

Both adapters feed the same pipeline topology:

```text
SDP-META source view
  -> selected quality engine
  -> checked view
      -> valid target
      -> quarantine target
      -> optional metrics target
```

This avoids a separate post-pipeline notebook and applies quality checks to the
DataFrame produced by the SDP-META reader and custom transformations.

## Runtime design

### 1. Separate rule evaluation from writing

Introduce a small internal representation:

```python
@dataclass(frozen=True)
class QuarantineRule:
    name: str
    valid_expression: str
```

Create helpers that:

- validate rule names and expressions;
- handle only the new valid-predicate semantics;
- build a combined valid predicate;
- build a combined invalid predicate; and
- construct the `_errors` expression.

The legacy path remains separate and is never normalized or reinterpreted by
these helpers. Keep the helpers independent of decorators so their behavior
can be tested with real Spark DataFrames.

### 2. Build one checked input

Declare one additional `dp.temporary_view` per quality-enabled flow. It reads
the existing SDP-META input view and derives:

```text
checked input
  = source columns
  + _errors array
```

Each failed rule contributes one struct to `_errors`. Empty entries are
removed from the array. The main and quarantine `dp.table` nodes both read this
checked streaming view. This adds one logical graph node per flow; it does not
launch a separate notebook, materialize and reread an external table, or add a
second source definition. Lakeflow owns execution and checkpointing for both
downstream branches.

Every Lakeflow rule is normalized to one deterministic, null-safe expression:

```text
COALESCE((customer_expression), FALSE)
```

Use those exact normalized strings both when constructing `_errors` and when
building the native decorator dictionary. Reject non-deterministic expressions
in new Lakeflow mode so evaluating predicates for diagnostics and native
metrics cannot produce different routing decisions.

### 3. Write the main target

The main target should:

- preserve existing non-quarantine `expect` metrics;
- preserve `expect_or_fail` behavior;
- preserve explicit `expect_or_drop` behavior;
- use `dp.expect_all_or_drop(valid_quarantine_rules)` for new Lakeflow-mode
  quarantine rules, so failed-rule counts remain visible in the native
  pipeline event log;
- drop `_errors` before publishing unless an explicit diagnostic option says
  otherwise.

This deliberately evaluates the valid predicates once while constructing
`_errors` and again in the main-table expectation decorator. The duplicate
predicate evaluation is the cost of retaining native expectation metrics.
Filtering `_errors` in the checked view instead would hide quarantine-rule
failures from the main table's event-log metrics and is not the selected
design. The predicates are duplicated internally by SDP-META, never in
customer configuration.

Do not change the shared zero-argument `write_to_delta()` reader used by legacy
and standard paths. Add dedicated readers for the new quality path:

```python
def read_quality_main(self):
    return dp.read_stream(self.checked_view_name).drop("_errors")

def read_quality_quarantine(self):
    return (
        dp.read_stream(self.checked_view_name)
        .where(F.size("_errors") > 0)
    )
```

Declare the main `dp.table` from `read_quality_main` and wrap it with
`dp.expect_all_or_drop` using the null-safe valid-rule dictionary. Declare the
quarantine `dp.table` from `read_quality_quarantine`. This keeps legacy
`write_to_delta()` behavior untouched and makes main/quarantine routing
explicit.

### 4. Write the quarantine target

The quarantine target should:

- read the same checked pipeline dataset;
- retain rows where `_errors` is non-empty;
- preserve `_errors`;
- honor configured catalog, schema, table, path, table properties,
  partitioning, clustering, automatic clustering, and row filter; and
- fail onboarding or pipeline planning when quarantine rules exist but no
  quarantine target is configured.

The runtime must also raise when the resolved quarantine table name is empty.
Persisted dataflow-spec rows may bypass new onboarding validation, so replacing
the existing warning-and-return branch with a clear runtime error is required.

### 5. Preserve pipeline modes

Verify behavior independently for:

- Bronze and Silver standard flows.
- Declarative Pipeline publishing mode and legacy publishing mode.
- Unity Catalog managed tables and path-based legacy targets.
- Row filters on main and quarantine targets.
- `expect`, `expect_or_drop`, and `expect_or_fail` combined with quarantine.

Legacy CDC already calls `cdc_apply_changes()` and then declares quarantine
against the raw input view. Golden tests must pin its current target and
quarantine contents before refactoring. New Lakeflow and DQX modes for CDC,
snapshot, append-flow, and multi-source CDC still require explicit design
decisions. Do not claim new-engine support until each path has an executable
routing test. If a path cannot safely fan out, reject that configuration during
onboarding with a clear message rather than silently producing incorrect data.

Silver quarantine is wired today: Silver onboarding reads
`silver_data_quality_expectations_json_{env}` and calls the layer-generic
quarantine-details helper with `layer="silver"`. Unlike Bronze, it does so
without first testing the Silver quarantine table for truthiness. Add
executable onboarding and routing tests before promising parity, especially
for empty or missing Silver quarantine fields.

## Onboarding changes

Add new nullable, typed fields without modifying the meaning of existing
fields:

```yaml
bronze_quality_engine: dqx
bronze_quality_rules_path_prod: /Volumes/catalog/config/dqx/customers.yml
bronze_quarantine_table: customers_quarantine
bronze_quality_metrics_table: customers_quality_metrics
```

Requirements:

- Deliverable A accepts `lakeflow`; Deliverable B adds `dqx`.
- Read and validate engine-specific JSON or YAML during onboarding.
- Persist a nullable `qualityConfig` object in Bronze and Silver dataflow-spec
  tables containing the engine, serialized rules, source path, content hash,
  output targets, and engine options.
- Preserve `qualityConfig = null` for all existing rows and old schemas.
- Include it in generated DAB templates and App-based onboarding.
- Reject mixed legacy and new quality configuration.
- Require a quarantine target when the selected engine is configured to route
  invalid rows.
- Reuse every existing layer-prefixed quarantine field:
  - `*_catalog_quarantine_{env}`
  - `*_database_quarantine_{env}`
  - `*_quarantine_table`
  - `*_quarantine_table_path_{env}`
  - `*_quarantine_table_properties`
  - `*_quarantine_table_partitions`
  - `*_quarantine_table_cluster_by`
  - `*_quarantine_table_cluster_by_auto`
  - `*_quarantine_row_filter`
- Do not introduce a duplicate `*_quality_quarantine_*` hierarchy.
- For the legacy path, replace the current warning-and-return behavior for an
  empty quarantine table name with onboarding validation. Existing valid
  configurations are unaffected; broken configurations fail before pipeline
  deployment instead of silently omitting quarantine.
- Warn when a rule name is duplicated across rule groups.

Customers should keep rule files in Git. DAB or the onboarding launcher stages
them to a UC Volume, and onboarding snapshots the validated rules into
`qualityConfig`. Pipelines consume that immutable snapshot rather than reading
mutable files during every graph evaluation.

## Repository examples

Audit every `expect_or_quarantine` occurrence in:

- `demo/conf/json/dqe/`
- `demo/conf/yml/dqe/`
- `examples/json/dqe/`
- `examples/yml/dqe/`
- `integration_tests/conf/json/dqe/`
- `integration_tests/conf/yml/dqe/`
- `tests/resources/dqe/`
- the DAB quickstart template;
- the interactive demo; and
- the Databricks App spec editor.

Do not rewrite these files as part of the additive implementation. They are
legacy compatibility fixtures and must continue producing identical output.

Add separate Lakeflow-quality and DQX examples using valid-rule semantics:

```yaml
bronze_quality_engine: lakeflow
bronze_quality_rules_path_prod: /Volumes/catalog/config/quality/customers.yml
bronze_quarantine_table: customers_quarantine
```

The new Lakeflow rules file uses the same four action groups as the existing
format, but every expression describes valid data:

```yaml
expect:
  region_present: region IS NOT NULL
expect_or_drop:
  parse_succeeded: _rescued_data IS NULL
expect_or_fail:
  source_id_present: source_id IS NOT NULL
expect_or_quarantine:
  valid_id: id IS NOT NULL
  positive_amount: amount > 0
```

`expect`, `expect_or_drop`, and `expect_or_fail` supply the behaviors promised
by the main-target design. `expect_or_quarantine` drives the null-safe native
metrics decorator plus the `_errors`-based quarantine branch. This explicitly
maps the rules file to its onboarding field. DQX examples use
`bronze_quality_engine: dqx` plus the same
`bronze_quality_rules_path_{env}` field, but their file contents use DQX's
metadata schema and remain separate from native rule files.

## Documentation corrections

Update:

- `docs/docs/concepts/data-quality.md`
- `docs/docs/reference/dq-rules.md`
- `docs/docs/concepts/architecture.md`
- `demo/README.md`
- compatibility guidance and release notes

Documentation must state:

- whether rules describe valid or invalid records;
- whether failing rows also remain in the main target;
- how multiple rules combine;
- how SQL `NULL` is handled;
- the exact quarantine diagnostic schema;
- that legacy quarantine table names come from
  `bronze_quarantine_table`/`silver_quarantine_table` and are not automatically
  derived as `<target_table>_quarantine`;
- that `expect_or_drop` only drops rows and never routes them to quarantine;
- supported pipeline modes and unsupported combinations; and
- how to opt into a new quality engine while leaving legacy rules unchanged.

Remove the existing claim that quarantine has an `_error` column until the
runtime actually creates it. Once implemented, document the final `_errors`
schema exactly.

Correct these additional inaccurate claims:

- Legacy quarantine tables are not automatically named
  `<target_table>_quarantine`; the name is supplied by
  `bronze_quarantine_table` or `silver_quarantine_table`.
- `expect_or_drop` never routes rows to quarantine. It only drops failures from
  its decorated target. Legacy quarantine requires a separately configured
  `expect_or_quarantine` group and quarantine target.

Release notes must identify the two intentional legacy-path corrections:

- quarantine-only specs now declare the previously missing main target; and
- empty quarantine target names now fail onboarding/runtime instead of
  silently skipping quarantine.

## Test plan

### Unit tests

Test pure rule-evaluation helpers for:

- one passing and one failing rule;
- multiple independent failures;
- expressions returning `NULL`;
- duplicate names;
- malformed rule definitions;
- new valid-predicate semantics; and
- stable `_errors` ordering.

Also assert:

- `_errors` and the native decorator use byte-identical
  `COALESCE((expression), FALSE)` strings;
- non-deterministic Lakeflow expressions are rejected;
- `read_quality_main()` removes `_errors`;
- `read_quality_quarantine()` filters on `size(_errors) > 0`; and
- legacy and standard paths continue using the unchanged `write_to_delta()`.

### Spark behavior tests

Use real local Spark DataFrames to assert:

- passing rows appear only in main;
- error rows appear only in quarantine;
- a row failing any one of several rules is quarantined;
- a row failing several rules records every failed rule;
- `NULL` evaluation is treated as failure;
- main output does not expose `_errors`;
- quarantine output includes the expected `_errors`; and
- explicit drop and fail rules still behave correctly.

Mock-only decorator assertions are insufficient because they cannot prove row
routing semantics.

### Pipeline integration tests

Run a minimal serverless Lakeflow pipeline with deterministic input:

```text
8 source rows
5 valid rows
3 quarantine rows
at least 1 row failing multiple rules
```

Validate table contents, not only counts. Confirm the pipeline event log
contains expected native metrics and that a second update remains idempotent.

Test JSON and YAML configurations and both Bronze and Silver quarantine.

### Backward compatibility tests

- Run existing invalid-predicate rule files through the untouched legacy path.
- Confirm table contents and metrics remain identical.
- Pin the legacy `expect_or_quarantine`-only case and verify a main table plus
  quarantine table are both declared after the targeted bug fix.
- Pin current CDC behavior: AUTO CDC remains the main writer and quarantine
  continues to read the raw input view.
- Verify empty Bronze and Silver quarantine table names fail during onboarding
  instead of reaching the runtime warning-and-return branch.
- Confirm old dataflow-spec rows deserialize with `qualityConfig = null`.
- Confirm the dispatcher selects legacy behavior whenever only
  `dataQualityExpectations` is populated.
- Confirm mixed legacy and new configurations fail onboarding clearly.

Treat the existing golden fixtures as a permanent compatibility policy. Any
proposal to change their meaning requires an explicitly approved breaking
change; it is not an assertion that one test can enforce for all future
releases.

### DQX adapter tests

- Validate DQX JSON and YAML metadata during onboarding.
- Verify rule content and hash are persisted in `qualityConfig`.
- Apply DQX to a streaming DataFrame before target materialization.
- Verify DQX valid and quarantine outputs preserve `_errors` and `_warnings`
  semantics.
- Verify optional metrics output when configured.
- Verify missing DQX dependencies produce an actionable startup error.
- Verify native Lakeflow mode does not import or require DQX.

## Delivery sequence

### Deliverable A: native quarantine

1. Add golden tests for standard, quarantine-only, CDC, Bronze, Silver, and
   missing-target legacy behavior.
2. Ensure quarantine-only legacy specs still declare the main target.
3. Add fail-fast onboarding validation for missing quarantine targets.
4. Add nullable `qualityConfig`, explicit dispatch, and layer-prefixed
   Lakeflow onboarding fields.
5. Add the shared quality writer contract and Lakeflow adapter.
6. Implement additive Lakeflow valid-predicate routing and `_errors`.
7. Correct legacy and new-mode documentation, including explicit table naming
   and `expect_or_drop` behavior.
8. Run unit, Spark, serverless integration, and backward-compatibility suites.

Deliverable A is independently releasable at this point.

### Deliverable B: optional DQX

9. Accept `dqx` in the existing quality-engine fields and validation.
10. Implement the optional DQX adapter and dependency guard.
11. Add DQX enablement to DAB and App configuration.
12. Add DQX examples without modifying legacy fixtures.
13. Run DQX unit, Spark, serverless integration, and compatibility suites.
14. Expand new-engine support to CDC/snapshot/append-flow variants only after
    dedicated tests prove correct behavior.

## Acceptance criteria

- A single valid predicate is sufficient to route rows correctly.
- A legacy spec containing only `expect_or_quarantine` declares both its main
  target and configured quarantine target.
- Failing rows do not remain in the main target.
- Any failed quarantine rule routes the row to quarantine.
- Quarantine rows identify every failed rule.
- `NULL` predicate results are failures.
- Main and quarantine table options remain functional.
- Existing valid configurations retain identical behavior without changing
  rules or onboarding files.
- The two intentional legacy corrections are documented: quarantine-only
  specs gain their missing main target, and missing quarantine targets fail
  fast instead of being skipped.
- Old dataflow-spec rows continue through the legacy path.
- New Lakeflow JSON and YAML examples use identical valid-rule semantics.
- DQX rules use DQX's native metadata format.
- Runtime behavior and documentation agree.
- Unsupported source modes fail clearly rather than silently misrouting data.
- No DQX dependency is required for native DQE quarantine.
- DQX is imported and installed only when `bronze_quality_engine: dqx` or
  `silver_quality_engine: dqx` is selected.
- Mixing legacy fields with a new quality engine fails clearly.
- Native quarantine event-log metrics remain visible because the main target
  applies `dp.expect_all_or_drop` to the valid predicates.

## Relationship to DQX

Optional DQX support uses the shared quality-output contract:

```text
checked DataFrame
  -> valid target
  -> quarantine target
  -> structured errors
  -> optional metrics
```

Native DQE and DQX should remain separate engines:

- legacy DQE for unchanged customer configurations;
- new Lakeflow mode for valid-rule routing and native event-log metrics;
- DQX for richer checks, diagnostics, profiling, metrics, and alerting.

Both engines should produce consistent main/quarantine routing from the
customer's perspective. DQX remains an optional dependency, for example:

```bash
pip install "databricks-labs-sdp-meta[dqx]"
```

If DQX mode is configured without the optional dependency, pipeline startup
must fail with a direct installation instruction.
