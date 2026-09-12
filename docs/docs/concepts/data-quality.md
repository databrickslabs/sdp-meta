---
id: data-quality
title: Data Quality
sidebar_position: 4
---

# Data Quality

SDP-META has three quality paths:

- the legacy `dataQualityExpectations` path, retained without reinterpreting
  existing predicates;
- the opt-in `qualityConfig` path, selected with
  `<layer>_quality_engine: lakeflow`; and
- the optional built-in DQX engine, with additional external engines
  discoverable through the quality-engine SPI.

New flows should use the opt-in path. It uses valid-rule semantics, emits
structured diagnostics, and produces disjoint main and quarantine outputs.

## Opt-in Lakeflow engine

Keep rules in a separate JSON or YAML file. Every expression describes a
valid row:

```json
{
  "expect_or_quarantine": {
    "customer_id_not_null": "customer_id IS NOT NULL",
    "valid_order_amount": "order_amount > 0"
  },
  "expect": {
    "region_not_null": "region IS NOT NULL"
  },
  "expect_or_fail": {
    "event_date_not_null": "event_date IS NOT NULL"
  }
}
```

Reference the file and explicit quarantine target in onboarding:

```json
{
  "bronze_quality_engine": "lakeflow",
  "bronze_quality_rules_path_prod": "/Volumes/my_catalog/my_schema/my_volume/conf/quality/orders.json",
  "bronze_database_quarantine_prod": "orders_bronze",
  "bronze_quarantine_table": "orders_quarantine"
}
```

Replace `prod` with the onboarding environment. JSON and YAML documents may
contain `expect`, `expect_or_drop`, `expect_or_fail`, and
`expect_or_quarantine` groups. Rule names must be unique identifiers across
all groups.

- `expect` retains rows and reports native Lakeflow metrics.
- `expect_or_drop` removes failures from the main table but does not route
  them to quarantine.
- `expect_or_quarantine` removes failures from the main table and writes them
  to the configured quarantine table.
- `expect_or_fail` prevents either consuming flow from committing a
  micro-batch containing a failure. Previously committed prefixes can differ
  between the independent flows.

SQL `NULL` results are treated as failures. Rows that fail any quarantine rule
carry:

```text
_errors ARRAY<STRUCT<name STRING, expression STRING>>
```

The main table does not expose `_errors`. Table names are never derived
automatically: configure the existing
`<layer>_database_quarantine_{env}`, `<layer>_quarantine_table`, and path or
catalog fields explicitly.

The first release supports standard Bronze and Silver flows. Append flows,
AUTO CDC, snapshots, and multi-source CDC are rejected during onboarding and
again at pipeline startup.

The DAB template exposes a `quality_engine` prompt. Select `lakeflow` to
scaffold the engine fields, an explicit quarantine target, and a starter rules
file; the default `none` retains the legacy example.

## DQX adapter

Install SDP-META with its optional DQX dependency:

```text
pip install "databricks-labs-sdp-meta[dqx]"
```

Then configure `<layer>_quality_engine: dqx` and reference a native DQX
JSON/YAML list of checks. The adapter applies checks once through
`apply_checks_by_metadata`, sends rows without errors to the main output, and
sends rows with errors or warnings to quarantine. Warning-only rows therefore
appear in both outputs. The main table drops DQX-owned `_errors`, `_warnings`,
and `_dq_info` columns when present. Quarantine always retains `_errors` and
`_warnings`; `_dq_info` is check-dependent and appears only for checks that
produce supplemental information, such as anomaly checks.

The DAB template installs DQX and scaffolds DQX-native rules when
`quality_engine=dqx`.

## Changing quality engines

Changing the engine or diagnostic schema while reusing an existing quarantine
target requires an explicit migration. Prefer configuring a new quarantine
table. To reuse the table, set:

```yaml
bronze_quality_engine_migration: full_refresh
```

This acknowledgment is persisted in `qualityConfig`. Managed CLI and App
launchers, plus DAB bundles scaffolded with
`managed_quality_migrations=true`, first full-refresh only the affected
quarantine table. After that succeeds, they clear the pending flag with
compare-and-swap protection, retain the completed migration fingerprint, and
start a normal full-graph update. Direct unmanaged pipeline starts do not
execute this workflow.

## Legacy expectations

Legacy files continue to use
`<layer>_data_quality_expectations_json_{env}`. Existing
`expect_or_quarantine` expressions describe rows selected for quarantine,
rather than valid rows. When multiple legacy quarantine predicates are
configured, a row is selected only when all of them evaluate to true.
SDP-META does not rewrite or invert them.

New and re-onboarded legacy quarantine-only specifications declare the
previously missing main target. Persisted pre-upgrade rows preserve their
existing graph topology until they are re-onboarded.

Onboarding rejects a missing or blank `<layer>_quarantine_table` when legacy
quarantine rules are present. A persisted pre-upgrade row with this defect
continues to run its main path, logs an error naming the missing field, and
skips quarantine. Re-onboarding opts the row into strict behavior and requires
the target.

SDP-META tracks this compatibility boundary in the internal nullable
`dqeContract` spec-table column. `NULL` means pre-upgrade behavior; newly
written rows are stamped with the current contract. This field is managed by
onboarding and is not an onboarding-file setting.

## Monitoring

The opt-in Lakeflow engine binds normalized expressions to native
expectations, so rule metrics remain available in the pipeline event log and
UI. Quarantine `_errors` values use the same normalized expressions as those
bindings.

For the complete legacy expectation schema, see
[DQ Rules Reference](../reference/dq-rules.md).
