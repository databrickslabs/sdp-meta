---
id: dq-rules
title: Data Quality Rules Schema
sidebar_position: 3
---

# Data Quality Rules Schema

Data quality rules are defined in a separate JSON or YAML file. Legacy rules
use `<layer>_data_quality_expectations_json_{env}`. Opt-in Lakeflow rules use
`<layer>_quality_engine: lakeflow` with
`<layer>_quality_rules_path_{env}`. Each rule is a named SQL boolean
expression.

## Constraint types

| Constraint | Pipeline action | Use when |
|---|---|---|
| `expect` | Log violation, keep the row | Track quality issues without dropping data |
| `expect_or_drop` | Drop the failing row silently | Bad rows should not reach the main table and do not need to be inspected |
| `expect_or_quarantine` | Remove failures from main and preserve them in quarantine | Bad rows should be preserved for investigation rather than silently dropped |
| `expect_or_fail` | Halt the entire pipeline update | A violated rule indicates a critical upstream data problem |

:::tip
Prefer `expect_or_quarantine` over `expect_or_drop` when you want to inspect
failed rows later. `expect_or_drop` failures are discarded, not quarantined.
The opt-in Lakeflow quarantine table adds a structured `_errors` array; there
is no singular `_error` column.
:::

:::warning
`expect_or_fail` stops all pipeline processing for the current update. Use it only for genuine data contract breaches where continuing with bad data would cause irreversible harm.
:::

## Opt-in Lakeflow JSON schema

```json
{
  "expect": {
    "valid_order_amount": "order_amount > 0",
    "valid_status": "status IN ('active', 'pending', 'closed')"
  },
  "expect_or_drop": {
    "transaction_id_not_null": "transaction_id IS NOT NULL"
  },
  "expect_or_quarantine": {
    "customer_id_not_null": "customer_id IS NOT NULL"
  },
  "expect_or_fail": {
    "date_not_null": "order_date IS NOT NULL"
  }
}
```

Each key within a constraint block is the **rule name** (a unique identifier shown in pipeline metrics). The value is the SQL boolean expression evaluated per row. Do not swap them.

## Opt-in Lakeflow YAML equivalent

```yaml
expect:
  valid_order_amount: "order_amount > 0"
  valid_status: "status IN ('active', 'pending', 'closed')"

expect_or_drop:
  transaction_id_not_null: "transaction_id IS NOT NULL"

expect_or_quarantine:
  customer_id_not_null: "customer_id IS NOT NULL"

expect_or_fail:
  date_not_null: "order_date IS NOT NULL"
```

## Referencing the rules file

Reference opt-in rules using the engine and env-suffixed rules path. Replace
`prod` with your actual environment tag (`dev`, `stag`, etc.):

```json
{
  "bronze_quality_engine": "lakeflow",
  "bronze_quality_rules_path_prod": "/Volumes/my_catalog/my_schema/my_volume/conf/quality/orders.json",
  "bronze_database_quarantine_prod": "retail_bronze",
  "bronze_quarantine_table": "orders_quarantine"
}
```

For the legacy path, use the DQE field instead:

```json
{
  "bronze_data_quality_expectations_json_prod": "/Volumes/my_catalog/my_schema/my_volume/conf/dqe/orders.json",
  "bronze_database_quarantine_prod": "retail_bronze",
  "bronze_quarantine_table": "orders_quarantine"
}
```

The same field patterns are available with the `silver_` prefix.

## Quarantine behavior

`expect_or_drop` always drops failing rows and never writes them to
quarantine. Quarantine routing requires `expect_or_quarantine` and an explicit
`<layer>_quarantine_table`; table names are not derived.

The two rule paths intentionally have different predicate conventions:

- With the opt-in Lakeflow engine, predicates describe valid rows. A row is
  quarantined when **any** `expect_or_quarantine` predicate fails. Quarantine
  retains `_errors ARRAY<STRUCT<name STRING, expression STRING>>`.
- On the legacy path, `expect_or_quarantine` predicates describe rows to
  quarantine. When several are configured, they combine with all-of semantics:
  a row is quarantined only when every predicate evaluates to true. Legacy
  quarantine does not add `_error` or `_errors`.

:::tip
Use the quarantine table to inspect and reprocess failed rows.
:::

## Example files in the repository

- Opt-in JSON examples: [`examples/json/quality/`](https://github.com/databrickslabs/sdp-meta/tree/main/examples/json/quality)
- Opt-in YAML examples: [`examples/yml/quality/`](https://github.com/databrickslabs/sdp-meta/tree/main/examples/yml/quality)
- Legacy JSON examples: [`demo/conf/json/dqe/`](https://github.com/databrickslabs/sdp-meta/tree/main/demo/conf/json/dqe)
- Legacy YAML examples: [`demo/conf/yml/dqe/`](https://github.com/databrickslabs/sdp-meta/tree/main/demo/conf/yml/dqe)

:::warning
Files under `dqe/` use the legacy invalid-row predicate convention. Do not
copy those predicates into `quality/` rules without inverting their meaning.
:::
