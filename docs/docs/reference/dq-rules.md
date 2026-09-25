---
id: dq-rules
title: Data Quality Rules Schema
sidebar_position: 3
---

# Data Quality Rules Schema

Data quality rules are defined in a separate JSON or YAML file and referenced from the onboarding file via `bronze_data_quality_expectations_json` or `silver_data_quality_expectations_json`. Each rule is a named SQL boolean expression mapped directly to Declarative Pipeline constraint annotations.

## Constraint types

| Constraint | Pipeline action | Use when |
|---|---|---|
| `expect` | Log violation, keep the row | Track quality issues without dropping data |
| `expect_or_drop` | Drop the failing row silently | Bad rows should not reach the main table and do not need to be inspected |
| `expect_or_quarantine` | Route the failing row to a quarantine table | Bad rows should be preserved for investigation rather than silently dropped |
| `expect_or_fail` | Halt the entire pipeline update | A violated rule indicates a critical upstream data problem |

:::tip
Prefer `expect_or_quarantine` over `expect_or_drop` when you want to inspect failed rows later.
:::

:::warning
`expect_or_fail` stops all pipeline processing for the current update. Use it only for genuine data contract breaches where continuing with bad data would cause irreversible harm.
:::

## JSON schema

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

## YAML equivalent

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

Reference the DQE file from the onboarding entry using the env-suffixed field name. Replace `prod` with your actual environment tag (`dev`, `stag`, etc.):

```json
{
  "bronze_data_quality_expectations_json_prod": "/Volumes/my_catalog/my_schema/my_volume/conf/dqe/orders.json"
}
```

For silver:

```json
{
  "silver_data_quality_expectations_json_prod": "/Volumes/my_catalog/my_schema/my_volume/conf/dqe/orders_silver.json"
}
```

## Quarantine behavior

Quarantine target fields remain optional during onboarding for backward
compatibility. A DQE file containing only `expect`, `expect_or_drop`, or
`expect_or_fail` does not use any quarantine fields.

To create an output for Bronze quarantine rules, configure
`bronze_quarantine_table` and
`bronze_database_quarantine_{env}`. For Silver, configure
`silver_quarantine_table` and `silver_database_quarantine_{env}`. These fields
identify the quarantine target persisted in the DataflowSpec. Non-Unity
Catalog targets also use the corresponding
`bronze_quarantine_table_path_{env}` or
`silver_quarantine_table_path_{env}`.

Legacy onboarding files with non-empty `expect_or_quarantine` rules but no
target continue to onboard successfully. They do not produce a quarantine
table until target metadata is supplied, and onboarding emits a warning. If
`expect_or_quarantine` is the only non-empty constraint block, the pipeline
also does not declare the main output table. Add the quarantine table and
database fields to avoid a pipeline with no declared output. Missing targets
remain accepted only for backward compatibility.

Existing onboarding files may include optional quarantine metadata before
quarantine rules are added. SDP-META preserves that metadata, but no
quarantine output is created until `expect_or_quarantine` is non-empty.

:::tip
Use the quarantine table to inspect and reprocess failed rows.
:::

## Example files in the repository

- JSON examples: [`demo/conf/json/dqe/`](https://github.com/databrickslabs/sdp-meta/tree/main/demo/conf/json/dqe)
- YAML examples: [`demo/conf/yml/dqe/`](https://github.com/databrickslabs/sdp-meta/tree/main/demo/conf/yml/dqe)
