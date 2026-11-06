---
id: data-quality
title: Data Quality
sidebar_position: 4
---

# Data Quality

SDP-META supports Lakeflow Spark Declarative Pipelines' native Data Quality expectations. Define expectations in your onboarding YAML or JSON file; SDP-META registers and applies them at pipeline startup.

## Actions

| Action | Behavior |
|---|---|
| `warn` (`expect`) | Row passes through; a warning metric is recorded in the pipeline event log. |
| `drop` (`expect_or_drop` / `expect_or_quarantine`) | Row is excluded from the target table. With `expect_or_quarantine`, the row is written to a quarantine table. |
| `fail` (`expect_or_fail`) | The entire pipeline update is halted. |

## Defining expectations

DQE rules are stored in a separate JSON or YAML file and referenced from the onboarding file via `bronze_data_quality_expectations_json_{env}` or `silver_data_quality_expectations_json_{env}`. The format is a dict of `{rule_name: sql_expression}` grouped by constraint type:

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

:::note
Each key within a constraint block is the **rule name** (shown in pipeline metrics and event logs). The value is the SQL boolean expression evaluated per row. Do not swap these — the rule name must be a valid identifier, not a SQL expression.
:::

Reference the file in your onboarding entry:

```json
{
  "bronze_data_quality_expectations_json_prod": "/Volumes/my_catalog/my_schema/my_volume/conf/dqe/orders.json"
}
```

Replace `prod` with your environment tag (`dev`, `stag`, etc.) to match your `env` parameter.

## Quarantine table

`expect_or_quarantine` routes failing rows to the explicitly configured
quarantine target. Configure both the quarantine schema and table name in the
onboarding entry: `bronze_database_quarantine_{env}` and
`bronze_quarantine_table` for Bronze, or
`silver_database_quarantine_{env}` and `silver_quarantine_table` for Silver.
SDP-META does not derive a `<target_table>_quarantine` name automatically.

The quarantine output preserves the input schema. It does not currently add an
`_error` column identifying the failed expectation. Liquid clustering can be
configured for quarantine tables.

:::tip
Use `expect_or_quarantine` rather than `expect_or_drop` when you need to investigate failed rows. Quarantined data is preserved and queryable.
:::

## Onboarding file fields

| Field | Layer | Description |
|---|---|---|
| `bronze_data_quality_expectations_json_{env}` | Bronze | Path to the DQE JSON or YAML file for the bronze table (e.g. `/Volumes/.../dqe/orders.json`) |
| `silver_data_quality_expectations_json_{env}` | Silver | Path to the DQE JSON or YAML file for the silver table |

`{env}` is replaced by the environment tag you supply at onboarding time (e.g. `prod`, `dev`, `stag`).

:::note
In DAB bundles, storing expectations in a separate file is preferred — it can be reviewed and diff'd in git alongside the onboarding file.
:::

## Monitoring

Lakeflow Spark Declarative Pipelines records expectation metrics in the pipeline event log. Query from the pipeline UI or via SQL on the `event_log` system table.

For the complete expectation schema, see [DQ Rules Reference](../reference/dq-rules.md).
