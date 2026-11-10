---
id: silver-transformations
title: Silver Transformations Schema
sidebar_position: 2
---

# Silver Transformations Schema

The silver transformations file defines the SQL logic applied when writing from a bronze table to a silver table. Each entry maps one source to one silver target.

The file path is referenced in the onboarding file via the `silver_transformation_json_{env}` field (e.g. `silver_transformation_json_prod`).

Both JSON and YAML formats are supported.

---

## File Structure

The file is a top-level array of transformation objects. Each object defines the transformation for one silver table.

### JSON Example

```json
[
  {
    "target_table": "customers_silver",
    "select_exp": [
      "id as customer_id",
      "name",
      "upper(email) as email"
    ],
    "where_clause": ["is_active = true"]
  },
  {
    "target_table": "transactions_silver",
    "select_exp": [
      "transaction_id",
      "customer_id",
      "amount",
      "cast(transaction_date as date) as transaction_date"
    ]
  }
]
```

### YAML Equivalent

```yaml
- target_table: customers_silver
  select_exp:
    - "id as customer_id"
    - "name"
    - "upper(email) as email"
  where_clause:
    - "is_active = true"

- target_table: transactions_silver
  select_exp:
    - transaction_id
    - customer_id
    - amount
    - "cast(transaction_date as date) as transaction_date"
```

---

## Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `target_table` | string | Yes | Name of the silver target table. Must match the `silver_table` value in the onboarding file. |
| `select_exp` | array of strings | Yes | SQL column expressions applied to the source data. Each element is a valid Spark SQL expression, e.g. `"id as customer_id"`, `"upper(email) as email"` |
| `where_clause` | array of strings | No | Optional SQL filter expressions applied in order before writing to the silver table. Rows that do not match every expression are excluded from the silver output. |

---

## Usage Notes

:::tip
`select_exp` entries support any Spark SQL expression valid in a `SELECT` clause. This includes functions, casts, conditional expressions (`CASE WHEN`), and column aliases.
:::

:::note
If `where_clause` is used in combination with a silver fanout scenario, each transformation entry can have its own filter, effectively splitting the bronze data into multiple filtered silver tables.
:::

---

## Example Files in the Repository

- JSON: [`demo/conf/json/silver_transformations.json`](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/json/silver_transformations.json)
- YAML: [`demo/conf/yml/silver_transformations.yml`](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/yml/silver_transformations.yml)
