---
id: row-filters
title: Row Filters
sidebar_position: 8
---

# Row Filters

Row filters restrict which rows a pipeline materializes at runtime via the `where_clause` field in the silver transformations file.

## Configuration

```json
[
  {
    "target_table": "customers_silver",
    "source_format": "delta",
    "select_exp": [
      "customer_id",
      "name",
      "email",
      "region",
      "status"
    ],
    "where_clause": "status = 'active'"
  }
]
```

The `where_clause` value is any valid Spark SQL boolean expression.

## Example: region-filtered silver tables

Produce one silver table per region from a single bronze table:

**`silver_transformations_us.json`:**

```json
[
  {
    "target_table": "customers_us_silver",
    "source_format": "delta",
    "select_exp": ["customer_id", "name", "email"],
    "where_clause": "region = 'US'"
  }
]
```

**`silver_transformations_eu.json`:**

```json
[
  {
    "target_table": "customers_eu_silver",
    "source_format": "delta",
    "select_exp": ["customer_id", "name", "email"],
    "where_clause": "region = 'EU'"
  }
]
```

Each onboarding entry references its own transformations file:

```json
[
  {
    "data_flow_id": "1",
    "data_flow_group": "customers_group",
    "source_format": "cloudFiles",
    "source_details": {
      "source_path_dev": "s3://my-bucket/customers/"
    },
    "bronze_catalog_dev": "my_catalog",
    "bronze_database_dev": "retail_bronze",
    "bronze_table": "customers_bronze",
    "silver_catalog_dev": "my_catalog",
    "silver_database_dev": "retail_silver",
    "silver_table": "customers_us_silver",
    "silver_transformation_json_prod": "/Volumes/my_catalog/my_schema/my_volume/conf/silver_transformations_us.json"
  },
  {
    "data_flow_id": "2",
    "data_flow_group": "customers_group",
    "source_format": "delta",
    "source_details": {
      "source_database": "my_catalog.retail_bronze",
      "source_table": "customers_bronze"
    },
    "silver_catalog_dev": "my_catalog",
    "silver_database_dev": "retail_silver",
    "silver_table": "customers_eu_silver",
    "silver_transformation_json_prod": "/Volumes/my_catalog/my_schema/my_volume/conf/silver_transformations_eu.json"
  }
]
```

:::tip
This pattern combines naturally with [Silver Fanout](./silver-fanout). Run additional silver entries in append mode (`"overwrite": "false"`).
:::

## Row filters vs. data quality rules

| Feature | Row Filters (`where_clause`) | DQ Rules (`expect_or_drop`) |
|---|---|---|
| Configured in | Silver transformations file | DQE rules file |
| Effect | Rows not matching are silently excluded | Action is explicit per rule: retain, drop, quarantine, or fail the update |
| Metrics tracked | No | Yes |
| Applies to layer | Silver | Bronze or Silver |

Use `where_clause` for intentional business logic exclusion. Use `expect_or_drop` for data quality violations that need visibility.

## Related

- [Silver Transformations Schema](../reference/silver-transformations)
- [Data Quality Rules Schema](../reference/dq-rules)
- [Silver Fanout](./silver-fanout)
