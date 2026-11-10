---
id: multi-source-cdc
title: Multi-Source CDC
sidebar_position: 7
---

# Multi-Source CDC

Multi-source CDC merges CDC events from multiple independent source paths into a single target table — for example, regional data feeds (US, EU, APAC) landing in separate S3 prefixes.

Two patterns are supported:

| Pattern | When to use |
|---|---|
| **Append + CDC** (`bronze_append_flows` + `bronze_cdc_apply_changes` / `silver_cdc_apply_changes`) | Sources are heterogeneous; land all raw events in bronze first, then run a single CDC merge |
| **Native multi-source CDC** (`bronze_cdc_apply_changes_flows` / `silver_cdc_apply_changes_flows`) | Sources share the same CDC schema; merge directly without a separate bronze landing step |

## Pattern 1 — Append + CDC

### How it works

1. Define a primary source in `source_details`.
2. Use `bronze_append_flows` to define additional sources that write to the same bronze streaming table via the Declarative Pipeline `append_flow` API.
3. Configure `bronze_cdc_apply_changes` (or `silver_cdc_apply_changes`) — the single `create_auto_cdc_flow` call merges events from all contributing sources.

### Configuration

```json
[
  {
    "data_flow_id": "1",
    "data_flow_group": "customers_group",
    "source_format": "cloudFiles",
    "source_details": {
      "source_schema_path": "/Volumes/my_catalog/my_schema/my_volume/schema/customers_cdc.ddl",
      "source_path_dev": "s3://my-bucket/cdc/customers/us/"
    },
    "bronze_catalog_dev": "my_catalog",
    "bronze_database_dev": "retail_bronze",
    "bronze_table": "customers_cdc",
    "bronze_reader_options": {
      "cloudFiles.format": "json",
      "cloudFiles.inferColumnTypes": "true"
    },
    "bronze_append_flows": [
      {
        "name": "customers_eu_flow",
        "create_streaming_table": false,
        "source_format": "cloudFiles",
        "source_details": {
          "source_path_dev": "s3://my-bucket/cdc/customers/eu/",
          "source_schema_path": "/Volumes/my_catalog/my_schema/my_volume/schema/customers_cdc.ddl"
        },
        "reader_options": {
          "cloudFiles.format": "json",
          "cloudFiles.inferColumnTypes": "true"
        },
        "once": false
      },
      {
        "name": "customers_apac_flow",
        "create_streaming_table": false,
        "source_format": "cloudFiles",
        "source_details": {
          "source_path_dev": "s3://my-bucket/cdc/customers/apac/",
          "source_schema_path": "/Volumes/my_catalog/my_schema/my_volume/schema/customers_cdc.ddl"
        },
        "reader_options": {
          "cloudFiles.format": "json",
          "cloudFiles.inferColumnTypes": "true"
        },
        "once": false
      }
    ],
    "bronze_cdc_apply_changes": {
      "keys": ["customer_id"],
      "sequence_by": "dmsTimestamp",
      "scd_type": "1",
      "apply_as_deletes": "Op = 'D'",
      "except_column_list": ["Op", "dmsTimestamp"]
    },
    "silver_catalog_dev": "my_catalog",
    "silver_database_dev": "retail_silver",
    "silver_table": "customers",
    "silver_transformation_json_prod": "/Volumes/my_catalog/my_schema/my_volume/conf/silver_transformations.json",
    "silver_cdc_apply_changes": {
      "keys": ["customer_id"],
      "sequence_by": "dmsTimestamp",
      "scd_type": "2",
      "apply_as_deletes": "Op = 'D'",
      "except_column_list": ["Op", "dmsTimestamp"]
    }
  }
]
```

### `bronze_append_flows` entry fields

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | Yes | Unique name for this append flow within the pipeline |
| `create_streaming_table` | boolean | Yes | Set to `false` when writing to an existing streaming table |
| `source_format` | string | Yes | Source format: `cloudFiles`, `delta`, `kafka`, etc. |
| `source_details` | object | Yes | Source connection details — same structure as top-level `source_details` |
| `reader_options` | object | No | Reader options for this flow |
| `once` | boolean | No | `true` for batch execution; `false` for continuous streaming |

:::note
All append flows targeting the same bronze table must produce a schema compatible with the streaming table. The primary source establishes the schema; append flows must match it.
:::

## Pattern 2 — Native multi-source CDC

Use `bronze_cdc_apply_changes_flows` (or `silver_cdc_apply_changes_flows` at the silver layer) when all sources share the same CDC schema. A single `create_auto_cdc_flow` is issued per flow group — no separate bronze landing table required.

### Configuration

```json
[
  {
    "data_flow_id": "2",
    "data_flow_group": "orders_group",
    "bronze_catalog_dev": "my_catalog",
    "bronze_database_dev": "retail_bronze",
    "bronze_table": "orders_cdc",
    "bronze_cdc_apply_changes_flows": {
      "keys": ["order_id"],
      "sequence_by": "updated_at",
      "scd_type": "1",
      "apply_as_deletes": "Op = 'D'",
      "except_column_list": ["Op"],
      "flows": [
        {
          "name": "orders_us_flow",
          "source_details": {
            "source_format": "cloudFiles",
            "source_path_dev": "s3://my-bucket/cdc/orders/us/",
            "source_schema_path": "/Volumes/my_catalog/my_schema/vol/schema/orders.ddl"
          }
        },
        {
          "name": "orders_eu_flow",
          "source_details": {
            "source_format": "cloudFiles",
            "source_path_dev": "s3://my-bucket/cdc/orders/eu/",
            "source_schema_path": "/Volumes/my_catalog/my_schema/vol/schema/orders.ddl"
          }
        }
      ]
    },
    "silver_catalog_dev": "my_catalog",
    "silver_database_dev": "retail_silver",
    "silver_table": "orders",
    "silver_cdc_apply_changes_flows": {
      "keys": ["order_id"],
      "sequence_by": "updated_at",
      "scd_type": "2",
      "apply_as_deletes": "Op = 'D'",
      "except_column_list": ["Op"],
      "flows": [
        {
          "name": "orders_silver_us_flow",
          "source_details": {
            "source_format": "delta",
            "source_path_dev": "my_catalog.retail_bronze.orders_us"
          }
        },
        {
          "name": "orders_silver_eu_flow",
          "source_details": {
            "source_format": "delta",
            "source_path_dev": "my_catalog.retail_bronze.orders_eu"
          }
        }
      ]
    }
  }
]
```

:::note
`bronze_cdc_apply_changes_flows` and `bronze_cdc_apply_changes` are mutually exclusive on the same onboarding row. Similarly for `silver_cdc_apply_changes_flows` and `silver_cdc_apply_changes`.
:::

:::caution
Multi-source Auto CDC flows do not support `expect_or_quarantine` rules.
Use `expect`, `expect_or_drop`, or `expect_or_fail` instead. SDP-META rejects
an onboarding configuration that combines `expect_or_quarantine` with
`bronze_cdc_apply_changes_flows` or `silver_cdc_apply_changes_flows`, because
quarantine routing is not implemented for this flow type.
:::

## Pipeline diagram

![Multi-source CDC silver pipeline](/img/multi-source-cdc-silver.png)

![Multi-source CDC demo result](/img/multi-source-cdc-silver-demo.png)

## Demo

```bash
python -m demo.launch_multi_source_cdc_demo \
  --uc_catalog_name=<your_catalog> \
  --profile=<profile-name>
```

## Related

- [`append_flow` API reference](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-append-flow)
- [`create_auto_cdc_flow` API reference](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-apply-changes)
- [CDC guide](./cdc)
- [Onboarding fields — `bronze_append_flows`](../reference/onboarding-fields#bronze-layer-fields)
- [Onboarding fields — `silver_cdc_apply_changes_flows`](../reference/onboarding-fields#silver-layer-fields)
