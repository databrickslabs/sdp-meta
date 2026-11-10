---
id: silver-fanout
title: Silver Fanout
sidebar_position: 5
---

# Silver Fanout

Silver fanout is a topology where a single bronze table feeds multiple silver tables, each with its own transformation logic, filter conditions, and CDC configuration.

## How it works

1. Run the first onboarding job to create the bronze table and the first silver table.
2. Run a second onboarding job in **append mode** (`"overwrite": "False"`) with a new onboarding file defining the additional silver table.
3. The pipeline reads all silver entries for the group and materializes each silver table in the same update.

## `silver_append_flows` configuration

For a second silver table within the same onboarding entry, use `silver_append_flows`:

```json
{
  "silver_append_flows": [
    {
      "name": "customers_active_silver_flow",
      "create_streaming_table": false,
      "source_format": "delta",
      "source_details": {
        "source_database": "my_catalog.retail_bronze",
        "source_table": "customers_bronze"
      },
      "reader_options": {},
      "once": false
    }
  ]
}
```

## Pipeline topology

![Silver fanout workflow](/img/silver_fanout_workflow.png)

![Silver fanout in Declarative Pipeline](/img/silver_fanout_dlt.png)

## Full example: customers_bronze → two silver tables

**First onboarding file** (run with `"overwrite": "True"`):

```json
[
  {
    "data_flow_id": "1",
    "data_flow_group": "customers_group",
    "source_format": "cloudFiles",
    "source_details": {
      "source_schema_path": "/Volumes/my_catalog/my_schema/my_volume/schema/customers.ddl",
      "source_path_dev": "s3://my-bucket/cdc/customers/"
    },
    "bronze_catalog_dev": "my_catalog",
    "bronze_database_dev": "retail_bronze",
    "bronze_table": "customers_bronze",
    "silver_catalog_dev": "my_catalog",
    "silver_database_dev": "retail_silver",
    "silver_table": "customers_silver",
    "silver_transformation_json_prod": "/Volumes/my_catalog/my_schema/my_volume/conf/silver_transformations.json"
  }
]
```

**Second onboarding file** (run with `"overwrite": "False"`):

```json
[
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
    "silver_table": "customers_active_silver",
    "silver_transformation_json_prod": "/Volumes/my_catalog/my_schema/my_volume/conf/silver_transformations_fanout.json"
  }
]
```

The `silver_transformations_fanout.json` file applies a `where_clause` to restrict to active customers:

```json
[
  {
    "target_table": "customers_active_silver",
    "select_exp": ["customer_id", "name", "email", "region"],
    "where_clause": ["is_active = true"]
  }
]
```

## Running the demo

```bash
python -m demo.launch_silver_fanout_demo \
  --uc_catalog_name=<your_catalog> \
  --profile=<profile-name>
```

## Related

- [Silver Transformations Schema](../reference/silver-transformations) — `where_clause` configuration
- [Row Filters](./row-filters) — alternative approach using pipeline-time row filtering
- Example onboarding files: [`demo/conf/json/onboarding.template`](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/json/onboarding.template)
