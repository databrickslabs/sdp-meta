---
id: pipeline-chaining
title: Pipeline Chaining
sidebar_position: 3
---

# Pipeline Chaining

Pipeline chaining connects a Bronze pipeline (which writes Delta tables) to a Silver pipeline (which reads those tables as its source). SDP-META supports this through the `layer=bronze_silver` configuration and two deployment modes.

## Split vs combined

When you set `layer=bronze_silver`, the `pipeline_mode` setting controls deployment:

### Split (default)

Two separate Lakeflow Spark Declarative Pipelines: one for Bronze, one for Silver.

- Each pipeline has independent update history, rollback, and cluster configuration.
- Silver waits for Bronze to finish (enforced by the workflow job in DAB deployments).
- Recommended when layers have different SLAs or need independent lifecycle management.

### Combined

One pipeline with `configuration.layer = bronze_silver` that materializes both layers in a single DAG.

- Lower overhead — one pipeline to schedule and monitor.
- Lakeflow Spark Declarative Pipelines manages intra-pipeline ordering automatically.
- Best for development environments or tightly coupled Bronze/Silver workloads.

## Configuring data_flow_group for chaining

Both Bronze and Silver flows must share the same `data_flow_group`. In the onboarding file:

```yaml
# Bronze flow — group G1, writes cloudFiles source to orders_raw
- data_flow_id: 1
  data_flow_group: G1
  source_format: cloudFiles
  source_details:
    source_schema_path: /Volumes/my_catalog/my_schema/my_volume/schema/orders.ddl
    source_path_prod: s3://my-bucket/orders/
  bronze_catalog_prod: my_catalog
  bronze_database_prod: retail_bronze
  bronze_table: orders_raw

# Silver flow — also group G1, reads from the bronze orders_raw table
- data_flow_id: 101
  data_flow_group: G1
  source_format: delta
  source_details:
    source_database: my_catalog.retail_bronze
    source_table: orders_raw
  silver_catalog_prod: my_catalog
  silver_database_prod: retail_silver
  silver_table: orders_clean
  silver_transformation_json_prod: /Volumes/my_catalog/my_schema/my_volume/conf/silver_transformations.json
```

The pipeline is configured with `bronze.group = G1` and `silver.group = G1`.

:::warning
If the `data_flow_group` in Silver onboarding entries does not match the group name on the Silver pipeline, those flows will not be processed. `bundle-validate` checks for this mismatch.
:::

## Silver fan-out

One Bronze table can feed multiple Silver tables. Define multiple Silver onboarding entries with the same `source_table` but different `silver_table` values, each pointing to its own `silver_transformation_json_{env}` file with the appropriate `where_clause` or `select_exp`:

```yaml
- data_flow_id: 101
  data_flow_group: G1
  source_format: delta
  source_details:
    source_database: my_catalog.retail_bronze
    source_table: orders_raw
  silver_catalog_prod: my_catalog
  silver_database_prod: retail_silver
  silver_table: orders_by_region
  silver_transformation_json_prod: /Volumes/my_catalog/my_schema/my_volume/conf/silver_by_region.json

- data_flow_id: 102
  data_flow_group: G1
  source_format: delta
  source_details:
    source_database: my_catalog.retail_bronze
    source_table: orders_raw
  silver_catalog_prod: my_catalog
  silver_database_prod: retail_silver
  silver_table: orders_by_customer
  silver_transformation_json_prod: /Volumes/my_catalog/my_schema/my_volume/conf/silver_by_customer.json
```

Each transformation file specifies the `select_exp` and optional `where_clause` for that silver table. See the [Silver Fanout guide](../guides/silver-fanout) and [Silver Transformations reference](../reference/silver-transformations) for the file format.

## Switching between modes

`pipeline_mode` selects the initial topology when the bundle is scaffolded; changing
the variable later does not regenerate resource YAML. To change an existing topology,
edit `resources/sdp_meta_pipelines.yml`, run `bundle-validate`, and redeploy. To keep
the existing topology and add another split or combined group, use
`bundle-add-pipeline`.
