# Databricks notebook source
from pyspark.sql import functions as F

dbutils.widgets.text("uc_catalog_name", "")
dbutils.widgets.text("sdp_meta_schema", "")
dbutils.widgets.text("bronze_schema", "")
dbutils.widgets.text("silver_schema", "")
dbutils.widgets.text("gold_schema", "")
dbutils.widgets.text("table_count", "100")
dbutils.widgets.dropdown("phase", "1", ["1", "2"])

catalog = dbutils.widgets.get("uc_catalog_name")
meta_schema = dbutils.widgets.get("sdp_meta_schema")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema = dbutils.widgets.get("gold_schema")
table_count = int(dbutils.widgets.get("table_count"))
phase = dbutils.widgets.get("phase")

explicit_count = max(1, table_count // 10)
hinted_count = max(1, table_count // 10)
inferred_count = table_count - explicit_count - hinted_count
hinted_table = f"hinted_{inferred_count + 1:03d}"
explicit_table = f"explicit_{inferred_count + hinted_count + 1:03d}"


def table_identity(index):
    if index == 1:
        return "orders"
    if index <= inferred_count:
        return f"inferred_{index:03d}"
    if index <= inferred_count + hinted_count:
        return f"hinted_{index:03d}"
    return f"explicit_{index:03d}"


expected_tables = {table_identity(index) for index in range(1, table_count + 1)}


def schema_table_names(schema):
    return {
        row["tableName"]
        for row in spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
    }


bronze_tables = schema_table_names(bronze_schema)
silver_tables = schema_table_names(silver_schema)
missing_bronze = expected_tables - bronze_tables
missing_silver = expected_tables - silver_tables
if missing_bronze:
    raise AssertionError(f"Missing Bronze tables: {sorted(missing_bronze)}")
if missing_silver:
    raise AssertionError(f"Missing Silver tables: {sorted(missing_silver)}")

# COMMAND ----------

bronze_specs = spark.table(
    f"{catalog}.{meta_schema}.bronze_dataflowspec_cdc"
).where(F.col("dataFlowGroup") == "AT_SCALE_AUTOLOADER")
silver_specs = spark.table(
    f"{catalog}.{meta_schema}.silver_dataflowspec_cdc"
).where(F.col("dataFlowGroup") == "AT_SCALE_AUTOLOADER")

if bronze_specs.count() != table_count:
    raise AssertionError("Bronze DataflowSpec count does not match table_count")
if silver_specs.count() != table_count:
    raise AssertionError("Silver DataflowSpec count does not match table_count")

schema_counts = {
    row["has_schema"]: row["count"]
    for row in (
        bronze_specs.select(
            F.col("schema").isNotNull().alias("has_schema")
        )
        .groupBy("has_schema")
        .count()
        .collect()
    )
}
if schema_counts.get(True, 0) != explicit_count:
    raise AssertionError(
        f"Expected {explicit_count} explicit schemas, got {schema_counts}"
    )
if schema_counts.get(False, 0) != table_count - explicit_count:
    raise AssertionError(
        "Inference/hint specs unexpectedly persisted an explicit schema"
    )

# COMMAND ----------

hinted = spark.table(f"{catalog}.{bronze_schema}.{hinted_table}")
hinted_types = {
    field.name: field.dataType.simpleString()
    for field in hinted.schema.fields
}
for column, expected_type in {
    "event_id": "bigint",
    "customer_id": "bigint",
    "event_ts": "timestamp",
    "amount": "decimal(12,2)",
}.items():
    if hinted_types.get(column) != expected_type:
        raise AssertionError(
            f"{hinted_table}.{column}: expected {expected_type}, "
            f"got {hinted_types.get(column)}"
        )

explicit = spark.table(f"{catalog}.{bronze_schema}.{explicit_table}")
explicit_types = {
    field.name: field.dataType.simpleString()
    for field in explicit.schema.fields
}
for column, expected_type in {
    "event_id": "bigint",
    "event_ts": "timestamp",
    "amount": "decimal(12,2)",
}.items():
    if explicit_types.get(column) != expected_type:
        raise AssertionError(
            f"{explicit_table}.{column}: expected {expected_type}, "
            f"got {explicit_types.get(column)}"
        )

if phase == "1" and "device_type" in hinted_types:
    raise AssertionError("device_type must not exist during phase 1")

# COMMAND ----------

quarantine_fqn = f"{catalog}.{bronze_schema}.orders_quarantine"
quarantine = spark.table(quarantine_fqn)
if quarantine.count() != 1:
    raise AssertionError(
        f"Expected one quarantined order, found {quarantine.count()}"
    )
if quarantine.where(F.col("event_id").isNotNull()).count() != 0:
    raise AssertionError("The quarantine row should have a null event_id")

silver_orders = spark.table(f"{catalog}.{silver_schema}.orders")
required_custom_columns = {"customer_name", "customer_tier", "loaded_at"}
missing_custom_columns = required_custom_columns - set(silver_orders.columns)
if missing_custom_columns:
    raise AssertionError(
        f"Silver callback columns missing: {sorted(missing_custom_columns)}"
    )
tiers = {
    row["customer_id"]: row["customer_tier"]
    for row in (
        silver_orders.select("customer_id", "customer_tier")
        .distinct()
        .collect()
    )
}
if tiers != {1: "gold", 2: "platinum", 3: "bronze"}:
    raise AssertionError(f"Windowed dimension join returned {tiers}")

# COMMAND ----------

if phase == "2":
    evolved = spark.table(f"{catalog}.{bronze_schema}.{hinted_table}")
    evolved_types = {
        field.name: field.dataType.simpleString()
        for field in evolved.schema.fields
    }
    if evolved_types.get("device_type") != "string":
        raise AssertionError(
            f"device_type was not added as STRING: {evolved_types}"
        )
    if evolved.count() != 6:
        raise AssertionError(
            f"Expected 6 evolved rows, found {evolved.count()}"
        )
    rescued = evolved.where(F.col("_rescued_data").isNotNull()).collect()
    if len(rescued) != 1 or "not-a-decimal" not in rescued[0][
        "_rescued_data"
    ]:
        raise AssertionError(
            f"Expected one rescued incompatible amount, found {rescued}"
        )

    gold = spark.table(
        f"{catalog}.{gold_schema}.orders_by_tier_region"
    )
    gold_order_count = gold.agg(F.sum("order_count")).first()[0]
    if gold_order_count != 4:
        raise AssertionError(
            f"Gold should summarize 4 orders, got {gold_order_count}"
        )

print(
    f"At-scale phase {phase} validated: {table_count} Bronze tables, "
    f"{table_count} Silver tables, schema cohorts, callback, and quarantine."
)
