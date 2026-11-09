# Databricks notebook source
from pyspark.sql import functions as F

dbutils.widgets.text("uc_catalog_name", "")
dbutils.widgets.text("sdp_meta_schema", "")
dbutils.widgets.text("bronze_schema", "")
dbutils.widgets.dropdown("phase", "1", ["1", "2"])

catalog = dbutils.widgets.get("uc_catalog_name")
sdp_meta_schema = dbutils.widgets.get("sdp_meta_schema")
bronze_schema = dbutils.widgets.get("bronze_schema")
phase = dbutils.widgets.get("phase")

table_fqn = f"{catalog}.{bronze_schema}.events_inferred"
spec_fqn = f"{catalog}.{sdp_meta_schema}.bronze_dataflowspec_cdc"

# COMMAND ----------

spec_rows = spark.sql(
    f"""
    SELECT schema, readerConfigOptions
    FROM {spec_fqn}
    WHERE dataFlowId = 'schema-evolution-events'
    """
).collect()
if len(spec_rows) != 1:
    raise AssertionError(
        f"Expected one schema-evolution DataflowSpec, found {len(spec_rows)}"
    )

spec = spec_rows[0]
if spec["schema"] is not None:
    raise AssertionError(
        "The demo must not persist an explicit source schema; "
        f"found {spec['schema']!r}"
    )

options = dict(spec["readerConfigOptions"] or {})
expected_options = {
    "cloudFiles.inferColumnTypes": "true",
    "cloudFiles.schemaHints": (
        "event_id BIGINT, event_ts TIMESTAMP, amount DECIMAL(10,2)"
    ),
    "cloudFiles.schemaEvolutionMode": "addNewColumns",
    "cloudFiles.rescuedDataColumn": "_rescued_data",
}
for key, expected in expected_options.items():
    actual = options.get(key)
    if actual != expected:
        raise AssertionError(f"{key}: expected {expected!r}, got {actual!r}")

# COMMAND ----------

df = spark.table(table_fqn)
types = {field.name: field.dataType.simpleString() for field in df.schema.fields}

expected_types = {
    "event_id": "bigint",
    "event_ts": "timestamp",
    "amount": "decimal(10,2)",
    "region": "string",
    "_rescued_data": "string",
}
for column, expected_type in expected_types.items():
    actual_type = types.get(column)
    if actual_type != expected_type:
        raise AssertionError(
            f"{column}: expected type {expected_type}, got {actual_type}"
        )

expected_count = 3 if phase == "1" else 5
actual_count = df.count()
if actual_count != expected_count:
    raise AssertionError(
        f"Phase {phase}: expected {expected_count} rows, found {actual_count}"
    )

if phase == "1":
    if "device_type" in types:
        raise AssertionError(
            "device_type must not exist before the phase-2 schema evolution"
        )
    print("Phase 1 passed: schema inferred without DDL and hints applied.")
else:
    if types.get("device_type") != "string":
        raise AssertionError(
            "Phase 2 did not evolve device_type as a STRING column; "
            f"schema={types}"
        )
    rescued_rows = df.where(F.col("_rescued_data").isNotNull()).collect()
    if len(rescued_rows) != 1:
        raise AssertionError(
            "Expected exactly one incompatible amount value in rescued data, "
            f"found {len(rescued_rows)}"
        )
    rescued_payload = rescued_rows[0]["_rescued_data"]
    if "not-a-decimal" not in rescued_payload:
        raise AssertionError(
            "Rescued payload does not contain the incompatible amount: "
            f"{rescued_payload}"
        )
    print(
        "Phase 2 passed: device_type evolved and the incompatible amount "
        "was preserved in _rescued_data."
    )

display(df.orderBy("event_id"))
