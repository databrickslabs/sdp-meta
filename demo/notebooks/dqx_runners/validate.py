# Databricks notebook source
# MAGIC %md
# MAGIC # SDP-META + DQX validation
# MAGIC
# MAGIC Error rows are excluded from the valid output. DQX warning-only rows
# MAGIC remain valid and also appear in quarantine, so valid and quarantine
# MAGIC counts are intentionally not complementary.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("uc_catalog_name", "")
dbutils.widgets.text("bronze_schema", "")

uc_catalog_name = dbutils.widgets.get("uc_catalog_name")
bronze_schema = dbutils.widgets.get("bronze_schema")
valid_table = f"{uc_catalog_name}.{bronze_schema}.customers_valid"
quarantine_table = (
    f"{uc_catalog_name}.{bronze_schema}.customers_quarantine"
)

# COMMAND ----------

valid_count = spark.table(valid_table).count()
quarantine_df = spark.table(quarantine_table)
quarantine_count = quarantine_df.count()

error_count = quarantine_df.where(
    F.coalesce(F.size("_errors"), F.lit(0)) > 0
).count()
warning_only_count = quarantine_df.where(
    (F.coalesce(F.size("_warnings"), F.lit(0)) > 0)
    & (F.coalesce(F.size("_errors"), F.lit(0)) == 0)
).count()

actual = {
    "valid": valid_count,
    "quarantine": quarantine_count,
    "error": error_count,
    "warning_only": warning_only_count,
}
expected = {
    "valid": 6,
    "quarantine": 3,
    "error": 2,
    "warning_only": 1,
}

print(f"DQX demo counts: {actual}")
failures = [
    f"{name}: expected={expected[name]}, actual={actual[name]}"
    for name in expected
    if actual[name] != expected[name]
]
if failures:
    raise AssertionError(
        "SDP-META DQX demo validation failed:\n  - "
        + "\n  - ".join(failures)
    )

print("SDP-META DQX demo validation passed.")
