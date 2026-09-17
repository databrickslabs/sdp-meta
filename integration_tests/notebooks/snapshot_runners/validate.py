# Databricks notebook source
import pandas as pd

run_id = dbutils.widgets.get("run_id")
uc_enabled = dbutils.widgets.get("uc_enabled").strip().lower() == "true"
uc_catalog_name = dbutils.widgets.get("uc_catalog_name")
output_file_path = dbutils.widgets.get("output_file_path")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
log_list = []

# Assumption is that to get to this notebook Bronze and Silver completed successfully
log_list.append("Completed mixed Snapshot and CloudFiles Bronze pipeline.")
log_list.append("Completed Snapshot Silver pipeline.")

UC_TABLES = {
    f"{uc_catalog_name}.{bronze_schema}.products": 20,
    f"{uc_catalog_name}.{bronze_schema}.stores": 2,
    f"{uc_catalog_name}.{bronze_schema}.mixed_cloudfiles": 2,
    f"{uc_catalog_name}.{silver_schema}.products": 20,
    f"{uc_catalog_name}.{silver_schema}.stores": 2
}

log_list.append("Validating Snapshot and CloudFiles table counts...")
for table, counts in UC_TABLES.items():
    query = spark.sql(f"SELECT count(*) as cnt FROM {table}")
    cnt = query.collect()[0].cnt

    log_list.append(f"Validating Counts for Table {table}.")
    try:
        assert int(cnt) >= counts
        log_list.append(f"Expected >= {counts} Actual: {cnt}. Passed!")
    except AssertionError:
        log_list.append(f"Expected > {counts} Actual: {cnt}. Failed!")

pd_df = pd.DataFrame(log_list)
pd_df.to_csv(output_file_path)
