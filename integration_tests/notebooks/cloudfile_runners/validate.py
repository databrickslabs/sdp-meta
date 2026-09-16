# Databricks notebook source
import pandas as pd

run_id = dbutils.widgets.get("run_id")
uc_enabled = dbutils.widgets.get("uc_enabled").strip().lower() == "true"
uc_catalog_name = dbutils.widgets.get("uc_catalog_name")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
output_file_path = dbutils.widgets.get("output_file_path")
log_list = []

# Assumption is that to get to this notebook Bronze and Silver completed successfully
log_list.append("Completed Bronze Lakeflow Spark Declarative Pipeline.")
log_list.append("Completed Silver Lakeflow Spark Declarative Pipeline.")

UC_TABLES = {
    f"{uc_catalog_name}.{bronze_schema}.transactions": 10002,
    f"{uc_catalog_name}.{bronze_schema}.transactions_quarantine": 6,
    f"{uc_catalog_name}.{bronze_schema}.customers": 51453,
    f"{uc_catalog_name}.{bronze_schema}.customers_quarantine": 256,
    f"{uc_catalog_name}.{silver_schema}.transactions": 8759,
    f"{uc_catalog_name}.{silver_schema}.customers": 73212,
}

NON_UC_TABLES = {
    f"{bronze_schema}.transactions": 10002,
    f"{bronze_schema}.transactions_quarantine": 6,
    f"{bronze_schema}.customers": 51453,
    f"{bronze_schema}.customers_quarantine": 256,
    f"{silver_schema}.transactions": 8759,
    f"{silver_schema}.customers": 73212,
}

log_list.append("Validating Lakeflow Spark Declarative Pipeline Bronze and Silver Table Counts...")
tables = UC_TABLES if uc_enabled else NON_UC_TABLES
for table, counts in tables.items():
    query = spark.sql(f"SELECT count(*) as cnt FROM {table}")
    cnt = query.collect()[0].cnt

    log_list.append(f"Validating Counts for Table {table}.")
    try:
        assert int(cnt) == counts
        log_list.append(f"Expected: {counts} Actual: {cnt}. Passed!")
    except AssertionError:
        log_list.append(f"Expected: {counts} Actual: {cnt}. Failed!")

# Regression coverage for issue #444: source_metadata nested under a bronze
# append flow must be serialized like top-level source metadata. Every
# transaction row comes from either `transactions/` or `transactions_af/`, so
# both selected metadata columns must be populated for the full table, and at
# least one path must prove that append-flow rows were included.
transactions_table = (
    f"{uc_catalog_name}.{bronze_schema}.transactions"
    if uc_enabled
    else f"{bronze_schema}.transactions"
)
metadata_stats = spark.sql(
    f"""
    SELECT
      COUNT(*) AS total_rows,
      COUNT(input_file_name) AS named_rows,
      COUNT(input_file_path) AS pathed_rows,
      SUM(
        CASE WHEN input_file_path LIKE '%/transactions_af/%'
          THEN 1 ELSE 0 END
      ) AS append_rows
    FROM {transactions_table}
    """
).collect()[0]
log_list.append(
    "Validating source metadata for bronze transaction append-flow rows."
)
try:
    assert metadata_stats.total_rows == metadata_stats.named_rows
    assert metadata_stats.total_rows == metadata_stats.pathed_rows
    assert metadata_stats.append_rows > 0
    log_list.append(
        f"All {metadata_stats.total_rows} rows have file metadata; "
        f"{metadata_stats.append_rows} rows came from transactions_af. Passed!"
    )
except AssertionError:
    log_list.append(
        "Append-flow metadata validation failed: "
        f"total={metadata_stats.total_rows}, "
        f"named={metadata_stats.named_rows}, "
        f"pathed={metadata_stats.pathed_rows}, "
        f"append={metadata_stats.append_rows}. Failed!"
    )

# Row filter wiring assertion (UC only). The cloudfiles customers flow declares
# bronze_row_filter / silver_row_filter on `operation` referencing the UDF
# `<catalog>.<bronze_schema>.customer_op_filter`. Confirm via
# information_schema that the filter is actually attached to both tables --
# this is a wiring check that's independent of who the validator runs as.
#
# Schema of <catalog>.information_schema.row_filters (Databricks UC):
#   table_catalog, table_schema, table_name, filter_name, target_columns
# Where `filter_name` is the fully-qualified UDF reference and
# `target_columns` is the ARRAY<STRING> of columns the filter is applied on.
if uc_enabled:
    log_list.append("Validating Row Filter Wiring on customers tables...")
    expected_filter_name = f"{uc_catalog_name}.{bronze_schema}.customer_op_filter"
    row_filter_targets = [
        (bronze_schema, "customers"),
        (silver_schema, "customers"),
    ]
    for schema_name, table_name in row_filter_targets:
        rf_df = spark.sql(
            f"""
            SELECT filter_name, target_columns
            FROM {uc_catalog_name}.information_schema.row_filters
            WHERE table_catalog = '{uc_catalog_name}'
              AND table_schema  = '{schema_name}'
              AND table_name    = '{table_name}'
            """
        )
        rows = rf_df.collect()
        try:
            assert len(rows) >= 1, "no row filter attached"
            attached_name = rows[0].filter_name
            assert attached_name.lower() == expected_filter_name.lower(), (
                f"unexpected filter name `{attached_name}`"
            )
            log_list.append(
                f"Row filter on {uc_catalog_name}.{schema_name}.{table_name} "
                f"-> {attached_name} on {rows[0].target_columns}. Passed!"
            )
        except AssertionError as exc:
            log_list.append(
                f"Row filter on {uc_catalog_name}.{schema_name}.{table_name}: "
                f"{exc}. Failed!"
            )

pd_df = pd.DataFrame(log_list)
pd_df.to_csv(output_file_path)
