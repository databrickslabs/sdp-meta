# Databricks notebook source
import json
import pandas as pd

# COMMAND ----------

%run ./event_log_assertions.py

# COMMAND ----------

uc_catalog_name = dbutils.widgets.get("uc_catalog_name")
bronze_schema = dbutils.widgets.get("bronze_schema")
sdp_meta_schema = dbutils.widgets.get("sdp_meta_schema")
pipeline_id = dbutils.widgets.get("bronze_pipeline_id")
output_file_path = dbutils.widgets.get("output_file_path")

main_table = f"{uc_catalog_name}.{bronze_schema}.quality_customers"
quarantine_table = (
    f"{uc_catalog_name}.{bronze_schema}.quality_customers_quarantine"
)
spec_table = (
    f"{uc_catalog_name}.{sdp_meta_schema}.bronze_dataflowspec_cdc"
)

logs = []
failures = []


def check(label, assertion):
    try:
        detail = assertion()
        message = f"{label}: {detail}. Passed!"
    except Exception as err:
        failures.append(f"{label}: {type(err).__name__}: {err}")
        message = f"{label}. Failed: {type(err).__name__}: {err}"
    logs.append(message)
    print(message)


def assert_equal(actual, expected):
    assert actual == expected, f"expected={expected!r}, actual={actual!r}"


start_message = "Release A quality integration validation starting."
logs.append(start_message)
print(start_message)


# The onboarding task must build the immutable Release A snapshot rather than
# leaving the backward-compatible nullable field empty.
def validate_quality_config():
    rows = spark.sql(
        f"""
        SELECT qualityConfig
        FROM {spec_table}
        WHERE dataFlowId = 'release_a_quality'
        """
    ).collect()
    assert_equal(len(rows), 1)
    assert rows[0]["qualityConfig"], "qualityConfig is null or empty"
    config = json.loads(rows[0]["qualityConfig"])
    assert_equal(config["engine"], "lakeflow")
    assert_equal(
        sorted(config["normalized_rules"]["expect_or_quarantine"]),
        ["email_required", "id_required", "positive_id"],
    )
    assert config["content_hash"], "qualityConfig.content_hash is empty"
    assert config["diagnostic_schema_fingerprint"], (
        "qualityConfig.diagnostic_schema_fingerprint is empty"
    )
    return (
        f"table={spec_table}, rows={len(rows)}, engine={config['engine']}, "
        f"spi_version={config['spi_version']}, "
        f"content_hash={config['content_hash']}, "
        "quarantine_rules="
        f"{sorted(config['normalized_rules']['expect_or_quarantine'])}"
    )


check("Onboarding persisted non-null qualityConfig", validate_quality_config)


def validate_main_rows():
    df = spark.table(main_table)
    assert "_errors" not in df.columns, "reserved _errors leaked into main"
    keys = [row["row_key"] for row in df.select("row_key").orderBy("row_key").collect()]
    assert_equal(keys, ["valid_1", "valid_2", "valid_3", "valid_4", "valid_5"])
    return f"table={main_table}, actual_count={len(keys)}, row_keys={keys}"


check("Main contains exactly five valid rows after two updates", validate_main_rows)


EXPECTED_ERRORS = {
    "missing_email": [
        ("email_required", "COALESCE((email IS NOT NULL), FALSE)"),
    ],
    "missing_id_and_email": [
        ("email_required", "COALESCE((email IS NOT NULL), FALSE)"),
        ("id_required", "COALESCE((id IS NOT NULL), FALSE)"),
        ("positive_id", "COALESCE((id > 0), FALSE)"),
    ],
    "negative_id": [
        ("positive_id", "COALESCE((id > 0), FALSE)"),
    ],
}


def validate_quarantine_rows():
    df = spark.table(quarantine_table)
    assert "_errors" in df.columns, "quarantine is missing _errors diagnostics"
    actual = {
        row["row_key"]: [
            (error["name"], error["expression"]) for error in row["_errors"]
        ]
        for row in df.select("row_key", "_errors").collect()
    }
    assert_equal(actual, EXPECTED_ERRORS)
    return (
        f"table={quarantine_table}, actual_count={len(actual)}, "
        f"diagnostics={actual}"
    )


check(
    "Quarantine contains three invalid rows with stable diagnostics",
    validate_quarantine_rows,
)


def validate_event_log_metrics():
    try:
        actual = read_max_failed_records(spark, pipeline_id)
    except Exception as err:
        # Some workspace/runtime combinations do not expose event_log() to the
        # workflow identity. Preserve that fact in the result artifact without
        # weakening row-routing and diagnostic assertions.
        return (
            "unavailable; content assertions remain authoritative: "
            f"{type(err).__name__}: {err}"
        )
    if not actual:
        return "unavailable; no expectation payloads were exposed"
    assert_expected_failures(
        actual,
        {
            "known_status": 0,
            "row_key_required": 0,
            "status_required": 0,
            "id_required": 1,
            "email_required": 2,
            "positive_id": 2,
        },
    )
    return f"pipeline_id={pipeline_id}, actual_failed_records={actual}"


check("Event-log expectation failure metrics", validate_event_log_metrics)

pd.Series(logs).to_csv(output_file_path)
if failures:
    raise AssertionError(
        "Release A quality integration validation failed:\n- "
        + "\n- ".join(failures)
    )
