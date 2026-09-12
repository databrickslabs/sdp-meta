# Databricks notebook source
import json
import pandas as pd

uc_catalog_name = dbutils.widgets.get("uc_catalog_name")
bronze_schema = dbutils.widgets.get("bronze_schema")
sdp_meta_schema = dbutils.widgets.get("sdp_meta_schema")
output_file_path = dbutils.widgets.get("output_file_path")

spec_table = (
    f"{uc_catalog_name}.{sdp_meta_schema}.bronze_dataflowspec_cdc"
)

logs = ["Release B DQX quality integration validation starting."]
failures = []
print(logs[0])


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


FLOW_RULE_COUNTS = {
    "release_b_dqx_quality": 4,
    "release_b_dqx_transactions": 3,
    "release_b_dqx_stores": 3,
    "release_b_dqx_products": 3,
}


def validate_quality_configs():
    rows = spark.sql(
        f"""
        SELECT dataFlowId, qualityConfig
        FROM {spec_table}
        WHERE dataFlowId IN ({", ".join(repr(key) for key in FLOW_RULE_COUNTS)})
        """
    ).collect()
    assert_equal(len(rows), len(FLOW_RULE_COUNTS))
    hashes = set()
    summaries = []
    for row in rows:
        config = json.loads(row["qualityConfig"])
        assert_equal(config["engine"], "dqx")
        assert_equal(
            config["plugin_package"], "databricks-labs-sdp-meta"
        )
        assert config["plugin_version"]
        assert_equal(config["engine_version"], "0.16.0")
        assert_equal(
            len(config["normalized_rules"]),
            FLOW_RULE_COUNTS[row["dataFlowId"]],
        )
        hashes.add(config["content_hash"])
        summaries.append(
            f"{row['dataFlowId']}={len(config['normalized_rules'])} rules"
        )
    assert_equal(len(hashes), len(FLOW_RULE_COUNTS))
    return ", ".join(sorted(summaries))


check(
    "Onboarding persisted four DQX quality snapshots",
    validate_quality_configs,
)


def rows_and_diagnostics(table):
    return {
        row["row_key"]: {
            "errors": sorted(
                item["name"] for item in (row["_errors"] or [])
            ),
            "warnings": sorted(
                item["name"] for item in (row["_warnings"] or [])
            ),
        }
        for row in spark.table(table).select(
            "row_key", "_errors", "_warnings"
        ).collect()
    }


def validate_feed(feed, expected_main, expected_diagnostics):
    main_table = f"{uc_catalog_name}.{bronze_schema}.quality_{feed}"
    quarantine_table = (
        f"{uc_catalog_name}.{bronze_schema}.quality_{feed}_quarantine"
    )
    main = spark.table(main_table)
    assert not {"_errors", "_warnings", "_dq_info"} & set(main.columns)
    main_keys = {
        row["row_key"] for row in main.select("row_key").collect()
    }
    assert_equal(main_keys, expected_main)

    diagnostics = rows_and_diagnostics(quarantine_table)
    assert_equal(diagnostics, expected_diagnostics)
    warning_overlap = main_keys & set(diagnostics)
    expected_overlap = {
        key
        for key, result in expected_diagnostics.items()
        if result["warnings"] and not result["errors"]
    }
    assert_equal(warning_overlap, expected_overlap)
    return (
        f"main_count={len(main_keys)}, quarantine_count={len(diagnostics)}, "
        f"warning_overlap={sorted(warning_overlap)}, "
        f"diagnostics={diagnostics}"
    )


FEEDS = {
    "customers": {
        "main": {"valid_1", "valid_2", "valid_3", "valid_4", "valid_5"},
        "diagnostics": {
            "valid_1": {
                "errors": [],
                "warnings": ["active_status_warning"],
            },
            "valid_3": {
                "errors": [],
                "warnings": ["active_status_warning"],
            },
            "valid_5": {
                "errors": [],
                "warnings": ["active_status_warning"],
            },
            "missing_email": {
                "errors": ["email_required"],
                "warnings": ["active_status_warning"],
            },
            "missing_id_and_email": {
                "errors": ["email_required", "id_required"],
                "warnings": [],
            },
            "negative_id": {
                "errors": ["positive_id"],
                "warnings": ["active_status_warning"],
            },
        },
    },
    "transactions": {
        "main": {"valid_1", "valid_2", "valid_3", "status_warning"},
        "diagnostics": {
            "status_warning": {
                "errors": [],
                "warnings": ["transaction_status_warning"],
            },
            "missing_transaction_id": {
                "errors": ["transaction_id_required"],
                "warnings": [],
            },
            "negative_amount": {
                "errors": ["non_negative_amount"],
                "warnings": [],
            },
        },
    },
    "stores": {
        "main": {"valid_1", "valid_2", "valid_3", "country_warning"},
        "diagnostics": {
            "country_warning": {
                "errors": [],
                "warnings": ["country_warning"],
            },
            "missing_store_id": {
                "errors": ["store_id_required"],
                "warnings": [],
            },
        },
    },
    "products": {
        "main": {"valid_1", "valid_2", "valid_3", "category_warning"},
        "diagnostics": {
            "category_warning": {
                "errors": [],
                "warnings": ["category_warning"],
            },
            "missing_product_id": {
                "errors": ["product_id_required"],
                "warnings": [],
            },
            "negative_price": {
                "errors": ["non_negative_price"],
                "warnings": [],
            },
        },
    },
}

for feed, expected in FEEDS.items():
    check(
        f"DQX routing for {feed}",
        lambda feed=feed, expected=expected: validate_feed(
            feed,
            expected["main"],
            expected["diagnostics"],
        ),
    )

pd.Series(logs).to_csv(output_file_path)
if failures:
    raise AssertionError(
        "Release B DQX quality integration validation failed:\n- "
        + "\n- ".join(failures)
    )
