"""Static contracts for the Release A serverless quality integration scenario."""

import json
from pathlib import Path

import yaml

from integration_tests.notebooks.quality_runners.event_log_assertions import (
    _expectations_from_details,
    assert_expected_failures,
)
from integration_tests.run_integration_tests import SDPMetaRunnerConf


ROOT = Path(__file__).parents[1]


def test_quality_json_and_yaml_templates_are_equivalent():
    json_template = json.loads(
        (ROOT / "integration_tests/conf/json/quality/onboarding.template").read_text()
    )
    yaml_template = yaml.safe_load(
        (
            ROOT
            / "integration_tests/conf/yml/quality/onboarding.template.yml"
        ).read_text()
    )

    json_row = json_template[0]
    yaml_row = yaml_template[0]
    rules_path_key = "bronze_quality_rules_path_it"
    assert json_row.pop(rules_path_key).endswith("/json/quality/rules.json")
    assert yaml_row.pop(rules_path_key).endswith("/yml/quality/rules.yml")
    assert json_row == yaml_row


def test_quality_rules_and_seed_data_pin_release_a_routing():
    json_rules = json.loads(
        (ROOT / "integration_tests/conf/json/quality/rules.json").read_text()
    )
    yaml_rules = yaml.safe_load(
        (ROOT / "integration_tests/conf/yml/quality/rules.yml").read_text()
    )
    assert json_rules == yaml_rules

    rows = [
        json.loads(line)
        for line in (
            ROOT / "integration_tests/resources/data/quality/customers.json"
        ).read_text().splitlines()
    ]
    assert len(rows) == 8
    assert sum(row["id"] is not None and row["id"] > 0 and row["email"] is not None for row in rows) == 5
    assert sum(row["id"] is None or row["email"] is None or row["id"] <= 0 for row in rows) == 3
    assert any(row["id"] is None and row["email"] is None for row in rows)


def test_quality_source_has_yaml_template_translation():
    conf = SDPMetaRunnerConf(run_id="quality", onboarding_file_format="yaml")
    assert conf.quality_template.endswith(
        "integration_tests/conf/yml/quality/onboarding.template.yml"
    )


def test_event_log_helper_extracts_and_checks_metrics():
    details = json.dumps(
        {
            "flow_progress": {
                "data_quality": {
                    "expectations": [
                        {"name": "id_required", "failed_records": 1},
                        {"name": "email_required", "failed_records": 2},
                    ]
                }
            }
        }
    )
    metrics = {
        item["name"]: item["failed_records"]
        for item in _expectations_from_details(details)
    }
    assert_expected_failures(
        metrics, {"id_required": 1, "email_required": 2}
    )


def test_dqx_quality_templates_and_rules_are_equivalent():
    json_rows = json.loads(
        (
            ROOT
            / "integration_tests/conf/json/quality_dqx/onboarding.template"
        ).read_text()
    )
    yaml_rows = yaml.safe_load(
        (
            ROOT
            / "integration_tests/conf/yml/quality_dqx/onboarding.template.yml"
        ).read_text()
    )
    assert len(json_rows) == len(yaml_rows) == 4
    rules_field = "bronze_quality_rules_path_it"
    for json_row, yaml_row in zip(json_rows, yaml_rows):
        assert "/json/quality_dqx/" in json_row.pop(rules_field)
        assert "/yml/quality_dqx/" in yaml_row.pop(rules_field)
        assert json_row == yaml_row
    assert {row["bronze_table"] for row in json_rows} == {
        "quality_customers",
        "quality_transactions",
        "quality_stores",
        "quality_products",
    }

    for stem in ("rules", "transactions_rules", "stores_rules",
                 "products_rules"):
        assert json.loads(
            (
                ROOT
                / f"integration_tests/conf/json/quality_dqx/{stem}.json"
            ).read_text()
        ) == yaml.safe_load(
            (
                ROOT
                / f"integration_tests/conf/yml/quality_dqx/{stem}.yml"
            ).read_text()
        )


def test_dqx_source_has_yaml_template_translation():
    conf = SDPMetaRunnerConf(
        run_id="quality-dqx",
        source="quality_dqx",
        onboarding_file_format="yaml",
    )
    assert conf.quality_dqx_template.endswith(
        "integration_tests/conf/yml/quality_dqx/onboarding.template.yml"
    )


def test_dqx_multi_feed_seed_counts_are_pinned():
    expected_counts = {
        "customers": 8,
        "transactions": 6,
        "stores": 5,
        "products": 6,
    }
    for feed, expected_count in expected_counts.items():
        path = (
            ROOT
            / "integration_tests/resources/data/quality_dqx"
            / feed
            / f"{feed}.json"
        )
        rows = [
            json.loads(line)
            for line in path.read_text().splitlines()
        ]
        assert len(rows) == expected_count
        assert len({row["row_key"] for row in rows}) == expected_count
