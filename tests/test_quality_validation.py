"""Tests for Lakeflow quality onboarding validation."""

import json
import os

from pyspark.sql import Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from unittest.mock import MagicMock, patch

from databricks.labs.sdp_meta.onboard_dataflowspec import OnboardDataflowspec
from databricks.labs.sdp_meta.quality.fields import QUALITY_CONFIG_KEYS
from databricks.labs.sdp_meta.quality.rules_loader import load_rule_document
from databricks.labs.sdp_meta.quality.validation import (
    InvalidQualityConfiguration,
    build_lakeflow_quality_config,
    normalize_expression,
    validate_runtime_quality_config,
    validate_lakeflow_rules,
)
from tests.utils import SDPFrameworkTestCase


class QualityValidationTests(SDPFrameworkTestCase):
    """Validate rule shape, SQL analysis, and persisted snapshots."""

    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("email", StringType(), True),
            StructField("operation", StringType(), True),
            StructField("operation_date", StringType(), True),
        ]
    )

    @staticmethod
    def _quality_row(layer="bronze", **overrides):
        row = {
            "data_flow_id": "quality-1",
            "source_format": "cloudFiles",
            f"{layer}_quality_engine": "lakeflow",
            f"{layer}_quality_rules_path_dev":
                "tests/resources/quality/lakeflow_rules.json",
            f"{layer}_quality_engine_migration": None,
            f"{layer}_data_quality_expectations_json_dev": None,
            f"{layer}_database_quarantine_dev": layer,
            f"{layer}_quarantine_table": "customers_quarantine",
            f"{layer}_quarantine_table_path_dev": "/tmp/customers_quarantine",
        }
        row.update(overrides)
        return row

    def test_load_and_validate_lakeflow_rules(self):
        document, _ = load_rule_document(
            self.spark, "tests/resources/quality/lakeflow_rules.json"
        )

        normalized, analysis_required = validate_lakeflow_rules(
            self.spark, document, self.schema
        )

        self.assertFalse(analysis_required)
        self.assertEqual(
            normalized["expect_or_quarantine"]["valid_id"],
            "COALESCE((id IS NOT NULL), FALSE)",
        )

    def test_rule_loader_preserves_plugin_native_list_shape(self):
        document, _ = load_rule_document(
            self.spark,
            "integration_tests/conf/json/quality_dqx/rules.json",
        )
        self.assertIsInstance(document, list)
        self.assertEqual(document[0]["name"], "id_required")

    def test_validation_rejects_duplicate_rule_names(self):
        with self.assertRaisesRegex(
            InvalidQualityConfiguration, "Duplicate quality rule name 'valid_id'"
        ):
            validate_lakeflow_rules(
                self.spark,
                {
                    "expect": {"valid_id": "id IS NOT NULL"},
                    "expect_or_drop": {"valid_id": "id > 0"},
                },
                self.schema,
            )

    def test_validation_rejects_invalid_sql_unknown_columns_and_non_boolean(self):
        scenarios = {
            "invalid Spark SQL": "id IS",
            "cannot be analyzed": "missing_column IS NOT NULL",
            "must return BOOLEAN": "id + 1",
        }
        for expected, expression in scenarios.items():
            with self.subTest(expression=expression):
                with self.assertRaisesRegex(
                    InvalidQualityConfiguration, expected
                ):
                    validate_lakeflow_rules(
                        self.spark,
                        {"expect": {"rule": expression}},
                        self.schema,
                    )

    def test_validation_rejects_random_functions(self):
        for expression in (
            "rand() > 0.5",
            "randn() > 0",
            "uuid() IS NOT NULL",
            "shuffle(array(id)) IS NOT NULL",
            "monotonically_increasing_id() > 0",
        ):
            with self.subTest(expression=expression):
                with self.assertRaisesRegex(
                    InvalidQualityConfiguration, "non-deterministic random function"
                ):
                    validate_lakeflow_rules(
                        self.spark,
                        {"expect": {"deterministic_rule": expression}},
                        self.schema,
                    )

    def test_validation_defers_schema_analysis_when_schema_is_unavailable(self):
        normalized, analysis_required = validate_lakeflow_rules(
            self.spark,
            {"expect": {"valid_id": "id IS NOT NULL"}},
        )
        self.assertTrue(analysis_required)
        self.assertEqual(
            normalized["expect"]["valid_id"],
            normalize_expression("id IS NOT NULL"),
        )

    def test_snapshot_contains_the_complete_contract(self):
        config = build_lakeflow_quality_config(
            rules_path="/Volumes/config/rules.yml",
            raw_text="expect:\n  valid_id: id IS NOT NULL\n",
            document={"expect": {"valid_id": "id IS NOT NULL"}},
            normalized_rules={
                "expect": {
                    "valid_id": "COALESCE((id IS NOT NULL), FALSE)"
                }
            },
            analysis_required=False,
            target_details={"database": "bronze", "table": "customers"},
            quarantine_target_details={
                "database": "bronze",
                "table": "customers_quarantine",
            },
        )

        self.assertEqual(tuple(config), QUALITY_CONFIG_KEYS)
        self.assertEqual(config["engine"], "lakeflow")
        self.assertFalse(config["engine_options"]["analysis_required"])
        self.assertEqual(
            config["output_targets"]["quarantine"]["table"],
            "customers_quarantine",
        )
        validate_runtime_quality_config(config)
        validate_runtime_quality_config({
            **config,
            "validation_policy_version": "0",
        })

        with self.assertRaisesRegex(
            InvalidQualityConfiguration,
            "normalized_rules must be a JSON object",
        ):
            validate_runtime_quality_config({
                **config,
                "normalized_rules": [],
            })
        with self.assertRaisesRegex(
            InvalidQualityConfiguration,
            "migration must be null or 'full_refresh'",
        ):
            validate_runtime_quality_config({
                **config,
                "migration": "incremental",
            })

        del config["content_hash"]
        with self.assertRaisesRegex(
            InvalidQualityConfiguration, "missing required fields: content_hash"
        ):
            validate_runtime_quality_config(config)

    def test_bronze_onboarding_builds_quality_config(self):
        onboarder = OnboardDataflowspec(
            self.spark, self.onboarding_bronze_silver_params_map
        )
        onboarding_row = self._quality_row()

        config_json, quarantine_details, _ = (
            onboarder._OnboardDataflowspec__get_quality_config(
                "dev",
                "bronze",
                onboarding_row,
                {"database": "bronze", "table": "customers"},
                json.dumps(self.schema.jsonValue()),
            )
        )

        config = json.loads(config_json)
        self.assertEqual(config["engine"], "lakeflow")
        self.assertEqual(quarantine_details["table"], "customers_quarantine")
        self.assertFalse(config["engine_options"]["analysis_required"])

    def _silver_quality_onboarding_params(
        self, rule_expression, *, create_source=True
    ):
        source_path = os.path.join(self.temp_delta_tables_path, "silver_source")
        if create_source:
            (
                self.spark.createDataFrame(
                    [(1, "customer@example.com")],
                    ["raw_id", "raw_email"],
                )
                .write.format("delta")
                .mode("overwrite")
                .save(source_path)
            )

        transformations_path = os.path.join(
            self.onboarding_spec_paths, "silver_transformations.json"
        )
        rules_path = os.path.join(
            self.onboarding_spec_paths, "silver_quality_rules.json"
        )
        onboarding_path = os.path.join(
            self.onboarding_spec_paths, "silver_quality_onboarding.json"
        )
        with open(transformations_path, "w", encoding="utf-8") as handle:
            json.dump(
                [
                    {
                        "target_table": "customers",
                        "select_exp": [
                            "raw_id AS id",
                            "raw_email AS email",
                        ],
                    }
                ],
                handle,
            )
        with open(rules_path, "w", encoding="utf-8") as handle:
            json.dump({"expect": {"valid_rule": rule_expression}}, handle)
        with open(onboarding_path, "w", encoding="utf-8") as handle:
            json.dump(
                [
                    {
                        "data_flow_id": "silver-quality-1",
                        "data_flow_group": "QUALITY",
                        "source_format": "delta",
                        "bronze_database_dev": "bronze",
                        "bronze_table": "raw_customers",
                        "bronze_table_path_dev": source_path,
                        "silver_database_dev": "silver",
                        "silver_table": "customers",
                        "silver_table_path_dev": os.path.join(
                            self.temp_delta_tables_path, "silver_target"
                        ),
                        "silver_transformation_json_dev": transformations_path,
                        "silver_quality_engine": "lakeflow",
                        "silver_quality_rules_path_dev": rules_path,
                        "silver_database_quarantine_dev": "silver",
                        "silver_quarantine_table": "customers_quarantine",
                        "silver_quarantine_table_path_dev": os.path.join(
                            self.temp_delta_tables_path, "silver_quarantine"
                        ),
                    }
                ],
                handle,
            )

        params = dict(self.onboarding_bronze_silver_params_map)
        params["onboarding_file_path"] = onboarding_path
        params["silver_dataflowspec_path"] = os.path.join(
            self.onboarding_spec_paths, "silver_quality_specs"
        )
        return params

    def test_silver_onboarding_analyzes_rules_after_select_transformation(self):
        params = self._silver_quality_onboarding_params("email IS NOT NULL")

        OnboardDataflowspec(self.spark, params).onboard_silver_dataflow_spec()

        row = self.spark.read.format("delta").load(
            params["silver_dataflowspec_path"]
        ).first()
        config = json.loads(row["qualityConfig"])
        self.assertFalse(config["engine_options"]["analysis_required"])

    def test_silver_onboarding_rejects_rule_missing_after_select_transformation(self):
        params = self._silver_quality_onboarding_params(
            "raw_email IS NOT NULL"
        )

        with self.assertRaisesRegex(
            InvalidQualityConfiguration, "cannot be analyzed"
        ):
            OnboardDataflowspec(
                self.spark, params
            ).onboard_silver_dataflow_spec()

    def test_silver_onboarding_defers_analysis_when_delta_source_is_unavailable(self):
        params = self._silver_quality_onboarding_params(
            "email IS NOT NULL", create_source=False
        )

        OnboardDataflowspec(self.spark, params).onboard_silver_dataflow_spec()

        row = self.spark.read.format("delta").load(
            params["silver_dataflowspec_path"]
        ).first()
        config = json.loads(row["qualityConfig"])
        self.assertTrue(config["engine_options"]["analysis_required"])

    def test_onboarding_rejects_engine_configuration_conflicts(self):
        onboarder = OnboardDataflowspec(
            self.spark, self.onboarding_bronze_silver_params_map
        )
        get_config = onboarder._OnboardDataflowspec__get_quality_config
        scenarios = (
            (
                "Quality engine 'missing' is not installed",
                self._quality_row(bronze_quality_engine="missing"),
            ),
            (
                "cannot combine",
                self._quality_row(
                    bronze_data_quality_expectations_json_dev=(
                        "tests/resources/dqe/products.json"
                    )
                ),
            ),
            (
                "does not support: bronze_append_flows",
                self._quality_row(bronze_append_flows=[{"name": "extra"}]),
            ),
            (
                "missing: bronze_quarantine_table",
                self._quality_row(bronze_quarantine_table=""),
            ),
        )
        for expected, row in scenarios:
            with self.subTest(expected=expected):
                with self.assertRaisesRegex(
                    InvalidQualityConfiguration, expected
                ):
                    get_config(
                        "dev",
                        "bronze",
                        row,
                        {"database": "bronze", "table": "customers"},
                        json.dumps(self.schema.jsonValue()),
                    )

    def test_new_silver_path_rejects_cluster_alias_conflict(self):
        onboarder = OnboardDataflowspec(
            self.spark, self.onboarding_bronze_silver_params_map
        )
        row = self._quality_row(
            "silver",
            silver_quarantine_table_cluster_by=["id"],
            silver_quarantine_cluster_by=["email"],
        )
        with self.assertRaisesRegex(
            InvalidQualityConfiguration,
            "Conflicting silver_quarantine_table_cluster_by",
        ):
            onboarder._OnboardDataflowspec__get_quality_config(
                "dev",
                "silver",
                row,
                {"database": "silver", "table": "customers"},
            )

    def test_engine_switch_requires_new_target_or_full_refresh(self):
        onboarder = OnboardDataflowspec(
            self.spark, self.onboarding_bronze_silver_params_map
        )
        previous_config = json.dumps(
            {
                "engine": "lakeflow",
                "diagnostic_schema_fingerprint": "old",
            }
        )
        existing = MagicMock()
        existing.columns = ["qualityConfig"]
        existing.collect.return_value = [
            Row(
                dataFlowId="quality-1",
                qualityConfig=previous_config,
                quarantineTargetDetails={
                    "database": "quality_old",
                    "table": "quarantine",
                },
            )
        ]

        def new_specs(
            migration=None,
            table="quarantine",
            database="quality_old",
        ):
            current = MagicMock()
            current.collect.return_value = [
                Row(
                    dataFlowId="quality-1",
                    qualityConfig=json.dumps(
                        {
                            "engine": "lakeflow",
                            "diagnostic_schema_fingerprint": "new",
                            "migration": migration,
                        }
                    ),
                    quarantineTargetDetails={
                        "database": database,
                        "table": table,
                    },
                )
            ]
            return current

        read_existing = (
            "_OnboardDataflowspec__read_existing_dataflow_specs"
        )
        validate_switch = (
            onboarder._OnboardDataflowspec__validate_quality_engine_switch
        )
        with patch.object(onboarder, read_existing, return_value=existing):
            with self.assertRaisesRegex(
                InvalidQualityConfiguration,
                "configure a new table or set quality engine migration",
            ):
                validate_switch(
                    new_specs(),
                    self.onboarding_bronze_silver_params_map,
                    "bronze",
                )
            validate_switch(
                new_specs(table="new_quarantine"),
                self.onboarding_bronze_silver_params_map,
                "bronze",
            )
            validate_switch(
                new_specs(database="quality_new"),
                self.onboarding_bronze_silver_params_map,
                "bronze",
            )
            validate_switch(
                new_specs(migration="full_refresh"),
                self.onboarding_bronze_silver_params_map,
                "bronze",
            )

        with patch.object(onboarder, read_existing, return_value=None):
            validate_switch(
                new_specs(),
                self.onboarding_bronze_silver_params_map,
                "bronze",
            )

        legacy_existing = MagicMock()
        legacy_existing.columns = ["dataQualityExpectations"]
        legacy_existing.collect.return_value = [
            Row(
                dataFlowId="quality-1",
                dataQualityExpectations=json.dumps({
                    "expect_or_quarantine": {"bad_id": "id IS NULL"}
                }),
                quarantineTargetDetails={
                    "database": "quality_old",
                    "table": "quarantine",
                },
            )
        ]
        with patch.object(
            onboarder, read_existing, return_value=legacy_existing
        ):
            with self.assertRaisesRegex(
                InvalidQualityConfiguration,
                "configure a new table or set quality engine migration",
            ):
                validate_switch(
                    new_specs(),
                    self.onboarding_bronze_silver_params_map,
                    "bronze",
                )
            validate_switch(
                new_specs(migration="full_refresh"),
                self.onboarding_bronze_silver_params_map,
                "bronze",
            )

    def test_engine_switch_skips_existing_table_without_current_quality(self):
        onboarder = OnboardDataflowspec(
            self.spark, self.onboarding_bronze_silver_params_map
        )
        new_specs = MagicMock()
        new_specs.where.return_value.limit.return_value.count.return_value = 0
        read_existing = (
            "_OnboardDataflowspec__read_existing_dataflow_specs"
        )
        with patch.object(onboarder, read_existing) as existing:
            result = (
                onboarder
                ._OnboardDataflowspec__validate_quality_engine_switch(
                    new_specs,
                    self.onboarding_bronze_silver_params_map,
                    "bronze",
                )
            )

        self.assertIs(result, new_specs)
        existing.assert_not_called()
        new_specs.collect.assert_not_called()
