"""Local Spark and graph-shape tests for the built-in quality engine."""

import json
import sys
from datetime import datetime
from unittest.mock import MagicMock, patch

from pyspark.sql import functions as f

sys.modules["pyspark.pipelines"] = MagicMock()

from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline  # noqa: E402
from databricks.labs.sdp_meta.dataflow_spec import BronzeDataflowSpec  # noqa: E402
from databricks.labs.sdp_meta.quality.lakeflow_engine import (  # noqa: E402
    LakeflowQualityEngine,
)  # noqa: E402
from databricks.labs.sdp_meta.quality.validation import (  # noqa: E402
    build_lakeflow_quality_config,
    validate_lakeflow_rules,
)  # noqa: E402
from tests.utils import SDPFrameworkTestCase  # noqa: E402


class LakeflowQualityEngineSparkTests(SDPFrameworkTestCase):
    """Prove routing, diagnostics, and bindings with real DataFrames."""

    def setUp(self):
        super().setUp()
        self.document = {
            "expect_or_drop": {"positive_id": "id > 0"},
            "expect_or_fail": {
                "email_shape": "email IS NULL OR email LIKE '%@%'"
            },
            "expect_or_quarantine": {
                "id_required": "id IS NOT NULL",
                "email_required": "email IS NOT NULL",
            },
        }
        self.normalized, _ = validate_lakeflow_rules(
            self.spark,
            self.document,
            self.spark.createDataFrame(
                [(1, "a@example.com")], "id int, email string"
            ).schema,
        )
        self.engine = LakeflowQualityEngine(self.spark)

    def test_diagnostics_and_routing_are_stable(self):
        rows = self.spark.createDataFrame(
            [
                (1, "a@example.com"),
                (2, None),
                (None, None),
                (-1, "negative@example.com"),
            ],
            "id int, email string",
        )

        checked = self.engine.apply(rows, self.normalized, {})
        invalid = self.engine.get_invalid(checked)
        main_input = self.engine.get_main_input(checked)
        main = main_input.filter(
            f.expr(self.normalized["expect_or_drop"]["positive_id"])
        )
        for expression in self.normalized["expect_or_quarantine"].values():
            main = main.filter(f.expr(expression))

        actual_errors = {
            row["id"]: [error["name"] for error in row["_errors"]]
            for row in invalid.orderBy(f.col("id").asc_nulls_last()).collect()
        }
        self.assertEqual(actual_errors[2], ["email_required"])
        self.assertEqual(
            actual_errors[None], ["id_required", "email_required"]
        )
        self.assertEqual([row["id"] for row in main.collect()], [1])
        self.assertEqual(invalid.count(), 2)
        for rule_name, expression in self.normalized[
            "expect_or_quarantine"
        ].items():
            expected_failures = checked.filter(~f.expr(expression)).count()
            diagnostic_failures = checked.filter(
                f.exists("_errors", lambda error: error["name"] == rule_name)
            ).count()
            self.assertEqual(expected_failures, diagnostic_failures)

    def test_null_predicate_results_fail_quality_rules(self):
        rows = self.spark.createDataFrame([(None,)], "id int")
        normalized, _ = validate_lakeflow_rules(
            self.spark,
            {"expect_or_quarantine": {"positive_id": "id > 0"}},
            rows.schema,
        )
        checked = self.engine.apply(rows, normalized, {})
        errors = checked.first()["_errors"]
        self.assertEqual([error["name"] for error in errors], ["positive_id"])

    def test_native_bindings_use_identical_normalized_expressions(self):
        bindings = self.engine.native_expectations(self.normalized, {})
        by_action = {(binding.target, binding.action): binding.rules for binding in bindings}

        self.assertEqual(
            by_action[("main", "expect_or_drop")],
            self.normalized["expect_or_quarantine"],
        )
        self.assertIn(
            self.normalized["expect_or_drop"],
            [binding.rules for binding in bindings],
        )
        self.assertEqual(
            by_action[("checked", "expect_or_fail")],
            self.normalized["expect_or_fail"],
        )


class QualityWriterGraphTests(SDPFrameworkTestCase):
    """Pin the checked-view/main/quarantine declaration graph."""

    @patch("databricks.labs.sdp_meta.quality.writer.dp")
    def test_writer_declares_one_checked_view_and_two_tables(self, mock_dp):
        mock_dp.temporary_view.side_effect = lambda func, **kwargs: func
        mock_dp.table.side_effect = lambda func, **kwargs: func
        mock_dp.expect_all.side_effect = lambda rules: lambda func: func
        mock_dp.expect_all_or_drop.side_effect = lambda rules: lambda func: func
        mock_dp.expect_all_or_fail.side_effect = lambda rules: lambda func: func

        spec_values = {
            "dataFlowId": "quality-graph",
            "dataFlowGroup": "A1",
            "sourceFormat": "cloudFiles",
            "sourceDetails": {"path": "/tmp/input"},
            "readerConfigOptions": {},
            "targetFormat": "delta",
            "targetDetails": {
                "database": "bronze",
                "table": "customers",
                "path": "/tmp/customers",
            },
            "tableProperties": {},
            "schema": None,
            "partitionColumns": [],
            "cdcApplyChanges": None,
            "applyChangesFromSnapshot": None,
            "dataQualityExpectations": None,
            "quarantineTargetDetails": {
                "database": "bronze_quarantine",
                "table": "invalid_rows",
                "path": "/tmp/invalid_rows",
                "cluster_by": "['id']",
                "comment": None,
            },
            "quarantineTableProperties": {},
            "appendFlows": None,
            "appendFlowsSchemas": {},
            "version": "v1",
            "createDate": datetime.now(),
            "createdBy": "test",
            "updateDate": datetime.now(),
            "updatedBy": "test",
            "clusterBy": [],
            "clusterByAuto": False,
            "sinks": None,
            "cdcApplyChangesFlows": None,
            "cdcApplyChangesFlowsSchemas": None,
            "rowFilter": None,
            "quarantineRowFilter": None,
        }
        document = {
            "expect": {"known_id": "id IS NOT NULL"},
            "expect_or_quarantine": {"positive_id": "id > 0"},
        }
        normalized, _ = validate_lakeflow_rules(
            self.spark,
            document,
            self.spark.createDataFrame([(1,)], "id int").schema,
        )
        config = build_lakeflow_quality_config(
            rules_path="/Volumes/config/rules.json",
            raw_text=json.dumps(document),
            document=document,
            normalized_rules=normalized,
            analysis_required=False,
            target_details=spec_values["targetDetails"],
            quarantine_target_details=spec_values["quarantineTargetDetails"],
        )
        config["validation_policy_version"] = "0"
        config["engine_options"]["analysis_required"] = True
        spec_values["qualityConfig"] = json.dumps(config)
        spec = BronzeDataflowSpec(**spec_values)
        pipeline = DataflowPipeline(
            self.spark, spec, f"{spec.targetDetails['table']}_inputview"
        )

        pipeline.write()

        mock_dp.temporary_view.assert_called_once()
        self.assertEqual(mock_dp.table.call_count, 2)
        self.assertEqual(
            [call.kwargs["name"] for call in mock_dp.table.call_args_list],
            [
                pipeline._get_target_table_info()[1],
                pipeline._build_table_name(
                    None, "bronze_quarantine", "invalid_rows"
                ),
            ],
        )
        quarantine_call = mock_dp.table.call_args_list[1]
        self.assertEqual(quarantine_call.kwargs["cluster_by"], ["id"])
        self.assertEqual(
            quarantine_call.kwargs["comment"],
            "quality quarantine table "
            + pipeline._build_table_name(
                None, "bronze_quarantine", "invalid_rows"
            ),
        )
        mock_dp.expect_all.assert_called_once_with(normalized["expect"])
        mock_dp.expect_all_or_drop.assert_called_once_with(
            normalized["expect_or_quarantine"]
        )

        source = self.spark.createDataFrame([(1,), (-1,)], "id int")
        checked_factory = mock_dp.temporary_view.call_args.args[0]
        mock_dp.read_stream.return_value = source
        with patch.object(
            LakeflowQualityEngine,
            "validate",
            side_effect=AssertionError(
                "runtime reapplied onboarding policy"
            ),
        ):
            checked = checked_factory()
        self.assertIn("_errors", checked.columns)

        main_factory = mock_dp.table.call_args_list[0].args[0]
        quarantine_factory = mock_dp.table.call_args_list[1].args[0]
        mock_dp.read_stream.return_value = checked
        main = main_factory()
        quarantine = quarantine_factory()
        self.assertNotIn("_errors", main.columns)
        self.assertEqual(
            [row["id"] for row in main.orderBy("id").collect()],
            [-1, 1],
        )
        self.assertIn("_errors", quarantine.columns)
        self.assertEqual([row["id"] for row in quarantine.collect()], [-1])

        reserved_source = self.spark.createDataFrame(
            [(1, [])], "id int, _errors array<string>"
        )
        mock_dp.read_stream.return_value = reserved_source
        with self.assertRaisesRegex(
            ValueError, "reserved quality columns: _errors"
        ):
            checked_factory()
