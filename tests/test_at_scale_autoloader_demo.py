import json
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock

import yaml

from demo.at_scale_autoloader_config import (
    GROUP,
    build_onboarding,
    cohort_counts,
    generated_onboarding_path,
    write_onboarding,
)
from demo.launch_at_scale_autoloader_demo import (
    SDPMetaAtScaleAutoLoaderDemo,
    parse_arguments,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
RUNNERS = REPO_ROOT / "demo/notebooks/at_scale_autoloader_runners"


def _payload(table_count=100):
    return build_onboarding(
        table_count=table_count,
        uc_volume_path="/Volumes/main/meta/files/",
        uc_catalog_name="main",
        bronze_schema="bronze",
        silver_schema="silver",
    )


class AtScaleConfigTests(TestCase):

    def test_default_workload_has_80_10_10_schema_cohorts(self):
        self.assertEqual(
            cohort_counts(100),
            {"inferred": 80, "hinted": 10, "explicit": 10},
        )
        payload = _payload()
        inferred = [
            flow
            for flow in payload
            if "source_schema_path" not in flow["source_details"]
            and "cloudFiles.schemaHints"
            not in flow["bronze_reader_options"]
        ]
        hinted = [
            flow
            for flow in payload
            if "cloudFiles.schemaHints"
            in flow["bronze_reader_options"]
        ]
        explicit = [
            flow
            for flow in payload
            if "source_schema_path" in flow["source_details"]
        ]
        self.assertEqual((len(inferred), len(hinted), len(explicit)), (80, 10, 10))
        self.assertTrue(
            all(flow["data_flow_group"] == GROUP for flow in payload)
        )

    def test_json_and_yaml_serializations_are_equivalent(self):
        payload = _payload(table_count=12)
        with tempfile.TemporaryDirectory() as tmp:
            json_path = Path(tmp) / "onboarding.json"
            yaml_path = Path(tmp) / "onboarding.yml"
            write_onboarding(payload, json_path, "json")
            write_onboarding(payload, yaml_path, "yaml")

            self.assertEqual(
                json.loads(json_path.read_text(encoding="utf-8")),
                yaml.safe_load(yaml_path.read_text(encoding="utf-8")),
            )

    def test_generated_paths_are_unique_per_run(self):
        first = generated_onboarding_path("json", "run-one")
        second = generated_onboarding_path("json", "run-two")
        yaml_path = generated_onboarding_path("yaml", "run-one")

        self.assertNotEqual(first, second)
        self.assertTrue(first.name.endswith("run-one.json"))
        self.assertTrue(yaml_path.name.endswith("run-one.yml"))

    def test_quarantine_rule_has_complete_target_configuration(self):
        orders = _payload()[0]
        self.assertEqual(orders["bronze_table"], "orders")
        self.assertIn(
            "bronze_data_quality_expectations_json_demo", orders
        )
        self.assertEqual(
            orders["bronze_catalog_quarantine_demo"], "main"
        )
        self.assertEqual(
            orders["bronze_database_quarantine_demo"], "bronze"
        )
        self.assertEqual(
            orders["bronze_quarantine_table"], "orders_quarantine"
        )

    def test_small_workload_still_includes_every_schema_strategy(self):
        payload = _payload(table_count=3)
        self.assertEqual(len(payload), 3)
        self.assertEqual(cohort_counts(3), {
            "inferred": 1,
            "hinted": 1,
            "explicit": 1,
        })


class AtScaleWorkflowTests(TestCase):

    def _runner_conf(self):
        return SimpleNamespace(
            run_id="run123",
            remote_whl_path="/Volumes/main/meta/files/demo.whl",
            uc_catalog_name="main",
            sdp_meta_schema="meta",
            bronze_schema="bronze",
            silver_schema="silver",
            gold_schema="gold",
            uc_volume_path="/Volumes/main/meta/files/",
            onboarding_file_path=(
                "demo/conf/json/onboarding_at_scale_autoloader.json"
            ),
            onboarding_file_format="json",
            env="demo",
            table_count=12,
            bronze_pipeline_id="bronze-pipeline",
            silver_pipeline_id="silver-pipeline",
            runners_nb_path="/Users/demo/at-scale",
            job_run_id=None,
        )

    def test_workflow_orders_all_phases_and_retries_evolution(self):
        demo = SDPMetaAtScaleAutoLoaderDemo.__new__(
            SDPMetaAtScaleAutoLoaderDemo
        )
        demo.ws = MagicMock()
        demo.ws.jobs.create.return_value = SimpleNamespace(job_id=123)

        demo.create_workflow_spec(self._runner_conf())

        tasks = demo.ws.jobs.create.call_args.kwargs["tasks"]
        task_by_key = {task.task_key: task for task in tasks}
        self.assertEqual(
            list(task_by_key),
            [
                "generate_phase_1",
                "onboard_specs",
                "bronze_phase_1",
                "silver_phase_1",
                "validate_phase_1",
                "stage_phase_2",
                "bronze_phase_2",
                "silver_phase_2",
                "build_gold",
                "validate_final",
            ],
        )
        phase_2 = task_by_key["bronze_phase_2"]
        self.assertEqual(phase_2.max_retries, 2)
        self.assertEqual(phase_2.min_retry_interval_millis, 10000)

    def test_cli_defaults_and_keep_resources_flag(self):
        args = parse_arguments(
            ["--uc_catalog_name", "main", "--keep-resources"]
        )
        self.assertEqual(args["table_count"], 100)
        self.assertEqual(args["onboarding_file_format"], "json")
        self.assertTrue(args["keep_resources"])

    def test_cli_rejects_catalog_name_that_is_not_safe_for_sql(self):
        with self.assertRaises(SystemExit):
            parse_arguments(["--uc_catalog_name", "bad-catalog"])

    def test_cleanup_cancels_active_run_before_deleting_resources(self):
        demo = SDPMetaAtScaleAutoLoaderDemo.__new__(
            SDPMetaAtScaleAutoLoaderDemo
        )
        demo.ws = MagicMock()
        demo.ws.jobs.get_run.return_value = SimpleNamespace(
            state=SimpleNamespace(life_cycle_state="RUNNING")
        )
        cancel_waiter = MagicMock()
        demo.ws.jobs.cancel_run.return_value = cancel_waiter
        demo.ws.tables.list.return_value = []
        demo.ws.volumes.list.return_value = []
        conf = self._runner_conf()
        conf.job_id = 123
        conf.job_run_id = 456
        conf.bronze_pipeline_A2_id = None

        demo.clean_up(conf)

        demo.ws.jobs.cancel_run.assert_called_once_with(run_id=456)
        cancel_waiter.result.assert_called_once()
        demo.ws.jobs.delete.assert_called_once_with(123)

    def test_cleanup_continues_when_active_run_cancellation_fails(self):
        demo = SDPMetaAtScaleAutoLoaderDemo.__new__(
            SDPMetaAtScaleAutoLoaderDemo
        )
        demo.ws = MagicMock()
        demo.ws.jobs.get_run.return_value = SimpleNamespace(
            state=SimpleNamespace(life_cycle_state="RUNNING")
        )
        demo.ws.jobs.cancel_run.return_value.result.side_effect = (
            RuntimeError("cancel failed")
        )
        demo.ws.tables.list.return_value = []
        demo.ws.volumes.list.return_value = []
        conf = self._runner_conf()
        conf.job_id = 123
        conf.job_run_id = 456
        conf.bronze_pipeline_A2_id = None

        with self.assertRaisesRegex(
            RuntimeError, "cleanup was incomplete.*cancel failed"
        ):
            demo.clean_up(conf)

        demo.ws.jobs.delete.assert_called_once_with(123)
        self.assertEqual(demo.ws.pipelines.delete.call_count, 2)
        self.assertEqual(demo.ws.schemas.delete.call_count, 4)

    def test_cleanup_removes_all_per_run_resources(self):
        demo = SDPMetaAtScaleAutoLoaderDemo.__new__(
            SDPMetaAtScaleAutoLoaderDemo
        )
        demo.ws = MagicMock()
        demo.ws.tables.list.return_value = []
        demo.ws.volumes.list.return_value = []
        conf = self._runner_conf()
        conf.job_id = 123
        conf.bronze_pipeline_A2_id = None

        demo.clean_up(conf)

        demo.ws.jobs.delete.assert_called_once_with(123)
        self.assertEqual(demo.ws.pipelines.delete.call_count, 2)
        deleted_schemas = {
            call.args[0]
            for call in demo.ws.schemas.delete.call_args_list
        }
        self.assertEqual(
            deleted_schemas,
            {
                "main.bronze",
                "main.silver",
                "main.meta",
                "main.gold",
            },
        )
        self.assertTrue(
            all(
                call.kwargs == {"force": True}
                for call in demo.ws.schemas.delete.call_args_list
            )
        )
        demo.ws.workspace.delete.assert_called_once_with(
            "/Users/demo/at-scale", recursive=True
        )

    def test_run_preserves_workflow_failure_when_cleanup_also_fails(self):
        demo = SDPMetaAtScaleAutoLoaderDemo.__new__(
            SDPMetaAtScaleAutoLoaderDemo
        )
        demo.init_sdp_meta_runner_conf = MagicMock(
            side_effect=RuntimeError("workflow failed")
        )
        demo.clean_up = MagicMock(
            side_effect=RuntimeError("cleanup failed")
        )
        conf = self._runner_conf()
        conf.keep_resources = False
        conf.local_onboarding_path = None

        with self.assertRaisesRegex(RuntimeError, "workflow failed"):
            demo.run(conf)

        demo.clean_up.assert_called_once_with(conf)

    def test_runner_notebooks_cover_callback_gold_dq_and_validation(self):
        pipeline = (RUNNERS / "init_sdp_meta_pipeline.py").read_text(
            encoding="utf-8"
        )
        generator = (RUNNERS / "generate_data.py").read_text(
            encoding="utf-8"
        )
        validator = (RUNNERS / "validate.py").read_text(encoding="utf-8")
        gold = (RUNNERS / "build_gold.py").read_text(encoding="utf-8")

        for marker in (
            "silver_custom_transform_func=transform_silver",
            "Window.partitionBy",
            "F.broadcast(latest_customer)",
        ):
            self.assertIn(marker, pipeline)
        self.assertIn('"expect"', generator)
        self.assertIn('"expect_or_quarantine"', generator)
        self.assertIn('"not-a-decimal"', generator)
        self.assertIn("orders_quarantine", validator)
        self.assertIn("_rescued_data", validator)
        self.assertIn('groupBy("customer_tier", "region")', gold)
