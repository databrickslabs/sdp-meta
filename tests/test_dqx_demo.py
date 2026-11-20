"""Offline contract tests for the SDP-META + DQX demo."""

import csv
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
DEMO_ROOT = REPO_ROOT / "demo"
JSON_ONBOARDING = DEMO_ROOT / "conf" / "json" / "dqx-onboarding.template"
YAML_ONBOARDING = (
    DEMO_ROOT / "conf" / "yml" / "dqx-onboarding.template.yml"
)
JSON_CHECKS = (
    DEMO_ROOT / "conf" / "json" / "dqx" / "customers_checks.json"
)
YAML_CHECKS = (
    DEMO_ROOT / "conf" / "yml" / "dqx" / "customers_checks.yml"
)
SEED_DATA = (
    DEMO_ROOT
    / "resources"
    / "data"
    / "dqx_demo"
    / "data"
    / "customers"
    / "customers.csv"
)
SEED_DDL = (
    DEMO_ROOT
    / "resources"
    / "data"
    / "dqx_demo"
    / "ddl"
    / "customers.ddl"
)
RUNNERS = DEMO_ROOT / "notebooks" / "dqx_runners"
LAUNCHER = DEMO_ROOT / "launch_dqx_demo.py"


def _load_launcher():
    spec = importlib.util.spec_from_file_location("launch_dqx_demo", LAUNCHER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class DQXDemoConfigurationTests(TestCase):
    def test_demo_artifacts_exist(self):
        for path in (
            JSON_ONBOARDING,
            YAML_ONBOARDING,
            JSON_CHECKS,
            YAML_CHECKS,
            SEED_DATA,
            SEED_DDL,
            RUNNERS / "init_sdp_meta_pipeline.py",
            RUNNERS / "validate.py",
            LAUNCHER,
        ):
            self.assertTrue(path.is_file(), f"missing DQX demo file: {path}")

    def test_json_and_yaml_onboarding_templates_are_equivalent(self):
        json_config = json.loads(JSON_ONBOARDING.read_text())
        yaml_config = yaml.safe_load(YAML_ONBOARDING.read_text())

        rules_field = "bronze_quality_rules_path_demo"
        self.assertTrue(
            json_config[0].pop(rules_field).endswith(
                "customers_checks.json"
            )
        )
        self.assertTrue(
            yaml_config[0].pop(rules_field).endswith(
                "customers_checks.yml"
            )
        )
        self.assertEqual(json_config, yaml_config)
        self.assertEqual(json_config[0]["source_format"], "cloudFiles")
        self.assertEqual(json_config[0]["bronze_table"], "customers_valid")

    def test_onboarding_uses_dqx_instead_of_legacy_dqe(self):
        flow = json.loads(JSON_ONBOARDING.read_text())[0]
        self.assertEqual(flow["bronze_quality_engine"], "dqx")
        self.assertFalse(
            [
                key
                for key in flow
                if "data_quality_expectations" in key
            ]
        )

    def test_json_and_yaml_dqx_checks_are_equivalent(self):
        json_checks = json.loads(JSON_CHECKS.read_text())
        yaml_checks = yaml.safe_load(YAML_CHECKS.read_text())

        self.assertEqual(json_checks, yaml_checks)
        self.assertEqual(
            {check["criticality"] for check in json_checks},
            {"error", "warn"},
        )
        for check in json_checks:
            self.assertIn("name", check)
            self.assertIn("function", check["check"])
            self.assertIsInstance(check["check"]["arguments"], dict)

    def test_dqx_string_literals_are_quoted_for_version_016(self):
        checks = json.loads(JSON_CHECKS.read_text())
        status_check = next(
            check for check in checks if check["name"] == "status_recognized"
        )

        self.assertEqual(
            status_check["check"]["arguments"]["allowed"],
            ["'ACTIVE'", "'INACTIVE'"],
        )

    def test_seed_data_pins_expected_dqx_routing(self):
        with SEED_DATA.open(newline="", encoding="utf-8") as seed:
            rows = list(csv.DictReader(seed))

        error_rows = [
            row for row in rows if not row["customer_id"] or not row["email"]
        ]
        warning_only_rows = [
            row
            for row in rows
            if row["customer_id"]
            and row["email"]
            and row["status"] not in {"ACTIVE", "INACTIVE"}
        ]
        valid_rows = [row for row in rows if row not in error_rows]
        quarantine_rows = error_rows + warning_only_rows

        self.assertEqual(len(rows), 8)
        self.assertEqual(len(valid_rows), 6)
        self.assertEqual(len(quarantine_rows), 3)
        self.assertEqual(len(error_rows), 2)
        self.assertEqual(len(warning_only_rows), 1)


class DQXDemoWorkflowTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.launcher = _load_launcher()

    def _runner_conf(self, file_format="json"):
        return SimpleNamespace(
            run_id="run-435",
            remote_whl_path="/Volumes/main/meta/wheels/sdp_meta.whl",
            uc_catalog_name="main",
            sdp_meta_schema="meta_schema",
            bronze_schema="bronze_schema",
            silver_schema="silver_schema",
            bronze_pipeline_id="pipeline-435",
            runners_nb_path="/Workspace/dqx-demo",
            uc_volume_path="/Volumes/main/meta/demo/",
            onboarding_file_path="demo/conf/json/dqx_onboarding.json",
            onboarding_file_format=file_format,
            env="demo",
        )

    def test_launcher_pins_dqx_dependency(self):
        self.assertEqual(self.launcher.DQX_VERSION, "0.16.0")
        self.assertEqual(
            self.launcher.DQX_DEPENDENCY,
            "databricks-labs-dqx==0.16.0",
        )

    def test_workflow_orders_onboarding_pipeline_and_validation(self):
        runner = object.__new__(self.launcher.SDPMETADQXDemo)
        runner.ws = SimpleNamespace(jobs=SimpleNamespace(create=MagicMock()))

        runner._create_workflow_spec(self._runner_conf())

        create_args = runner.ws.jobs.create.call_args.kwargs
        tasks = create_args["tasks"]
        self.assertEqual(
            [task.task_key for task in tasks],
            [
                "onboarding_job",
                "sdp_meta_pipeline",
                "validate",
            ],
        )
        self.assertEqual(
            [dependency.task_key for dependency in tasks[1].depends_on],
            ["onboarding_job"],
        )
        self.assertEqual(
            [dependency.task_key for dependency in tasks[2].depends_on],
            ["sdp_meta_pipeline"],
        )

        dependencies = create_args["environments"][0].spec.dependencies
        self.assertIn(self.launcher.DQX_DEPENDENCY, dependencies)
        self.assertEqual(
            create_args["environments"][0].spec.environment_version,
            "4",
        )

    def test_runner_notebooks_pin_same_pipeline_boundary(self):
        init_source = (RUNNERS / "init_sdp_meta_pipeline.py").read_text()
        validate_source = (RUNNERS / "validate.py").read_text()

        self.assertIn("DataflowPipeline.invoke_dlt_pipeline", init_source)
        self.assertNotIn("dqx_adapter_whl", init_source)
        self.assertIn("databricks-labs-dqx==0.16.0", init_source)
        self.assertFalse((RUNNERS / "apply_dqx.py").exists())
        self.assertIn("warning_only", validate_source)
        self.assertIn("raise AssertionError", validate_source)
