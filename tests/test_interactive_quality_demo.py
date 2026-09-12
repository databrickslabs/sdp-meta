"""Offline contracts for selectable quality in the interactive demo."""

import json
from pathlib import Path
from unittest import TestCase

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK = REPO_ROOT / "demo" / "SDP_META_INTERACTIVE_DEMO.py"
LAUNCHER = REPO_ROOT / "demo" / "launch_interactive_demo.py"
JSON_ONBOARDING = REPO_ROOT / "demo" / "conf" / "json" / "sample_onboarding.json"
YAML_ONBOARDING = REPO_ROOT / "demo" / "conf" / "yml" / "sample_onboarding.yml"


class InteractiveQualityDemoTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.notebook = NOTEBOOK.read_text()
        cls.launcher = LAUNCHER.read_text()

    def test_widget_and_launcher_offer_all_quality_modes(self):
        for source in (self.notebook, self.launcher):
            self.assertIn('"legacy"', source)
            self.assertIn('"lakeflow"', source)
            self.assertIn('"dqx"', source)
        self.assertIn('name="quality_engine"', self.notebook)
        self.assertIn('"quality_engine": args.quality_engine', self.launcher)

    def test_new_engines_replace_legacy_bronze_rule_field(self):
        self.assertIn(
            'feed.pop("bronze_data_quality_expectations_json_prod", None)',
            self.notebook,
        )
        self.assertIn(
            'feed["bronze_quality_engine"] = quality_engine',
            self.notebook,
        )
        self.assertIn(
            'feed["bronze_quality_rules_path_prod"]',
            self.notebook,
        )

    def test_quality_mode_applies_to_all_main_bronze_feeds(self):
        self.assertIn(
            '_configure_bronze_quality(feed, feed["bronze_table"])',
            self.notebook,
        )
        self.assertIn(
            '_configure_bronze_quality(products_feed, "products")',
            self.notebook,
        )
        self.assertIn(
            '_configure_bronze_quality(stores_feed, "stores")',
            self.notebook,
        )
        for table in ("customers", "transactions", "products", "stores"):
            self.assertIn(f'"{table}": {{', self.notebook)

    def test_dqx_requirement_comes_from_installed_package_metadata(self):
        self.assertIn(
            '_distribution_requires("databricks-labs-sdp-meta")',
            self.notebook,
        )
        self.assertNotIn("databricks-labs-dqx==", self.notebook)
        self.assertIn(
            '"quality_engine_dependency": quality_engine_dependency',
            self.notebook,
        )
        self.assertIn(
            '"quality_engine_dependency", "packaging"',
            self.notebook,
        )

    def test_smoke_validation_checks_snapshot_and_runtime_diagnostics(self):
        self.assertIn(
            'persisted_engine = json.loads(raw_config).get("engine")',
            self.notebook,
        )
        self.assertIn(
            'else {"_errors", "_warnings"}',
            self.notebook,
        )
        self.assertIn(
            "missing_diagnostics = expected_diagnostics - actual_columns",
            self.notebook,
        )

    def test_existing_json_and_yaml_samples_remain_equivalent_legacy_inputs(self):
        json_flows = json.loads(JSON_ONBOARDING.read_text())
        yaml_flows = yaml.safe_load(YAML_ONBOARDING.read_text())
        for json_flow, yaml_flow in zip(json_flows, yaml_flows, strict=True):
            for field in (
                "bronze_data_quality_expectations_json_prod",
                "silver_data_quality_expectations_json_prod",
                "silver_transformation_json_prod",
            ):
                json_path = json_flow.pop(field)
                yaml_path = yaml_flow.pop(field)
                self.assertEqual(
                    json_path.removesuffix(".json"),
                    yaml_path.removesuffix(".yml"),
                )
        self.assertEqual(json_flows, yaml_flows)
