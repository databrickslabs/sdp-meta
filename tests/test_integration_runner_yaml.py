"""Unit tests for YAML support in the integration test runner.

These tests exercise the format-handling logic in ``SDPMetaRunnerConf`` and
``SDPMETARunner._write_onboarding_file`` without requiring a Databricks
workspace, so they can run in CI alongside the rest of the unit suite.
"""
import json
import os
import sys
import tempfile
import unittest

import yaml

# Make the integration_tests package importable.
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _PROJECT_ROOT)

from integration_tests.run_integration_tests import (  # noqa: E402
    SDPMetaRunnerConf,
    SDPMETARunner,
)


class IntegrationRunnerYamlTests(unittest.TestCase):
    """Verify YAML wiring in the integration runner."""

    def test_default_format_is_json(self):
        conf = SDPMetaRunnerConf(run_id="abc")
        self.assertEqual(conf.onboarding_file_format, "json")
        self.assertTrue(conf.onboarding_file_path.endswith(".json"))
        self.assertTrue(conf.onboarding_A2_file_path.endswith(".json"))

    def test_yaml_format_swaps_extensions(self):
        conf = SDPMetaRunnerConf(run_id="abc", onboarding_file_format="yaml")
        self.assertEqual(conf.onboarding_file_format, "yaml")
        self.assertTrue(conf.onboarding_file_path.endswith(".yml"))
        self.assertTrue(conf.onboarding_A2_file_path.endswith(".yml"))
        self.assertFalse(conf.onboarding_file_path.endswith(".json"))

    def test_yml_alias_normalized_to_yaml(self):
        conf = SDPMetaRunnerConf(run_id="abc", onboarding_file_format="yml")
        self.assertEqual(conf.onboarding_file_format, "yaml")
        self.assertTrue(conf.onboarding_file_path.endswith(".yml"))

    def test_invalid_format_raises(self):
        with self.assertRaises(ValueError):
            SDPMetaRunnerConf(run_id="abc", onboarding_file_format="toml")

    def test_to_yaml_variant_swaps_bucket_and_extension(self):
        result = SDPMetaRunnerConf._to_yaml_variant(
            "demo/conf/json/onboarding.json"
        )
        self.assertEqual(result, "demo/conf/yml/onboarding.yml")

    def test_to_yaml_variant_appends_yml_to_template(self):
        result = SDPMetaRunnerConf._to_yaml_variant(
            "integration_tests/conf/json/cloudfiles-onboarding.template"
        )
        self.assertEqual(
            result, "integration_tests/conf/yml/cloudfiles-onboarding.template.yml"
        )

    def test_to_yaml_variant_is_idempotent_for_yaml_paths(self):
        already_yaml = "demo/conf/yml/onboarding.yml"
        self.assertEqual(SDPMetaRunnerConf._to_yaml_variant(already_yaml), already_yaml)

    def test_to_yaml_variant_handles_none_and_empty(self):
        self.assertIsNone(SDPMetaRunnerConf._to_yaml_variant(None))
        self.assertEqual(SDPMetaRunnerConf._to_yaml_variant(""), "")

    def test_write_onboarding_file_json(self):
        payload = [{"data_flow_id": "100", "data_flow_group": "A1"}]
        tmp = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
        tmp.close()
        try:
            SDPMETARunner._write_onboarding_file(tmp.name, payload, "json")
            with open(tmp.name) as fh:
                self.assertEqual(json.load(fh), payload)
        finally:
            os.unlink(tmp.name)

    def test_write_onboarding_file_yaml_round_trip(self):
        payload = [
            {
                "data_flow_id": "100",
                "data_flow_group": "A1",
                "source_details": {"source_database": "APP", "source_table": "CUSTOMERS"},
            }
        ]
        tmp = tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False)
        tmp.close()
        try:
            SDPMETARunner._write_onboarding_file(tmp.name, payload, "yaml")
            with open(tmp.name) as fh:
                loaded = yaml.safe_load(fh)
            self.assertEqual(loaded, payload)
        finally:
            os.unlink(tmp.name)

    def test_cloudfiles_quarantine_compat_flow_matches_json_and_yaml(self):
        json_path = os.path.join(
            _PROJECT_ROOT,
            "integration_tests/conf/json/cloudfiles-onboarding.template",
        )
        yaml_path = os.path.join(
            _PROJECT_ROOT,
            "integration_tests/conf/yml/cloudfiles-onboarding.template.yml",
        )
        with open(json_path) as fh:
            json_payload = json.load(fh)
        with open(yaml_path) as fh:
            yaml_payload = yaml.safe_load(fh)

        json_flow = next(
            row for row in json_payload if row["data_flow_id"] == "190"
        )
        yaml_flow = next(
            row for row in yaml_payload if row["data_flow_id"] == "190"
        )
        for flow in (json_flow, yaml_flow):
            self.assertEqual(
                flow["data_flow_group"], "QUARANTINE_COMPAT"
            )
            self.assertFalse(
                any("quarantine" in key for key in flow)
            )
            self.assertIn(
                "bronze_data_quality_expectations_json_it", flow
            )
            self.assertIn(
                "silver_data_quality_expectations_json_it", flow
            )

        self.assertEqual(
            json_flow["bronze_table"], yaml_flow["bronze_table"]
        )
        self.assertEqual(
            json_flow["silver_table"], yaml_flow["silver_table"]
        )

        json_dqe_only = next(
            row for row in json_payload if row["data_flow_id"] == "191"
        )
        yaml_dqe_only = next(
            row for row in yaml_payload if row["data_flow_id"] == "191"
        )
        for flow in (json_dqe_only, yaml_dqe_only):
            self.assertEqual(
                flow["data_flow_group"], "DQE_NO_QUARANTINE"
            )
            self.assertFalse(
                any("quarantine" in key for key in flow)
            )
            self.assertIn(
                "no_quarantine_rules",
                flow["bronze_data_quality_expectations_json_it"],
            )
            self.assertIn(
                "no_quarantine_rules",
                flow["silver_data_quality_expectations_json_it"],
            )

        dqe_json_path = os.path.join(
            _PROJECT_ROOT,
            "integration_tests/conf/json/dqe/customers/"
            "no_quarantine_rules.json",
        )
        dqe_yaml_path = os.path.join(
            _PROJECT_ROOT,
            "integration_tests/conf/yml/dqe/customers/"
            "no_quarantine_rules.yml",
        )
        with open(dqe_json_path) as fh:
            json_dqe = json.load(fh)
        with open(dqe_yaml_path) as fh:
            yaml_dqe = yaml.safe_load(fh)
        self.assertEqual(json_dqe, yaml_dqe)
        self.assertIn("expect_or_drop", json_dqe)
        self.assertNotIn("expect_or_quarantine", json_dqe)

    def test_interactive_demo_dqe_without_quarantine_metadata_parity(self):
        json_path = os.path.join(
            _PROJECT_ROOT, "demo/conf/json/sample_onboarding.json"
        )
        yaml_path = os.path.join(
            _PROJECT_ROOT, "demo/conf/yml/sample_onboarding.yml"
        )
        with open(json_path) as fh:
            json_payload = json.load(fh)
        with open(yaml_path) as fh:
            yaml_payload = yaml.safe_load(fh)

        for payload in (json_payload, yaml_payload):
            customers = next(
                row for row in payload if row["data_flow_id"] == "100"
            )
            transactions = next(
                row for row in payload if row["data_flow_id"] == "101"
            )
            for flow in (customers, transactions):
                self.assertIn(
                    "silver_data_quality_expectations_json_prod", flow
                )
                self.assertFalse(
                    any(
                        key.startswith("silver_")
                        and "quarantine" in key
                        for key in flow
                    )
                )


class SilverDqeYamlPathRewriteTests(unittest.TestCase):
    """Verify silver/DQ path rewriting in YAML mode against dedicated .yml siblings.

    The runner does NOT generate .yml files at runtime — it expects committed
    ``.yml`` siblings next to the ``.json`` files in ``integration_tests/conf/``.
    These tests stage both ``.json`` and ``.yml`` fixtures in a temp tree and
    assert path rewriting (and missing-sibling detection) work correctly.
    """

    def setUp(self):
        self.tmp_root = tempfile.mkdtemp(prefix="it_yaml_rewrite_")
        self.conf_dir = os.path.join(self.tmp_root, "integration_tests", "conf")
        # The runner now expects format-bucket subdirs.
        self.json_dir = os.path.join(self.conf_dir, "json")
        self.yml_dir = os.path.join(self.conf_dir, "yml")
        os.makedirs(os.path.join(self.json_dir, "dqe", "customers"), exist_ok=True)
        os.makedirs(os.path.join(self.yml_dir, "dqe", "customers"), exist_ok=True)

        self.silver_json = os.path.join(self.json_dir, "silver_transformations.json")
        self.dqe_json = os.path.join(
            self.json_dir, "dqe", "customers", "bronze_data_quality_expectations.json"
        )
        silver_payload = [{"target_table": "customers", "select_exp": ["id", "email"]}]
        dqe_payload = {"expect_or_drop": {"valid_id": "id IS NOT NULL"}}
        with open(self.silver_json, "w") as fh:
            json.dump(silver_payload, fh)
        with open(self.dqe_json, "w") as fh:
            json.dump(dqe_payload, fh)
        # Dedicated, committed .yml siblings under yml/ bucket.
        self.silver_yml = os.path.join(self.yml_dir, "silver_transformations.yml")
        self.dqe_yml = os.path.join(
            self.yml_dir, "dqe", "customers", "bronze_data_quality_expectations.yml"
        )
        with open(self.silver_yml, "w") as fh:
            yaml.safe_dump(silver_payload, fh, sort_keys=False)
        with open(self.dqe_yml, "w") as fh:
            yaml.safe_dump(dqe_payload, fh, sort_keys=False)

        self.onboarding_path = os.path.join(self.yml_dir, "onboarding.yml")
        spec = [
            {
                "data_flow_id": "100",
                "silver_transformation_json_it": (
                    "/Volumes/test_cat/test_schema/test_vol/integration_tests/conf/json/"
                    "silver_transformations.json"
                ),
                "bronze_data_quality_expectations_json_it": (
                    "/Volumes/test_cat/test_schema/test_vol/integration_tests/conf/json/"
                    "dqe/customers/bronze_data_quality_expectations.json"
                ),
                "some_unrelated_path": "/data/keep_me.json",
            }
        ]
        with open(self.onboarding_path, "w") as fh:
            yaml.safe_dump(spec, fh, sort_keys=False)

        self.runner_conf = SDPMetaRunnerConf(
            run_id="test",
            onboarding_file_format="yaml",
            int_tests_dir=os.path.join(self.tmp_root, "integration_tests"),
            onboarding_file_path=self.onboarding_path,
            onboarding_A2_file_path=os.path.join(self.yml_dir, "onboarding_A2.yml"),
        )
        self.runner = SDPMETARunner.__new__(SDPMETARunner)

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmp_root, ignore_errors=True)

    def test_onboarding_paths_rewritten_to_yml(self):
        self.runner._rewrite_silver_and_dqe_paths_to_yml(self.runner_conf)

        with open(self.onboarding_path) as fh:
            spec = yaml.safe_load(fh)
        entry = spec[0]
        self.assertTrue(entry["silver_transformation_json_it"].endswith(".yml"))
        self.assertTrue(entry["bronze_data_quality_expectations_json_it"].endswith(".yml"))
        self.assertEqual(entry["some_unrelated_path"], "/data/keep_me.json")

    def test_runner_does_not_create_yml_siblings(self):
        # Pre-condition: dedicated yml siblings already exist (created in setUp).
        # Delete them and assert the runner refuses rather than auto-generating.
        os.remove(self.silver_yml)
        os.remove(self.dqe_yml)
        with self.assertRaises(FileNotFoundError):
            self.runner._rewrite_silver_and_dqe_paths_to_yml(self.runner_conf)

    def test_noop_when_format_is_json(self):
        self.runner_conf.onboarding_file_format = "json"
        # Prove no IO happens by removing the yml siblings first.
        os.remove(self.silver_yml)
        os.remove(self.dqe_yml)
        self.runner._rewrite_silver_and_dqe_paths_to_yml(self.runner_conf)
        with open(self.onboarding_path) as fh:
            spec = yaml.safe_load(fh)
        self.assertTrue(spec[0]["silver_transformation_json_it"].endswith(".json"))


if __name__ == "__main__":
    unittest.main()
