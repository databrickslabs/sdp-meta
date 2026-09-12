"""Contract tests for SDP-META's optional built-in DQX engine."""

import json
from importlib.metadata import PackageNotFoundError
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, patch

from databricks.labs.sdp_meta.quality.registry import (
    resolve_quality_engine,
)
from databricks.labs.sdp_meta.quality.testing import (
    assert_quality_engine_contract,
)


CHECKS = [
    {
        "name": "id_required",
        "criticality": "error",
        "check": {
            "function": "is_not_null",
            "arguments": {"column": "id"},
        },
    },
    {
        "name": "email_recommended",
        "criticality": "warn",
        "check": {
            "function": "is_not_null",
            "arguments": {"column": "email"},
        },
    },
]


class DQXAdapterTests(TestCase):
    def setUp(self):
        self.plugin = resolve_quality_engine("dqx", MagicMock())
        self.engine = MagicMock()
        self.engine.validate_checks.return_value = SimpleNamespace(
            errors=[]
        )
        self.plugin._dq_engine = self.engine
        self.input_df = SimpleNamespace(columns=["id"], schema=object())

    def test_contract_kit_and_engine_delegation(self):
        checked = SimpleNamespace(
            columns=["id", "_errors", "_warnings", "_dq_info"],
            schema=object(),
        )
        main = SimpleNamespace(columns=["id"], schema=object())
        invalid = SimpleNamespace(
            columns=["id", "_errors", "_warnings", "_dq_info"],
            schema=object(),
        )
        self.engine.apply_checks_by_metadata.return_value = checked
        self.engine.get_valid.return_value = main
        self.engine.get_invalid.return_value = invalid
        assert_quality_engine_contract(
            self.plugin,
            self.input_df,
            CHECKS,
            {},
        )
        self.engine.apply_checks_by_metadata.assert_called_with(
            self.input_df, CHECKS
        )
        self.assertTrue(self.plugin.allows_overlap)
        self.assertEqual(
            self.plugin.overlap_diagnostics, ("_warnings", "_errors")
        )

    def test_adapter_metadata(self):
        self.assertEqual(
            self.plugin.plugin_package,
            "databricks-labs-sdp-meta",
        )
        self.assertEqual(self.plugin.plugin_version, "0.1.0")
        self.assertEqual(self.plugin.engine_version, "0.16.0")
        self.assertEqual(
            self.plugin.reserved_columns(),
            ["_errors", "_warnings", "_dq_info"],
        )

    def test_missing_dqx_distribution_has_install_instruction(self):
        with patch(
            "databricks.labs.sdp_meta.quality.dqx_engine.version",
            side_effect=PackageNotFoundError,
        ):
            with self.assertRaisesRegex(
                ImportError, r"databricks-labs-sdp-meta\[dqx\]"
            ):
                _ = self.plugin.engine_version

    def test_external_adapter_snapshot_remains_compatible(self):
        from databricks.labs.sdp_meta.quality.registry import (
            validate_plugin_snapshot,
        )

        validate_plugin_snapshot(
            self.plugin,
            {
                "spi_version": self.plugin.api_version,
                "plugin_package": "databricks-labs-sdp-meta-dqx",
                "plugin_version": "0.1.0",
                "engine_version": "0.16.0",
                "diagnostic_schema_fingerprint": (
                    self.plugin.diagnostic_schema_fingerprint
                ),
                "engine_options": {},
            },
        )

    def test_integration_rule_documents_are_valid_dqx_metadata(self):
        from databricks.labs.dqx.engine import DQEngine

        rules_dir = (
            Path(__file__).parents[1]
            / "integration_tests/conf/json/quality_dqx"
        )
        for name in (
            "rules.json",
            "transactions_rules.json",
            "stores_rules.json",
            "products_rules.json",
        ):
            with self.subTest(name=name):
                status = DQEngine.validate_checks(
                    json.loads((rules_dir / name).read_text())
                )
                self.assertEqual(status.errors, [])
