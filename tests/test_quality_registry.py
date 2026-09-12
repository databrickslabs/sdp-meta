"""Tests for the experimental quality-engine SPI and discovery."""

from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from databricks.labs.sdp_meta.quality.registry import (
    QualityEngineNotAvailable,
    resolve_quality_engine,
    validate_plugin_snapshot,
)
from databricks.labs.sdp_meta.quality.spi import (
    ExpectationBinding,
    QualityEnginePlugin,
)
from databricks.labs.sdp_meta.quality.testing import (
    assert_quality_engine_contract,
)
from databricks.labs.sdp_meta.quality.validation import (
    InvalidQualityConfiguration,
    build_plugin_quality_config,
    validate_runtime_quality_config,
)


class SyntheticPlugin(QualityEnginePlugin):
    plugin_package = "synthetic-quality-plugin"
    plugin_version = "1.0.0"
    engine_version = "1"
    diagnostic_schema_fingerprint = "a" * 64

    def __init__(self, spark=None):
        self.spark = spark

    def validate(self, document, options):
        if "invalid" in document:
            raise ValueError("invalid")
        return document

    def apply(self, input_df, rules, options):
        return SimpleNamespace(
            columns=[*input_df.columns, "_synthetic_errors"],
            schema=input_df.schema,
        )

    def get_main_input(self, checked_df):
        return checked_df

    def get_invalid(self, checked_df):
        return checked_df

    def native_expectations(self, rules, options):
        return [
            ExpectationBinding(
                target="main", action="expect", rules=rules
            )
        ]

    def reserved_columns(self):
        return ["_synthetic_errors"]


class MiniFrame:
    def __init__(self, rows):
        self.rows = rows
        self.columns = list(rows[0]) if rows else []
        self.schema = object()

    def select(self, *columns):
        return MiniFrame([
            {column: row[column] for column in columns}
            for row in self.rows
        ])

    def collect(self):
        return self.rows


class OverlapRoutingPlugin(QualityEnginePlugin):
    allows_overlap = True
    overlap_diagnostics = ("_warnings", "_errors")

    def validate(self, document, options):
        if "invalid" in document:
            raise ValueError("invalid")
        return document

    def apply(self, input_df, rules, options):
        return MiniFrame([
            {
                **row,
                "_warnings": ["warning"] if row["id"] == 1 else [],
                "_errors": ["error"] if row["id"] == 2 else [],
            }
            for row in input_df.rows
        ])

    def get_main_input(self, checked_df):
        return MiniFrame([
            row for row in checked_df.rows if not row["_errors"]
        ])

    def get_invalid(self, checked_df):
        return MiniFrame([
            row
            for row in checked_df.rows
            if row["_errors"] or row["_warnings"]
        ])

    def native_expectations(self, rules, options):
        return []

    def reserved_columns(self):
        return ["_warnings", "_errors"]


class QualityRegistryTests(TestCase):
    def test_discovers_installed_entry_point(self):
        plugin = resolve_quality_engine("synthetic", object())
        self.assertEqual(
            plugin.plugin_package,
            "sdp-meta-synthetic-quality-plugin",
        )

    def test_missing_engine_has_actionable_error(self):
        with patch(
            "databricks.labs.sdp_meta.quality.registry._entry_points",
            return_value=[],
        ):
            with self.assertRaisesRegex(
                QualityEngineNotAvailable, "not installed"
            ):
                resolve_quality_engine("missing", object())

    def test_adapter_drift_warns_or_fails_in_strict_mode(self):
        plugin = SyntheticPlugin()
        snapshot = {
            "spi_version": plugin.api_version,
            "plugin_version": "0.9.0",
            "diagnostic_schema_fingerprint": "a" * 64,
            "engine_options": {},
        }
        with self.assertWarns(RuntimeWarning):
            validate_plugin_snapshot(plugin, snapshot)
        snapshot["engine_options"]["strict_version"] = True
        with self.assertRaisesRegex(
            QualityEngineNotAvailable, "version drift"
        ):
            validate_plugin_snapshot(plugin, snapshot)

    def test_contract_kit_accepts_bare_result_for_schema_less_source(self):
        frame = SimpleNamespace(columns=["id"], schema=object())
        assert_quality_engine_contract(
            SyntheticPlugin(),
            frame,
            {"expect": {"id": "id IS NOT NULL"}},
            {"invalid": {}},
        )

    def test_contract_kit_checks_overlap_routing(self):
        assert_quality_engine_contract(
            OverlapRoutingPlugin(),
            MiniFrame([{"id": 1}, {"id": 2}, {"id": 3}]),
            {"checks": []},
            {"invalid": {}},
            row_id_column="id",
            expected_main_ids={1, 3},
            expected_invalid_ids={1, 2},
            expected_overlap_ids={1},
        )

    def test_external_plugin_snapshot_contract(self):
        plugin = SyntheticPlugin()
        config = build_plugin_quality_config(
            engine="synthetic",
            plugin=plugin,
            rules_path="/rules.json",
            raw_text='{"rules":{"positive":"id > 0"}}',
            document={"rules": {"positive": "id > 0"}},
            normalized_rules={"positive": "id > 0"},
            analysis_required=False,
            target_details={"database": "bronze", "table": "valid"},
            quarantine_target_details={
                "database": "quality",
                "table": "invalid",
            },
        )
        validate_runtime_quality_config(config)

    def test_plugin_snapshot_rejects_invalid_migration(self):
        with self.assertRaisesRegex(
            InvalidQualityConfiguration, "must be 'full_refresh'"
        ):
            build_plugin_quality_config(
                engine="synthetic",
                plugin=SyntheticPlugin(),
                rules_path="/rules.json",
                raw_text="{}",
                document={},
                normalized_rules={},
                analysis_required=False,
                target_details={"database": "bronze", "table": "valid"},
                quarantine_target_details={
                    "database": "quality",
                    "table": "invalid",
                },
                migration="partial",
            )
