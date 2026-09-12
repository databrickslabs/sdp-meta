"""Tests for Spark-free quality onboarding validation."""

import unittest

from databricks.labs.sdp_meta.quality.onboarding_preflight import (
    collect_quality_configuration_errors,
)


class QualityOnboardingPreflightTests(unittest.TestCase):
    def _row(self, **overrides):
        row = {
            "data_flow_id": "quality-1",
            "source_format": "cloudFiles",
            "bronze_quality_engine": "lakeflow",
            "bronze_quality_rules_path_dev": "rules.yml",
            "bronze_database_quarantine_dev": "bronze",
            "bronze_quarantine_table": "invalid_rows",
        }
        row.update(overrides)
        return row

    def _errors(self, row, *, env="dev", uc_enabled=True, supported=None):
        return collect_quality_configuration_errors(
            [row],
            env=env,
            uc_enabled=uc_enabled,
            supported_engines=supported,
        )

    def test_valid_quality_row_has_no_errors(self):
        self.assertEqual(self._errors(self._row()), [])

    def test_common_contract_errors_are_aggregated(self):
        row = self._row(
            source_format="snapshot",
            bronze_quality_engine_migration="partial",
            bronze_append_flows=[{"name": "extra"}],
            bronze_quarantine_table="",
        )
        errors = "\n".join(self._errors(row))
        self.assertIn("snapshot sources", errors)
        self.assertIn("must be 'full_refresh'", errors)
        self.assertIn("does not support: bronze_append_flows", errors)
        self.assertIn("missing: bronze_quarantine_table", errors)

    def test_engine_is_required_for_orphan_quality_fields(self):
        row = self._row(bronze_quality_engine=None)
        errors = "\n".join(self._errors(row))
        self.assertIn("bronze_quality_engine is required", errors)

    def test_non_uc_requires_quarantine_path(self):
        errors = "\n".join(
            self._errors(self._row(), uc_enabled=False)
        )
        self.assertIn("bronze_quarantine_table_path_dev", errors)

    def test_any_environment_mode_discovers_suffixes(self):
        self.assertEqual(self._errors(self._row(), env=None), [])

    def test_external_engine_is_core_valid_but_can_be_ui_restricted(self):
        row = self._row(bronze_quality_engine="external")
        self.assertEqual(self._errors(row), [])
        restricted = "\n".join(
            self._errors(
                row, supported=("lakeflow", "dqx")
            )
        )
        self.assertIn("must be one of", restricted)

    def test_silver_cluster_alias_conflict_is_rejected(self):
        row = {
            "data_flow_id": "quality-1",
            "silver_quality_engine": "lakeflow",
            "silver_quality_rules_path_dev": "rules.yml",
            "silver_database_quarantine_dev": "silver",
            "silver_quarantine_table": "invalid_rows",
            "silver_quarantine_table_cluster_by": ["id"],
            "silver_quarantine_cluster_by": ["email"],
        }
        errors = "\n".join(self._errors(row))
        self.assertIn(
            "Conflicting silver_quarantine_table_cluster_by", errors
        )


if __name__ == "__main__":
    unittest.main()
