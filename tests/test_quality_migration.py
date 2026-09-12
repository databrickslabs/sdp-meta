"""Unit tests for managed quality migration orchestration."""

import json
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import ANY, MagicMock, call, patch

from databricks.labs.sdp_meta.quality.migration import (
    PendingQualityMigration,
    complete_migrations,
    find_pending_migrations,
    migration_fingerprint,
    reconcile_migration_request,
    run_managed_quality_update,
)
from databricks.labs.sdp_meta.quality.migration_main import parse_args


def _pending():
    config = {
        "engine": "lakeflow",
        "diagnostic_schema_fingerprint": "new",
        "output_targets": {
            "quarantine": {
                "catalog": "main",
                "database": "quality",
                "table": "invalid_rows",
            }
        },
        "migration": "full_refresh",
        "migration_fingerprint": "fingerprint",
        "completed_migration_fingerprint": None,
    }
    completed = {
        **config,
        "migration": None,
        "migration_fingerprint": None,
        "completed_migration_fingerprint": "fingerprint",
    }
    return PendingQualityMigration(
        spec_table="main.meta.bronze_specs",
        dataflow_id="100",
        original_config_json=json.dumps(config),
        completed_config_json=json.dumps(completed),
        fingerprint="fingerprint",
        quarantine_target=config["output_targets"]["quarantine"],
    )


class ManagedQualityUpdateTests(TestCase):
    def _workspace(self, *, direct=True, state="COMPLETED"):
        ws = MagicMock()
        ws.pipelines.get.return_value = SimpleNamespace(
            spec=SimpleNamespace(
                catalog="main" if direct else None,
                schema="bronze" if direct else None,
                target=None if direct else "bronze",
            )
        )
        ws.pipelines.start_update.return_value = SimpleNamespace(
            update_id="update-1"
        )
        ws.pipelines.get_update.return_value = SimpleNamespace(
            update=SimpleNamespace(state=state)
        )
        return ws

    def test_selective_full_refresh_then_normal_full_graph_update(self):
        ws = self._workspace()
        pending = [_pending()]
        with (
            patch(
                "databricks.labs.sdp_meta.quality.migration."
                "find_pending_migrations",
                return_value=pending,
            ),
            patch(
                "databricks.labs.sdp_meta.quality.migration."
                "complete_migrations"
            ) as complete,
        ):
            run_managed_quality_update(
                ws,
                MagicMock(),
                "pipeline-1",
                {"bronze": "main.meta.bronze_specs"},
                poll_interval_seconds=0,
            )

        self.assertEqual(
            ws.pipelines.start_update.call_args_list,
            [
                call(
                    pipeline_id="pipeline-1",
                    full_refresh_selection=[
                        "main.quality.invalid_rows"
                    ],
                ),
                call(pipeline_id="pipeline-1"),
            ],
        )
        complete.assert_called_once_with(ANY, pending)

    def test_legacy_publishing_uses_unqualified_dataset(self):
        ws = self._workspace(direct=False)
        with (
            patch(
                "databricks.labs.sdp_meta.quality.migration."
                "find_pending_migrations",
                return_value=[_pending()],
            ),
            patch(
                "databricks.labs.sdp_meta.quality.migration."
                "complete_migrations"
            ),
        ):
            run_managed_quality_update(
                ws,
                MagicMock(),
                "pipeline-1",
                {},
                poll_interval_seconds=0,
            )
        self.assertEqual(
            ws.pipelines.start_update.call_args_list[0].kwargs[
                "full_refresh_selection"
            ],
            ["invalid_rows"],
        )

    def test_no_pending_migration_runs_one_normal_full_graph_update(self):
        ws = self._workspace()
        with patch(
            "databricks.labs.sdp_meta.quality.migration."
            "find_pending_migrations",
            return_value=[],
        ):
            run_managed_quality_update(
                ws,
                MagicMock(),
                "pipeline-1",
                {},
                poll_interval_seconds=0,
            )
        ws.pipelines.start_update.assert_called_once_with(
            pipeline_id="pipeline-1"
        )

    def test_failed_update_does_not_clear_metadata(self):
        ws = self._workspace(state="FAILED")
        with (
            patch(
                "databricks.labs.sdp_meta.quality.migration."
                "find_pending_migrations",
                return_value=[_pending()],
            ),
            patch(
                "databricks.labs.sdp_meta.quality.migration."
                "complete_migrations"
            ) as complete,
        ):
            with self.assertRaisesRegex(
                RuntimeError, "migration remains pending"
            ):
                run_managed_quality_update(
                    ws,
                    MagicMock(),
                    "pipeline-1",
                    {},
                    poll_interval_seconds=0,
                )
        complete.assert_not_called()

    def test_completion_failure_stops_before_normal_update(self):
        ws = self._workspace()
        with (
            patch(
                "databricks.labs.sdp_meta.quality.migration."
                "find_pending_migrations",
                return_value=[_pending()],
            ),
            patch(
                "databricks.labs.sdp_meta.quality.migration."
                "complete_migrations",
                side_effect=RuntimeError("metadata unavailable"),
            ),
        ):
            with self.assertLogs(
                "databricks.labs.sdp_meta", level="CRITICAL"
            ):
                with self.assertRaisesRegex(
                    RuntimeError, "metadata unavailable"
                ):
                    run_managed_quality_update(
                        ws,
                        MagicMock(),
                        "pipeline-1",
                        {},
                        poll_interval_seconds=0,
                    )
        ws.pipelines.start_update.assert_called_once_with(
            pipeline_id="pipeline-1",
            full_refresh_selection=["main.quality.invalid_rows"],
        )

    def test_compare_and_swap_must_update_exactly_one_row(self):
        spark = MagicMock()
        spark.table.return_value.where.return_value.count.return_value = 0
        with (
            patch(
                "databricks.labs.sdp_meta.quality.migration.f"
            ),
            patch("delta.tables.DeltaTable.forName"),
        ):
            with self.assertRaisesRegex(
                RuntimeError, "compare-and-swap failed"
            ):
                complete_migrations(spark, [_pending()])

    def test_pending_migration_rejects_target_divergence(self):
        config = json.loads(_pending().original_config_json)
        frame = MagicMock()
        frame.columns = [
            "dataFlowId",
            "qualityConfig",
            "quarantineTargetDetails",
        ]
        frame.select.return_value.where.return_value.collect.return_value = [
            {
                "dataFlowId": "100",
                "qualityConfig": json.dumps(config),
                "quarantineTargetDetails": {
                    **config["output_targets"]["quarantine"],
                    "table": "other_invalid_rows",
                },
            }
        ]
        spark = MagicMock()
        spark.table.return_value = frame
        with patch(
            "databricks.labs.sdp_meta.quality.migration.f"
        ):
            with self.assertRaisesRegex(
                RuntimeError, "differs from its immutable"
            ):
                find_pending_migrations(
                    spark, {"bronze": "main.meta.bronze_specs"}
                )

    def test_legacy_spec_table_without_quality_config_has_no_migrations(self):
        frame = MagicMock()
        frame.columns = [
            "dataFlowId",
            "dataFlowGroup",
            "quarantineTargetDetails",
        ]
        spark = MagicMock()
        spark.table.return_value = frame

        pending = find_pending_migrations(
            spark, {"bronze": "main.meta.bronze_specs"}
        )

        self.assertEqual(pending, [])
        frame.select.assert_not_called()

    def test_path_spec_reference_is_loaded_as_delta(self):
        config = json.loads(_pending().original_config_json)
        frame = MagicMock()
        frame.columns = [
            "dataFlowId",
            "qualityConfig",
            "quarantineTargetDetails",
        ]
        frame.select.return_value.where.return_value.collect.return_value = [
            {
                "dataFlowId": "100",
                "qualityConfig": json.dumps(config),
                "quarantineTargetDetails": config["output_targets"][
                    "quarantine"
                ],
            }
        ]
        spark = MagicMock()
        spark.read.format.return_value.load.return_value = frame
        reference = {"path": "dbfs:/specs/bronze"}

        with patch(
            "databricks.labs.sdp_meta.quality.migration.f"
        ):
            pending = find_pending_migrations(
                spark, {"bronze": reference}
            )

        spark.read.format.assert_called_once_with("delta")
        spark.read.format.return_value.load.assert_called_once_with(
            "dbfs:/specs/bronze"
        )
        self.assertEqual(pending[0].spec_table, reference)

    def test_path_spec_completion_uses_delta_for_path(self):
        original = _pending()
        migration = PendingQualityMigration(
            spec_table={"path": "dbfs:/specs/bronze"},
            dataflow_id=original.dataflow_id,
            original_config_json=original.original_config_json,
            completed_config_json=original.completed_config_json,
            fingerprint=original.fingerprint,
            quarantine_target=original.quarantine_target,
        )
        spark = MagicMock()
        (
            spark.read.format.return_value.load.return_value
            .where.return_value.count.return_value
        ) = 1
        with (
            patch(
                "databricks.labs.sdp_meta.quality.migration.f"
            ),
            patch("delta.tables.DeltaTable.forPath") as for_path,
        ):
            complete_migrations(spark, [migration])

        for_path.assert_called_once_with(spark, "dbfs:/specs/bronze")
        for_path.return_value.update.assert_called_once()

    def test_fingerprint_ignores_rule_content(self):
        base = {
            "engine": "lakeflow",
            "diagnostic_schema_fingerprint": "schema",
            "output_targets": {
                "quarantine": {
                    "database": "quality",
                    "table": "invalid",
                }
            },
            "rule_document": {"expect": {"a": "id > 0"}},
        }
        changed_rules = {
            **base,
            "rule_document": {"expect": {"b": "id > 1"}},
        }
        self.assertEqual(
            migration_fingerprint(base),
            migration_fingerprint(changed_rules),
        )

    def test_wheel_entrypoint_arguments(self):
        args = parse_args([
            "--pipeline_id", "pipeline-1",
            "--spec_tables", '{"bronze":"main.meta.bronze_specs"}',
            "--groups", '{"bronze":"A1"}',
        ])
        self.assertEqual(args.pipeline_id, "pipeline-1")
        self.assertEqual(
            json.loads(args.spec_tables)["bronze"],
            "main.meta.bronze_specs",
        )

    def test_completed_fingerprint_is_not_rearmed(self):
        current = json.loads(_pending().original_config_json)
        previous = {
            "completed_migration_fingerprint": "fingerprint"
        }
        reconciled = reconcile_migration_request(current, previous)
        self.assertIsNone(reconciled["migration"])
        self.assertIsNone(reconciled["migration_fingerprint"])
        self.assertEqual(
            reconciled["completed_migration_fingerprint"],
            "fingerprint",
        )
