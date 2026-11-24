"""Unit tests for managed quality migration orchestration."""

import json
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import ANY, MagicMock, call, patch

from databricks.labs.sdp_meta.quality.migration import (
    PendingQualityMigration,
    _delta_spec_table,
    _read_spec_frame,
    complete_migrations,
    find_pending_migrations,
    migration_fingerprint,
    reconcile_migration_request,
    run_managed_quality_update,
)
from databricks.labs.sdp_meta.quality.migration_main import main, parse_args


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
    active = {
        **config,
        "migration": "managed_full_refresh",
    }
    return PendingQualityMigration(
        spec_table="main.meta.bronze_specs",
        dataflow_id="100",
        original_config_json=json.dumps(config),
        completed_config_json=json.dumps(completed),
        fingerprint="fingerprint",
        quarantine_target=config["output_targets"]["quarantine"],
        active_config_json=json.dumps(active),
    )


class ManagedQualityUpdateTests(TestCase):
    def setUp(self):
        self.activate_patch = patch(
            "databricks.labs.sdp_meta.quality.migration."
            "activate_migrations"
        )
        self.restore_patch = patch(
            "databricks.labs.sdp_meta.quality.migration."
            "restore_migrations"
        )
        self.activate = self.activate_patch.start()
        self.restore = self.restore_patch.start()
        self.addCleanup(self.activate_patch.stop)
        self.addCleanup(self.restore_patch.stop)

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
        self.activate.assert_called_once_with(ANY, pending)
        self.restore.assert_not_called()

    def test_timeout_deadline_is_shared_across_both_updates(self):
        ws = self._workspace()
        ws.pipelines.get_update.side_effect = [
            SimpleNamespace(
                update=SimpleNamespace(state="COMPLETED")
            ),
            SimpleNamespace(update=SimpleNamespace(state="RUNNING")),
        ]
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
            patch(
                "databricks.labs.sdp_meta.quality.migration.time.monotonic",
                side_effect=[100, 100, 110],
            ),
        ):
            with self.assertRaisesRegex(
                TimeoutError, "normal full-graph update failed"
            ):
                run_managed_quality_update(
                    ws,
                    MagicMock(),
                    "pipeline-1",
                    {},
                    poll_interval_seconds=0,
                    timeout_seconds=10,
                )
        ws.pipelines.start_update.assert_called_once_with(
            pipeline_id="pipeline-1",
            full_refresh_selection=["main.quality.invalid_rows"],
        )

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

    def test_spec_references_require_paths_and_empty_layers_are_skipped(self):
        spark = MagicMock()
        for reader in (_read_spec_frame, _delta_spec_table):
            with self.subTest(reader=reader.__name__):
                with self.assertRaisesRegex(
                    ValueError, "requires 'path'"
                ):
                    reader(spark, {})
        self.assertEqual(
            find_pending_migrations(spark, {"bronze": None}),
            [],
        )
        spark.table.assert_not_called()

    def test_non_migration_snapshots_are_ignored(self):
        frame = MagicMock()
        frame.columns = [
            "dataFlowId",
            "dataFlowGroup",
            "qualityConfig",
            "quarantineTargetDetails",
        ]
        frame.filter.return_value = frame
        frame.select.return_value.where.return_value.collect.return_value = [{
            "dataFlowId": "100",
            "qualityConfig": json.dumps({"migration": None}),
            "quarantineTargetDetails": {},
        }]
        spark = MagicMock()
        spark.table.return_value = frame
        with patch("databricks.labs.sdp_meta.quality.migration.f"):
            self.assertEqual(
                find_pending_migrations(
                    spark,
                    {"bronze": "main.meta.bronze_specs"},
                    {"bronze": "A1"},
                ),
                [],
            )

    def test_interrupted_managed_migration_is_recovered_as_pending(self):
        migration = _pending()
        frame = MagicMock()
        frame.columns = [
            "dataFlowId",
            "dataFlowGroup",
            "qualityConfig",
            "quarantineTargetDetails",
        ]
        frame.filter.return_value = frame
        frame.select.return_value.where.return_value.collect.return_value = [{
            "dataFlowId": migration.dataflow_id,
            "qualityConfig": migration.active_config_json,
            "quarantineTargetDetails": migration.quarantine_target,
        }]
        spark = MagicMock()
        spark.table.return_value = frame

        with patch("databricks.labs.sdp_meta.quality.migration.f"):
            recovered = find_pending_migrations(
                spark,
                {"bronze": migration.spec_table},
                {"bronze": "A1"},
            )

        self.assertEqual(len(recovered), 1)
        self.assertEqual(
            json.loads(recovered[0].original_config_json)["migration"],
            "full_refresh",
        )
        self.assertEqual(
            recovered[0].activation_expected_config_json,
            migration.active_config_json,
        )

    def test_timeout_before_normal_update_starts(self):
        ws = self._workspace()
        with (
            patch(
                "databricks.labs.sdp_meta.quality.migration."
                "find_pending_migrations",
                return_value=[],
            ),
            patch(
                "databricks.labs.sdp_meta.quality.migration.time.monotonic",
                side_effect=[100, 110],
            ),
        ):
            with self.assertRaisesRegex(
                TimeoutError, "before starting the next update"
            ):
                run_managed_quality_update(
                    ws,
                    MagicMock(),
                    "pipeline-1",
                    {},
                    timeout_seconds=10,
                )
        ws.pipelines.start_update.assert_not_called()

    def test_normal_update_failure_reports_completed_migration(self):
        ws = self._workspace()
        ws.pipelines.get_update.side_effect = [
            SimpleNamespace(update=SimpleNamespace(state="COMPLETED")),
            SimpleNamespace(update=SimpleNamespace(state="FAILED")),
        ]
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
            with self.assertRaisesRegex(
                RuntimeError, "quarantine migration completed"
            ):
                run_managed_quality_update(
                    ws,
                    MagicMock(),
                    "pipeline-1",
                    {},
                    poll_interval_seconds=0,
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
        self.restore.assert_called_once_with(ANY, ANY)

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
        frame.filter.return_value = frame
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
                    spark,
                    {"bronze": "main.meta.bronze_specs"},
                    {"bronze": "A1"},
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

    def test_missing_group_scope_fails_instead_of_scanning_all(self):
        # Without a group scope, force-refreshing every group sharing the spec
        # table would be destructive, and silently continuing would bypass a
        # required migration.
        frame = MagicMock()
        frame.columns = [
            "dataFlowId",
            "dataFlowGroup",
            "qualityConfig",
            "quarantineTargetDetails",
        ]
        spark = MagicMock()
        spark.table.return_value = frame

        with self.assertRaisesRegex(ValueError, "No dataflow group scope"):
            find_pending_migrations(
                spark,
                {"bronze": "main.meta.bronze_specs"},
                {"bronze": ""},
            )

        frame.filter.assert_not_called()
        frame.select.assert_not_called()

    def test_path_spec_reference_is_loaded_as_delta(self):
        config = json.loads(_pending().original_config_json)
        frame = MagicMock()
        frame.columns = [
            "dataFlowId",
            "qualityConfig",
            "quarantineTargetDetails",
        ]
        frame.filter.return_value = frame
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
                spark, {"bronze": reference}, {"bronze": "A1"}
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

    def test_wheel_entrypoint_runs_with_active_or_created_spark(self):
        argv = [
            "--pipeline_id", "pipeline-1",
            "--spec_tables", '{"bronze":"main.meta.bronze_specs"}',
            "--groups", '{"bronze":"A1"}',
            "--timeout_seconds", "30",
        ]
        for active_spark in ("active", None):
            with self.subTest(active_spark=active_spark):
                spark_session = MagicMock()
                spark_session.getActiveSession.return_value = active_spark
                spark_session.builder.getOrCreate.return_value = "created"
                with (
                    patch(
                        "databricks.sdk.WorkspaceClient",
                        return_value="workspace",
                    ),
                    patch(
                        "pyspark.sql.SparkSession",
                        spark_session,
                    ),
                    patch(
                        "databricks.labs.sdp_meta.quality.migration."
                        "run_managed_quality_update",
                        return_value="result",
                    ) as run_update,
                ):
                    self.assertEqual(main(argv), "result")
                run_update.assert_called_once_with(
                    "workspace",
                    active_spark or "created",
                    "pipeline-1",
                    {"bronze": "main.meta.bronze_specs"},
                    {"bronze": "A1"},
                    timeout_seconds=30,
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
