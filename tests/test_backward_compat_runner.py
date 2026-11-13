"""Unit tests for standard-compute legacy upgrade orchestration."""

import copy
import json
import os
import tempfile
from io import BytesIO
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, call as mock_call, patch

from databricks.sdk.service import compute, jobs
from databricks.sdk.service.pipelines import PipelineCluster

from integration_tests.run_backward_compat_tests import (
    BCRunnerConf,
    BackwardCompatRunner,
)
from integration_tests.version_profiles import (
    CURRENT,
    DEFAULT_TARGET_REF,
    LEGACY,
    resolve_profile,
)


class BackwardCompatFixtureTests(TestCase):
    def test_append_flow_metadata_is_removed_without_changing_top_level_metadata(self):
        top_level_metadata = {"select_metadata_cols": {"name": "_metadata.file_name"}}
        payload = [
            {
                "source_details": {"source_metadata": top_level_metadata},
                "bronze_append_flows": [
                    {
                        "source_details": {
                            "path": "/source",
                            "source_metadata": {
                                "select_metadata_cols": {
                                    "path": "_metadata.file_path"
                                }
                            },
                        }
                    }
                ],
            }
        ]

        removed = BackwardCompatRunner._remove_append_flow_source_metadata(payload)

        self.assertEqual(removed, 1)
        self.assertEqual(
            payload[0]["source_details"]["source_metadata"], top_level_metadata
        )
        self.assertEqual(
            payload[0]["bronze_append_flows"][0]["source_details"],
            {"path": "/source"},
        )

    def test_row_filters_are_removed_from_compatibility_fixture(self):
        payload = [
            {
                "bronze_row_filter": "ROW FILTER catalog.schema.fn ON (operation)",
                "bronze_quarantine_row_filter": (
                    "ROW FILTER catalog.schema.fn ON (operation)"
                ),
                "silver_row_filter": "ROW FILTER catalog.schema.fn ON (operation)",
                "silver_quarantine_row_filter": (
                    "ROW FILTER catalog.schema.fn ON (operation)"
                ),
                "bronze_table": "customers",
            }
        ]

        removed = BackwardCompatRunner._remove_row_filter_configuration(payload)

        self.assertEqual(removed, 4)
        self.assertEqual(payload, [{"bronze_table": "customers"}])


class CurrentUpgradeRunnerTests(TestCase):
    def setUp(self):
        self.ws = MagicMock()
        self.runner = BackwardCompatRunner({}, self.ws)
        self.conf = BCRunnerConf(
            run_id="current-upgrade",
            uc_catalog_name="catalog",
            source_ref="v0.1.0",
            target_ref="v0.1.1",
            source_profile=CURRENT,
            target_profile=CURRENT,
            bronze_a1_pipeline_id="bronze-a1",
            bronze_a2_pipeline_id="bronze-a2",
            silver_pipeline_id="silver",
            source_main_whl_remote="/Volumes/test/source-0.1.0.whl",
            target_main_whl_remote="/Volumes/test/target-0.1.1.whl",
        )

    def test_current_upgrade_preserves_canonical_runner_and_wheel_key(self):
        phase1 = self.runner._build_phase1_pipeline_config(
            self.conf, "bronze", "A1"
        )
        phase2 = self.runner._build_phase2_pipeline_config(
            self.conf, "bronze", "A1"
        )

        self.assertEqual(
            self.runner._runner_path(self.conf, self.conf.source_profile),
            "/runners/init_sdp_meta_pipeline.py",
        )
        self.assertEqual(
            phase1["sdp_meta_whl"], self.conf.source_main_whl_remote
        )
        self.assertEqual(
            phase2["sdp_meta_whl"], self.conf.target_main_whl_remote
        )
        self.assertNotIn("dlt_meta_whl", phase1)
        self.assertNotIn("dlt_meta_whl", phase2)

    def test_current_profile_resolves_all_canonical_refs(self):
        self.assertIs(resolve_profile("v0.2.0"), CURRENT)
        self.assertIs(resolve_profile("v0.2.4"), CURRENT)
        self.assertIs(resolve_profile("main"), CURRENT)
        self.assertIs(resolve_profile("v0.1.1"), CURRENT)
        self.assertEqual(DEFAULT_TARGET_REF, "main")

    @patch("builtins.print")
    def test_main_is_an_exact_ref_not_a_branch_prefix(self, warning):
        self.assertIs(resolve_profile("main/feature"), CURRENT)
        warning.assert_called_once()

    def test_current_upgrade_validator_receives_version_line_context(self):
        self.runner.build_phase2_job(self.conf)

        validate_task = next(
            task
            for task in self.ws.jobs.create.call_args.kwargs["tasks"]
            if task.task_key == "phase2_validate"
        )
        parameters = validate_task.notebook_task.base_parameters
        self.assertEqual(parameters["source_profile"], "current")
        self.assertEqual(parameters["source_ref"], "v0.1.0")
        self.assertEqual(parameters["target_ref"], "v0.1.1")

    def test_v010_source_removes_unsupported_append_flow_metadata(self):
        payload = [
            {
                "bronze_append_flows": [
                    {"source_details": {"source_metadata": {"enabled": "true"}}}
                ]
            }
        ]

        removed_metadata, removed_row_filters = (
            self.runner._normalize_source_baseline_fixture(self.conf, payload)
        )

        self.assertEqual((removed_metadata, removed_row_filters), (1, 0))
        self.assertEqual(
            payload[0]["bronze_append_flows"][0]["source_details"], {}
        )

    def test_capable_source_retains_append_metadata_but_removes_row_filter(self):
        self.conf.source_ref = "v0.1.1"
        metadata = {"select_metadata_cols": {"name": "_metadata.file_name"}}
        payload = [
            {
                "bronze_row_filter": "ROW FILTER catalog.schema.fn ON (operation)",
                "bronze_append_flows": [
                    {"source_details": {"source_metadata": metadata}}
                ],
            }
        ]

        removed_metadata, removed_row_filters = (
            self.runner._normalize_source_baseline_fixture(self.conf, payload)
        )

        self.assertEqual((removed_metadata, removed_row_filters), (0, 1))
        self.assertEqual(
            payload[0]["bronze_append_flows"][0]["source_details"][
                "source_metadata"
            ],
            metadata,
        )
        self.assertNotIn("bronze_row_filter", payload[0])

    def test_custom_current_source_defaults_to_latest_capabilities(self):
        self.conf.source_ref = "feature/future-release"

        self.assertTrue(
            self.conf.source_supports_append_flow_source_metadata
        )


class StandardLegacyUpgradeRunnerTests(TestCase):
    """Ensure standard-compute tests retain legacy pipeline publishing."""

    def setUp(self):
        self.ws = MagicMock()
        self.runner = BackwardCompatRunner({}, self.ws)
        self.conf = BCRunnerConf(
            run_id="test-run",
            uc_catalog_name="catalog",
            source_ref="v0.0.10",
            target_ref="v0.2.0",
            source_profile=LEGACY,
            target_profile=CURRENT,
            pipeline_mode="standard_legacy",
            pipeline_num_workers=2,
            bronze_schema="bronze",
            silver_schema="silver",
            sdp_meta_schema="specs",
            runners_nb_path="/Users/test/runners",
            source_main_whl_remote="/Volumes/test/source.whl",
            target_main_whl_remote="/Volumes/test/target.whl",
        )

    def _legacy_spec(self, target, wheel, *, wheel_key="dlt_meta_whl"):
        runner = (
            "init_dlt_meta_pipeline.py"
            if wheel_key == "dlt_meta_whl"
            else "init_sdp_meta_pipeline.py"
        )
        return SimpleNamespace(
            catalog="catalog",
            name="pipeline",
            target=target,
            schema=None,
            serverless=False,
            clusters=[PipelineCluster(label="default", num_workers=2)],
            libraries=[
                SimpleNamespace(
                    notebook=SimpleNamespace(path=f"/runners/{runner}")
                )
            ],
            configuration={wheel_key: wheel},
        )

    def test_standard_legacy_execution_kwargs_use_target_and_cluster(self):
        kwargs = self.runner._pipeline_execution_kwargs(self.conf, "bronze")

        self.assertEqual(kwargs["target"], "bronze")
        self.assertFalse(kwargs["serverless"])
        self.assertNotIn("schema", kwargs)
        self.assertEqual(kwargs["clusters"][0].label, "default")
        self.assertEqual(kwargs["clusters"][0].num_workers, 2)

    def test_legacy_source_does_not_support_append_flow_metadata(self):
        self.assertFalse(
            self.conf.source_supports_append_flow_source_metadata
        )

    def test_serverless_dpm_execution_kwargs_use_schema(self):
        self.conf.pipeline_mode = "serverless_dpm"
        kwargs = self.runner._pipeline_execution_kwargs(self.conf, "bronze")

        self.assertEqual(kwargs, {"serverless": True, "schema": "bronze"})

    @patch("integration_tests.run_backward_compat_tests.sdk_create_pipeline")
    def test_create_standard_legacy_pipeline_bypasses_schema_fallback(self, sdk_create):
        self.ws.pipelines.create.return_value = SimpleNamespace(pipeline_id="p1")
        self.ws.pipelines.get.return_value = SimpleNamespace(
            spec=self._legacy_spec("bronze", self.conf.source_main_whl_remote)
        )

        pipeline_id = self.runner.create_pipeline(
            self.conf, "pipeline", "bronze", "A1", "bronze"
        )

        self.assertEqual(pipeline_id, "p1")
        sdk_create.assert_not_called()
        kwargs = self.ws.pipelines.create.call_args.kwargs
        self.assertEqual(kwargs["target"], "bronze")
        self.assertFalse(kwargs["serverless"])
        self.assertNotIn("schema", kwargs)
        self.assertEqual(kwargs["clusters"][0].num_workers, 2)

    @patch("integration_tests.run_backward_compat_tests.sdk_create_pipeline")
    def test_pipeline_verification_accepts_extensionless_workspace_path(
        self, sdk_create
    ):
        self.ws.pipelines.create.return_value = SimpleNamespace(
            pipeline_id="p1"
        )
        spec = self._legacy_spec(
            "bronze", self.conf.source_main_whl_remote
        )
        spec.libraries[0].notebook.path = (
            "/runners/init_dlt_meta_pipeline"
        )
        self.ws.pipelines.get.return_value = SimpleNamespace(spec=spec)

        self.assertEqual(
            self.runner.create_pipeline(
                self.conf, "pipeline", "bronze", "A1", "bronze"
            ),
            "p1",
        )
        sdk_create.assert_not_called()

    def test_pipeline_verification_uses_explicit_profile_for_shared_key(self):
        target_profile = SimpleNamespace(
            pipeline_config_whl_key="sdp_meta_whl",
            runner_notebook_local_path="/local/target_runner.py",
        )
        spec = self._serverless_spec(
            "bronze",
            self.conf.target_main_whl_remote,
            wheel_key="sdp_meta_whl",
        )
        spec.libraries[0].notebook.path = "/runners/target_runner"
        self.ws.pipelines.get.return_value = SimpleNamespace(spec=spec)
        self.conf.pipeline_mode = "serverless_dpm"

        self.runner._verify_pipeline_mode(
            self.conf,
            "pipeline-id",
            "bronze",
            {"sdp_meta_whl": self.conf.target_main_whl_remote},
            target_profile,
        )

    def test_pipeline_verification_rejects_retained_legacy_wheel_key(self):
        self.conf.pipeline_mode = "serverless_dpm"
        spec = self._serverless_spec(
            "bronze",
            self.conf.target_main_whl_remote,
            wheel_key="sdp_meta_whl",
        )
        spec.configuration["dlt_meta_whl"] = (
            self.conf.source_main_whl_remote
        )
        self.ws.pipelines.get.return_value = SimpleNamespace(spec=spec)

        with self.assertRaisesRegex(
            RuntimeError, "dlt_meta_whl"
        ):
            self.runner._verify_pipeline_mode(
                self.conf,
                "pipeline-id",
                "bronze",
                {"sdp_meta_whl": self.conf.target_main_whl_remote},
                self.conf.target_profile,
            )

    def test_swap_standard_legacy_pipeline_preserves_target_and_cluster(self):
        self.conf.bronze_a1_pipeline_id = "bronze-a1"
        self.conf.bronze_a2_pipeline_id = "bronze-a2"
        self.conf.silver_pipeline_id = "silver"
        self.ws.pipelines.get.side_effect = [
            SimpleNamespace(spec=self._legacy_spec("bronze", self.conf.source_main_whl_remote)),
            SimpleNamespace(spec=self._legacy_spec(
                "bronze", self.conf.target_main_whl_remote,
                wheel_key="sdp_meta_whl",
            )),
            SimpleNamespace(spec=self._legacy_spec("bronze", self.conf.source_main_whl_remote)),
            SimpleNamespace(spec=self._legacy_spec(
                "bronze", self.conf.target_main_whl_remote,
                wheel_key="sdp_meta_whl",
            )),
            SimpleNamespace(spec=self._legacy_spec("silver", self.conf.source_main_whl_remote)),
            SimpleNamespace(spec=self._legacy_spec(
                "silver", self.conf.target_main_whl_remote,
                wheel_key="sdp_meta_whl",
            )),
        ]

        self.runner.swap_pipelines_to_target(self.conf)

        self.assertEqual(self.ws.pipelines.update.call_count, 3)
        calls = self.ws.pipelines.update.call_args_list
        self.assertEqual(calls[0].kwargs["target"], "bronze")
        self.assertEqual(calls[1].kwargs["target"], "bronze")
        self.assertEqual(calls[2].kwargs["target"], "silver")
        for call in calls:
            self.assertFalse(call.kwargs["serverless"])
            self.assertNotIn("schema", call.kwargs)
            self.assertEqual(call.kwargs["clusters"][0].num_workers, 2)
            self.assertEqual(
                call.kwargs["configuration"]["sdp_meta_whl"],
                self.conf.target_main_whl_remote,
            )
            self.assertNotIn("dlt_meta_whl", call.kwargs["configuration"])
            notebook = call.kwargs["libraries"][0].notebook.path
            self.assertTrue(notebook.endswith("/init_sdp_meta_pipeline.py"))

    def _serverless_spec(self, schema, wheel, *, wheel_key="dlt_meta_whl"):
        runner = (
            "init_dlt_meta_pipeline.py"
            if wheel_key == "dlt_meta_whl"
            else "init_sdp_meta_pipeline.py"
        )
        return SimpleNamespace(
            catalog="catalog",
            name="pipeline",
            target=None,
            schema=schema,
            serverless=True,
            clusters=[],
            libraries=[
                SimpleNamespace(
                    notebook=SimpleNamespace(path=f"/runners/{runner}")
                )
            ],
            configuration={wheel_key: wheel},
        )

    @patch("integration_tests.run_backward_compat_tests.sdk_create_pipeline")
    def test_create_serverless_pipeline_verifies_wheel_key(self, sdk_create):
        self.conf.pipeline_mode = "serverless_dpm"
        sdk_create.return_value = SimpleNamespace(pipeline_id="p1")
        self.ws.pipelines.get.return_value = SimpleNamespace(
            spec=self._serverless_spec("bronze", self.conf.source_main_whl_remote)
        )

        pipeline_id = self.runner.create_pipeline(
            self.conf, "pipeline", "bronze", "A1", "bronze"
        )

        self.assertEqual(pipeline_id, "p1")
        sdk_create.assert_called_once()
        kwargs = sdk_create.call_args.kwargs
        self.assertEqual(kwargs["schema"], "bronze")
        self.assertTrue(kwargs["serverless"])

    @patch("integration_tests.run_backward_compat_tests.sdk_create_pipeline")
    def test_create_serverless_pipeline_fails_on_wrong_wheel(self, sdk_create):
        self.conf.pipeline_mode = "serverless_dpm"
        sdk_create.return_value = SimpleNamespace(pipeline_id="p1")
        self.ws.pipelines.get.return_value = SimpleNamespace(
            spec=self._serverless_spec("bronze", "/Volumes/test/stale.whl")
        )

        with self.assertRaisesRegex(RuntimeError, "dlt_meta_whl"):
            self.runner.create_pipeline(
                self.conf, "pipeline", "bronze", "A1", "bronze"
            )

    @patch("integration_tests.run_backward_compat_tests.sdk_create_pipeline")
    def test_create_serverless_pipeline_fails_on_wrong_runner(self, sdk_create):
        self.conf.pipeline_mode = "serverless_dpm"
        sdk_create.return_value = SimpleNamespace(pipeline_id="p1")
        spec = self._serverless_spec(
            "bronze", self.conf.source_main_whl_remote
        )
        spec.libraries[0].notebook.path = "/runners/stale_runner.py"
        self.ws.pipelines.get.return_value = SimpleNamespace(spec=spec)

        with self.assertRaisesRegex(RuntimeError, "notebook libraries"):
            self.runner.create_pipeline(
                self.conf, "pipeline", "bronze", "A1", "bronze"
            )

    def test_standard_legacy_requires_positive_worker_count(self):
        runner = BackwardCompatRunner(
            {
                "uc_catalog_name": "catalog",
                "pipeline_mode": "standard_legacy",
            },
            self.ws,
        )
        self.ws.current_user.me.return_value = SimpleNamespace(
            user_name="test@example.com"
        )

        with self.assertRaisesRegex(ValueError, "pipeline_num_workers"):
            runner._build_runner_conf()

    def test_phase2_validator_receives_source_profile(self):
        self.runner.build_phase2_job(self.conf)

        validate_task = next(
            task
            for task in self.ws.jobs.create.call_args.kwargs["tasks"]
            if task.task_key == "phase2_validate"
        )
        self.assertEqual(
            validate_task.notebook_task.base_parameters["source_profile"],
            "legacy",
        )

    def test_migrate_phase1_job_updates_wheel_tasks_and_environment(self):
        self.conf.phase1_job_id = 42
        self.conf.source_main_whl_remote = (
            "/Volumes/test/dlt_meta-0.0.10-py3-none-any.whl"
        )
        self.conf.target_main_whl_remote = (
            "/Volumes/test/"
            "databricks_labs_sdp_meta-0.2.0-py3-none-any.whl"
        )
        source_settings = jobs.JobSettings(
            tasks=[
                jobs.Task(
                    task_key="onboard",
                    environment_key="source-env",
                    python_wheel_task=jobs.PythonWheelTask(
                        package_name=LEGACY.distribution,
                        entry_point="run",
                    ),
                ),
                jobs.Task(
                    task_key="pipeline",
                    pipeline_task=jobs.PipelineTask(
                        pipeline_id="pipeline-id"
                    ),
                ),
            ],
            environments=[
                jobs.JobEnvironment(
                    environment_key="source-env",
                    spec=compute.Environment(
                        client="1",
                        dependencies=[
                            self.conf.source_main_whl_remote,
                            "customer-package==1.2.3",
                        ],
                    ),
                )
            ],
        )
        get_calls = 0

        def get_job(_job_id):
            nonlocal get_calls
            get_calls += 1
            if get_calls == 1:
                return SimpleNamespace(settings=source_settings)
            api_settings = copy.deepcopy(
                self.ws.jobs.update.call_args.kwargs["new_settings"]
            )
            api_settings.environments[0].spec.environment_version = None
            api_settings.environments[0].spec.client = "2"
            return SimpleNamespace(settings=api_settings)

        self.ws.jobs.get.side_effect = get_job

        migrated_from_api = self.runner.migrate_phase1_job_to_target(
            self.conf
        )

        update = self.ws.jobs.update.call_args.kwargs
        self.assertEqual(update["job_id"], 42)
        migrated = update["new_settings"]
        wheel_task = next(
            task.python_wheel_task
            for task in migrated.tasks
            if task.python_wheel_task is not None
        )
        self.assertEqual(
            wheel_task.package_name,
            self.conf.target_profile.distribution,
        )
        self.assertEqual(
            migrated.environments[0].spec.dependencies,
            [
                self.conf.target_main_whl_remote,
                "customer-package==1.2.3",
            ],
        )
        self.assertIsNone(migrated.environments[0].spec.client)
        self.assertEqual(
            migrated.environments[0].spec.environment_version,
            "2",
        )
        self.assertEqual(
            source_settings.tasks[0].python_wheel_task.package_name,
            LEGACY.distribution,
        )
        self.assertEqual(
            migrated_from_api.environments[0].spec.client,
            "2",
        )

    def test_migrated_onboarding_smoke_submits_one_non_overwriting_task(self):
        self.conf.phase1_job_id = 42
        settings = jobs.JobSettings(
            tasks=[
                jobs.Task(
                    task_key="phase1_onboard_A2",
                    environment_key="target-env",
                    python_wheel_task=jobs.PythonWheelTask(
                        package_name=CURRENT.distribution,
                        entry_point="run",
                        named_parameters={
                            "onboarding_file_path": "A2.json",
                            "overwrite": "False",
                        },
                    ),
                ),
                jobs.Task(
                    task_key="phase1_onboard_A1",
                    environment_key="target-env",
                    python_wheel_task=jobs.PythonWheelTask(
                        package_name=CURRENT.distribution,
                        entry_point="run",
                        named_parameters={"overwrite": "True"},
                    ),
                )
            ],
            environments=[
                jobs.JobEnvironment(
                    environment_key="target-env",
                    spec=compute.Environment(
                        client="2",
                        dependencies=[self.conf.target_main_whl_remote],
                    ),
                )
            ],
        )
        self.ws.jobs.get.return_value = SimpleNamespace(settings=settings)
        self.ws.jobs.submit.return_value.result.return_value = SimpleNamespace(
            state=SimpleNamespace(result_state=jobs.RunResultState.SUCCESS)
        )

        self.runner.run_migrated_onboarding_smoke(self.conf, settings)

        submit = self.ws.jobs.submit.call_args.kwargs
        self.assertEqual(len(submit["tasks"]), 1)
        task = submit["tasks"][0]
        self.assertIsInstance(task, jobs.SubmitTask)
        self.assertEqual(task.task_key, "migrated_onboarding_smoke")
        self.assertEqual(
            task.python_wheel_task.named_parameters["overwrite"],
            "False",
        )
        self.assertNotIn(
            "onboarding_file_path",
            task.python_wheel_task.named_parameters,
        )
        self.assertEqual(len(submit["environments"]), 1)

    def test_phase2_runs_incremental_load_before_pipelines(self):
        self.conf.bronze_a1_pipeline_id = "bronze-a1"
        self.conf.silver_pipeline_id = "silver"

        self.runner.build_phase2_job(self.conf)

        kwargs = self.ws.jobs.create.call_args.kwargs
        self.assertIsNone(kwargs["environments"])
        phase2_bronze = next(
            task for task in kwargs["tasks"]
            if task.task_key == "phase2_bronze"
        )
        self.assertEqual(
            phase2_bronze.depends_on[0].task_key,
            "phase2_add_incremental",
        )

    def test_phase2_append_onboarding_runs_twice_before_pipelines(self):
        self.conf.phase2_append_onboarding = True
        self.conf.phase2_onboarding_file = "conf/phase2.json"
        self.conf.uc_volume_path = "/Volumes/catalog/specs/volume/"
        self.conf.bronze_a1_pipeline_id = "bronze-a1"
        self.conf.silver_pipeline_id = "silver"

        self.runner.build_phase2_job(self.conf)

        kwargs = self.ws.jobs.create.call_args.kwargs
        self.assertEqual(
            kwargs["environments"][0].spec.dependencies,
            ["/Volumes/test/target.whl"],
        )
        tasks = {task.task_key: task for task in kwargs["tasks"]}
        first = tasks["phase2_append_onboarding"]
        repeat = tasks["phase2_append_onboarding_idempotency"]
        self.assertEqual(
            first.python_wheel_task.package_name,
            "databricks_labs_sdp_meta",
        )
        self.assertEqual(
            first.python_wheel_task.named_parameters["overwrite"],
            "False",
        )
        self.assertEqual(
            first.python_wheel_task.named_parameters[
                "onboarding_file_path"
            ],
            "/Volumes/catalog/specs/volume/conf/phase2.json",
        )
        self.assertEqual(
            repeat.depends_on[0].task_key,
            "phase2_append_onboarding",
        )
        self.assertEqual(
            tasks["phase2_bronze"].depends_on[0].task_key,
            "phase2_append_onboarding_idempotency",
        )
        self.assertEqual(
            tasks["phase2_validate"].notebook_task.base_parameters[
                "phase2_append_onboarding"
            ],
            "True",
        )

    def test_phase2_append_onboarding_uses_target_distribution(self):
        self.conf.target_profile = LEGACY
        self.conf.phase2_append_onboarding = True
        self.conf.phase2_onboarding_file = "conf/phase2.json"
        self.conf.uc_volume_path = "/Volumes/catalog/specs/volume/"
        self.conf.bronze_a1_pipeline_id = "bronze-a1"
        self.conf.silver_pipeline_id = "silver"

        self.runner.build_phase2_job(self.conf)

        tasks = {
            task.task_key: task
            for task in self.ws.jobs.create.call_args.kwargs["tasks"]
        }
        self.assertEqual(
            tasks["phase2_append_onboarding"].python_wheel_task.package_name,
            LEGACY.distribution,
        )

    def test_phase2_append_onboarding_fixture_has_current_fields_and_new_row(
        self,
    ):
        self.conf.phase2_append_onboarding = True
        self.conf.uc_volume_path = "/Volumes/catalog/specs/volume/"
        self.conf.uc_catalog_name = "catalog"
        self.conf.bronze_schema = "bronze"
        self.conf.silver_schema = "silver"
        with tempfile.TemporaryDirectory() as tmp:
            self.conf.a1_onboarding_file = os.path.join(tmp, "a1.json")
            self.conf.a2_onboarding_file = os.path.join(tmp, "a2.json")
            self.conf.phase2_onboarding_file = os.path.join(
                tmp, "phase2.json"
            )

            self.runner.generate_onboarding_files(self.conf)

            with open(
                self.conf.phase2_onboarding_file, encoding="utf-8"
            ) as fh:
                payload = json.load(fh)

        inserted = next(
            row
            for row in payload
            if row["data_flow_id"] == "schema-evolution-new"
        )
        self.assertEqual(
            inserted["data_flow_group"], "schema_evolution"
        )
        self.assertTrue(inserted["bronze_cluster_by_auto"])
        self.assertTrue(inserted["silver_cluster_by_auto"])

    @patch("integration_tests.run_backward_compat_tests.GitRefWheelBuilder")
    def test_build_wheels_builds_only_source_and_canonical_target(
        self, builder_cls
    ):
        self.conf.build_target_from_worktree = True
        builder = builder_cls.return_value
        builder.build.return_value = "/tmp/dlt_meta-0.0.10-py3-none-any.whl"
        builder.build_from_worktree.return_value = (
            "/tmp/databricks_labs_sdp_meta-0.2.0-py3-none-any.whl"
        )

        self.runner.build_wheels(self.conf)

        builder.build.assert_called_once_with("v0.0.10")
        builder.build_from_worktree.assert_called_once_with()
        self.assertEqual(
            self.conf.target_main_whl_local,
            "/tmp/databricks_labs_sdp_meta-0.2.0-py3-none-any.whl",
        )
        builder.cleanup.assert_called_once()

    def test_upload_runner_notebooks_ignores_non_file_entries(self):
        with tempfile.TemporaryDirectory() as tmp:
            runners = os.path.join(
                tmp, "notebooks", "backward_compat_runners"
            )
            os.makedirs(os.path.join(runners, "__pycache__"))
            notebook = os.path.join(runners, "validate.py")
            with open(notebook, "wb") as fh:
                fh.write(b"print('valid runner')\n")
            self.conf.int_tests_dir = tmp

            self.runner.upload_runner_notebooks(self.conf)

        self.ws.workspace.upload.assert_called_once()
        upload = self.ws.workspace.upload.call_args.kwargs
        self.assertTrue(upload["path"].endswith("/validate.py"))
        self.assertEqual(upload["content"], b"print('valid runner')\n")

    def test_pypi_phase2_rewrites_runner_with_force_reinstall(self):
        self.conf.install_mode = "pypi"
        source = (
            b"sdp_meta_whl = spark.conf.get('sdp_meta_whl')\n"
            b"%pip install $sdp_meta_whl\n"
        )

        rendered = self.runner._notebook_source_for_upload(
            self.conf,
            "init_sdp_meta_pipeline.py",
            source,
            phase2=True,
        )

        self.assertIn(
            b"%pip install --force-reinstall $sdp_meta_whl",
            rendered,
        )

    def test_pypi_phase1_rewrites_legacy_runner_with_force_reinstall(self):
        self.conf.install_mode = "pypi"
        source = (
            b"dlt_meta_whl = spark.conf.get('dlt_meta_whl')\n"
            b"%pip install $dlt_meta_whl\n"
        )

        rendered = self.runner._notebook_source_for_upload(
            self.conf,
            "init_dlt_meta_pipeline.py",
            source,
            phase2=False,
        )

        self.assertIn(
            b"%pip install --force-reinstall $dlt_meta_whl",
            rendered,
        )

    def test_pypi_run_reuploads_phase2_runner(self):
        self.conf.install_mode = "pypi"
        self.conf.phase2_append_onboarding = True
        self.runner._build_runner_conf = MagicMock(return_value=self.conf)
        for method_name in (
            "build_wheels",
            "initialize_uc_resources",
            "generate_onboarding_files",
            "upload_files",
            "create_all_pipelines",
            "launch_job",
            "download_phase_output",
            "migrate_phase1_job_to_target",
            "run_migrated_onboarding_smoke",
            "swap_pipelines_to_target",
        ):
            setattr(self.runner, method_name, MagicMock())
        self.runner.build_phase1_job = MagicMock(
            return_value=SimpleNamespace(job_id=1)
        )
        self.runner.build_phase2_job = MagicMock(
            return_value=SimpleNamespace(job_id=2)
        )
        self.runner.upload_runner_notebooks = MagicMock()

        self.assertEqual(self.runner.run(), 0)

        self.runner.upload_runner_notebooks.assert_called_once_with(
            self.conf, phase2=True
        )
        self.assertEqual(
            [call.args[0] for call in self.runner.launch_job.call_args_list],
            [1, 2],
        )
        self.runner.migrate_phase1_job_to_target.assert_called_once_with(
            self.conf
        )
        self.runner.run_migrated_onboarding_smoke.assert_not_called()

    def test_current_cross_version_upgrade_runs_target_entry_point(self):
        self.conf.source_profile = CURRENT
        self.conf.source_ref = "v0.1.1"
        self.runner._build_runner_conf = MagicMock(return_value=self.conf)
        for method_name in (
            "build_wheels",
            "initialize_uc_resources",
            "generate_onboarding_files",
            "upload_files",
            "create_all_pipelines",
            "launch_job",
            "download_phase_output",
            "migrate_phase1_job_to_target",
            "run_migrated_onboarding_smoke",
            "swap_pipelines_to_target",
        ):
            setattr(self.runner, method_name, MagicMock())
        self.runner.build_phase1_job = MagicMock(
            return_value=SimpleNamespace(job_id=1)
        )
        self.runner.build_phase2_job = MagicMock(
            return_value=SimpleNamespace(job_id=2)
        )
        migrated_settings = jobs.JobSettings()
        self.runner.migrate_phase1_job_to_target.return_value = (
            migrated_settings
        )
        events = []
        self.runner.download_phase_output.side_effect = (
            lambda *_args, **_kwargs: events.append("validate")
        )
        self.runner.run_migrated_onboarding_smoke.side_effect = (
            lambda *_args, **_kwargs: events.append("smoke")
        )

        self.assertEqual(self.runner.run(), 0)

        self.runner.migrate_phase1_job_to_target.assert_called_once_with(
            self.conf
        )
        self.runner.run_migrated_onboarding_smoke.assert_called_once_with(
            self.conf, migrated_settings
        )
        self.assertEqual(events, ["validate", "validate", "smoke"])

    def test_same_ref_with_different_install_specs_requires_migration(self):
        self.conf.source_profile = CURRENT
        self.conf.target_profile = CURRENT
        self.conf.source_ref = self.conf.target_ref = "main"

        self.assertTrue(self.runner._requires_job_migration(self.conf))

    def test_same_profile_and_install_spec_skips_job_migration(self):
        self.conf.source_profile = CURRENT
        self.conf.target_profile = CURRENT
        self.conf.source_ref = self.conf.target_ref = "main"
        self.conf.target_main_whl_remote = self.conf.source_main_whl_remote

        self.assertFalse(self.runner._requires_job_migration(self.conf))

    def test_download_phase_output_rejects_failed_assertions(self):
        self.ws.workspace.download.return_value = BytesIO(
            b',0\n0,"A compatibility invariant. Failed!"\n'
        )
        with tempfile.TemporaryDirectory() as tmp:
            output = os.path.join(tmp, "phase2.csv")

            with self.assertRaisesRegex(
                AssertionError, "compatibility invariant"
            ):
                self.runner.download_phase_output("/Workspace/phase2.csv", output)

    def test_download_phase_output_accepts_passing_report(self):
        self.ws.workspace.download.return_value = BytesIO(
            b',0\n0,"A compatibility invariant. Passed!"\n'
        )
        with tempfile.TemporaryDirectory() as tmp:
            output = os.path.join(tmp, "phase2.csv")

            self.assertEqual(
                self.runner.download_phase_output("/Workspace/phase2.csv", output),
                output,
            )

    @patch("integration_tests.run_backward_compat_tests.time.sleep")
    def test_download_phase_output_retries_transient_download_failure(
        self, sleep
    ):
        self.ws.workspace.download.side_effect = [
            OSError("temporary workspace error"),
            BytesIO(b',0\n0,"A compatibility invariant. Passed!"\n'),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            output = os.path.join(tmp, "phase2.csv")

            self.assertEqual(
                self.runner.download_phase_output("/Workspace/phase2.csv", output),
                output,
            )

        self.assertEqual(self.ws.workspace.download.call_count, 2)
        sleep.assert_called_once_with(2)

    @patch("integration_tests.run_backward_compat_tests.time.sleep")
    def test_download_phase_output_reports_exhausted_retries(self, sleep):
        self.ws.workspace.download.side_effect = OSError(
            "temporary workspace error"
        )
        with tempfile.TemporaryDirectory() as tmp:
            output = os.path.join(tmp, "phase2.csv")

            with self.assertRaisesRegex(
                RuntimeError,
                "after 3 attempts; validation status is unavailable",
            ):
                self.runner.download_phase_output("/Workspace/phase2.csv", output)

        self.assertEqual(self.ws.workspace.download.call_count, 3)
        self.assertEqual(sleep.call_args_list, [mock_call(2), mock_call(4)])

    def test_pypi_mode_derives_source_and_target_package_versions(self):
        self.ws.current_user.me.return_value = SimpleNamespace(
            user_name="test@example.com"
        )
        runner = BackwardCompatRunner(
            {
                "uc_catalog_name": "catalog",
                "install_mode": "pypi",
                "source_version": "v0.0.10",
                "target_version": "v0.2.0",
            },
            self.ws,
        )

        conf = runner._build_runner_conf()

        self.assertEqual(conf.source_package_version, "0.0.10")
        self.assertEqual(conf.target_package_version, "0.2.0")
        self.assertEqual(runner.install_spec_source_main(conf), "dlt-meta==0.0.10")
        self.assertEqual(
            runner.install_spec_target_main(conf),
            "databricks-labs-sdp-meta==0.2.0",
        )

    def test_pypi_mode_requires_version_for_non_release_target_refs(self):
        self.ws.current_user.me.return_value = SimpleNamespace(
            user_name="test@example.com"
        )
        for target_ref in ("main", "origin/main", "feature/example"):
            args = {
                "uc_catalog_name": "catalog",
                "install_mode": "pypi",
            }
            if target_ref != "main":
                args["target_version"] = target_ref
            runner = BackwardCompatRunner(args, self.ws)

            with self.subTest(target_ref=target_ref), self.assertRaisesRegex(
                ValueError,
                r"requires a released package version.*--target_version",
            ):
                runner._build_runner_conf()

    def test_pypi_mode_accepts_explicit_package_version_for_main(self):
        self.ws.current_user.me.return_value = SimpleNamespace(
            user_name="test@example.com"
        )
        runner = BackwardCompatRunner(
            {
                "uc_catalog_name": "catalog",
                "install_mode": "pypi",
                "target_package_version": "0.2.0rc1",
            },
            self.ws,
        )

        conf = runner._build_runner_conf()

        self.assertEqual(conf.target_ref, "main")
        self.assertEqual(conf.target_package_version, "0.2.0rc1")
        self.assertEqual(
            runner.install_spec_target_main(conf),
            "databricks-labs-sdp-meta==0.2.0rc1",
        )

    def test_pypi_mode_uses_legacy_target_distribution(self):
        self.conf.install_mode = "pypi"
        self.conf.target_profile = LEGACY
        self.conf.target_package_version = "0.0.10"

        self.assertEqual(
            self.runner.install_spec_target_main(self.conf),
            "dlt-meta==0.0.10",
        )

    def test_pypi_mode_rejects_invalid_source_package_version(self):
        self.ws.current_user.me.return_value = SimpleNamespace(
            user_name="test@example.com"
        )
        runner = BackwardCompatRunner(
            {
                "uc_catalog_name": "catalog",
                "install_mode": "pypi",
                "source_package_version": "0.0.10 --pre",
            },
            self.ws,
        )

        with self.assertRaisesRegex(ValueError, "source_package_version"):
            runner._build_runner_conf()
