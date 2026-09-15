"""Unit tests for standard-compute legacy upgrade orchestration."""

import os
import tempfile
import zipfile
from io import BytesIO
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, call as mock_call, patch

from databricks.sdk.service.pipelines import PipelineCluster

from integration_tests.run_backward_compat_tests import (
    BCRunnerConf,
    BackwardCompatRunner,
)
from integration_tests.version_profiles import CURRENT, LEGACY


class StandardLegacyUpgradeRunnerTests(TestCase):
    """Ensure standard-compute tests retain legacy pipeline publishing."""

    def setUp(self):
        self.ws = MagicMock()
        self.runner = BackwardCompatRunner({}, self.ws)
        self.conf = BCRunnerConf(
            run_id="test-run",
            uc_catalog_name="catalog",
            source_ref="v0.0.10",
            target_ref="v0.1.0",
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

    def _legacy_spec(self, target, wheel):
        return SimpleNamespace(
            catalog="catalog",
            name="pipeline",
            target=target,
            schema=None,
            serverless=False,
            clusters=[PipelineCluster(label="default", num_workers=2)],
            libraries=[],
            configuration={"dlt_meta_whl": wheel},
        )

    def test_standard_legacy_execution_kwargs_use_target_and_cluster(self):
        kwargs = self.runner._pipeline_execution_kwargs(self.conf, "bronze")

        self.assertEqual(kwargs["target"], "bronze")
        self.assertFalse(kwargs["serverless"])
        self.assertNotIn("schema", kwargs)
        self.assertEqual(kwargs["clusters"][0].label, "default")
        self.assertEqual(kwargs["clusters"][0].num_workers, 2)

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

    def test_swap_standard_legacy_pipeline_preserves_target_and_cluster(self):
        self.conf.bronze_a1_pipeline_id = "bronze-a1"
        self.conf.bronze_a2_pipeline_id = "bronze-a2"
        self.conf.silver_pipeline_id = "silver"
        self.ws.pipelines.get.side_effect = [
            SimpleNamespace(spec=self._legacy_spec("bronze", self.conf.source_main_whl_remote)),
            SimpleNamespace(spec=self._legacy_spec("bronze", self.conf.target_main_whl_remote)),
            SimpleNamespace(spec=self._legacy_spec("bronze", self.conf.source_main_whl_remote)),
            SimpleNamespace(spec=self._legacy_spec("bronze", self.conf.target_main_whl_remote)),
            SimpleNamespace(spec=self._legacy_spec("silver", self.conf.source_main_whl_remote)),
            SimpleNamespace(spec=self._legacy_spec("silver", self.conf.target_main_whl_remote)),
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

    def _serverless_spec(self, schema, wheel):
        return SimpleNamespace(
            catalog="catalog",
            name="pipeline",
            target=None,
            schema=schema,
            serverless=True,
            clusters=[],
            libraries=[],
            configuration={"dlt_meta_whl": wheel},
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

    def test_compat_wheelhouse_uses_legacy_distribution_spec(self):
        self.conf.target_install_surface = "compat_wheelhouse"
        self.conf.target_package_version = "0.1.0"

        self.assertEqual(
            self.runner.install_spec_target_main(self.conf),
            "dlt-meta==0.1.0",
        )
        config = self.runner._build_phase2_pipeline_config(
            self.conf, "bronze", "A1"
        )
        self.assertEqual(config["dlt_meta_whl"], "dlt-meta==0.1.0")

    @patch.object(BackwardCompatRunner, "_download_compat_runtime_wheels")
    @patch("integration_tests.run_backward_compat_tests.GitRefWheelBuilder")
    def test_compat_wheelhouse_builds_primary_redirect_and_dependency_wheels(
        self, builder_cls, download_runtime_wheels
    ):
        self.conf.target_install_surface = "compat_wheelhouse"
        self.conf.build_target_from_worktree = True
        builder = builder_cls.return_value
        builder.build.return_value = "/tmp/dlt_meta-0.0.10-py3-none-any.whl"
        builder.build_from_worktree.side_effect = [
            "/tmp/databricks_labs_sdp_meta-0.1.0-py3-none-any.whl",
            "/tmp/dlt_meta-0.1.0-py3-none-any.whl",
        ]

        self.runner.build_wheels(self.conf)

        builder.build.assert_called_once_with("v0.0.10")
        self.assertEqual(
            builder.build_from_worktree.call_args_list,
            [mock_call(), mock_call(subdir="compat")],
        )
        self.assertEqual(
            self.conf.target_compat_whl_local,
            "/tmp/dlt_meta-0.1.0-py3-none-any.whl",
        )
        # target_package_version was left unset (None) -> derived from the
        # built wheels' shared version.
        self.assertEqual(self.conf.target_package_version, "0.1.0")
        download_runtime_wheels.assert_called_once_with(self.conf)
        builder.cleanup.assert_called_once()

    def test_compat_wheelhouse_renders_find_links_only_in_uploaded_copy(self):
        self.conf.target_install_surface = "compat_wheelhouse"
        self.conf.target_wheelhouse_remote = "/Volumes/test/wheelhouse/"
        source = b"dlt_meta_whl = spark.conf.get('dlt_meta_whl')\n%pip install $dlt_meta_whl\n"

        rendered = self.runner._notebook_source_for_upload(
            self.conf, "init_dlt_meta_pipeline.py", source
        )

        self.assertIn(
            b"%pip install --force-reinstall --no-index --find-links "
            b"/Volumes/test/wheelhouse/ $dlt_meta_whl",
            rendered,
        )
        self.assertNotEqual(rendered, source)
        self.assertIn(b"%pip install $dlt_meta_whl", source)

    def test_compat_install_flags_are_uploaded_only_for_phase2(self):
        self.conf.target_install_surface = "compat_wheelhouse"
        self.conf.target_wheelhouse_remote = "/Volumes/test/wheelhouse/"

        self.runner.upload_runner_notebooks(self.conf)
        phase1_upload = next(
            call
            for call in self.ws.workspace.upload.call_args_list
            if call.kwargs["path"].endswith("/init_dlt_meta_pipeline.py")
        )
        self.assertIn(b"%pip install $dlt_meta_whl", phase1_upload.kwargs["content"])
        self.assertNotIn(b"--force-reinstall", phase1_upload.kwargs["content"])

        self.ws.workspace.upload.reset_mock()
        self.runner.upload_runner_notebooks(self.conf, phase2=True)
        phase2_upload = next(
            call
            for call in self.ws.workspace.upload.call_args_list
            if call.kwargs["path"].endswith("/init_dlt_meta_pipeline.py")
        )
        self.assertIn(
            b"%pip install --force-reinstall --no-index --find-links",
            phase2_upload.kwargs["content"],
        )

    def test_pypi_phase2_rewrites_runner_with_force_reinstall(self):
        self.conf.install_mode = "pypi"
        source = (
            b"dlt_meta_whl = spark.conf.get('dlt_meta_whl')\n"
            b"%pip install $dlt_meta_whl\n"
        )

        rendered = self.runner._notebook_source_for_upload(
            self.conf, "init_dlt_meta_pipeline.py", source
        )

        self.assertIn(
            b"%pip install --force-reinstall $dlt_meta_whl",
            rendered,
        )

    def test_pypi_run_reuploads_phase2_runner(self):
        self.conf.install_mode = "pypi"
        self.runner._build_runner_conf = MagicMock(return_value=self.conf)
        for method_name in (
            "build_wheels",
            "initialize_uc_resources",
            "generate_onboarding_files",
            "upload_files",
            "create_all_pipelines",
            "launch_job",
            "download_phase_output",
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

    def test_compat_wheelhouse_requires_local_cross_namespace_upgrade(self):
        self.ws.current_user.me.return_value = SimpleNamespace(
            user_name="test@example.com"
        )
        git_runner = BackwardCompatRunner(
            {
                "uc_catalog_name": "catalog",
                "target_install_surface": "compat_wheelhouse",
                "install_mode": "git",
            },
            self.ws,
        )
        with self.assertRaisesRegex(ValueError, "requires --install_mode=local"):
            git_runner._build_runner_conf()

        same_namespace_runner = BackwardCompatRunner(
            {
                "uc_catalog_name": "catalog",
                "target_install_surface": "compat_wheelhouse",
                "source_version": "v0.1.0",
                "target_version": "v0.1.1",
            },
            self.ws,
        )
        with self.assertRaisesRegex(ValueError, "legacy-to-current"):
            same_namespace_runner._build_runner_conf()

    def test_compat_python_version_must_be_major_minor(self):
        self.ws.current_user.me.return_value = SimpleNamespace(
            user_name="test@example.com"
        )
        for bad in ("3.12.1", "312", "cp312"):
            runner = BackwardCompatRunner(
                {"uc_catalog_name": "catalog", "compat_python_version": bad},
                self.ws,
            )
            with self.assertRaisesRegex(ValueError, "MAJOR.MINOR"):
                runner._build_runner_conf()

        ok_runner = BackwardCompatRunner(
            {"uc_catalog_name": "catalog", "compat_python_version": "3.11"},
            self.ws,
        )
        self.assertEqual(
            ok_runner._build_runner_conf().compat_python_version, "3.11"
        )

    def test_pypi_mode_derives_source_and_target_package_versions(self):
        self.ws.current_user.me.return_value = SimpleNamespace(
            user_name="test@example.com"
        )
        runner = BackwardCompatRunner(
            {
                "uc_catalog_name": "catalog",
                "install_mode": "pypi",
                "source_version": "v0.0.10",
                "target_version": "v0.1.0",
            },
            self.ws,
        )

        conf = runner._build_runner_conf()

        self.assertEqual(conf.source_package_version, "0.0.10")
        self.assertEqual(conf.target_package_version, "0.1.0")
        self.assertEqual(runner.install_spec_source_main(conf), "dlt-meta==0.0.10")
        self.assertEqual(runner.install_spec_target_main(conf), "dlt-meta==0.1.0")

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


class CompatWheelhouseVersionTests(TestCase):
    """Version-pinning and wheelhouse-completeness guards for compat mode."""

    def setUp(self):
        self.ws = MagicMock()
        self.runner = BackwardCompatRunner({}, self.ws)
        self.conf = BCRunnerConf(
            run_id="test-run",
            uc_catalog_name="catalog",
            source_ref="v0.0.10",
            target_ref="v0.1.0",
            source_profile=LEGACY,
            target_profile=CURRENT,
            target_install_surface="compat_wheelhouse",
        )

    def test_wheel_version_parses_version_field(self):
        self.assertEqual(
            BackwardCompatRunner._wheel_version(
                "/tmp/databricks_labs_sdp_meta-0.2.0-py3-none-any.whl"
            ),
            "0.2.0",
        )

    def test_wheel_version_rejects_malformed_name(self):
        with self.assertRaisesRegex(ValueError, "Unrecognized wheel filename"):
            BackwardCompatRunner._wheel_version("/tmp/not_a_wheel-0.1.0.whl")

    def test_resolve_version_derives_from_wheels_when_unset(self):
        self.conf.target_package_version = None
        self.conf.target_main_whl_local = (
            "/tmp/databricks_labs_sdp_meta-0.2.0-py3-none-any.whl"
        )
        self.conf.target_compat_whl_local = "/tmp/dlt_meta-0.2.0-py3-none-any.whl"

        self.runner._resolve_target_package_version(self.conf)

        self.assertEqual(self.conf.target_package_version, "0.2.0")

    def test_resolve_version_rejects_explicit_mismatch(self):
        self.conf.target_package_version = "0.1.0"
        self.conf.target_main_whl_local = (
            "/tmp/databricks_labs_sdp_meta-0.2.0-py3-none-any.whl"
        )
        self.conf.target_compat_whl_local = "/tmp/dlt_meta-0.2.0-py3-none-any.whl"

        with self.assertRaisesRegex(RuntimeError, "does not match the built"):
            self.runner._resolve_target_package_version(self.conf)

    def test_resolve_version_rejects_disagreeing_wheels(self):
        self.conf.target_package_version = None
        self.conf.target_main_whl_local = (
            "/tmp/databricks_labs_sdp_meta-0.2.0-py3-none-any.whl"
        )
        self.conf.target_compat_whl_local = "/tmp/dlt_meta-0.1.0-py3-none-any.whl"

        with self.assertRaisesRegex(RuntimeError, "disagree on version"):
            self.runner._resolve_target_package_version(self.conf)

    def _make_primary_wheel(self, tmp, requires):
        """Write a minimal wheel zip carrying the given Requires-Dist lines."""
        wheel_path = os.path.join(
            tmp, "databricks_labs_sdp_meta-0.1.0-py3-none-any.whl"
        )
        metadata = ["Metadata-Version: 2.1", "Name: databricks-labs-sdp-meta"]
        metadata += [f"Requires-Dist: {req}" for req in requires]
        with zipfile.ZipFile(wheel_path, "w") as zf:
            zf.writestr(
                "databricks_labs_sdp_meta-0.1.0.dist-info/METADATA",
                "\n".join(metadata) + "\n",
            )
        return wheel_path

    def test_primary_wheel_required_dists_skips_conditional(self):
        with tempfile.TemporaryDirectory() as tmp:
            wheel = self._make_primary_wheel(
                tmp,
                [
                    "databricks-sdk>=0.20,<1",
                    "PyYAML>=6.0,<7",
                    'pytest; extra == "dev"',
                    'tomli; python_version < "3.11"',
                ],
            )
            self.assertEqual(
                BackwardCompatRunner._primary_wheel_required_dists(wheel),
                {"databricks_sdk", "pyyaml"},
            )

    def test_assert_wheelhouse_complete_passes_when_deps_present(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.conf.target_main_whl_local = self._make_primary_wheel(
                tmp, ["databricks-sdk>=0.20,<1", "PyYAML>=6.0,<7"]
            )
            self.conf.target_dependency_whls_local = [
                "/tmp/wh/databricks_sdk-0.30.0-py3-none-any.whl",
                "/tmp/wh/PyYAML-6.0.1-cp312-cp312-manylinux2014_x86_64.whl",
            ]
            # Should not raise.
            self.runner._assert_wheelhouse_complete(self.conf, tmp)

    def test_assert_wheelhouse_complete_flags_missing_dep(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.conf.target_main_whl_local = self._make_primary_wheel(
                tmp, ["databricks-sdk>=0.20,<1", "PyYAML>=6.0,<7"]
            )
            self.conf.target_dependency_whls_local = [
                "/tmp/wh/databricks_sdk-0.30.0-py3-none-any.whl",
            ]
            with self.assertRaisesRegex(RuntimeError, "pyyaml"):
                self.runner._assert_wheelhouse_complete(self.conf, tmp)

    @patch.object(BackwardCompatRunner, "_assert_wheelhouse_complete")
    @patch("integration_tests.run_backward_compat_tests.os.listdir", return_value=[])
    @patch("integration_tests.run_backward_compat_tests.os.makedirs")
    @patch("integration_tests.run_backward_compat_tests.shutil.rmtree")
    @patch("integration_tests.run_backward_compat_tests.subprocess.run")
    def test_download_uses_configured_interpreter_and_timeout(
        self, mock_run, _rmtree, _makedirs, _listdir, _assert_complete
    ):
        self.conf.compat_python_version = "3.11"
        self.conf.target_main_whl_local = (
            "/tmp/databricks_labs_sdp_meta-0.1.0-py3-none-any.whl"
        )
        self.conf.target_compat_whl_local = "/tmp/dlt_meta-0.1.0-py3-none-any.whl"
        mock_run.return_value = SimpleNamespace(returncode=0, stdout="", stderr="")

        self.runner._download_compat_runtime_wheels(self.conf)

        args, kwargs = mock_run.call_args
        command = args[0]
        self.assertIn("--python-version", command)
        self.assertEqual(command[command.index("--python-version") + 1], "3.11")
        self.assertIn("cp311", command)
        self.assertIn("abi3", command)
        self.assertEqual(
            kwargs["timeout"], BackwardCompatRunner.COMPAT_DOWNLOAD_TIMEOUT_SEC
        )
