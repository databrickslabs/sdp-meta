import unittest
import os
from unittest.mock import MagicMock, patch, mock_open
import json
from databricks.sdk.service.catalog import VolumeType
from databricks.labs.sdp_meta.__about__ import __version__
from databricks.labs.sdp_meta.cli import SDP_META_RUNNER_NOTEBOOK, DeployCommand, SDPMeta, OnboardCommand, main


class CliTests(unittest.TestCase):
    onboarding_file_path = "tests/resources/onboarding.json"
    onboard_cmd_with_uc = OnboardCommand(
        onboarding_file_path=onboarding_file_path,
        onboarding_files_dir_path="tests/resources/",
        onboard_layer="bronze",
        env="dev",
        import_author="John Doe",
        version="1.0",
        cloud="aws",
        sdp_meta_schema="sdp_meta",
        bronze_dataflowspec_path="tests/resources/bronze_dataflowspec",
        silver_dataflowspec_path="tests/resources/silver_dataflowspec",
        uc_enabled=True,
        uc_catalog_name="uc_catalog",
        uc_volume_path="uc_catalog/sdp_meta/files",
        overwrite=True,
        bronze_dataflowspec_table="bronze_dataflowspec",
        silver_dataflowspec_table="silver_dataflowspec",
        update_paths=True,
    )

    onboard_cmd_without_uc = OnboardCommand(
        onboarding_file_path=onboarding_file_path,
        onboarding_files_dir_path="tests/resources/",
        onboard_layer="bronze",
        env="dev",
        import_author="John Doe",
        version="1.0",
        cloud="aws",
        sdp_meta_schema="sdp_meta",
        bronze_dataflowspec_path="tests/resources/bronze_dataflowspec",
        silver_dataflowspec_path="tests/resources/silver_dataflowspec",
        uc_enabled=False,
        dbfs_path="/dbfs",
        overwrite=True,
        bronze_dataflowspec_table="bronze_dataflowspec",
        silver_dataflowspec_table="silver_dataflowspec",
        update_paths=True,
    )

    deploy_cmd = DeployCommand(
        layer="bronze_silver",
        onboard_bronze_group="A1",
        onboard_silver_group="A1",
        sdp_meta_bronze_schema="dlt_bronze_schema",
        sdp_meta_silver_schema="dlt_silver_schema",
        dataflowspec_bronze_table="bronze_dataflowspec_table",
        dataflowspec_silver_table="silver_dataflowspec_table",
        num_workers=1,
        uc_catalog_name="uc_catalog",
        pipeline_name="unittest_dlt_pipeline",
        dlt_target_schema="dlt_target_schema",
        uc_enabled=True,
        serverless=False,
        dbfs_path="/dbfs",
    )

    def test_copy_to_dbfs(self):
        mock_ws = MagicMock()
        sdp_meta = SDPMeta(mock_ws)
        with patch("os.walk") as mock_walk:
            mock_walk.return_value = [
                ("/path/to/src", [], ["file1.txt", "file2.txt"]),
                ("/path/to/src/subdir", [], ["file3.txt"]),
            ]
            with patch("builtins.open") as mock_open:
                mock_open.return_value = MagicMock()
                mock_dbfs_upload = MagicMock()
                mock_ws.dbfs.upload = mock_dbfs_upload
                sdp_meta.copy_to_dbfs("file:/path/to/src", "/dbfs/path/to/dst")
                self.assertEqual(mock_dbfs_upload.call_count, 3)

    @patch("databricks.labs.sdp_meta.cli.WorkspaceClient")
    @patch("builtins.open", new_callable=MagicMock)
    def test_onboard_with_uc(self, mock_open, mock_workspace_client):
        mock_jobs = MagicMock()
        mock_open.return_value = MagicMock()
        mock_workspace_client.jobs = mock_jobs
        mock_workspace_client.jobs.create.return_value = MagicMock(job_id="job_id")
        mock_workspace_client.jobs.run_now.return_value = MagicMock(run_id="run_id")
        sdp_meta = SDPMeta(mock_workspace_client)
        sdp_meta._wsi = mock_workspace_client.return_value
        sdp_meta.update_ws_onboarding_paths = MagicMock()
        sdp_meta.create_uc_schema = MagicMock()
        sdp_meta.create_uc_volume = MagicMock()
        sdp_meta.copy_to_uc_volume = MagicMock()
        with patch.object(sdp_meta._wsi, "_upload_wheel", return_value="/path/to/wheel"):
            sdp_meta.onboard(self.onboard_cmd_with_uc)
        sdp_meta.create_uc_volume.assert_called_once_with(
            self.onboard_cmd_with_uc.uc_catalog_name,
            self.onboard_cmd_with_uc.sdp_meta_schema
        )
        sdp_meta.create_uc_schema.assert_called_once_with(
            self.onboard_cmd_with_uc.uc_catalog_name,
            self.onboard_cmd_with_uc.sdp_meta_schema
        )
        mock_workspace_client.jobs.create.assert_called_once()
        mock_workspace_client.jobs.run_now.assert_called_once_with(job_id="job_id")

    @patch("databricks.labs.sdp_meta.cli.WorkspaceClient")
    @patch("builtins.open", new_callable=MagicMock)
    def test_onboard_without_uc(self, mock_open, mock_workspace_client):
        mock_dbfs = MagicMock()
        mock_jobs = MagicMock()
        mock_open.return_value = MagicMock()
        mock_workspace_client.dbfs = mock_dbfs
        mock_workspace_client.jobs = mock_jobs
        mock_workspace_client.dbfs.mkdirs.return_value = None
        mock_workspace_client.dbfs.upload.return_value = None
        mock_copy_to_dbfs = MagicMock()
        mock_workspace_client.jobs.create.return_value = MagicMock(job_id="job_id")
        mock_workspace_client.jobs.run_now.return_value = MagicMock(run_id="run_id")

        sdp_meta = SDPMeta(mock_workspace_client)
        sdp_meta._wsi = mock_workspace_client.return_value
        sdp_meta.copy_to_dbfs = mock_copy_to_dbfs.return_value
        sdp_meta.update_ws_onboarding_paths = MagicMock()
        with patch.object(sdp_meta._wsi, "_upload_wheel", return_value="/path/to/wheel"):
            sdp_meta.onboard(self.onboard_cmd_without_uc)
        mock_workspace_client.dbfs.mkdirs.assert_called_once_with("/dbfs/sdp_meta_conf/")
        # The onboard() upload uses ``with open(...) as ob_file:`` (a
        # context manager), so the object handed to dbfs.upload is the
        # ``__enter__()`` result, not the raw ``open()`` return value.
        mock_workspace_client.dbfs.upload.assert_called_with(
            "/dbfs/sdp_meta_conf/onboarding.json",
            mock_open.return_value.__enter__.return_value,
            overwrite=True
        )
        mock_workspace_client.jobs.create.assert_called_once()
        mock_workspace_client.jobs.run_now.assert_called_once_with(job_id="job_id")

    @patch("databricks.labs.sdp_meta.cli.WorkspaceClient")
    def test_create_onnboarding_job(self, mock_workspace_client):

        mock_workspace_client.jobs.create.return_value = MagicMock(job_id="job_id")
        sdp_meta = SDPMeta(mock_workspace_client)
        sdp_meta._wsi = mock_workspace_client.return_value
        with patch.object(sdp_meta._wsi, "_upload_wheel", return_value="/path/to/wheel"):
            job = sdp_meta.create_onnboarding_job(self.onboard_cmd_with_uc)

        mock_workspace_client.jobs.create.assert_called_once()
        self.assertEqual(job.job_id, "job_id")

    @patch("databricks.labs.sdp_meta.cli.WorkspaceClient")
    def test_create_onboarding_job_uses_wheel_dependency(self, mock_workspace_client):
        mock_workspace_client.jobs.create.return_value = MagicMock(job_id="job_id")
        sdp_meta = SDPMeta(mock_workspace_client)
        whl_path = "/Volumes/main/sdp_meta_wheels/sdp_meta_wheels/databricks_labs_sdp_meta.whl"
        cmd = OnboardCommand(
            onboarding_file_path=self.onboarding_file_path,
            onboarding_files_dir_path="tests/resources/",
            onboard_layer="bronze",
            env="dev",
            import_author="John Doe",
            version="1.0",
            cloud="aws",
            sdp_meta_schema="sdp_meta",
            bronze_dataflowspec_path="tests/resources/bronze_dataflowspec",
            silver_dataflowspec_path="tests/resources/silver_dataflowspec",
            uc_enabled=True,
            uc_catalog_name="uc_catalog",
            # ``_get_onboarding_named_parameters`` reads ``cmd.uc_volume_path``
            # when ``uc_enabled=True`` to compose the remote
            # ``onboarding_file_path`` (``{volume}/sdp_meta_conf/tmp/<file>``).
            # Without this set the path-builder dereferences ``None`` and
            # raises before the wheel-dependency code path under test runs.
            uc_volume_path="/Volumes/uc_catalog/sdp_meta/files",
            overwrite=True,
            bronze_dataflowspec_table="bronze_dataflowspec",
            silver_dataflowspec_table="silver_dataflowspec",
            sdp_meta_dependency=whl_path,
        )

        sdp_meta.create_onnboarding_job(cmd)

        job_kwargs = mock_workspace_client.jobs.create.call_args.kwargs
        self.assertEqual(
            job_kwargs["environments"][0].spec.dependencies,
            [whl_path],
        )

    @patch("databricks.labs.sdp_meta.cli.WorkspaceClient")
    def test_create_onboarding_job_defaults_to_pypi_distribution_name(
        self, mock_workspace_client
    ):
        # Regression guard: with no explicit dependency, the onboarding job
        # must default to the PyPI *distribution* name
        # (databricks-labs-sdp-meta), never the labs project name (sdp-meta),
        # which does not exist on PyPI and fails at environment-install time.
        mock_workspace_client.jobs.create.return_value = MagicMock(job_id="job_id")
        sdp_meta = SDPMeta(mock_workspace_client)
        cmd = OnboardCommand(
            onboarding_file_path=self.onboarding_file_path,
            onboarding_files_dir_path="tests/resources/",
            onboard_layer="bronze",
            env="dev",
            import_author="John Doe",
            version="1.0",
            cloud="aws",
            sdp_meta_schema="sdp_meta",
            bronze_dataflowspec_path="tests/resources/bronze_dataflowspec",
            silver_dataflowspec_path="tests/resources/silver_dataflowspec",
            uc_enabled=True,
            uc_catalog_name="uc_catalog",
            uc_volume_path="/Volumes/uc_catalog/sdp_meta/files",
            overwrite=True,
            bronze_dataflowspec_table="bronze_dataflowspec",
            silver_dataflowspec_table="silver_dataflowspec",
            # sdp_meta_dependency deliberately unset -> PyPI fallback
        )

        sdp_meta.create_onnboarding_job(cmd)

        expected = f"databricks-labs-sdp-meta=={sdp_meta.version}"
        job_kwargs = mock_workspace_client.jobs.create.call_args.kwargs
        self.assertEqual(
            job_kwargs["environments"][0].spec.dependencies,
            [expected],
        )

    @patch("databricks.labs.sdp_meta.cli.WorkspaceClient")
    def test_install_folder(self, mock_workspace_client):
        sdp_meta = SDPMeta(mock_workspace_client)
        sdp_meta._wsi = mock_workspace_client.return_value
        sdp_meta._install_folder = MagicMock(return_value="/Users/name/sdp-meta")
        folder = sdp_meta._install_folder()
        self.assertEqual(folder, "/Users/name/sdp-meta")

    @patch("databricks.labs.sdp_meta.cli.WorkspaceClient")
    def test_create_sdp_meta_pipeline(self, mock_workspace_client):
        mock_workspace_client.pipelines.create.return_value = MagicMock(
            pipeline_id="pipeline_id"
        )
        mock_workspace_client.workspace.mkdirs.return_value = None
        mock_workspace_client.workspace.upload.return_value = None
        sdp_meta = SDPMeta(mock_workspace_client)
        sdp_meta._wsi = mock_workspace_client.return_value
        sdp_meta._wsi._upload_wheel.return_value = None
        sdp_meta._my_username = MagicMock(return_value="name")
        sdp_meta._create_sdp_meta_pipeline(self.deploy_cmd)
        runner_notebook_py = SDP_META_RUNNER_NOTEBOOK.format(
            dependency=f"databricks-labs-sdp-meta=={__version__}"
        ).encode("utf8")
        runner_notebook_path = f"{sdp_meta._install_folder()}/init_sdp_meta_pipeline.py"
        mock_workspace_client.workspace.mkdirs.assert_called_once_with(
            "/Users/name/sdp-meta"
        )
        mock_workspace_client.workspace.upload.assert_called_once_with(
            runner_notebook_path, runner_notebook_py, overwrite=True
        )
        mock_workspace_client.pipelines.create.assert_called_once()

        mock_workspace_client.pipelines.create.assert_called_once()

    def test_get_onboarding_named_parameters(self):
        cmd = OnboardCommand(
            onboarding_file_path="tests/resources/onboarding.json",
            onboarding_files_dir_path="tests/resources/",
            onboard_layer="bronze_silver",
            env="dev",
            import_author="Ravi Gawai",
            version="1.0",
            sdp_meta_schema="sdp_meta",
            bronze_dataflowspec_path="tests/resources/bronze_dataflowspec",
            silver_dataflowspec_path="tests/resources/silver_dataflowspec",
            uc_enabled=True,
            uc_catalog_name="uc_catalog",
            uc_volume_path="uc_catalog/sdp_meta/files",
            overwrite=True,
            bronze_dataflowspec_table="bronze_dataflowspec",
            silver_dataflowspec_table="silver_dataflowspec",
            update_paths=True,
        )
        sdp_meta = SDPMeta(None)
        named_parameters = sdp_meta._get_onboarding_named_parameters(
            cmd
        )
        expected_named_parameters = {
            "onboard_layer": "bronze_silver",
            "database": "uc_catalog.sdp_meta" if cmd.uc_enabled else "sdp_meta",
            # New path scheme (cli.py:683-690 comment): on UC the
            # onboarding spec is stashed under a ``sdp_meta_conf/tmp/``
            # subdir of the run's volume and addressed by BASENAME
            # only \u2014 the original ``tests/resources/`` prefix from
            # ``cmd.onboarding_file_path`` is intentionally dropped
            # because by this point the file has already been uploaded
            # to a known location and the local prefix is meaningless
            # to the remote job.
            "onboarding_file_path": "uc_catalog/sdp_meta/files/sdp_meta_conf/tmp/onboarding.json",
            "import_author": "Ravi Gawai",
            "version": "1.0",
            "overwrite": "True",
            "env": "dev",
            "uc_enabled": "True",
            "bronze_dataflowspec_table": "bronze_dataflowspec",
            "silver_dataflowspec_table": "silver_dataflowspec",
        }
        self.assertEqual(named_parameters, expected_named_parameters)

    @patch("databricks.labs.sdp_meta.cli.WorkspaceClient")
    def test_create_uc_volume(self, mock_workspace_client):
        mock_volumes_create = MagicMock()
        mock_workspace_client.volumes.create = mock_volumes_create
        mock_volumes_create.return_value = MagicMock(
            catalog_name="uc_catalog",
            schema_name="sdp_meta",
            name="sdp_meta"
        )
        sdp_meta = SDPMeta(mock_workspace_client)
        volume_path = sdp_meta.create_uc_volume("uc_catalog", "sdp_meta")
        self.assertEqual(
            volume_path,
            f"/Volumes/{mock_volumes_create.return_value.catalog_name}/"
            f"{mock_volumes_create.return_value.schema_name}/"
            f"{mock_volumes_create.return_value.schema_name}/"
        )
        mock_volumes_create.assert_called_once_with(
            catalog_name="uc_catalog",
            schema_name="sdp_meta",
            name="sdp_meta",
            volume_type=VolumeType.MANAGED
        )

    @patch("databricks.labs.sdp_meta.cli.SchemasAPI")
    @patch("databricks.labs.sdp_meta.cli.WorkspaceClient")
    def test_create_uc_schema(self, mock_workspace_client, mock_schemas_api):
        mock_schemas_api_instance = mock_schemas_api.return_value
        mock_schemas_api_instance.get.side_effect = Exception("Schema not found")
        mock_schemas_api_instance.create.return_value = None

        sdp_meta = SDPMeta(mock_workspace_client)
        sdp_meta.create_uc_schema("uc_catalog", "sdp_meta")

        mock_schemas_api_instance.get.assert_called_once_with(full_name="uc_catalog.sdp_meta")
        mock_schemas_api_instance.create.assert_called_once_with(
            catalog_name="uc_catalog",
            name="sdp_meta",
            comment="sdp_meta framework schema"
        )

    @patch("databricks.labs.sdp_meta.cli.SchemasAPI")
    @patch("databricks.labs.sdp_meta.cli.WorkspaceClient")
    def test_create_uc_schema_already_exists(self, mock_workspace_client, mock_schemas_api):
        mock_schemas_api_instance = mock_schemas_api.return_value
        mock_schemas_api_instance.get.return_value = None

        sdp_meta = SDPMeta(mock_workspace_client)
        sdp_meta.create_uc_schema("uc_catalog", "sdp_meta")

        mock_schemas_api_instance.get.assert_called_once_with(full_name="uc_catalog.sdp_meta")
        mock_schemas_api_instance.create.assert_not_called()

    @patch("databricks.labs.sdp_meta.cli.WorkspaceClient")
    def test_deploy(self, mock_workspace_client):
        mock_pipelines_create = MagicMock()
        mock_pipelines_start_update = MagicMock()
        mock_workspace_client.pipelines.create = mock_pipelines_create
        mock_workspace_client.pipelines.start_update = mock_pipelines_start_update
        mock_pipelines_create.return_value = MagicMock(pipeline_id="pipeline_id")
        mock_pipelines_start_update.return_value = MagicMock(update_id="update_id")

        sdp_meta = SDPMeta(mock_workspace_client)
        sdp_meta._wsi = mock_workspace_client.return_value
        sdp_meta._install_folder = MagicMock(return_value="/Users/name/sdp-meta")
        sdp_meta._my_username = MagicMock(return_value="name")

        sdp_meta._create_sdp_meta_pipeline = MagicMock(return_value="pipeline_id")

        deploy_cmd = DeployCommand(
            layer="bronze",
            onboard_bronze_group="A1",
            sdp_meta_bronze_schema="sdp_meta",
            pipeline_name="unittest_dlt_pipeline",
            dataflowspec_bronze_table="dataflowspec_table",
            dlt_target_schema="dlt_target_schema",
            num_workers=1,
            uc_catalog_name="uc_catalog",
            dataflowspec_bronze_path="tests/resources/dataflowspec",
            uc_enabled=True,
            serverless=False,
            dbfs_path="/dbfs",
        )

        sdp_meta.deploy(deploy_cmd)

        sdp_meta._create_sdp_meta_pipeline.assert_called_once_with(deploy_cmd)
        mock_pipelines_start_update.assert_called_once_with(pipeline_id="pipeline_id")

    @patch("databricks.labs.sdp_meta.cli.WorkspaceInstaller")
    @patch("databricks.labs.sdp_meta.cli.WorkspaceClient")
    def test_load_onboard_config(self, mock_workspace_client, mock_workspace_installer):
        mock_ws_installer = mock_workspace_installer.return_value
        mock_ws_installer._choice.side_effect = ['True', 'True', 'bronze_silver', 'False', 'True', 'False']
        mock_ws_installer._question.side_effect = [
            "uc_catalog", "demo/conf/onboarding.template",
            "/demo/", "sdp_meta_dataflowspecs", "sdp_meta_bronze", "sdp_meta_silver",
            "bronze_dataflowspec", "silver_dataflowspec", "v1", "prod", "author", "True"
        ]
        sdp_meta = SDPMeta(mock_workspace_client)
        cmd = sdp_meta._load_onboard_config()

        self.assertTrue(cmd.uc_enabled)
        self.assertEqual(cmd.uc_catalog_name, "uc_catalog")
        self.assertEqual(cmd.dbfs_path, None)
        self.assertEqual(cmd.onboarding_file_path, "demo/conf/onboarding.template")
        self.assertEqual(cmd.onboarding_files_dir_path, "file:/demo/")
        self.assertEqual(cmd.sdp_meta_schema, "sdp_meta_dataflowspecs")
        self.assertEqual(cmd.bronze_schema, "sdp_meta_bronze")
        self.assertEqual(cmd.silver_schema, "sdp_meta_silver")
        self.assertEqual(cmd.onboard_layer, "bronze_silver")
        self.assertEqual(cmd.bronze_dataflowspec_table, "bronze_dataflowspec")
        self.assertEqual(cmd.bronze_dataflowspec_path, None)
        self.assertEqual(cmd.silver_dataflowspec_table, "silver_dataflowspec")
        self.assertEqual(cmd.silver_dataflowspec_path, None)
        self.assertEqual(cmd.version, "v1")
        self.assertEqual(cmd.env, "prod")
        self.assertEqual(cmd.import_author, "author")
        self.assertTrue(cmd.update_paths)

    @patch("databricks.labs.sdp_meta.cli.WorkspaceInstaller")
    @patch("databricks.labs.sdp_meta.cli.WorkspaceClient")
    def test_load_onboard_config_without_uc(self, mock_workspace_client, mock_workspace_installer):
        mock_ws_installer = mock_workspace_installer.return_value
        mock_ws_installer._choice.side_effect = ['False', 'False', 'aws',
                                                 'bronze_silver', 'False', 'True', 'False']
        mock_ws_installer._question.side_effect = [
            'dbfs_path', "dbrx", "demo/conf/onboarding.template",
            "/demo/", "sdp_meta_dataflowspecs", "sdp_meta_bronze",
            "sdp_meta_silver", "bronze_dataflowspec_table",
            "bronze_dataflowspec_path", "silver_dataflowspec_table",
            "silver_dataflowspec_path", "v1", "prod", "author", "True"
        ]
        sdp_meta = SDPMeta(mock_workspace_client)
        cmd = sdp_meta._load_onboard_config()

        self.assertFalse(cmd.uc_enabled)
        self.assertFalse(cmd.serverless)
        self.assertEqual(cmd.dbfs_path, "dbfs_path")
        self.assertEqual(cmd.onboarding_file_path, "demo/conf/onboarding.template")
        self.assertEqual(cmd.onboarding_files_dir_path, "file:/demo/")
        self.assertEqual(cmd.sdp_meta_schema, "sdp_meta_dataflowspecs")
        self.assertEqual(cmd.bronze_schema, "sdp_meta_bronze")
        self.assertEqual(cmd.silver_schema, "sdp_meta_silver")
        self.assertEqual(cmd.onboard_layer, "bronze_silver")
        self.assertEqual(cmd.bronze_dataflowspec_table, "bronze_dataflowspec_table")
        self.assertEqual(cmd.silver_dataflowspec_table, "silver_dataflowspec_table")
        self.assertEqual(cmd.bronze_dataflowspec_path, "bronze_dataflowspec_path")
        self.assertEqual(cmd.silver_dataflowspec_path, "silver_dataflowspec_path")
        self.assertEqual(cmd.version, "v1")
        self.assertEqual(cmd.env, "prod")
        self.assertEqual(cmd.import_author, "author")
        self.assertTrue(cmd.update_paths)

    @patch("databricks.labs.sdp_meta.cli.WorkspaceInstaller")
    @patch("databricks.labs.sdp_meta.cli.WorkspaceClient")
    def test_load_deploy_config_with_uc_enabled(self, mock_workspace_client, mock_workspace_installer):
        mock_workspace_installer._choice.side_effect = ["No", "True", "True", "bronze_silver"]
        mock_workspace_installer._question.side_effect = [
            "uc_catalog", "group", "sdp_meta_schema", "bronze_dataflowspec",
            "group", "sdp_meta_schema", "silver_dataflowspec",
            "pipeline_name", "dlt_target_schema"
        ]
        sdp_meta = SDPMeta(mock_workspace_client)
        sdp_meta._wsi = mock_workspace_installer
        deploy_cmd = sdp_meta._load_deploy_config()

        self.assertTrue(deploy_cmd.uc_enabled)
        self.assertTrue(deploy_cmd.serverless)
        self.assertEqual(deploy_cmd.uc_catalog_name, "uc_catalog")
        self.assertEqual(deploy_cmd.layer, "bronze_silver")
        self.assertEqual(deploy_cmd.onboard_bronze_group, "group")
        self.assertEqual(deploy_cmd.sdp_meta_bronze_schema, "sdp_meta_schema")
        self.assertEqual(deploy_cmd.dataflowspec_bronze_table, "bronze_dataflowspec")
        self.assertEqual(deploy_cmd.sdp_meta_silver_schema, "sdp_meta_schema")
        self.assertEqual(deploy_cmd.dataflowspec_silver_table, "silver_dataflowspec")
        self.assertEqual(deploy_cmd.num_workers, None)
        self.assertEqual(deploy_cmd.pipeline_name, "pipeline_name")
        self.assertEqual(deploy_cmd.dlt_target_schema, "dlt_target_schema")

    @patch("databricks.labs.sdp_meta.cli.WorkspaceInstaller")
    @patch("databricks.labs.sdp_meta.cli.WorkspaceClient")
    def test_load_deploy_config_without_uc_enabled(self, mock_workspace_client, mock_workspace_installer):
        mock_workspace_installer._choice.side_effect = ["No", "False", "bronze"]
        mock_workspace_installer._question.side_effect = [
            "group", "sdp_meta_schema", "bronze_dataflowspec",
            "dataflowspec_path", 4, "pipeline_name", "dlt_target_schema"
        ]
        sdp_meta = SDPMeta(mock_workspace_client)
        sdp_meta._install_folder = MagicMock(return_value="/Users/name/sdp-meta")
        sdp_meta._wsi = mock_workspace_installer
        deploy_cmd = sdp_meta._load_deploy_config()

        self.assertFalse(deploy_cmd.uc_enabled)
        self.assertFalse(deploy_cmd.serverless)
        self.assertEqual(deploy_cmd.layer, "bronze")
        self.assertEqual(deploy_cmd.onboard_bronze_group, "group")
        self.assertEqual(deploy_cmd.sdp_meta_bronze_schema, "sdp_meta_schema")
        self.assertEqual(deploy_cmd.dataflowspec_bronze_table, "bronze_dataflowspec")
        self.assertEqual(deploy_cmd.dataflowspec_bronze_path, "dataflowspec_path")
        self.assertEqual(deploy_cmd.num_workers, 4)
        self.assertEqual(deploy_cmd.pipeline_name, "pipeline_name")
        self.assertEqual(deploy_cmd.dlt_target_schema, "dlt_target_schema")

    @patch("databricks.labs.sdp_meta.cli.WorkspaceClient")
    @patch("databricks.labs.sdp_meta.cli.SDPMeta")
    @patch("databricks.labs.sdp_meta.cli.json.loads")
    def test_main_onboard(self, mock_json_loads, mock_sdp_meta, mock_workspace_client):
        mock_json_loads.return_value = {
            "command": "onboard",
            "flags": {"log_level": "info"}
        }
        mock_ws_instance = mock_workspace_client.return_value

        with patch("databricks.labs.sdp_meta.cli.onboard"):
            main("{}")
            mock_workspace_client.assert_called_once_with(product='sdp-meta', product_version=__version__)
            mock_sdp_meta.assert_called_once_with(mock_ws_instance)

    @patch("databricks.labs.sdp_meta.cli.WorkspaceClient")
    @patch("databricks.labs.sdp_meta.cli.SDPMeta")
    @patch("databricks.labs.sdp_meta.cli.json.loads")
    def test_main_deploy(self, mock_json_loads, mock_sdp_meta, mock_workspace_client):
        mock_json_loads.return_value = {
            "command": "deploy",
            "flags": {"log_level": "info"}
        }
        mock_ws_instance = mock_workspace_client.return_value

        with patch("databricks.labs.sdp_meta.cli.deploy"):
            main("{}")
            mock_workspace_client.assert_called_once_with(product='sdp-meta', product_version=__version__)
            mock_sdp_meta.assert_called_once_with(mock_ws_instance)

    @patch("databricks.labs.sdp_meta.cli.json.loads")
    def test_main_invalid_command(self, mock_json_loads):
        mock_json_loads.return_value = {
            "command": "invalid_command",
            "flags": {"log_level": "info"}
        }
        with self.assertRaises(KeyError):
            main("{}")

    @patch("databricks.labs.sdp_meta.cli.WorkspaceClient")
    @patch("databricks.labs.sdp_meta.cli.SDPMeta")
    @patch("databricks.labs.sdp_meta.cli.json.loads")
    def test_main_log_level_disabled(self, mock_json_loads, mock_sdp_meta, mock_workspace_client):
        mock_json_loads.return_value = {
            "command": "onboard",
            "flags": {"log_level": "disabled"}
        }
        mock_ws_instance = mock_workspace_client.return_value

        with patch("databricks.labs.sdp_meta.cli.onboard"):
            main("{}")
            mock_workspace_client.assert_called_once_with(product='sdp-meta', product_version=__version__)
            mock_sdp_meta.assert_called_once_with(mock_ws_instance)

    @patch("databricks.labs.sdp_meta.cli.WorkspaceClient")
    def test_update_ws_onboarding_paths_with_uc_enabled(self, mock_workspace_client):
        """H-2: UC-enabled flow uploads the rendered template
        DIRECTLY to UC Volume \u2014 no local staging file. The
        cluster running the onboarding job reads from UC, so writing
        the rendered file to the App's local filesystem (in /tmp, in
        the App wheel folder, or in the user's local_directory)
        would either leak files, risk overwriting the user's own
        content, or put the spec somewhere the job can't open. We
        assert on the ``ws.files.upload`` call shape and confirm
        ``cmd.onboarding_file_path`` was rewritten to the UC path."""
        cmd = OnboardCommand(
            onboarding_file_path="tests/resources/template/onboarding.template",
            onboarding_files_dir_path="tests/resources/",
            onboard_layer="bronze",
            env="dev",
            import_author="John Doe",
            version="1.0",
            cloud="aws",
            sdp_meta_schema="sdp_meta",
            uc_enabled=True,
            uc_catalog_name="uc_catalog",
            uc_volume_path="/Volumes/uc_catalog/sdp_meta/sdp_meta",
            overwrite=True,
            bronze_dataflowspec_table="bronze_dataflowspec",
            silver_dataflowspec_table="silver_dataflowspec",
            update_paths=True,
        )
        sdp_meta = SDPMeta(mock_workspace_client)
        sdp_meta._wsi = mock_workspace_client.return_value
        sdp_meta.update_ws_onboarding_paths(cmd)

        # 1. files.upload was called with the UC Volume destination.
        upload = mock_workspace_client.files.upload
        self.assertTrue(
            upload.called,
            "update_ws_onboarding_paths must publish rendered bytes to "
            "UC Volume directly when uc_enabled \u2014 instead it produced "
            "no upload (local-staging regression).",
        )
        kwargs = upload.call_args.kwargs
        expected_uc_dest = (
            "/Volumes/uc_catalog/sdp_meta/sdp_meta/sdp_meta_conf/tmp/"
            "onboarding.json"
        )
        self.assertEqual(kwargs.get("file_path"), expected_uc_dest)
        self.assertTrue(kwargs.get("overwrite"))
        # The contents are a file-like object holding the rendered
        # bytes \u2014 read them back to confirm substitution actually ran.
        contents = kwargs.get("contents")
        self.assertIsNotNone(contents)
        body = contents.read()
        if isinstance(body, bytes):
            body = body.decode("utf-8")
        self.assertIn("uc_catalog", body)
        self.assertNotIn("{uc_catalog_name}", body)

        # 2. cmd.onboarding_file_path now points at UC, NOT a local path.
        self.assertEqual(cmd.onboarding_file_path, expected_uc_dest)

        # 3. NO local staging file was written next to the source.
        leaked = "tests/resources/template/onboarding.json"
        self.assertFalse(
            os.path.exists(leaked),
            f"UC flow leaked a local staging file at {leaked}",
        )

    @patch("databricks.labs.sdp_meta.cli.WorkspaceClient")
    def test_update_ws_onboarding_paths_uc_app_tempfile_source(
        self, mock_workspace_client
    ):
        """Same UC-direct-upload contract when the App downloaded the
        source to a ``/tmp/sdp_onboarding_*`` tempfile (the original
        H-2 trigger). Nothing should land on the App's local
        filesystem; the rendered bytes go straight to UC Volume."""
        import shutil
        import tempfile

        tmp_src_dir = tempfile.mkdtemp(prefix="sdp_meta_test_src_")
        try:
            tempfile_path = os.path.join(tmp_src_dir, "sdp_onboarding_xyz.template")
            with open(tempfile_path, "w", encoding="utf-8") as fh:
                fh.write(
                    '[{"data_flow_id": "1", '
                    '"uc_volume_path": "{uc_volume_path}"}]'
                )

            cmd = OnboardCommand(
                onboarding_file_path=tempfile_path,
                onboarding_files_dir_path="/Workspace/dlt-meta/demo/",
                onboard_layer="bronze",
                env="dev",
                import_author="t",
                version="1.0",
                cloud="aws",
                sdp_meta_schema="sdp_meta",
                uc_enabled=True,
                uc_catalog_name="uc_catalog",
                uc_volume_path="/Volumes/uc_catalog/sdp_meta/sdp_meta",
                bronze_schema="b",
                silver_schema="s",
                update_paths=True,
            )

            sdp_meta = SDPMeta(mock_workspace_client)
            sdp_meta._wsi = mock_workspace_client.return_value
            sdp_meta.update_ws_onboarding_paths(cmd)

            upload = mock_workspace_client.files.upload
            self.assertTrue(upload.called)
            self.assertEqual(
                upload.call_args.kwargs.get("file_path"),
                "/Volumes/uc_catalog/sdp_meta/sdp_meta/sdp_meta_conf/tmp/"
                "onboarding.json",
            )

            # cmd.onboarding_file_path is now the UC destination,
            # NOT the /tmp tempfile dirname.
            self.assertTrue(
                cmd.onboarding_file_path.startswith("/Volumes/"),
                f"expected UC path, got {cmd.onboarding_file_path!r}",
            )

            # Nothing leaked in /tmp next to the tempfile.
            leaked = os.path.join(tmp_src_dir, "onboarding.json")
            self.assertFalse(
                os.path.exists(leaked),
                f"rendered file leaked next to tempfile: {leaked}",
            )
        finally:
            shutil.rmtree(tmp_src_dir, ignore_errors=True)

    @patch("databricks.labs.sdp_meta.cli.WorkspaceClient")
    def test_update_ws_onboarding_paths_dbfs_writes_locally(
        self, mock_workspace_client
    ):
        """Non-UC (DBFS) flow keeps the historical behaviour: render
        next to the source on local disk, then let ``onboard()`` push
        it via ``dbfs.upload``. The DBFS SDK surface and its staging
        contract are intentionally untouched here."""
        import shutil
        import tempfile

        src_dir = tempfile.mkdtemp(prefix="sdp_meta_test_dbfs_")
        try:
            src = os.path.join(src_dir, "foo.template")
            with open(src, "w", encoding="utf-8") as fh:
                fh.write(
                    '[{"data_flow_id": "1", '
                    '"uc_volume_path": "{uc_volume_path}"}]'
                )

            cmd = OnboardCommand(
                onboarding_file_path=src,
                onboarding_files_dir_path=src_dir,
                onboard_layer="bronze",
                env="dev",
                import_author="t",
                version="1.0",
                cloud="aws",
                sdp_meta_schema="sdp_meta",
                uc_enabled=False,
                dbfs_path="/dbfs/sdp_meta",
                bronze_dataflowspec_path="/dbfs/sdp_meta/bronze",
                silver_dataflowspec_path="/dbfs/sdp_meta/silver",
                bronze_schema="b",
                silver_schema="s",
                update_paths=True,
            )

            sdp_meta = SDPMeta(mock_workspace_client)
            sdp_meta._wsi = mock_workspace_client.return_value
            sdp_meta.update_ws_onboarding_paths(cmd)

            # DBFS path \u2014 NO UC upload, AND a local file at the
            # expected stable basename.
            self.assertFalse(mock_workspace_client.files.upload.called)
            expected_local = os.path.join(src_dir, "onboarding.json")
            self.assertTrue(os.path.exists(expected_local))
            self.assertEqual(cmd.onboarding_file_path, expected_local)
        finally:
            shutil.rmtree(src_dir, ignore_errors=True)

    @patch("databricks.labs.sdp_meta.cli.WorkspaceClient")
    def test_my_username(self, mock_workspace_client):
        mock_workspace_client.current_user.me.return_value = MagicMock(user_name="test_user")
        mock_workspace_client.current_user.me.return_value.user_name = "test_user"
        mock_workspace_client._me.return_value = MagicMock(user_name="test_user")
        sdp_meta = SDPMeta(mock_workspace_client)
        username = sdp_meta._my_username()
        self.assertEqual(username, mock_workspace_client._me.user_name)

    def test_onboard_command_post_init(self):
        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="bronze",
                env="dev",
                import_author="John Doe",
                version="1.0",
                sdp_meta_schema="sdp_meta",
                dbfs_path="/dbfs",
                overwrite=True,
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="",
                onboard_layer="bronze",
                env="dev",
                import_author="John Doe",
                version="1.0",
                sdp_meta_schema="sdp_meta",
                dbfs_path="/dbfs",
                overwrite=True,
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="",
                env="dev",
                import_author="John Doe",
                version="1.0",
                sdp_meta_schema="sdp_meta",
                dbfs_path="/dbfs",
                overwrite=True,
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="invalid_layer",
                env="dev",
                import_author="John Doe",
                version="1.0",
                sdp_meta_schema="sdp_meta",
                dbfs_path="/dbfs",
                overwrite=True,
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="bronze",
                env="dev",
                import_author="John Doe",
                version="1.0",
                sdp_meta_schema="sdp_meta",
                dbfs_path=None,
                uc_enabled=False,
                overwrite=True,
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="bronze",
                env="dev",
                import_author="John Doe",
                version="1.0",
                sdp_meta_schema="sdp_meta",
                dbfs_path="/dbfs",
                serverless=False,
                cloud=None,
                dbr_version=None,
                overwrite=True,
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="bronze_silver",
                env="dev",
                import_author="John Doe",
                version="1.0",
                sdp_meta_schema="sdp_meta",
                dbfs_path="/dbfs",
                uc_enabled=False,
                bronze_dataflowspec_path=None,
                silver_dataflowspec_path=None,
                overwrite=True,
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="silver",
                env="dev",
                import_author="John Doe",
                version="1.0",
                sdp_meta_schema="sdp_meta",
                dbfs_path="/dbfs",
                uc_enabled=False,
                silver_dataflowspec_path=None,
                overwrite=True,
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="bronze",
                env="dev",
                import_author="John Doe",
                version="1.0",
                sdp_meta_schema=None,
                dbfs_path="/dbfs",
                overwrite=True,
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="bronze",
                env="dev",
                import_author="John Doe",
                version="1.0",
                sdp_meta_schema="sdp_meta",
                dbfs_path="/dbfs",
                overwrite=False,
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="bronze",
                env="dev",
                import_author=None,
                version="1.0",
                sdp_meta_schema="sdp_meta",
                dbfs_path="/dbfs",
                overwrite=True,
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="bronze",
                env=None,
                import_author="John Doe",
                version="1.0",
                sdp_meta_schema="sdp_meta",
                dbfs_path="/dbfs",
                overwrite=True,
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="bronze",
                env="dev",
                import_author="John Doe",
                version="1.0",
                sdp_meta_schema="sdp_meta",
                dbfs_path="/dbfs",
                overwrite=True,
                serverless=False,
                cloud="aws"
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="bronze",
                env="dev",
                import_author="John Doe",
                version="1.0",
                sdp_meta_schema="sdp_meta",
                dbfs_path="/dbfs",
                overwrite=True,
                serverless=False
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="bronze",
                env="dev",
                import_author="John Doe",
                version="1.0",
                sdp_meta_schema="sdp_meta",
                dbfs_path="/dbfs",
                overwrite=True,
                serverless=False,
                cloud="aws"
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="bronze_silver",
                env="dev",
                import_author="John Doe",
                version="1.0",
                sdp_meta_schema="sdp_meta",
                dbfs_path="/dbfs",
                overwrite=True,
                serverless=False,
                cloud="aws",
                dbr_version="7.3",
                uc_enabled=False
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="bronze_silver",
                env="dev",
                import_author="John Doe",
                version="1.0",
                sdp_meta_schema="sdp_meta",
                dbfs_path="/dbfs",
                overwrite=True,
                serverless=False,
                cloud="aws",
                dbr_version="7.3",
                uc_enabled=False,
                bronze_dataflowspec_path="tests/resources/bronze_dataflowspec"
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="bronze_silver",
                env="dev",
                import_author="John Doe",
                version="1.0",
                sdp_meta_schema="sdp_meta",
                dbfs_path="/dbfs",
                overwrite=True,
                serverless=False,
                cloud="aws",
                dbr_version="7.3",
                uc_enabled=False,
                silver_dataflowspec_path="tests/resources/silver_dataflowspec"
            )
        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="silver",
                env="dev",
                import_author="John Doe",
                version="1.0",
                sdp_meta_schema="sdp_meta",
                dbfs_path="/dbfs",
                overwrite=True,
                serverless=False,
                cloud="aws",
                dbr_version="7.3",
                uc_enabled=False
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="silver",
                env="dev",
                import_author="John Doe",
                version="1.0",
                sdp_meta_schema="sdp_meta",
                dbfs_path="/dbfs",
                overwrite=True,
                serverless=False,
                cloud="aws",
                dbr_version="7.3",
                uc_enabled=False,
                silver_dataflowspec_table="silver_dataflowspec"
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="silver",
                sdp_meta_schema=None,
                env="dev",
                import_author="John Doe",
                version="1.0",
                overwrite=True,
                serverless=True,
                uc_enabled=True,
                silver_dataflowspec_table="silver_dataflowspec"
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="silver",
                sdp_meta_schema="sdp_meta",
                env="dev",
                import_author=None,
                version="1.0",
                overwrite=True,
                serverless=True,
                uc_enabled=True,
                silver_dataflowspec_table="silver_dataflowspec"
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="silver",
                sdp_meta_schema="sdp_meta",
                env=None,
                import_author="author",
                version="1.0",
                overwrite=True,
                serverless=True,
                uc_enabled=True,
                silver_dataflowspec_table="silver_dataflowspec"
            )

    def test_deploy_command_post_init(self):
        with self.assertRaises(ValueError):
            DeployCommand(
                layer="bronze",
                onboard_bronze_group="A1",
                sdp_meta_bronze_schema="sdp_meta",
                dataflowspec_bronze_table="dataflowspec_table",
                pipeline_name="unittest_dlt_pipeline",
                dlt_target_schema="dlt_target_schema",
                uc_enabled=True,
                uc_catalog_name=None,
            )

        with self.assertRaises(ValueError):
            DeployCommand(
                layer="bronze",
                onboard_bronze_group="A1",
                sdp_meta_bronze_schema="sdp_meta",
                dataflowspec_bronze_table="dataflowspec_table",
                pipeline_name="unittest_dlt_pipeline",
                dlt_target_schema="dlt_target_schema",
                serverless=False,
                num_workers=None,
            )

        with self.assertRaises(ValueError):
            DeployCommand(
                layer=None,
                onboard_bronze_group="A1",
                sdp_meta_bronze_schema="sdp_meta",
                dataflowspec_bronze_table="dataflowspec_table",
                pipeline_name="unittest_dlt_pipeline",
                dlt_target_schema="dlt_target_schema",
            )

        with self.assertRaises(ValueError):
            DeployCommand(
                layer="bronze",
                onboard_bronze_group=None,
                sdp_meta_bronze_schema="sdp_meta",
                dataflowspec_bronze_table="dataflowspec_table",
                pipeline_name="unittest_dlt_pipeline",
                dlt_target_schema="dlt_target_schema",
            )

        with self.assertRaises(ValueError):
            DeployCommand(
                layer="bronze",
                onboard_bronze_group="A1",
                sdp_meta_bronze_schema="sdp_meta",
                dataflowspec_bronze_table=None,
                pipeline_name="unittest_dlt_pipeline",
                dlt_target_schema="dlt_target_schema",
            )

        with self.assertRaises(ValueError):
            DeployCommand(
                layer="bronze",
                onboard_bronze_group="A1",
                sdp_meta_bronze_schema="sdp_meta",
                dataflowspec_bronze_table="dataflowspec_table",
                pipeline_name=None,
                dlt_target_schema="dlt_target_schema",
            )

        with self.assertRaises(ValueError):
            DeployCommand(
                layer="bronze",
                onboard_bronze_group="A1",
                sdp_meta_bronze_schema="sdp_meta",
                dataflowspec_bronze_table="dataflowspec_table",
                pipeline_name="unittest_dlt_pipeline",
                dlt_target_schema=None,
            )

    def test_deploy_command_post_init_additional(self):
        with self.assertRaises(ValueError):
            DeployCommand(
                layer="",
                onboard_bronze_group="A1",
                sdp_meta_bronze_schema="sdp_meta",
                dataflowspec_bronze_table="dataflowspec_table",
                pipeline_name="unittest_dlt_pipeline",
                dlt_target_schema="dlt_target_schema",
                num_workers=1,
            )

        with self.assertRaises(ValueError):
            DeployCommand(
                layer="bronze",
                onboard_bronze_group="",
                sdp_meta_bronze_schema="sdp_meta",
                dataflowspec_bronze_table="dataflowspec_table",
                pipeline_name="unittest_dlt_pipeline",
                dlt_target_schema="dlt_target_schema",
                num_workers=1,
            )

        with self.assertRaises(ValueError):
            DeployCommand(
                layer="bronze",
                onboard_bronze_group="A1",
                sdp_meta_bronze_schema="sdp_meta",
                dataflowspec_bronze_table="",
                pipeline_name="unittest_dlt_pipeline",
                dlt_target_schema="dlt_target_schema",
                num_workers=1,
            )

        with self.assertRaises(ValueError):
            DeployCommand(
                layer="bronze",
                onboard_bronze_group="A1",
                sdp_meta_bronze_schema="sdp_meta",
                dataflowspec_bronze_table="dataflowspec_table",
                pipeline_name="",
                dlt_target_schema="dlt_target_schema",
                num_workers=1,
            )

        with self.assertRaises(ValueError):
            DeployCommand(
                layer="bronze",
                onboard_bronze_group="A1",
                sdp_meta_bronze_schema="sdp_meta",
                dataflowspec_bronze_table="dataflowspec_table",
                pipeline_name="unittest_dlt_pipeline",
                dlt_target_schema="",
                num_workers=1,
            )

    @patch("databricks.labs.sdp_meta.cli.SDPMeta._install_folder", return_value="/Users/test/sdp-meta")
    @patch("databricks.labs.sdp_meta.cli.WorkspaceClient")
    def test_create_sdp_meta_pipeline_with_uc_enabled(self, mock_workspace_client, mock_install_folder):
        sdp_meta = SDPMeta(mock_workspace_client)
        sdp_meta.version = "1.2.3"
        cmd = DeployCommand(
            layer="bronze",
            onboard_bronze_group="groupA",
            sdp_meta_bronze_schema="schemaA",
            dataflowspec_bronze_table="tableA",
            pipeline_name="my_pipeline",
            dlt_target_schema="my_dlt_schema",
            uc_enabled=True,
            uc_catalog_name="my_catalog",
            serverless=True,
            num_workers=None,
        )
        mock_created = MagicMock()
        mock_created.pipeline_id = "12345"
        mock_workspace_client.pipelines.create.return_value = mock_created

        pipeline_id = sdp_meta._create_sdp_meta_pipeline(cmd)
        self.assertEqual(pipeline_id, "12345")
        mock_workspace_client.pipelines.create.assert_called_once()

    @patch("databricks.labs.sdp_meta.cli.SDPMeta._install_folder", return_value="/Users/test/sdp-meta")
    @patch("databricks.labs.sdp_meta.cli.WorkspaceClient")
    def test_create_sdp_meta_pipeline_without_uc_enabled(self, mock_workspace_client, mock_install_folder):
        sdp_meta = SDPMeta(mock_workspace_client)
        sdp_meta.version = "0.9.1"
        cmd = DeployCommand(
            layer="silver",
            onboard_silver_group="groupB",
            sdp_meta_silver_schema="schemaB",
            dataflowspec_silver_table="tableB",
            pipeline_name="silver_pipeline",
            dlt_target_schema="silver_target_schema",
            dataflowspec_silver_path="tests/resources/silver_dataflowspec",
            uc_enabled=False,
            uc_catalog_name=None,
            serverless=False,
            num_workers=5,
        )
        mock_created = MagicMock()
        mock_created.pipeline_id = "98765"
        mock_workspace_client.pipelines.create.return_value = mock_created

        pipeline_id = sdp_meta._create_sdp_meta_pipeline(cmd)
        self.assertEqual(pipeline_id, "98765")
        mock_workspace_client.pipelines.create.assert_called_once()

    @patch("databricks.labs.sdp_meta.cli.SDPMeta._install_folder", return_value="/Users/test/sdp-meta")
    @patch("databricks.labs.sdp_meta.cli.WorkspaceClient")
    def test_create_sdp_meta_pipeline_invalid_layer_raises_value_error(
        self, mock_workspace_client, mock_install_folder
    ):
        sdp_meta = SDPMeta(mock_workspace_client)
        cmd = DeployCommand(
            layer="invalid",
            serverless=True,
            onboard_bronze_group="group",
            sdp_meta_bronze_schema="schema",
            dataflowspec_bronze_table="table",
            pipeline_name="test_pipeline",
            dlt_target_schema="target_schema",
        )
        with self.assertRaises(ValueError):
            sdp_meta._create_sdp_meta_pipeline(cmd)

    @patch("databricks.labs.sdp_meta.cli.SDPMeta._install_folder", return_value="/Users/test/sdp-meta")
    @patch("databricks.labs.sdp_meta.cli.WorkspaceClient")
    def test_create_sdp_meta_pipeline_raise_exception_on_no_creation(self, mock_workspace_client, mock_install_folder):
        sdp_meta = SDPMeta(mock_workspace_client)
        cmd = DeployCommand(
            layer="bronze",
            serverless=True,
            uc_enabled=True,
            uc_catalog_name="catalog",
            onboard_bronze_group="group",
            sdp_meta_bronze_schema="schema",
            dataflowspec_bronze_table="table",
            pipeline_name="test_pipeline",
            dlt_target_schema="target_schema",
        )
        mock_workspace_client.pipelines.create.return_value = None
        with self.assertRaises(Exception):
            sdp_meta._create_sdp_meta_pipeline(cmd)

    # ── sdp_meta version tag tests ─────────────────────────────────
    #
    # Every pipeline created by SDP-META gets a ``sdp_meta`` tag whose
    # value is the SDP-META version (e.g. "0.1.0"). The Monitor tab in
    # the Databricks App relies on this tag to filter SDP-META pipelines
    # out of the workspace-wide pipelines list — and uses the value to
    # surface a "v<version>" chip alongside the pipeline name. These two
    # tests pin the producer-side contract for both the UC-enabled and
    # UC-disabled code paths.

    @patch("databricks.labs.sdp_meta.cli.SDPMeta._install_folder", return_value="/Users/test/sdp-meta")
    @patch("databricks.labs.sdp_meta.cli.WorkspaceClient")
    def test_create_sdp_meta_pipeline_tags_carry_version_uc_enabled(
        self, mock_workspace_client, mock_install_folder
    ):
        sdp_meta = SDPMeta(mock_workspace_client)
        sdp_meta.version = "9.9.9"
        cmd = DeployCommand(
            layer="bronze",
            onboard_bronze_group="groupA",
            sdp_meta_bronze_schema="schemaA",
            dataflowspec_bronze_table="tableA",
            pipeline_name="my_pipeline",
            dlt_target_schema="my_dlt_schema",
            uc_enabled=True,
            uc_catalog_name="my_catalog",
            serverless=True,
            num_workers=None,
        )
        mock_created = MagicMock()
        mock_created.pipeline_id = "12345"
        mock_workspace_client.pipelines.create.return_value = mock_created
        sdp_meta._create_sdp_meta_pipeline(cmd)
        kwargs = mock_workspace_client.pipelines.create.call_args.kwargs
        self.assertEqual(kwargs["tags"], {"sdp_meta": "9.9.9"})

    @patch("databricks.labs.sdp_meta.cli.SDPMeta._install_folder", return_value="/Users/test/sdp-meta")
    @patch("databricks.labs.sdp_meta.cli.WorkspaceClient")
    def test_create_sdp_meta_pipeline_tags_carry_version_uc_disabled(
        self, mock_workspace_client, mock_install_folder
    ):
        sdp_meta = SDPMeta(mock_workspace_client)
        sdp_meta.version = "0.0.1"
        cmd = DeployCommand(
            layer="silver",
            onboard_silver_group="groupB",
            sdp_meta_silver_schema="schemaB",
            dataflowspec_silver_table="tableB",
            dataflowspec_silver_path="tests/resources/silver_dataflowspec",
            pipeline_name="silver_pipeline",
            dlt_target_schema="silver_target_schema",
            uc_enabled=False,
            uc_catalog_name=None,
            serverless=False,
            num_workers=2,
        )
        mock_created = MagicMock()
        mock_created.pipeline_id = "98765"
        mock_workspace_client.pipelines.create.return_value = mock_created
        sdp_meta._create_sdp_meta_pipeline(cmd)
        kwargs = mock_workspace_client.pipelines.create.call_args.kwargs
        self.assertEqual(kwargs["tags"], {"sdp_meta": "0.0.1"})

    @patch("databricks.labs.sdp_meta.cli.WorkspaceInstaller")
    @patch("databricks.labs.sdp_meta.cli.WorkspaceClient")
    def test_load_deploy_config_with_json(self, mock_workspace_client, mock_workspace_installer):
        mock_workspace_installer._choice.side_effect = ["Yes", "True", "True", "bronze"]
        mock_workspace_installer._question.side_effect = [
            "uc_catalog", "group", "pipeline_name", "dlt_target_schema"
        ]
        oc_job_details_json = {
            "sdp_meta_schema": "sdp_meta_schema",
            "bronze_dataflowspec_table": "bronze_dataflowspec_table",
            "bronze_dataflowspec_path": "bronze_dataflowspec_path"
        }
        with patch("builtins.open", mock_open(read_data=json.dumps(oc_job_details_json))):
            sdp_meta = SDPMeta(mock_workspace_client)
            sdp_meta._wsi = mock_workspace_installer
            deploy_cmd = sdp_meta._load_deploy_config()
        self.assertTrue(deploy_cmd.uc_enabled)
        self.assertTrue(deploy_cmd.serverless)
        self.assertEqual(deploy_cmd.uc_catalog_name, "uc_catalog")
        self.assertEqual(deploy_cmd.layer, "bronze")
        self.assertEqual(deploy_cmd.onboard_bronze_group, "group")
        self.assertEqual(deploy_cmd.sdp_meta_bronze_schema, "sdp_meta_schema")
        self.assertEqual(deploy_cmd.dataflowspec_bronze_table, "bronze_dataflowspec_table")
        self.assertEqual(deploy_cmd.pipeline_name, "pipeline_name")
        self.assertEqual(deploy_cmd.dlt_target_schema, "dlt_target_schema")

    @patch("databricks.labs.sdp_meta.cli.WorkspaceInstaller")
    @patch("databricks.labs.sdp_meta.cli.WorkspaceClient")
    def test_load_deploy_config_nouc_json(self, mock_workspace_client, mock_workspace_installer):
        mock_workspace_installer._choice.side_effect = ["Yes", "False", "bronze_silver"]
        mock_workspace_installer._question.side_effect = [
            "bronze_group", "silver_group", "4", "pipeline_name",
            "dlt_target_schema"
        ]
        oc_job_details_json = {
            "sdp_meta_schema": "sdp_meta_schema",
            "bronze_dataflowspec_path": "bronze_dataflowspec_path",
            "silver_dataflowspec_path": "silver_dataflowspec_path"
        }
        with patch("builtins.open", mock_open(read_data=json.dumps(oc_job_details_json))):
            sdp_meta = SDPMeta(mock_workspace_client)
            sdp_meta._wsi = mock_workspace_installer
            deploy_cmd = sdp_meta._load_deploy_config()
        self.assertFalse(deploy_cmd.uc_enabled)
        self.assertFalse(deploy_cmd.serverless)
        self.assertEqual(deploy_cmd.layer, "bronze_silver")
        self.assertEqual(deploy_cmd.onboard_bronze_group, "bronze_group")
        self.assertEqual(deploy_cmd.dataflowspec_bronze_path, "bronze_dataflowspec_path")
        self.assertEqual(deploy_cmd.dataflowspec_silver_path, "silver_dataflowspec_path")
        self.assertEqual(deploy_cmd.onboard_silver_group, "silver_group")
        self.assertEqual(deploy_cmd.pipeline_name, "pipeline_name")
        self.assertEqual(deploy_cmd.dlt_target_schema, "dlt_target_schema")

    @patch("databricks.labs.sdp_meta.cli.WorkspaceInstaller")
    @patch("databricks.labs.sdp_meta.cli.WorkspaceClient")
    def test_load_deploy_config_without_json(self, mock_workspace_client, mock_workspace_installer):
        mock_workspace_installer._choice.side_effect = ["No", "True", "True", "bronze"]
        mock_workspace_installer._question.side_effect = [
            "uc_catalog", "group", "sdp_meta_schema", "bronze_dataflowspec",
            "pipeline_name", "dlt_target_schema"
        ]
        sdp_meta = SDPMeta(mock_workspace_client)
        sdp_meta._wsi = mock_workspace_installer
        deploy_cmd = sdp_meta._load_deploy_config()

        self.assertTrue(deploy_cmd.uc_enabled)
        self.assertTrue(deploy_cmd.serverless)
        self.assertEqual(deploy_cmd.uc_catalog_name, "uc_catalog")
        self.assertEqual(deploy_cmd.layer, "bronze")
        self.assertEqual(deploy_cmd.onboard_bronze_group, "group")
        self.assertEqual(deploy_cmd.sdp_meta_bronze_schema, "sdp_meta_schema")
        self.assertEqual(deploy_cmd.dataflowspec_bronze_table, "bronze_dataflowspec")
        self.assertEqual(deploy_cmd.pipeline_name, "pipeline_name")
        self.assertEqual(deploy_cmd.dlt_target_schema, "dlt_target_schema")

    @patch("databricks.labs.sdp_meta.cli.WorkspaceInstaller")
    @patch("databricks.labs.sdp_meta.cli.WorkspaceClient")
    def test_load_deploy_config_with_silver_layer(self, mock_workspace_client, mock_workspace_installer):
        mock_workspace_installer._choice.side_effect = ["No", "True", "True", "silver"]
        mock_workspace_installer._question.side_effect = [
            "uc_catalog", "group", "sdp_meta_schema", "silver_dataflowspec",
            "pipeline_name", "dlt_target_schema"
        ]
        sdp_meta = SDPMeta(mock_workspace_client)
        sdp_meta._wsi = mock_workspace_installer
        deploy_cmd = sdp_meta._load_deploy_config()

        self.assertTrue(deploy_cmd.uc_enabled)
        self.assertTrue(deploy_cmd.serverless)
        self.assertEqual(deploy_cmd.uc_catalog_name, "uc_catalog")
        self.assertEqual(deploy_cmd.layer, "silver")
        self.assertEqual(deploy_cmd.onboard_silver_group, "group")
        self.assertEqual(deploy_cmd.sdp_meta_silver_schema, "sdp_meta_schema")
        self.assertEqual(deploy_cmd.dataflowspec_silver_table, "silver_dataflowspec")
        self.assertEqual(deploy_cmd.pipeline_name, "pipeline_name")
        self.assertEqual(deploy_cmd.dlt_target_schema, "dlt_target_schema")

    @patch("databricks.labs.sdp_meta.cli.WorkspaceInstaller")
    @patch("databricks.labs.sdp_meta.cli.WorkspaceClient")
    def test_load_deploy_config_with_bronze_silver_layer(self, mock_workspace_client, mock_workspace_installer):
        mock_workspace_installer._choice.side_effect = ["No", "True", "True", "bronze_silver"]
        mock_workspace_installer._question.side_effect = [
            "uc_catalog", "bronze_group", "sdp_meta_bronze_schema", "bronze_dataflowspec",
            "silver_group", "sdp_meta_silver_schema", "silver_dataflowspec",
            "pipeline_name", "dlt_target_schema"
        ]
        sdp_meta = SDPMeta(mock_workspace_client)
        sdp_meta._wsi = mock_workspace_installer
        deploy_cmd = sdp_meta._load_deploy_config()

        self.assertTrue(deploy_cmd.uc_enabled)
        self.assertTrue(deploy_cmd.serverless)
        self.assertEqual(deploy_cmd.uc_catalog_name, "uc_catalog")
        self.assertEqual(deploy_cmd.layer, "bronze_silver")
        self.assertEqual(deploy_cmd.onboard_bronze_group, "bronze_group")
        self.assertEqual(deploy_cmd.sdp_meta_bronze_schema, "sdp_meta_bronze_schema")
        self.assertEqual(deploy_cmd.dataflowspec_bronze_table, "bronze_dataflowspec")
        self.assertEqual(deploy_cmd.onboard_silver_group, "silver_group")
        self.assertEqual(deploy_cmd.sdp_meta_silver_schema, "sdp_meta_silver_schema")
        self.assertEqual(deploy_cmd.dataflowspec_silver_table, "silver_dataflowspec")
        self.assertEqual(deploy_cmd.pipeline_name, "pipeline_name")
        self.assertEqual(deploy_cmd.dlt_target_schema, "dlt_target_schema")

    @patch("databricks.labs.sdp_meta.cli.WorkspaceInstaller")
    @patch("databricks.labs.sdp_meta.cli.WorkspaceClient")
    def test_load_deploy_config_from_json_file(self, mock_workspace_client, mock_workspace_installer):
        mock_workspace_installer._choice.side_effect = ["Yes", "True", "True", "bronze_silver"]
        oc_job_details_json = "tests/resources/onboarding_job_details.json"
        import shutil
        shutil.copyfile(oc_job_details_json, "onboarding_job_details.json")
        mock_workspace_installer._question.side_effect = [
            "uc_catalog", "bronze_group", "silver_group",
            "pipeline_name", "dlt_target_schema"
        ]
        sdp_meta = SDPMeta(mock_workspace_client)
        sdp_meta._wsi = mock_workspace_installer
        deploy_cmd = sdp_meta._load_deploy_config()
        self.assertTrue(deploy_cmd.uc_enabled)
        self.assertTrue(deploy_cmd.serverless)
        self.assertEqual(deploy_cmd.uc_catalog_name, "uc_catalog")
        self.assertEqual(deploy_cmd.layer, "bronze_silver")
        self.assertEqual(deploy_cmd.onboard_bronze_group, "bronze_group")
        self.assertEqual(deploy_cmd.onboard_silver_group, "silver_group")
        self.assertEqual(deploy_cmd.sdp_meta_bronze_schema, "sdp_meta_dataflowspecs")
        self.assertEqual(deploy_cmd.dataflowspec_bronze_table, "bronze_dataflowspec")
        self.assertEqual(deploy_cmd.sdp_meta_silver_schema, "sdp_meta_dataflowspecs")
        self.assertEqual(deploy_cmd.dataflowspec_silver_table, "silver_dataflowspec")
        self.assertEqual(deploy_cmd.pipeline_name, "pipeline_name")
        self.assertEqual(deploy_cmd.dlt_target_schema, "dlt_target_schema")

    @patch("os.walk")
    @patch("builtins.open", new_callable=mock_open)
    @patch("databricks.labs.sdp_meta.cli.SDPMeta._my_username", return_value="test_user")
    def test_copy_to_uc_volume(self, mock_my_username, mock_open, mock_os_walk):
        mock_ws = MagicMock()
        sdp_meta = SDPMeta(mock_ws)
        mock_os_walk.return_value = [
            ("/path/to/src", [], ["file1.txt", "file2.txt"]),
            ("/path/to/src/subdir", [], ["file3.txt"]),
        ]
        mock_ws.files.upload = MagicMock()
        sdp_meta.copy_to_uc_volume("file:/path/to/src", "/uc_volume/path/to/dst")
        expected_calls = [
            ("/uc_volume/path/to/dst/src/file1.txt", mock_open.return_value, True),
            ("/uc_volume/path/to/dst/src/file2.txt", mock_open.return_value, True),
            ("/uc_volume/path/to/dst/src/subdir/file3.txt", mock_open.return_value, True),
        ]
        actual_calls = [
            (call[1]["file_path"], call[1]["contents"], call[1]["overwrite"])
            for call in mock_ws.files.upload.call_args_list
        ]
        self.assertEqual(expected_calls, actual_calls)
        self.assertEqual(mock_ws.files.upload.call_count, 3)

    @patch("os.walk", return_value=[])
    @patch("databricks.labs.sdp_meta.cli.SDPMeta._my_username", return_value="test_user")
    def test_copy_to_uc_volume_local_source_empty_raises(self, mock_my_username, mock_os_walk):
        """An empty local supporting-files directory used to no-op silently
        and the onboarding job then produced empty tables. Now it raises so
        the operator sees the problem immediately instead of debugging an
        empty pipeline."""
        mock_ws = MagicMock()
        sdp_meta = SDPMeta(mock_ws)
        with self.assertRaises(FileNotFoundError) as ctx:
            sdp_meta.copy_to_uc_volume("file:/path/to/empty", "/uc_volume/dst")
        self.assertIn("walked local directory", str(ctx.exception))
        self.assertIn("zero files", str(ctx.exception))
        mock_ws.files.upload.assert_not_called()

    @patch("databricks.labs.sdp_meta.cli.SDPMeta._my_username", return_value="test_user")
    def test_copy_to_uc_volume_uc_source_happy_path(self, mock_my_username):
        """When ``local_directory`` is itself a UC Volume path (e.g. when the
        App's Supporting Files Directory field points at a curated UC
        location), os.walk cannot see /Volumes/ — the App container has no
        such mount. The SDK Files API has to do the enumeration instead.

        This test mocks ``list_directory_contents`` with a tiny tree:

            /Volumes/src_cat/sch/vol/root/
                ├── onboarding.yml
                ├── conf/
                │     ├── transforms.json
                │     └── dqe/
                │           └── rules.yml
        """
        from databricks.labs.sdp_meta import cli as cli_mod  # noqa: F401
        mock_ws = MagicMock()
        sdp_meta = SDPMeta(mock_ws)

        def _file(path):
            entry = MagicMock()
            entry.path = path
            entry.is_directory = False
            return entry

        def _dir(path):
            entry = MagicMock()
            entry.path = path
            entry.is_directory = True
            return entry

        tree = {
            "/Volumes/src_cat/sch/vol/root": [
                _file("/Volumes/src_cat/sch/vol/root/onboarding.yml"),
                _dir("/Volumes/src_cat/sch/vol/root/conf"),
            ],
            "/Volumes/src_cat/sch/vol/root/conf": [
                _file("/Volumes/src_cat/sch/vol/root/conf/transforms.json"),
                _dir("/Volumes/src_cat/sch/vol/root/conf/dqe"),
            ],
            "/Volumes/src_cat/sch/vol/root/conf/dqe": [
                _file("/Volumes/src_cat/sch/vol/root/conf/dqe/rules.yml"),
            ],
        }
        mock_ws.files.list_directory_contents.side_effect = lambda p: iter(tree.get(p, []))
        mock_ws.files.download.side_effect = lambda p: MagicMock(contents=f"contents-of:{p}".encode())

        sdp_meta.copy_to_uc_volume(
            "/Volumes/src_cat/sch/vol/root",
            "/Volumes/dst_cat/sch/sch/sdp_meta_conf/",
        )

        expected_upload_targets = {
            "/Volumes/dst_cat/sch/sch/sdp_meta_conf/root/onboarding.yml",
            "/Volumes/dst_cat/sch/sch/sdp_meta_conf/root/conf/transforms.json",
            "/Volumes/dst_cat/sch/sch/sdp_meta_conf/root/conf/dqe/rules.yml",
        }
        actual_targets = {
            call.kwargs["file_path"] for call in mock_ws.files.upload.call_args_list
        }
        self.assertEqual(expected_upload_targets, actual_targets)
        self.assertEqual(mock_ws.files.upload.call_count, 3)
        # Every upload should have overwrite=True so re-runs are idempotent.
        for call in mock_ws.files.upload.call_args_list:
            self.assertTrue(call.kwargs["overwrite"])

    @patch("databricks.labs.sdp_meta.cli.SDPMeta._my_username", return_value="test_user")
    def test_copy_to_uc_volume_uc_source_empty_raises(self, mock_my_username):
        """The original bug: pointing the App's Supporting Files Directory at
        an empty (or unmounted) UC Volume returns zero files. The previous
        os.walk-only implementation logged 'complete!!!' and the onboarding
        job then produced empty tables. Now the operator sees the failure
        at copy time."""
        mock_ws = MagicMock()
        sdp_meta = SDPMeta(mock_ws)
        mock_ws.files.list_directory_contents.return_value = iter([])
        with self.assertRaises(FileNotFoundError) as ctx:
            sdp_meta.copy_to_uc_volume(
                "/Volumes/src_cat/sch/vol/empty",
                "/Volumes/dst_cat/sch/sch/sdp_meta_conf/",
            )
        self.assertIn("empty or unreadable", str(ctx.exception))
        self.assertIn("tables are empty", str(ctx.exception))
        mock_ws.files.upload.assert_not_called()

    @patch("databricks.labs.sdp_meta.cli.SDPMeta._my_username", return_value="test_user")
    def test_copy_to_uc_volume_uc_source_list_failure_raises(self, mock_my_username):
        """If the SDK raises while listing the source (permission denied,
        path doesn't exist, transient API error), surface it as a clear
        FileNotFoundError with the underlying cause chained."""
        mock_ws = MagicMock()
        sdp_meta = SDPMeta(mock_ws)
        mock_ws.files.list_directory_contents.side_effect = PermissionError("READ_VOLUME denied")
        with self.assertRaises(FileNotFoundError) as ctx:
            sdp_meta.copy_to_uc_volume(
                "/Volumes/src_cat/sch/vol/forbidden",
                "/Volumes/dst_cat/sch/sch/sdp_meta_conf/",
            )
        self.assertIn("Could not list UC Volume directory", str(ctx.exception))
        self.assertIn("READ_VOLUME", str(ctx.exception))
        mock_ws.files.upload.assert_not_called()

    def test_onboard_command_silver_layer_validation(self):
        """Test validation for silver layer specific cases."""
        # Test silver layer without silver_dataflowspec_table (line 91)
        with self.assertRaises(ValueError) as context:
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="silver",
                env="dev",
                import_author="John Doe",
                version="1.0",
                sdp_meta_schema="sdp_meta",
                dbfs_path="/dbfs",
                uc_enabled=False,
                silver_dataflowspec_table=None,
                silver_dataflowspec_path="/path/to/silver",
                overwrite=True,
            )
        self.assertIn("silver_dataflowspec_table is required", str(context.exception))

        # Test silver layer without silver_dataflowspec_path when uc_enabled=False (line 94)
        with self.assertRaises(ValueError) as context:
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="silver",
                env="dev",
                import_author="John Doe",
                version="1.0",
                sdp_meta_schema="sdp_meta",
                dbfs_path="/dbfs",
                uc_enabled=False,
                silver_dataflowspec_table="silver_table",
                silver_dataflowspec_path=None,
                overwrite=True,
            )
        self.assertIn("silver_dataflowspec_path is required", str(context.exception))

    def test_onboard_command_version_validation(self):
        """Test version validation (line 100)."""
        with self.assertRaises(ValueError) as context:
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="bronze",
                env="dev",
                import_author="John Doe",
                version=None,
                sdp_meta_schema="sdp_meta",
                dbfs_path="/dbfs",
                uc_enabled=False,
                bronze_dataflowspec_path="/path/to/bronze",
                overwrite=True,
            )
        self.assertIn("version is required", str(context.exception))

    def test_deploy_command_validation_cases(self):
        """Test DeployCommand validation cases for missing coverage."""
        # Test bronze layer without dataflowspec_bronze_table when uc_enabled=True (line 136)
        with self.assertRaises(ValueError) as context:
            DeployCommand(
                layer="bronze",
                onboard_bronze_group="A1",
                sdp_meta_bronze_schema="bronze_schema",
                dataflowspec_bronze_table=None,
                pipeline_name="test_pipeline",
                dlt_target_schema="target_schema",
                uc_enabled=True,
                uc_catalog_name="test_catalog",  # Need this to pass earlier validation
                serverless=True,
            )
        self.assertIn("dataflowspec_bronze_table is required", str(context.exception))

        # Test silver layer without onboard_silver_group (line 141)
        with self.assertRaises(ValueError) as context:
            DeployCommand(
                layer="silver",
                onboard_silver_group=None,
                sdp_meta_silver_schema="silver_schema",
                dataflowspec_silver_table="silver_table",
                pipeline_name="test_pipeline",
                dlt_target_schema="target_schema",
                uc_enabled=True,
                uc_catalog_name="test_catalog",
                serverless=True,
            )
        self.assertIn("onboard_silver_group is required", str(context.exception))

        # Test silver layer without dataflowspec_silver_table when uc_enabled=True (line 143)
        with self.assertRaises(ValueError) as context:
            DeployCommand(
                layer="silver",
                onboard_silver_group="A1",
                sdp_meta_silver_schema="silver_schema",
                dataflowspec_silver_table=None,
                pipeline_name="test_pipeline",
                dlt_target_schema="target_schema",
                uc_enabled=True,
                uc_catalog_name="test_catalog",
                serverless=True,
            )
        self.assertIn("dataflowspec_silver_table is required", str(context.exception))

        # Test silver layer without dataflowspec_silver_path when uc_enabled=False (line 145)
        with self.assertRaises(ValueError) as context:
            DeployCommand(
                layer="silver",
                onboard_silver_group="A1",
                sdp_meta_silver_schema="silver_schema",
                dataflowspec_silver_table="silver_table",
                dataflowspec_silver_path=None,
                pipeline_name="test_pipeline",
                dlt_target_schema="target_schema",
                uc_enabled=False,
                serverless=True,
            )
        self.assertIn("dataflowspec_silver_path is required", str(context.exception))

        # Test without pipeline_name (line 147)
        with self.assertRaises(ValueError) as context:
            DeployCommand(
                layer="bronze",
                onboard_bronze_group="A1",
                sdp_meta_bronze_schema="bronze_schema",
                dataflowspec_bronze_table="bronze_table",
                pipeline_name=None,
                dlt_target_schema="target_schema",
                uc_enabled=True,
                uc_catalog_name="test_catalog",
                serverless=True,
            )
        self.assertIn("pipeline_name is required", str(context.exception))

        # Test without dlt_target_schema (line 149)
        with self.assertRaises(ValueError) as context:
            DeployCommand(
                layer="bronze",
                onboard_bronze_group="A1",
                sdp_meta_bronze_schema="bronze_schema",
                dataflowspec_bronze_table="bronze_table",
                pipeline_name="test_pipeline",
                dlt_target_schema=None,
                uc_enabled=True,
                uc_catalog_name="test_catalog",
                serverless=True,
            )
        self.assertIn("dlt_target_schema is required", str(context.exception))

    def test_my_username_method_without_me_attribute(self):
        """Test _my_username method when _me attribute doesn't exist (line 162)."""
        mock_ws = MagicMock()
        # Remove the _me attribute to test the if condition
        if hasattr(mock_ws, '_me'):
            delattr(mock_ws, '_me')

        mock_current_user = MagicMock()
        mock_me = MagicMock()
        mock_me.user_name = "test_user_no_me"
        mock_current_user.me.return_value = mock_me
        mock_ws.current_user = mock_current_user

        sdp_meta = SDPMeta(mock_ws)
        username = sdp_meta._my_username()

        self.assertEqual(username, "test_user_no_me")
        mock_current_user.me.assert_called_once()

    @patch("databricks.labs.sdp_meta.cli.uuid.uuid4")
    @patch("builtins.open", new_callable=mock_open)
    def test_load_onboard_config_ui_unity_catalog_enabled(self, mock_open_file, mock_uuid):
        """Test _load_onboard_config_ui with Unity Catalog enabled."""
        mock_uuid.return_value.hex = "test_uuid"
        mock_ws = MagicMock()
        mock_ws.clusters.select_spark_version.return_value = "14.3.x-scala2.12"
        sdp_meta = SDPMeta(mock_ws)
        sdp_meta._wsi = MagicMock()
        sdp_meta._wsi._short_name = "test_user"

        form_data = {
            'unity_catalog_enabled': "1",
            'unity_catalog_name': "test_catalog",
            'serverless': "1",
            'onboarding_file_path': 'custom/path/onboarding.json',
            'local_directory': '/custom/dir/',
            'sdp_meta_schema': 'custom_schema',
            'bronze_schema': 'custom_bronze',
            'silver_schema': 'custom_silver',
            'sdp_meta_layer': "1",  # bronze_silver
            'bronze_table': 'custom_bronze_table',
            'overwrite': "1",
            'version': 'v2',
            'environment': 'dev',
            'author': 'custom_author',
            'update_paths': "1"
        }

        result = sdp_meta._load_onboard_config_ui(form_data)

        # Verify Unity Catalog settings
        self.assertTrue(result.uc_enabled)
        self.assertEqual(result.uc_catalog_name, "test_catalog")
        self.assertIsNone(result.dbfs_path)

        # Verify serverless settings
        self.assertTrue(result.serverless)
        self.assertIsNone(result.cloud)
        self.assertIsNone(result.dbr_version)

        # Verify other settings
        self.assertEqual(result.onboard_layer, "bronze_silver")
        self.assertEqual(result.bronze_dataflowspec_table, "custom_bronze_table")
        self.assertTrue(result.overwrite)
        self.assertEqual(result.version, "v2")
        self.assertEqual(result.env, "dev")
        self.assertEqual(result.import_author, "custom_author")
        self.assertTrue(result.update_paths)

    @patch("databricks.labs.sdp_meta.cli.uuid.uuid4")
    @patch("builtins.open", new_callable=mock_open)
    def test_load_onboard_config_ui_unity_catalog_disabled(self, mock_open_file, mock_uuid):
        """Test _load_onboard_config_ui with Unity Catalog disabled."""
        mock_uuid.return_value.hex = "test_uuid"
        mock_ws = MagicMock()
        mock_ws.clusters.select_spark_version.return_value = "14.3.x-scala2.12"
        sdp_meta = SDPMeta(mock_ws)
        sdp_meta._wsi = MagicMock()
        sdp_meta._wsi._short_name = "test_user"

        form_data = {
            'unity_catalog_enabled': "0",  # Disabled
            'serverless': "0",  # Disabled
            'sdp_meta_layer': "0",  # bronze
        }

        result = sdp_meta._load_onboard_config_ui(form_data)

        # Verify Unity Catalog settings
        self.assertFalse(result.uc_enabled)
        self.assertEqual(result.dbfs_path, "dbfs:/sdp-meta_cli_demo_test_uuid")

        # Verify non-serverless settings
        self.assertFalse(result.serverless)
        self.assertEqual(result.cloud, "aws")
        self.assertEqual(result.dbr_version, "14.3.x-scala2.12")

        # Verify layer settings
        self.assertEqual(result.onboard_layer, "bronze")

    @patch("databricks.labs.sdp_meta.cli.uuid.uuid4")
    @patch("builtins.open", new_callable=mock_open)
    def test_load_onboard_config_ui_silver_layer(self, mock_open_file, mock_uuid):
        """Test _load_onboard_config_ui with silver layer."""
        mock_uuid.return_value.hex = "test_uuid"
        mock_ws = MagicMock()
        # Mock the clusters.select_spark_version to return a string instead of MagicMock
        mock_ws.clusters.select_spark_version.return_value = "13.3.x-scala2.12"
        sdp_meta = SDPMeta(mock_ws)
        sdp_meta._wsi = MagicMock()
        sdp_meta._wsi._short_name = "test_user"

        form_data = {
            'unity_catalog_enabled': "0",
            'sdp_meta_layer': "2",  # silver
        }

        result = sdp_meta._load_onboard_config_ui(form_data)

        # Verify layer settings
        self.assertEqual(result.onboard_layer, "silver")
        self.assertEqual(result.silver_dataflowspec_table, "silver_dataflowspec")

    @patch("os.path.isfile")
    @patch("builtins.open", new_callable=mock_open)
    def test_load_deploy_config_ui_with_onboarding_json(self, mock_open_file, mock_isfile):
        """Test _load_deploy_config_ui with existing onboarding JSON."""
        mock_isfile.return_value = True
        onboarding_data = {
            "sdp_meta_schema": "test_schema",
            "bronze_dataflowspec_table": "bronze_table",
            "silver_dataflowspec_table": "silver_table",
            "bronze_dataflowspec_path": "/bronze/path",
            "silver_dataflowspec_path": "/silver/path"
        }
        mock_open_file.return_value.read.return_value = json.dumps(onboarding_data)

        mock_ws = MagicMock()
        sdp_meta = SDPMeta(mock_ws)

        input_params = {
            "load_from_ojd_json": True,
            "uc_enabled": True,
            "uc_catalog_name": "test_catalog",
            "serverless": True,
            "layer": "bronze_silver",
            "onboard_bronze_group": "B1",
            "onboard_silver_group": "S1",
            "pipeline_name": "test_pipeline",
            "dlt_target_schema": "target_schema"
        }

        result = sdp_meta._load_deploy_config_ui(input_params)

        # Verify settings loaded from JSON
        self.assertTrue(result.uc_enabled)
        self.assertEqual(result.uc_catalog_name, "test_catalog")
        self.assertTrue(result.serverless)
        self.assertEqual(result.layer, "bronze_silver")
        self.assertEqual(result.sdp_meta_bronze_schema, "test_schema")
        self.assertEqual(result.dataflowspec_bronze_table, "bronze_table")
        self.assertEqual(result.sdp_meta_silver_schema, "test_schema")
        self.assertEqual(result.dataflowspec_silver_table, "silver_table")

    @patch("os.path.isfile")
    def test_load_deploy_config_ui_without_onboarding_json(self, mock_isfile):
        """Test _load_deploy_config_ui without onboarding JSON."""
        mock_isfile.return_value = False

        mock_ws = MagicMock()
        sdp_meta = SDPMeta(mock_ws)

        input_params = {
            "load_from_ojd_json": False,
            "uc_enabled": False,
            "layer": "bronze",
            "onboard_bronze_group": "B1",
            "sdp_meta_bronze_schema": "bronze_schema",
            "dataflowspec_bronze_table": "bronze_table",
            "dataflowspec_bronze_path": "/bronze/path",
            "num_workers": 8,
            "pipeline_name": "test_pipeline",
            "dlt_target_schema": "target_schema"
        }

        result = sdp_meta._load_deploy_config_ui(input_params)

        # Verify settings
        self.assertFalse(result.uc_enabled)
        self.assertFalse(result.serverless)
        self.assertEqual(result.layer, "bronze")
        self.assertEqual(result.onboard_bronze_group, "B1")
        self.assertEqual(result.sdp_meta_bronze_schema, "bronze_schema")
        self.assertEqual(result.dataflowspec_bronze_table, "bronze_table")
        self.assertEqual(result.dataflowspec_bronze_path, "/bronze/path")
        self.assertEqual(result.num_workers, 8)

    @patch("os.path.isfile")
    @patch("builtins.open", new_callable=mock_open)
    def test_load_deploy_config_ui_non_serverless(self, mock_open_file, mock_isfile):
        """Test _load_deploy_config_ui with non-serverless configuration."""
        mock_isfile.return_value = True
        onboarding_data = {
            "sdp_meta_schema": "test_schema",
            "silver_dataflowspec_path": "/test/path/silver"
        }
        mock_open_file.return_value.read.return_value = json.dumps(onboarding_data)

        mock_ws = MagicMock()
        sdp_meta = SDPMeta(mock_ws)

        input_params = {
            "load_from_ojd_json": True,
            "uc_enabled": False,
            "serverless": False,
            "layer": "silver",
            "onboard_silver_group": "S1",
            "num_workers": 6,
            "pipeline_name": "test_pipeline",
            "dlt_target_schema": "target_schema"
        }

        result = sdp_meta._load_deploy_config_ui(input_params)

        # Verify non-serverless settings
        self.assertFalse(result.serverless)
        self.assertEqual(result.num_workers, 6)

    def test_create_uc_volume_exception_handling(self):
        """Test create_uc_volume exception handling (lines 207-208)."""
        mock_ws = MagicMock()
        mock_ws.volumes.create.side_effect = Exception("Volume already exists")

        sdp_meta = SDPMeta(mock_ws)

        result = sdp_meta.create_uc_volume("test_catalog", "test_schema")

        self.assertEqual(result, "/Volumes/test_catalog/test_schema/test_schema/")
        mock_ws.volumes.create.assert_called_once()

    def test_onboard_ui_function(self):
        """Test onboard_ui wrapper function (lines 758-760)."""
        from databricks.labs.sdp_meta.cli import onboard_ui
        mock_sdp_meta = MagicMock()
        form_data = {"test": "data"}
        onboard_ui(mock_sdp_meta, form_data)
        # Verify the function calls the SDPMeta methods
        mock_sdp_meta._load_onboard_config_ui.assert_called_once_with(form_data)
        mock_sdp_meta.onboard.assert_called_once()

    def test_deploy_function(self):
        """Test deploy wrapper function (lines 763-766)."""
        from databricks.labs.sdp_meta.cli import deploy
        mock_sdp_meta = MagicMock()
        deploy(mock_sdp_meta)
        # Verify the function calls the SDPMeta methods
        mock_sdp_meta._load_deploy_config.assert_called_once()
        mock_sdp_meta.deploy.assert_called_once()

    def test_deploy_ui_function(self):
        """Test deploy_ui wrapper function (lines 770-772)."""
        from databricks.labs.sdp_meta.cli import deploy_ui
        mock_sdp_meta = MagicMock()
        form_data = {"test": "data"}
        deploy_ui(mock_sdp_meta, form_data)
        # Verify the function calls the SDPMeta methods
        mock_sdp_meta._load_deploy_config_ui.assert_called_once_with(form_data)
        mock_sdp_meta.deploy.assert_called_once()

    @patch("databricks.labs.sdp_meta.cli.WorkspaceClient")
    @patch("databricks.labs.sdp_meta.cli.MAPPING")
    def test_main_function_ui_commands(self, mock_mapping, mock_workspace_client):
        """Test main function with UI commands (line 798)."""
        from databricks.labs.sdp_meta.cli import main
        import json

        # Mock the mapping dictionary
        mock_ui_func = MagicMock()
        mock_mapping.__getitem__.return_value = mock_ui_func
        mock_mapping.__contains__.return_value = True

        # Mock WorkspaceClient
        mock_ws = MagicMock()
        mock_workspace_client.return_value = mock_ws

        # Create payload in the format expected by main function
        payload = {
            "command": "onboard_ui",
            "flags": {"log_level": "disabled"},
            "test": "payload"
        }
        raw_json = json.dumps(payload)

        # Test UI command path (line 798)
        main(raw_json)

        # Verify the UI command was called with both sdp_meta and payload
        mock_ui_func.assert_called_once()
        args = mock_ui_func.call_args[0]
        self.assertEqual(len(args), 2)  # sdp_meta and payload

    @patch("databricks.labs.sdp_meta.cli.WorkspaceClient")
    @patch("databricks.labs.sdp_meta.cli.MAPPING")
    def test_main_function_non_ui_commands(self, mock_mapping, mock_workspace_client):
        """Test main function with non-UI commands (line 800)."""
        from databricks.labs.sdp_meta.cli import main
        import json

        # Mock the mapping dictionary
        mock_func = MagicMock()
        mock_mapping.__getitem__.return_value = mock_func
        mock_mapping.__contains__.return_value = True

        # Mock WorkspaceClient
        mock_ws = MagicMock()
        mock_workspace_client.return_value = mock_ws

        # Create payload in the format expected by main function
        payload = {
            "command": "deploy",
            "flags": {"log_level": "disabled"}
        }
        raw_json = json.dumps(payload)

        # Test non-UI command path (line 800)
        main(raw_json)

        # Verify the command was called with only sdp_meta
        mock_func.assert_called_once()
        args = mock_func.call_args[0]
        self.assertEqual(len(args), 1)  # only sdp_meta

    def test_bronze_layer_uc_disabled_path_validation(self):
        """Test bronze layer with UC disabled path requirement (line 89)."""
        with self.assertRaises(ValueError) as context:
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="bronze",
                env="dev",
                import_author="John Doe",
                version="1.0",
                sdp_meta_schema="sdp_meta",
                uc_enabled=False,
                dbfs_path="/dbfs",
                bronze_dataflowspec_path=None,  # This should trigger the error
                overwrite=True,
            )
        self.assertIn("bronze_dataflowspec_path is required", str(context.exception))

    def _build_onboard_kwargs(self, **overrides):
        """Minimal valid `OnboardCommand` kwargs for identifier-validation tests.

        Centralised so each test only has to declare the *one* field it
        wants to mutate (e.g. `uc_catalog_name="my-cat"`) rather than
        re-typing the full happy-path constructor every time.
        """
        kwargs = dict(
            onboarding_file_path="tests/resources/onboarding.json",
            onboarding_files_dir_path="tests/resources/",
            onboard_layer="bronze",
            env="dev",
            import_author="John Doe",
            version="1.0",
            cloud="aws",
            sdp_meta_schema="sdp_meta",
            bronze_dataflowspec_path="tests/resources/bronze_dataflowspec",
            silver_dataflowspec_path="tests/resources/silver_dataflowspec",
            uc_enabled=True,
            uc_catalog_name="uc_catalog",
            uc_volume_path="uc_catalog/sdp_meta/files",
            overwrite=True,
            bronze_dataflowspec_table="bronze_dataflowspec",
            silver_dataflowspec_table="silver_dataflowspec",
            update_paths=True,
        )
        kwargs.update(overrides)
        return kwargs

    def test_onboard_command_rejects_hyphenated_uc_catalog(self):
        # Hyphens are legal in UC but not in regular SQL identifiers; we
        # reject at construction time so users see a clear error instead
        # of a Spark name-resolution failure later (issue #261).
        with self.assertRaisesRegex(ValueError, r"regular identifier"):
            OnboardCommand(**self._build_onboard_kwargs(uc_catalog_name="my-cat"))

    def test_onboard_command_rejects_hyphenated_sdp_meta_schema(self):
        with self.assertRaisesRegex(ValueError, r"regular identifier"):
            OnboardCommand(**self._build_onboard_kwargs(sdp_meta_schema="bad-schema"))

    def test_onboard_command_rejects_dotted_table_name(self):
        # A period inside a single identifier is forbidden (it's the
        # multi-part separator). We must reject it here so it can't be
        # spliced into a SQL identifier downstream.
        with self.assertRaisesRegex(ValueError, r"regular identifier"):
            OnboardCommand(**self._build_onboard_kwargs(
                bronze_dataflowspec_table="bad.name",
            ))

    def test_onboard_command_rejects_leading_digit_table_name(self):
        with self.assertRaisesRegex(ValueError, r"regular identifier"):
            OnboardCommand(**self._build_onboard_kwargs(
                silver_dataflowspec_table="9bronze",
            ))

    def test_onboard_command_skips_uc_validation_when_uc_disabled(self):
        # When uc_enabled=False we don't read uc_catalog_name, so the
        # validator must not be triggered for it. (Bronze schema is
        # still validated since it's used in non-UC paths too.)
        cmd = OnboardCommand(**self._build_onboard_kwargs(
            uc_enabled=False,
            uc_catalog_name="any-thing-goes",
            uc_volume_path=None,
            dbfs_path="/dbfs",
        ))
        self.assertFalse(cmd.uc_enabled)

    def test_onboard_command_rejects_hyphenated_bronze_schema_even_without_uc(self):
        # ``bronze_schema`` / ``silver_schema`` get spliced into the
        # rendered onboarding template (see ``update_ws_onboarding_paths``)
        # and from there into pipeline SQL identifiers regardless of
        # whether UC is enabled. Reject hyphens at the input boundary so
        # the failure mode is "obvious validation error here" instead of
        # "confusing Spark identifier-resolution error five minutes into
        # the pipeline run" (issue #261).
        with self.assertRaisesRegex(ValueError, r"regular identifier"):
            OnboardCommand(**self._build_onboard_kwargs(
                uc_enabled=False,
                uc_volume_path=None,
                dbfs_path="/dbfs",
                bronze_schema="bad-schema",
            ))

    def test_onboard_command_rejects_hyphenated_silver_schema_even_without_uc(self):
        with self.assertRaisesRegex(ValueError, r"regular identifier"):
            OnboardCommand(**self._build_onboard_kwargs(
                uc_enabled=False,
                uc_volume_path=None,
                dbfs_path="/dbfs",
                silver_schema="bad-schema",
            ))

    def _build_deploy_kwargs(self, **overrides):
        kwargs = dict(
            layer="bronze_silver",
            onboard_bronze_group="A1",
            onboard_silver_group="A1",
            sdp_meta_bronze_schema="dlt_bronze_schema",
            sdp_meta_silver_schema="dlt_silver_schema",
            dataflowspec_bronze_table="bronze_dataflowspec_table",
            dataflowspec_silver_table="silver_dataflowspec_table",
            num_workers=1,
            uc_catalog_name="uc_catalog",
            pipeline_name="unittest_dlt_pipeline",
            dlt_target_schema="dlt_target_schema",
            uc_enabled=True,
            serverless=False,
            dbfs_path="/dbfs",
        )
        kwargs.update(overrides)
        return kwargs

    def test_deploy_command_rejects_hyphenated_uc_catalog(self):
        with self.assertRaisesRegex(ValueError, r"regular identifier"):
            DeployCommand(**self._build_deploy_kwargs(uc_catalog_name="my-cat"))

    def test_deploy_command_rejects_hyphenated_dlt_target_schema(self):
        # `dlt_target_schema` is spliced unquoted into pipeline target
        # configuration, so the same strict rule applies.
        with self.assertRaisesRegex(ValueError, r"regular identifier"):
            DeployCommand(**self._build_deploy_kwargs(
                dlt_target_schema="bad-schema",
            ))

    def test_deploy_command_rejects_dotted_dataflowspec_table(self):
        with self.assertRaisesRegex(ValueError, r"regular identifier"):
            DeployCommand(**self._build_deploy_kwargs(
                dataflowspec_bronze_table="bad.name",
            ))

    def test_get_schema_from_json_with_sdp_meta_key(self):
        """Test _get_schema_from_json returns value for sdp_meta_schema key."""
        oc_json = {"sdp_meta_schema": "my_schema"}
        result = SDPMeta._get_schema_from_json(oc_json)
        self.assertEqual(result, "my_schema")

    def test_get_schema_from_json_with_legacy_dlt_meta_key(self):
        """Test _get_schema_from_json returns value for legacy dlt_meta_schema key."""
        oc_json = {"dlt_meta_schema": "legacy_schema"}
        with self.assertLogs('databricks.labs.sdp_meta', level='WARNING') as cm:
            result = SDPMeta._get_schema_from_json(oc_json)
        self.assertEqual(result, "legacy_schema")
        self.assertTrue(any("legacy key 'dlt_meta_schema'" in msg for msg in cm.output))

    def test_get_schema_from_json_missing_key_raises(self):
        """Test _get_schema_from_json raises KeyError when neither key is present."""
        oc_json = {"some_other_key": "value"}
        with self.assertRaises(KeyError) as context:
            SDPMeta._get_schema_from_json(oc_json)
        self.assertIn("sdp_meta_schema", str(context.exception))
        self.assertIn("dlt_meta_schema", str(context.exception))


class CliCommandWiringTests(unittest.TestCase):
    """Lock-in: every command declared in `labs.yml` must have a matching
    handler in `cli.py::MAPPING`, and vice versa.

    Background: the four `bundle-*` commands shipped on issue_278 with full
    docstrings, docs, demo, and 122 unit/E2E tests -- but were never wired
    into `labs.yml` or `MAPPING`. Every existing test exercised the bundle
    handlers as plain Python imports, so the dispatcher gap (`cannot find
    command: bundle-init`) sailed past CI. These assertions fail loudly
    if anyone adds a command on one side without the other."""

    @classmethod
    def setUpClass(cls):
        import yaml
        from pathlib import Path
        repo_root = Path(__file__).resolve().parent.parent
        with open(repo_root / "labs.yml") as fh:
            cls._labs_yml = yaml.safe_load(fh)

    def _labs_yml_command_names(self):
        return {entry["name"] for entry in self._labs_yml["commands"]}

    def _mapping_keys(self):
        from databricks.labs.sdp_meta.cli import MAPPING
        return set(MAPPING.keys())

    def test_every_labs_yml_command_has_a_mapping_handler(self):
        labs_yml_cmds = self._labs_yml_command_names()
        mapping = self._mapping_keys()
        missing_in_mapping = labs_yml_cmds - mapping
        self.assertFalse(
            missing_in_mapping,
            f"labs.yml declares commands with no MAPPING handler: "
            f"{sorted(missing_in_mapping)}. Add a wrapper in cli.py and "
            f"register it in MAPPING.",
        )

    def test_every_non_ui_mapping_handler_is_in_labs_yml(self):
        # `*_ui` entries are intentionally NOT in labs.yml -- they're
        # invoked by the install UI, not by `databricks labs sdp-meta`.
        labs_yml_cmds = self._labs_yml_command_names()
        mapping = self._mapping_keys()
        non_ui_mapping = {k for k in mapping if not k.endswith("_ui")}
        missing_in_yaml = non_ui_mapping - labs_yml_cmds
        self.assertFalse(
            missing_in_yaml,
            f"MAPPING has handlers with no labs.yml entry: "
            f"{sorted(missing_in_yaml)}. Add a `- name: <cmd>` entry to "
            f"labs.yml so `databricks labs sdp-meta <cmd>` is reachable.",
        )

    def test_bundle_commands_are_wired_end_to_end(self):
        """Belt-and-suspenders: each of the four bundle-* commands is
        explicitly named here so the failure message is unambiguous if
        somebody deletes one side of the wiring during cleanup."""
        mapping = self._mapping_keys()
        labs_yml_cmds = self._labs_yml_command_names()
        for cmd in (
            "bundle-init",
            "bundle-prepare-wheel",
            "bundle-validate",
            "bundle-add-flow",
        ):
            self.assertIn(cmd, mapping, f"{cmd!r} missing from cli.MAPPING")
            self.assertIn(cmd, labs_yml_cmds, f"{cmd!r} missing from labs.yml")

    def test_main_dispatches_bundle_command_through_mapping(self):
        """Functional smoke test: the `main()` dispatcher actually finds
        and calls the wired bundle handler -- the exact code path that
        broke when the wiring was missing."""
        captured = {}

        def fake_handler(sdp_meta, flags=None):
            captured["called"] = True
            captured["sdp_meta_type"] = type(sdp_meta).__name__

        with patch.dict(
            "databricks.labs.sdp_meta.cli.MAPPING",
            {"bundle-init": fake_handler},
            clear=False,
        ), patch("databricks.labs.sdp_meta.cli.WorkspaceClient") as ws_cls:
            ws_cls.return_value = MagicMock()
            payload = json.dumps({
                "command": "bundle-init",
                "flags": {"log_level": "disabled"},
            })
            main(payload)

        self.assertTrue(captured.get("called"),
                        "main() did not dispatch bundle-init through MAPPING")
        self.assertEqual(captured.get("sdp_meta_type"), "SDPMeta")

    def test_main_raises_clearly_for_unknown_command(self):
        """Regression for the user-visible error string -- if anyone
        rewrites the dispatcher and changes the message format, docs
        and the bundle template's success_message go stale silently
        unless this test catches it."""
        with patch("databricks.labs.sdp_meta.cli.WorkspaceClient") as ws_cls:
            ws_cls.return_value = MagicMock()
            payload = json.dumps({
                "command": "definitely-not-a-real-command",
                "flags": {"log_level": "disabled"},
            })
            with self.assertRaises(KeyError) as ctx:
                main(payload)
            self.assertIn("definitely-not-a-real-command", str(ctx.exception))
            self.assertIn("Available", str(ctx.exception))

    def test_main_passes_flags_to_bundle_wrapper(self):
        """Wired with `cli.py::main()`: every `bundle-*` command must
        receive the labs.yml-declared flags as kwargs, not as a payload
        the wrapper has to re-parse. The `--quickstart` plumbing on
        bundle-init is the load-bearing case."""
        captured = {}

        def fake_handler(sdp_meta, flags=None):
            captured["flags"] = flags

        with patch.dict(
            "databricks.labs.sdp_meta.cli.MAPPING",
            {"bundle-init": fake_handler},
            clear=False,
        ), patch("databricks.labs.sdp_meta.cli.WorkspaceClient") as ws_cls:
            ws_cls.return_value = MagicMock()
            payload = json.dumps({
                "command": "bundle-init",
                "flags": {
                    "log_level": "disabled",
                    "quickstart": "true",
                    "output-dir": "/tmp/foo",
                },
            })
            main(payload)

        self.assertIsNotNone(captured.get("flags"))
        self.assertEqual(captured["flags"].get("quickstart"), "true")
        self.assertEqual(captured["flags"].get("output-dir"), "/tmp/foo")
        # log_level must be popped before reaching the wrapper -- otherwise
        # every wrapper would have to remember to filter it out.
        self.assertNotIn("log_level", captured["flags"])

    def test_main_uses_profile_flag_for_workspace_client(self):
        """The non-interactive wheel-upload path accepts --profile, so the
        shared WorkspaceClient must be built with the same profile before the
        wrapper runs."""
        with patch.dict(
            "databricks.labs.sdp_meta.cli.MAPPING",
            {"bundle-prepare-wheel": lambda sdp_meta, flags=None: None},
            clear=False,
        ), patch("databricks.labs.sdp_meta.cli.WorkspaceClient") as ws_cls:
            ws_cls.return_value = MagicMock()
            payload = json.dumps({
                "command": "bundle-prepare-wheel",
                "flags": {
                    "log_level": "disabled",
                    "profile": "DEFAULT",
                },
            })
            main(payload)

        ws_cls.assert_called_once_with(
            product="sdp-meta",
            product_version=__version__,
            profile="DEFAULT",
        )


class OnboardBuildWheelFlagTests(unittest.TestCase):
    """`onboard --build-and-upload-whl` uses the local wheel for the onboarding job."""

    def _onboard_cmd(self, **overrides):
        kwargs = dict(
            onboarding_file_path="demo/conf/json/onboarding.template",
            onboarding_files_dir_path="file:/demo/",
            onboard_layer="bronze_silver",
            env="prod",
            import_author="author",
            version="v1",
            sdp_meta_schema="sdp_meta_dataflowspecs",
            bronze_schema="sdp_meta_bronze",
            silver_schema="sdp_meta_silver",
            uc_enabled=True,
            uc_catalog_name="main",
            bronze_dataflowspec_table="bronze_dataflowspec",
            silver_dataflowspec_table="silver_dataflowspec",
        )
        kwargs.update(overrides)
        return OnboardCommand(**kwargs)

    def test_onboard_build_and_upload_whl_sets_dependency(self):
        from databricks.labs.sdp_meta.cli import onboard as cli_onboard

        sdp_meta = MagicMock()
        cmd = self._onboard_cmd()
        sdp_meta._load_onboard_config.return_value = cmd
        wheel_path = "/Volumes/main/sdp_meta_wheels/sdp_meta_wheels/databricks_labs_sdp_meta.whl"
        with patch("databricks.labs.sdp_meta.bundle.bundle_prepare_wheel", return_value=wheel_path) as fake_run:
            cli_onboard(
                sdp_meta,
                flags={
                    "build-and-upload-whl": "true",
                    "uc-schema-name": "sdp_meta_wheels",
                    "uc-volume-name": "sdp_meta_wheels",
                    "profile": "DEFAULT",
                },
            )

        build_cmd = fake_run.call_args[0][0]
        self.assertEqual(build_cmd.uc_catalog, "main")
        self.assertEqual(build_cmd.uc_schema, "sdp_meta_wheels")
        self.assertEqual(build_cmd.uc_volume, "sdp_meta_wheels")
        self.assertEqual(build_cmd.profile, "DEFAULT")
        self.assertEqual(cmd.sdp_meta_dependency, wheel_path)
        sdp_meta.onboard.assert_called_once_with(cmd)

    def test_onboard_build_and_upload_whl_accepts_normalized_flag_name(self):
        from databricks.labs.sdp_meta.cli import onboard as cli_onboard

        sdp_meta = MagicMock()
        cmd = self._onboard_cmd()
        sdp_meta._load_onboard_config.return_value = cmd
        wheel_path = "/Volumes/main/sdp_meta_wheels/sdp_meta_wheels/databricks_labs_sdp_meta.whl"
        with patch("databricks.labs.sdp_meta.bundle.bundle_prepare_wheel", return_value=wheel_path) as fake_run:
            cli_onboard(
                sdp_meta,
                flags={
                    "build_and_upload_whl": "true",
                    "uc_schema_name": "sdp_meta_wheels",
                    "uc_volume_name": "sdp_meta_wheels",
                },
            )

        build_cmd = fake_run.call_args[0][0]
        self.assertEqual(build_cmd.uc_schema, "sdp_meta_wheels")
        self.assertEqual(build_cmd.uc_volume, "sdp_meta_wheels")
        self.assertEqual(cmd.sdp_meta_dependency, wheel_path)
        sdp_meta.onboard.assert_called_once_with(cmd)

    def test_onboard_build_and_upload_whl_from_git_branch(self):
        from databricks.labs.sdp_meta.cli import onboard as cli_onboard

        sdp_meta = MagicMock()
        cmd = self._onboard_cmd()
        sdp_meta._load_onboard_config.return_value = cmd
        wheel_path = "/Volumes/main/sdp_meta_wheels/sdp_meta_wheels/databricks_labs_sdp_meta.whl"
        with patch("databricks.labs.sdp_meta.cli._build_and_upload_git_wheel", return_value=wheel_path) as fake_run:
            cli_onboard(
                sdp_meta,
                flags={
                    "build-and-upload-whl": "true",
                    "git-branch": "feature/sdp-meta",
                    "uc-schema-name": "sdp_meta_wheels",
                    "uc-volume-name": "sdp_meta_wheels",
                },
            )

        self.assertEqual(fake_run.call_args.kwargs["uc_catalog"], "main")
        self.assertEqual(fake_run.call_args.kwargs["uc_schema"], "sdp_meta_wheels")
        self.assertEqual(fake_run.call_args.kwargs["uc_volume"], "sdp_meta_wheels")
        self.assertEqual(
            fake_run.call_args.kwargs["source"],
            "git+https://github.com/databrickslabs/sdp-meta.git@feature/sdp-meta",
        )
        self.assertEqual(cmd.sdp_meta_dependency, wheel_path)
        sdp_meta.onboard.assert_called_once_with(cmd)

    def test_onboard_build_and_upload_whl_from_git_url(self):
        from databricks.labs.sdp_meta.cli import onboard as cli_onboard

        sdp_meta = MagicMock()
        cmd = self._onboard_cmd()
        sdp_meta._load_onboard_config.return_value = cmd
        wheel_path = "/Volumes/main/sdp_meta_wheels/sdp_meta_wheels/databricks_labs_sdp_meta.whl"
        with patch("databricks.labs.sdp_meta.cli._build_and_upload_git_wheel", return_value=wheel_path) as fake_run:
            cli_onboard(
                sdp_meta,
                flags={
                    "build-and-upload-whl": "true",
                    "git-url": "https://github.com/acme/dlt-meta.git",
                    "git-branch": "main",
                },
            )

        self.assertEqual(
            fake_run.call_args.kwargs["source"],
            "git+https://github.com/acme/dlt-meta.git@main",
        )
        self.assertEqual(cmd.sdp_meta_dependency, wheel_path)
        sdp_meta.onboard.assert_called_once_with(cmd)

    def test_onboard_build_and_upload_whl_requires_uc(self):
        from databricks.labs.sdp_meta.cli import onboard as cli_onboard

        sdp_meta = MagicMock()
        sdp_meta._load_onboard_config.return_value = self._onboard_cmd(
            uc_enabled=False,
            uc_catalog_name=None,
            dbfs_path="/dbfs",
            bronze_dataflowspec_path="bronze",
            silver_dataflowspec_path="silver",
        )
        with self.assertRaisesRegex(ValueError, "requires onboarding with unity catalog enabled"):
            cli_onboard(sdp_meta, flags={"build-and-upload-whl": "true"})
        sdp_meta.onboard.assert_not_called()

    def test_onboard_whl_file_path_sets_dependency_without_building(self):
        from databricks.labs.sdp_meta.cli import onboard as cli_onboard

        sdp_meta = MagicMock()
        cmd = self._onboard_cmd()
        sdp_meta._load_onboard_config.return_value = cmd
        whl_path = "/Volumes/main/sdp_meta_wheels/sdp_meta_wheels/databricks_labs_sdp_meta.whl"
        with patch("databricks.labs.sdp_meta.bundle.bundle_prepare_wheel") as fake_run:
            cli_onboard(sdp_meta, flags={"whl-file-path": whl_path})

        fake_run.assert_not_called()
        self.assertEqual(cmd.sdp_meta_dependency, whl_path)
        sdp_meta.onboard.assert_called_once_with(cmd)

    def test_onboard_whl_file_path_and_build_flag_are_mutually_exclusive(self):
        from databricks.labs.sdp_meta.cli import onboard as cli_onboard

        sdp_meta = MagicMock()
        sdp_meta._load_onboard_config.return_value = self._onboard_cmd()
        with self.assertRaisesRegex(ValueError, "mutually exclusive"):
            cli_onboard(
                sdp_meta,
                flags={
                    "whl-file-path": "/Volumes/main/sdp_meta_wheels/sdp_meta_wheels/pkg.whl",
                    "build-and-upload-whl": "true",
                },
            )
        sdp_meta.onboard.assert_not_called()

    def test_onboard_build_and_upload_whl_recovers_from_pflag_spillover(self):
        """`databricks labs sdp-meta onboard --build-and-upload-whl --profile foo`

        reaches the Python wrapper as ``flags["build-and-upload-whl"] == "--profile"``
        because the labs CLI registers every flag as a pflag *string* flag with
        no ``NoOptDefVal`` (see cmd/labs/project/proxy.go in the databricks CLI
        repo). The wrapper must still treat the flag as truthy and run the
        build path; otherwise the onboarding job silently falls back to
        ``sdp-meta==<version>``.
        """
        from databricks.labs.sdp_meta.cli import onboard as cli_onboard

        sdp_meta = MagicMock()
        cmd = self._onboard_cmd()
        sdp_meta._load_onboard_config.return_value = cmd
        wheel_path = "/Volumes/main/sdp_meta_wheels/sdp_meta_wheels/databricks_labs_sdp_meta.whl"
        with patch("databricks.labs.sdp_meta.bundle.bundle_prepare_wheel", return_value=wheel_path) as fake_run:
            cli_onboard(
                sdp_meta,
                flags={
                    "build-and-upload-whl": "--profile",
                    "profile": "",
                    "uc-schema-name": "sdp_meta_wheels",
                    "uc-volume-name": "sdp_meta_wheels",
                },
            )

        fake_run.assert_called_once()
        self.assertEqual(cmd.sdp_meta_dependency, wheel_path)
        sdp_meta.onboard.assert_called_once_with(cmd)

    def test_onboard_build_and_upload_whl_explicit_false_does_not_build(self):
        from databricks.labs.sdp_meta.cli import onboard as cli_onboard

        sdp_meta = MagicMock()
        cmd = self._onboard_cmd()
        sdp_meta._load_onboard_config.return_value = cmd
        with patch("databricks.labs.sdp_meta.bundle.bundle_prepare_wheel") as fake_run:
            cli_onboard(sdp_meta, flags={"build-and-upload-whl": "false"})

        fake_run.assert_not_called()
        self.assertIsNone(cmd.sdp_meta_dependency)
        sdp_meta.onboard.assert_called_once_with(cmd)

    def test_onboard_build_and_upload_whl_defaults_to_create_if_missing(self):
        """The labs CLI puts every declared flag in the payload (with empty
        string default), so plain key presence cannot be used to detect
        opt-out flags like ``--no-create-missing-uc``. Without an explicit
        truthy value we must still default to ``create_if_missing=True`` so
        the UC schema/volume is auto-created on first use.
        """
        from databricks.labs.sdp_meta.cli import onboard as cli_onboard

        sdp_meta = MagicMock()
        cmd = self._onboard_cmd()
        sdp_meta._load_onboard_config.return_value = cmd
        wheel_path = "/Volumes/main/sdp_meta_wheels/sdp_meta_wheels/databricks_labs_sdp_meta.whl"
        with patch("databricks.labs.sdp_meta.bundle.bundle_prepare_wheel", return_value=wheel_path) as fake_run:
            cli_onboard(
                sdp_meta,
                flags={
                    "build-and-upload-whl": "true",
                    "uc-schema-name": "sdp_meta_wheels",
                    "uc-volume-name": "sdp_meta_wheels",
                    "no-create-missing-uc": "",
                },
            )

        build_cmd = fake_run.call_args[0][0]
        self.assertTrue(build_cmd.create_if_missing)

    def test_onboard_build_and_upload_whl_honors_no_create_missing_uc_optout(self):
        from databricks.labs.sdp_meta.cli import onboard as cli_onboard

        sdp_meta = MagicMock()
        cmd = self._onboard_cmd()
        sdp_meta._load_onboard_config.return_value = cmd
        wheel_path = "/Volumes/main/sdp_meta_wheels/sdp_meta_wheels/databricks_labs_sdp_meta.whl"
        with patch("databricks.labs.sdp_meta.bundle.bundle_prepare_wheel", return_value=wheel_path) as fake_run:
            cli_onboard(
                sdp_meta,
                flags={
                    "build-and-upload-whl": "true",
                    "uc-schema-name": "sdp_meta_wheels",
                    "uc-volume-name": "sdp_meta_wheels",
                    "no-create-missing-uc": "true",
                },
            )

        build_cmd = fake_run.call_args[0][0]
        self.assertFalse(build_cmd.create_if_missing)


class DeployBuildWheelFlagTests(unittest.TestCase):
    """`deploy --build-and-upload-whl` / `--whl-file-path` plumb a wheel path
    into the SDP runner notebook's ``%pip install``, replacing the default
    ``databricks-labs-sdp-meta==<version>`` PyPI install.

    Motivation: workspaces without PyPI access (private preview rings,
    air-gapped customer envs) fail with
    ``Failed to run ' %pip install databricks-labs-sdp-meta==0.1.0' from
    notebook: /Users/.../init_sdp_meta_pipeline.py``. The wheel path version
    sidesteps that entirely.
    """

    def _deploy_cmd(self, **overrides):
        kwargs = dict(
            layer="bronze",
            pipeline_name="sdp_meta_pipeline",
            dlt_target_schema="my_dlt_schema",
            onboard_bronze_group="bronze_group",
            sdp_meta_bronze_schema="sdp_meta_bronze",
            dataflowspec_bronze_table="bronze_dataflowspec",
            uc_enabled=True,
            uc_catalog_name="main",
            serverless=True,
        )
        kwargs.update(overrides)
        return DeployCommand(**kwargs)

    def test_deploy_whl_file_path_sets_dependency_without_building(self):
        from databricks.labs.sdp_meta.cli import deploy as cli_deploy

        sdp_meta = MagicMock()
        cmd = self._deploy_cmd()
        sdp_meta._load_deploy_config.return_value = cmd
        whl_path = "/Volumes/main/sdp_meta_wheels/sdp_meta_wheels/databricks_labs_sdp_meta.whl"
        with patch("databricks.labs.sdp_meta.cli._build_and_upload_deploy_wheel") as fake_build, \
             patch("databricks.labs.sdp_meta.cli._read_dependency_from_onboarding_json", return_value=None):
            cli_deploy(sdp_meta, flags={"whl-file-path": whl_path})

        fake_build.assert_not_called()
        self.assertEqual(cmd.sdp_meta_dependency, whl_path)
        sdp_meta.deploy.assert_called_once_with(cmd)

    def test_deploy_build_and_upload_whl_sets_dependency(self):
        from databricks.labs.sdp_meta.cli import deploy as cli_deploy

        sdp_meta = MagicMock()
        cmd = self._deploy_cmd()
        sdp_meta._load_deploy_config.return_value = cmd
        wheel_path = "/Volumes/main/sdp_meta_bronze/sdp_meta_bronze/databricks_labs_sdp_meta.whl"
        with patch(
            "databricks.labs.sdp_meta.cli._build_and_upload_wheel",
            return_value=wheel_path,
        ) as fake_build:
            cli_deploy(
                sdp_meta,
                flags={
                    "build-and-upload-whl": "true",
                    "uc-schema-name": "sdp_meta_bronze",
                },
            )

        fake_build.assert_called_once()
        kwargs = fake_build.call_args.kwargs
        self.assertEqual(kwargs["uc_catalog"], "main")
        self.assertEqual(kwargs["default_schema"], "sdp_meta_bronze")
        self.assertEqual(cmd.sdp_meta_dependency, wheel_path)
        sdp_meta.deploy.assert_called_once_with(cmd)

    def test_deploy_build_and_upload_whl_recovers_from_pflag_spillover(self):
        """Same labs-CLI pflag spillover failure mode as onboard: when users
        type ``--build-and-upload-whl --profile profile_name`` the labs CLI's
        string-flag parser hands the wrapper
        ``flags["build-and-upload-whl"] == "--profile"``. Deploy must still
        run the build path so the SDP pipeline notebook does not silently
        fall back to a PyPI install.
        """
        from databricks.labs.sdp_meta.cli import deploy as cli_deploy

        sdp_meta = MagicMock()
        cmd = self._deploy_cmd()
        sdp_meta._load_deploy_config.return_value = cmd
        wheel_path = "/Volumes/main/sdp_meta_bronze/sdp_meta_bronze/databricks_labs_sdp_meta.whl"
        with patch(
            "databricks.labs.sdp_meta.cli._build_and_upload_wheel",
            return_value=wheel_path,
        ) as fake_build:
            cli_deploy(
                sdp_meta,
                flags={
                    "build-and-upload-whl": "--profile",
                    "profile": "",
                    "uc-schema-name": "sdp_meta_bronze",
                },
            )

        fake_build.assert_called_once()
        self.assertEqual(cmd.sdp_meta_dependency, wheel_path)

    def test_deploy_inherits_dependency_from_onboarding_json(self):
        """When neither flag is passed, deploy should auto-pick the wheel
        path that ``onboard --build-and-upload-whl=true`` persisted into
        ``onboarding_job_details.json``."""
        from databricks.labs.sdp_meta.cli import deploy as cli_deploy

        sdp_meta = MagicMock()
        cmd = self._deploy_cmd()
        sdp_meta._load_deploy_config.return_value = cmd
        wheel_path = "/Volumes/main/sdp_meta_bronze/sdp_meta_bronze/databricks_labs_sdp_meta.whl"
        with patch(
            "databricks.labs.sdp_meta.cli._read_dependency_from_onboarding_json",
            return_value=wheel_path,
        ):
            cli_deploy(sdp_meta, flags={})

        self.assertEqual(cmd.sdp_meta_dependency, wheel_path)
        sdp_meta.deploy.assert_called_once_with(cmd)

    def test_deploy_whl_file_path_and_build_flag_are_mutually_exclusive(self):
        from databricks.labs.sdp_meta.cli import deploy as cli_deploy

        sdp_meta = MagicMock()
        sdp_meta._load_deploy_config.return_value = self._deploy_cmd()
        with self.assertRaisesRegex(ValueError, "mutually exclusive"):
            cli_deploy(
                sdp_meta,
                flags={
                    "whl-file-path": "/Volumes/main/x/x/pkg.whl",
                    "build-and-upload-whl": "true",
                },
            )
        sdp_meta.deploy.assert_not_called()

    def test_deploy_runner_notebook_uses_dependency_when_set(self):
        """End-to-end check that the SDP runner notebook's
        ``%pip install`` line carries the wheel path, not the PyPI default.
        """
        from databricks.labs.sdp_meta.cli import SDPMeta as SDPMetaCls

        ws = MagicMock()
        sdp_meta = SDPMetaCls(ws)
        sdp_meta._my_username = MagicMock(return_value="me")
        sdp_meta.version = "0.1.0"
        cmd = self._deploy_cmd(
            sdp_meta_dependency=(
                "/Volumes/main/sdp_meta_bronze/sdp_meta_bronze/"
                "databricks_labs_sdp_meta-0.1.0-py3-none-any.whl"
            ),
        )
        ws.pipelines.create.return_value = MagicMock(pipeline_id="p1")

        sdp_meta._create_sdp_meta_pipeline(cmd)

        upload_args = ws.workspace.upload.call_args
        runner_bytes = upload_args[0][1]
        self.assertIn(
            b"%pip install /Volumes/main/sdp_meta_bronze/sdp_meta_bronze/"
            b"databricks_labs_sdp_meta-0.1.0-py3-none-any.whl",
            runner_bytes,
        )
        self.assertNotIn(b"databricks-labs-sdp-meta==", runner_bytes)

    def test_deploy_runner_notebook_falls_back_to_pypi_when_dependency_absent(self):
        from databricks.labs.sdp_meta.cli import SDPMeta as SDPMetaCls

        ws = MagicMock()
        sdp_meta = SDPMetaCls(ws)
        sdp_meta._my_username = MagicMock(return_value="me")
        sdp_meta.version = "0.1.0"
        cmd = self._deploy_cmd()
        ws.pipelines.create.return_value = MagicMock(pipeline_id="p1")

        sdp_meta._create_sdp_meta_pipeline(cmd)

        runner_bytes = ws.workspace.upload.call_args[0][1]
        self.assertIn(b"%pip install databricks-labs-sdp-meta==0.1.0", runner_bytes)


class OnboardPersistDependencyTests(unittest.TestCase):
    """`onboard --build-and-upload-whl` writes the resolved wheel path back
    into ``onboarding_job_details.json`` so a subsequent ``deploy`` (run
    from the same working directory) auto-discovers the wheel."""

    def test_onboard_persists_dependency_into_onboarding_job_details_json(self):
        import json as _json
        import os as _os
        import tempfile

        from databricks.labs.sdp_meta.cli import onboard as cli_onboard

        sdp_meta = MagicMock()
        cmd = OnboardCommand(
            onboarding_file_path="demo/conf/json/onboarding.template",
            onboarding_files_dir_path="file:/demo/",
            onboard_layer="bronze_silver",
            env="prod",
            import_author="author",
            version="v1",
            sdp_meta_schema="sdp_meta_dataflowspecs",
            bronze_schema="sdp_meta_bronze",
            silver_schema="sdp_meta_silver",
            uc_enabled=True,
            uc_catalog_name="main",
        )
        sdp_meta._load_onboard_config.return_value = cmd
        wheel_path = "/Volumes/main/x/x/databricks_labs_sdp_meta.whl"

        with tempfile.TemporaryDirectory() as tmp:
            cwd = _os.getcwd()
            _os.chdir(tmp)
            try:
                with open("onboarding_job_details.json", "w") as fh:
                    _json.dump({"uc_enabled": True}, fh)
                with patch(
                    "databricks.labs.sdp_meta.bundle.bundle_prepare_wheel",
                    return_value=wheel_path,
                ):
                    cli_onboard(sdp_meta, flags={"build-and-upload-whl": "true"})

                with open("onboarding_job_details.json") as fh:
                    persisted = _json.load(fh)
                self.assertEqual(persisted["sdp_meta_dependency"], wheel_path)
            finally:
                _os.chdir(cwd)


class BundleInitQuickstartFlagTests(unittest.TestCase):
    """The cli.py wrapper short-circuits the interactive prompt when
    `--quickstart` is on. These tests exercise the wrapper directly
    (not through main()) so they don't need the WorkspaceClient
    plumbing -- they just prove the wrapper picks the right code path
    for each flag combination."""

    def _patched_run(self):
        # Patch the bundle.bundle_init function the wrapper imports + calls
        # so we can assert what BundleInitCommand it was given without
        # actually invoking the databricks CLI.
        return patch("databricks.labs.sdp_meta.bundle.bundle_init")

    def test_quickstart_writes_config_file_and_passes_it(self):
        from databricks.labs.sdp_meta.cli import bundle_init as cli_bundle_init
        import tempfile

        sdp_meta = MagicMock()
        with tempfile.TemporaryDirectory() as tmp, self._patched_run() as fake_run:
            fake_run.return_value = 0
            cli_bundle_init(
                sdp_meta,
                flags={"quickstart": "true", "output-dir": tmp},
            )
            self.assertEqual(fake_run.call_count, 1)
            cmd = fake_run.call_args[0][0]
            self.assertEqual(cmd.output_dir, tmp)
            self.assertIsNotNone(cmd.config_file)
            cfg_path = cmd.config_file
            self.assertTrue(cfg_path.endswith(".json"))
            # The config file actually exists and is valid JSON pre-answering
            # every schema prompt.
            with open(cfg_path) as fh:
                data = json.load(fh)
            self.assertIn("bundle_name", data)
            self.assertIn("wheel_source", data)
            self.assertEqual(data["sdp_meta_dependency"], "__SET_ME__")
            # Interactive prompt MUST NOT have been called.
            sdp_meta._wsi._question.assert_not_called()
            sdp_meta._wsi._choice.assert_not_called()

    def test_no_quickstart_falls_back_to_interactive(self):
        from databricks.labs.sdp_meta.cli import bundle_init as cli_bundle_init

        sdp_meta = MagicMock()
        # Mimic the interactive prompt returning "."
        sdp_meta._wsi._question.return_value = "."
        with self._patched_run() as fake_run:
            fake_run.return_value = 0
            cli_bundle_init(sdp_meta, flags={})
            cmd = fake_run.call_args[0][0]
            # Interactive path produces a BundleInitCommand without a
            # config_file (the prompts answer the schema directly).
            self.assertIsNone(cmd.config_file)
            sdp_meta._wsi._question.assert_called()

    def test_output_dir_flag_overrides_interactive_answer(self):
        from databricks.labs.sdp_meta.cli import bundle_init as cli_bundle_init

        sdp_meta = MagicMock()
        sdp_meta._wsi._question.return_value = "/interactive/answer"
        with self._patched_run() as fake_run:
            fake_run.return_value = 0
            cli_bundle_init(sdp_meta, flags={"output-dir": "/cli/wins"})
            cmd = fake_run.call_args[0][0]
            self.assertEqual(cmd.output_dir, "/cli/wins")

    def test_bare_quickstart_recovers_swallowed_output_dir(self):
        """Regression: the labs string-flag parser lets a bare `--quickstart`
        eat the next token, arriving as
        ``{"output-dir": "", "quickstart": "--output-dir=./x"}``. The wrapper
        must recover the swallowed --output-dir so the bundle still lands where
        the user asked, and still take the quickstart (config-file) path."""
        from databricks.labs.sdp_meta.cli import bundle_init as cli_bundle_init

        sdp_meta = MagicMock()
        with self._patched_run() as fake_run:
            fake_run.return_value = 0
            cli_bundle_init(
                sdp_meta,
                flags={"output-dir": "", "quickstart": "--output-dir=./my_pipeline"},
            )
            cmd = fake_run.call_args[0][0]
            self.assertEqual(cmd.output_dir, "./my_pipeline")
            # quickstart path taken: a config file was written, no prompts.
            self.assertIsNotNone(cmd.config_file)
            sdp_meta._wsi._question.assert_not_called()

    def test_quickstart_false_disables_quickstart(self):
        """`--quickstart=false` must NOT trigger the non-interactive path.
        The old `bool(flags.get("quickstart"))` treated the string "false" as
        truthy; _coerce_bool fixes that."""
        from databricks.labs.sdp_meta.cli import bundle_init as cli_bundle_init

        sdp_meta = MagicMock()
        sdp_meta._wsi._question.return_value = "."
        with self._patched_run() as fake_run:
            fake_run.return_value = 0
            cli_bundle_init(sdp_meta, flags={"quickstart": "false"})
            cmd = fake_run.call_args[0][0]
            self.assertIsNone(cmd.config_file)
            sdp_meta._wsi._question.assert_called()

    def test_explicit_output_dir_not_clobbered_by_recovery(self):
        """If both a real --output-dir and a swallowed one are present, the
        real value wins (recovery only fills an empty slot)."""
        from databricks.labs.sdp_meta.cli import bundle_init as cli_bundle_init
        import tempfile

        sdp_meta = MagicMock()
        with tempfile.TemporaryDirectory() as tmp, self._patched_run() as fake_run:
            fake_run.return_value = 0
            cli_bundle_init(
                sdp_meta,
                flags={"output-dir": tmp, "quickstart": "--output-dir=/swallowed"},
            )
            cmd = fake_run.call_args[0][0]
            self.assertEqual(cmd.output_dir, tmp)


class TestFileUriHelpers(unittest.TestCase):
    """Test cases for file URI helper functions (Issue #251)."""

    def test_normalize_file_uri_unix_path(self):
        """Test normalizing Unix file URIs."""
        from databricks.labs.sdp_meta.cli import _normalize_file_uri_to_path

        self.assertEqual(_normalize_file_uri_to_path("file:/path/to/dir"), "/path/to/dir")
        self.assertEqual(_normalize_file_uri_to_path("/path/to/dir"), "/path/to/dir")
        self.assertEqual(_normalize_file_uri_to_path("file:///path/to/dir"), "/path/to/dir")

    def test_normalize_file_uri_windows_path(self):
        """Test normalizing Windows file URIs (Issue #251)."""
        from databricks.labs.sdp_meta.cli import _normalize_file_uri_to_path

        self.assertEqual(
            _normalize_file_uri_to_path("file:/C:\\projects\\dlt-meta\\conf"),
            "C:\\projects\\dlt-meta\\conf",
        )
        self.assertEqual(
            _normalize_file_uri_to_path("file:/C:/projects/dlt-meta/conf"),
            "C:/projects/dlt-meta/conf",
        )
        self.assertEqual(
            _normalize_file_uri_to_path("file:///C:/projects/dlt-meta/conf"),
            "C:/projects/dlt-meta/conf",
        )
        self.assertEqual(
            _normalize_file_uri_to_path("file:///C:\\projects\\dlt-meta\\conf"),
            "C:\\projects\\dlt-meta\\conf",
        )

    def test_path_to_file_uri_unix(self):
        """Test creating file URIs from Unix paths."""
        from databricks.labs.sdp_meta.cli import _path_to_file_uri

        self.assertEqual(_path_to_file_uri("/path/to/dir"), "file:/path/to/dir")
        self.assertEqual(_path_to_file_uri("file:/path/to/dir"), "file:/path/to/dir")

    def test_path_to_file_uri_windows(self):
        """Test creating file URIs from Windows paths (Issue #251)."""
        from databricks.labs.sdp_meta.cli import _path_to_file_uri

        self.assertEqual(
            _path_to_file_uri("C:\\projects\\dlt-meta\\conf"),
            "file:///C:/projects/dlt-meta/conf",
        )
        self.assertEqual(
            _path_to_file_uri("C:/projects/dlt-meta/conf"),
            "file:///C:/projects/dlt-meta/conf",
        )
        self.assertEqual(
            _path_to_file_uri("file:///C:/projects/dir"),
            "file:///C:/projects/dir",
        )

    def test_roundtrip_unix(self):
        """Test that Unix paths round-trip correctly through URI conversion."""
        from databricks.labs.sdp_meta.cli import _normalize_file_uri_to_path, _path_to_file_uri

        original_path = "/home/user/projects/demo"
        uri = _path_to_file_uri(original_path)
        restored_path = _normalize_file_uri_to_path(uri)
        self.assertEqual(original_path, restored_path)

    def test_roundtrip_windows(self):
        """Test that Windows paths round-trip correctly through URI conversion (Issue #251)."""
        from databricks.labs.sdp_meta.cli import _normalize_file_uri_to_path, _path_to_file_uri

        original_path = "C:/projects/dlt-meta/conf"
        uri = _path_to_file_uri(original_path)
        restored_path = _normalize_file_uri_to_path(uri)
        self.assertEqual(original_path, restored_path)

    @patch("os.walk")
    @patch("builtins.open", new_callable=mock_open)
    def test_copy_to_uc_volume_windows_path(self, mock_open_func, mock_os_walk):
        """Test that copy_to_uc_volume handles Windows paths correctly (Issue #251)."""
        mock_ws = MagicMock()
        sdp_meta = SDPMeta(mock_ws)
        windows_src = "file:///C:/projects/dlt-meta/conf"
        mock_os_walk.return_value = [
            ("C:/projects/dlt-meta/conf", [], ["file1.json"]),
        ]
        mock_ws.files.upload = MagicMock()

        sdp_meta.copy_to_uc_volume(windows_src, "/Volumes/catalog/schema/volume/")

        walk_call_arg = mock_os_walk.call_args[0][0]
        self.assertFalse(
            walk_call_arg.startswith('/C:'),
            f"Invalid Windows path passed to os.walk: {walk_call_arg}",
        )
        self.assertEqual(walk_call_arg, "C:/projects/dlt-meta/conf")

    @patch("os.walk")
    @patch("builtins.open", new_callable=mock_open)
    def test_copy_to_dbfs_windows_path(self, mock_open_func, mock_os_walk):
        """Test that copy_to_dbfs handles Windows paths correctly (Issue #251)."""
        mock_ws = MagicMock()
        sdp_meta = SDPMeta(mock_ws)
        windows_src = "file:///C:/projects/dlt-meta/conf"
        mock_os_walk.return_value = [
            ("C:/projects/dlt-meta/conf", [], ["file1.json"]),
        ]
        mock_ws.dbfs.upload = MagicMock()

        sdp_meta.copy_to_dbfs(windows_src, "dbfs:/dlt-meta/conf/")

        walk_call_arg = mock_os_walk.call_args[0][0]
        self.assertFalse(
            walk_call_arg.startswith('/C:'),
            f"Invalid Windows path passed to os.walk: {walk_call_arg}",
        )
        self.assertEqual(walk_call_arg, "C:/projects/dlt-meta/conf")


class LabsYmlFlagDeclarationTests(unittest.TestCase):
    """Lock-in: the labs.yml declares `--quickstart` (and other flags) so
    `databricks labs sdp-meta bundle-init --quickstart` is a recognized
    invocation. Without this, the labs CLI would reject the flag at
    parse time and the user would never reach the wrapper."""

    @classmethod
    def setUpClass(cls):
        import yaml
        from pathlib import Path
        repo_root = Path(__file__).resolve().parent.parent
        with open(repo_root / "labs.yml") as fh:
            cls._labs_yml = yaml.safe_load(fh)

    def _flags_for(self, cmd_name):
        for entry in self._labs_yml["commands"]:
            if entry["name"] == cmd_name:
                return {f["name"] for f in entry.get("flags") or []}
        return None

    def test_bundle_init_declares_quickstart_flag(self):
        flags = self._flags_for("bundle-init")
        self.assertIsNotNone(flags, "bundle-init missing from labs.yml")
        self.assertIn("quickstart", flags)
        self.assertIn("output-dir", flags)

    def test_onboard_declares_build_and_upload_whl_flags(self):
        flags = self._flags_for("onboard")
        self.assertIsNotNone(flags, "onboard missing from labs.yml")
        for flag in (
            "build-and-upload-whl",
            "whl-file-path",
            "git-branch",
            "git-url",
            "uc-schema",
            "uc-schema-name",
            "uc-volume",
            "uc-volume-name",
            "profile",
            "pip-index-url",
            "pip-extra-index-url",
            "no-create-missing-uc",
        ):
            self.assertIn(flag, flags)

    def test_deploy_declares_build_and_upload_whl_flags(self):
        """Deploy mirrors `onboard`'s wheel-handling flags so a follow-up
        `deploy` can either reuse the wheel from onboarding_job_details.json
        or build/upload a fresh one for the SDP runner notebook's
        ``%pip install``.
        """
        flags = self._flags_for("deploy")
        self.assertIsNotNone(flags, "deploy missing from labs.yml")
        for flag in (
            "build-and-upload-whl",
            "whl-file-path",
            "git-branch",
            "git-url",
            "uc-schema",
            "uc-schema-name",
            "uc-volume",
            "uc-volume-name",
            "profile",
            "pip-index-url",
            "pip-extra-index-url",
            "no-create-missing-uc",
        ):
            self.assertIn(flag, flags)


class RenderOnboardingTemplateTests(unittest.TestCase):
    """Unit tests for the pure ``render_onboarding_template`` helper.
    This is the substitution engine shared by ``update_ws_onboarding_paths``
    (real onboarding) and the App's ``/onboarding/preview`` endpoint —
    keeping the two from drifting on YAML / JSON handling, output format,
    or placeholder semantics."""

    SUBS = {
        "{uc_catalog_name}": "my_cat",
        "{bronze_schema}": "br",
        "{silver_schema}": "sv",
        "{uc_volume_path}": "/Volumes/my_cat/sch/sch/sdp_meta_conf/",
    }

    def test_yaml_round_trip_substitutes_and_preserves_format(self):
        from databricks.labs.sdp_meta.cli import render_onboarding_template
        yaml_in = (
            "- data_flow_id: '100'\n"
            "  bronze_catalog: '{uc_catalog_name}'\n"
            "  bronze_database: '{bronze_schema}'\n"
            "  silver_database: '{silver_schema}'\n"
            "  bronze_path: '{uc_volume_path}/data/bronze/customers'\n"
        )
        rendered, parsed = render_onboarding_template(yaml_in, ".yml", self.SUBS)
        # Output must be valid YAML (we round-tripped through safe_dump).
        import yaml as _yaml
        re_parsed = _yaml.safe_load(rendered)
        self.assertEqual(re_parsed[0]["bronze_catalog"], "my_cat")
        self.assertEqual(re_parsed[0]["bronze_database"], "br")
        self.assertEqual(re_parsed[0]["silver_database"], "sv")
        self.assertEqual(
            re_parsed[0]["bronze_path"],
            "/Volumes/my_cat/sch/sch/sdp_meta_conf//data/bronze/customers",
        )
        self.assertEqual(parsed, re_parsed)
        # No placeholders should remain.
        for token in self.SUBS:
            self.assertNotIn(token, rendered)

    def test_yaml_extension_uppercase_is_handled(self):
        from databricks.labs.sdp_meta.cli import render_onboarding_template
        # source_ext should be lower-cased before the extension dispatch.
        rendered, _ = render_onboarding_template(
            "- bronze_catalog: '{uc_catalog_name}'\n", ".YAML", self.SUBS
        )
        self.assertIn("my_cat", rendered)

    def test_json_round_trip_substitutes_and_preserves_format(self):
        from databricks.labs.sdp_meta.cli import render_onboarding_template
        json_in = json.dumps([{
            "data_flow_id": "100",
            "bronze_catalog": "{uc_catalog_name}",
            "bronze_database": "{bronze_schema}",
            "silver_database": "{silver_schema}",
            "bronze_path": "{uc_volume_path}/data/bronze/customers",
        }])
        rendered, parsed = render_onboarding_template(json_in, ".json", self.SUBS)
        re_parsed = json.loads(rendered)
        self.assertEqual(re_parsed[0]["bronze_catalog"], "my_cat")
        self.assertEqual(re_parsed[0]["bronze_database"], "br")
        self.assertEqual(parsed, re_parsed)

    def test_unknown_extension_falls_back_to_json(self):
        from databricks.labs.sdp_meta.cli import render_onboarding_template
        # ``.template`` is the legacy JSON-template suffix; anything not
        # ending in .yml/.yaml goes through the JSON branch.
        rendered, _ = render_onboarding_template(
            '[{"x": "{uc_catalog_name}"}]', ".template", self.SUBS
        )
        self.assertEqual(json.loads(rendered), [{"x": "my_cat"}])

    def test_none_substitution_becomes_empty_string(self):
        from databricks.labs.sdp_meta.cli import render_onboarding_template
        rendered, _ = render_onboarding_template(
            '[{"x": "{uc_catalog_name}"}]', ".json",
            {"{uc_catalog_name}": None},
        )
        # ``None`` must NOT render as the literal text 'None' (which would
        # be a sneaky bug — JSON would parse it but the cluster would then
        # try to look up a catalog called 'None').
        self.assertEqual(json.loads(rendered), [{"x": ""}])

    def test_malformed_json_raises(self):
        from databricks.labs.sdp_meta.cli import render_onboarding_template
        with self.assertRaises(json.JSONDecodeError):
            render_onboarding_template("{not valid json", ".json", self.SUBS)


class CoerceBoolTests(unittest.TestCase):
    """Unit tests for the ``_coerce_bool`` helper that converts the App's
    JSON-envelope string values ("1"/"0") into real Python booleans.

    The regression this guards against: when ``serverless`` arrives as
    the literal string "1" (from an HTML radio button) the SDK happily
    serializes it as ``"serverless": "1"`` in the request body — a JSON
    string, not a boolean — and the control-plane treats the field as
    missing on serverless-only workspaces, defaulting to classic
    compute and rejecting the pipeline with "You must use serverless
    compute in this workspace." Same trap on ``uc_enabled``."""

    def test_true_passthrough(self):
        from databricks.labs.sdp_meta.cli import _coerce_bool
        self.assertIs(_coerce_bool(True), True)

    def test_false_passthrough(self):
        from databricks.labs.sdp_meta.cli import _coerce_bool
        self.assertIs(_coerce_bool(False), False)

    def test_string_one_is_true(self):
        from databricks.labs.sdp_meta.cli import _coerce_bool
        self.assertIs(_coerce_bool("1"), True)

    def test_string_zero_is_false_not_truthy(self):
        from databricks.labs.sdp_meta.cli import _coerce_bool
        self.assertIs(_coerce_bool("0"), False)

    def test_empty_string_is_false(self):
        from databricks.labs.sdp_meta.cli import _coerce_bool
        self.assertIs(_coerce_bool(""), False)

    def test_none_is_false(self):
        from databricks.labs.sdp_meta.cli import _coerce_bool
        self.assertIs(_coerce_bool(None), False)

    def test_true_string_variants(self):
        from databricks.labs.sdp_meta.cli import _coerce_bool
        self.assertIs(_coerce_bool("True"), True)
        self.assertIs(_coerce_bool("TRUE"), True)
        self.assertIs(_coerce_bool("true"), True)
        self.assertIs(_coerce_bool("yes"), True)
        self.assertIs(_coerce_bool("on"), True)

    def test_false_string_variants(self):
        from databricks.labs.sdp_meta.cli import _coerce_bool
        self.assertIs(_coerce_bool("False"), False)
        self.assertIs(_coerce_bool("false"), False)
        self.assertIs(_coerce_bool("no"), False)
        self.assertIs(_coerce_bool("off"), False)
        self.assertIs(_coerce_bool("anything-else"), False)


class DeployConfigUiStringBoolTests(unittest.TestCase):
    """End-to-end check that ``_load_deploy_config_ui`` coerces the App's
    string-bool envelope into the real ``bool`` values that
    ``self._ws.pipelines.create(serverless=...)`` requires. Without
    coercion the resulting ``DeployCommand.serverless`` is the literal
    string ``"1"`` — truthy enough to pass Python ``if`` checks, but the
    SDK serializes it into the wire body and the control-plane rejects
    the pipeline on serverless-only workspaces."""

    def _make_sdp_meta(self):
        from databricks.labs.sdp_meta.cli import SDPMeta
        return SDPMeta(MagicMock())

    @patch('os.path.isfile')
    def test_string_one_for_serverless_yields_real_bool(self, mock_isfile):
        mock_isfile.return_value = False
        sdp_meta = self._make_sdp_meta()
        cmd = sdp_meta._load_deploy_config_ui({
            "uc_enabled": "1",
            "uc_catalog_name": "my_catalog",
            "serverless": "1",
            "layer": "bronze",
            "onboard_bronze_group": "A1",
            "sdp_meta_bronze_schema": "specs",
            "dataflowspec_bronze_table": "bronze_dataflowspec",
            "pipeline_name": "my_pipeline",
            "dlt_target_schema": "target",
        })
        self.assertIs(cmd.serverless, True)
        self.assertIs(cmd.uc_enabled, True)

    @patch('os.path.isfile')
    def test_string_zero_for_serverless_yields_real_bool(self, mock_isfile):
        mock_isfile.return_value = False
        sdp_meta = self._make_sdp_meta()
        cmd = sdp_meta._load_deploy_config_ui({
            "uc_enabled": "1",
            "uc_catalog_name": "my_catalog",
            "serverless": "0",
            "layer": "bronze",
            "onboard_bronze_group": "A1",
            "sdp_meta_bronze_schema": "specs",
            "dataflowspec_bronze_table": "bronze_dataflowspec",
            "pipeline_name": "my_pipeline",
            "dlt_target_schema": "target",
            "num_workers": 4,
        })
        self.assertIs(cmd.serverless, False)
        self.assertIs(cmd.uc_enabled, True)

    @patch('os.path.isfile')
    def test_python_bool_passthrough(self, mock_isfile):
        # If a programmatic caller already sends real bools, the
        # coercion must not regress them. Same envelope shape, just
        # booleans instead of strings.
        mock_isfile.return_value = False
        sdp_meta = self._make_sdp_meta()
        cmd = sdp_meta._load_deploy_config_ui({
            "uc_enabled": True,
            "uc_catalog_name": "my_catalog",
            "serverless": True,
            "layer": "bronze",
            "onboard_bronze_group": "A1",
            "sdp_meta_bronze_schema": "specs",
            "dataflowspec_bronze_table": "bronze_dataflowspec",
            "pipeline_name": "my_pipeline",
            "dlt_target_schema": "target",
        })
        self.assertIs(cmd.serverless, True)
        self.assertIs(cmd.uc_enabled, True)
