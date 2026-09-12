""" A script to run integration tests for DLT-Meta."""

# Import necessary modules
import argparse
import json
import os
import sys
import traceback
import uuid
import webbrowser
from dataclasses import dataclass
from datetime import timedelta

import yaml

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import compute, jobs
from databricks.sdk.service.catalog import SchemasAPI, VolumeInfo, VolumeType
from databricks.sdk.service.pipelines import NotebookLibrary, PipelineLibrary
from databricks.sdk.service.workspace import ImportFormat, Language

from databricks.labs.sdp_meta.identifiers import validate_uc_identifier
from databricks.labs.sdp_meta.install import WorkspaceInstaller

# Dictionary mapping cloud providers to node types
cloud_node_type_id_dict = {
    "aws": "i3.xlarge",
    "azure": "Standard_D3_v2",
    "gcp": "n1-highmem-4",
}


def create_pipeline(ws, **kwargs):
    """Create a pipeline via ``ws.pipelines.create(...)``, tolerating both
    the modern ``schema=`` parameter and the legacy ``target=`` parameter.

    The SDK requires exactly one of ``schema`` or ``target``. ``schema`` is
    the direct-publishing-mode (DPM) API and is what Unity-Catalog
    workspaces with DPM enabled expect; ``target`` is the legacy fallback
    still needed on workspaces / pipelines where DPM is not enabled.

    Shared by the integration test runners and every demo launcher under
    ``demo/`` (all of which already import from this module). The
    shipping ``databricks.labs.sdp_meta`` package does NOT depend on this
    helper -- production CLI code (``cli.py``) has its own private copy
    so the customer-facing wheel doesn't carry a demo/test compatibility
    API on its surface.

    Behavior:
      * If the caller passes ``schema=``, try it first and retry with
        ``target=`` (same value) on any exception.
      * If the caller passes ``target=`` (or neither), pass through
        untouched -- no fallback needed.
      * If both attempts fail, raise ``RuntimeError`` chaining both
        underlying errors so operators can diagnose the real cause.

    Deprecation note: ``target=`` is documented as deprecated for pipeline
    creation in favor of ``schema=``. When it is fully removed from the
    SDK, the fallback branch becomes dead code (``schema=`` succeeds on
    every DPM-enabled workspace, so we return before reaching the
    fallback) and this helper can be replaced with a plain
    ``ws.pipelines.create(**kwargs)`` call.
    """
    if "schema" not in kwargs:
        return ws.pipelines.create(**kwargs)
    schema_val = kwargs.pop("schema")
    try:
        return ws.pipelines.create(schema=schema_val, **kwargs)
    except Exception as e_schema:
        print(
            f"  [warn] pipelines.create(schema={schema_val!r}) failed: "
            f"{type(e_schema).__name__}: {e_schema}"
        )
        print(f"  [info] retrying with legacy target={schema_val!r} ...")
        try:
            return ws.pipelines.create(target=schema_val, **kwargs)
        except Exception as e_target:
            raise RuntimeError(
                "pipelines.create() failed with both parameter forms.\n"
                f"  schema={schema_val!r}: "
                f"{type(e_schema).__name__}: {e_schema}\n"
                f"  target={schema_val!r}: "
                f"{type(e_target).__name__}: {e_target}"
            ) from e_target


@dataclass
class SDPMetaRunnerConf:
    """
    A class to hold information required for running integration tests.

    Attributes:
    -----------
    run_id : str
        The ID of the test run.
    username : str, optional
        The username to use for the test run.
    uc_catalog_name : str, optional
        The name of the unified catalog to use for the test run.
    onboarding_file_path : str, optional
        The path to the onboarding file to use for the test run.
    int_tests_dir : str, optional
        The directory containing the integration tests.
    sdp_meta_schema : str, optional
        The name of the SDP meta schema to use for the test run.
    bronze_schema : str, optional
        The name of the bronze schema to use for the test run.
    silver_schema : str, optional
        The name of the silver schema to use for the test run.
    runners_nb_path : str, optional
        The path to the runners notebook.
    runners_full_local_path : str, optional
        The full local path to the runners notebook.
    source : str, optional
        The source to use for the test run.
    cloudfiles_template : str, optional
        The cloudfiles template to use for the test run.
    eventhub_template : str, optional
        The eventhub template to use for the test run.
    kafka_template : str, optional
        The Kafka template to use for the test run.
    env : str, optional
        The environment to use for the test run.
    whl_path : str, optional
        The path to the whl file to use for the test run.
    volume_info : VolumeInfo, optional
        The volume information to use for the test run.
    uc_volume_path : str, optional
        The path to the unified volume to use for the test run.
    uc_target_whl_path : str, optional
        The path to the unified catalog target whl file to use for the test run.
    node_type_id : str, optional
        The node type ID to use for the test run.
    bronze_pipeline_id : str, optional
        The ID of the bronze pipeline to use for the test run.
    silver_pipeline_id : str, optional
        The ID of the silver pipeline to use for the test run.
    job_id : str, optional
        The ID of the job to use for the test run.
    """

    run_id: str
    username: str = None
    run_name: str = None
    uc_catalog_name: str = None
    uc_volume_name: str = "dlt_meta_files"
    onboarding_file_format: str = "json"  # "json" | "yaml"
    onboarding_file_path: str = "integration_tests/conf/json/onboarding.json"
    onboarding_A2_file_path: str = "integration_tests/conf/json/onboarding_A2.json"
    # onboarding_fanout_file_path: str = "integration_tests/conf/json/onboarding.json"
    # onboarding_fanout_templates: str = None
    int_tests_dir: str = "integration_tests"
    sdp_meta_schema: str = None
    bronze_schema: str = None
    silver_schema: str = None
    runners_nb_path: str = None
    runners_full_local_path: str = None
    source: str = None
    env: str = "it"
    whl_path: str = None
    volume_info: VolumeInfo = None
    uc_volume_path: str = None
    uc_target_whl_path: str = None
    remote_whl_path: str = None
    node_type_id: str = None
    bronze_pipeline_id: str = None
    bronze_pipeline_A2_id: str = None
    silver_pipeline_id: str = None
    job_id: str = None
    test_output_file_path: str = None
    onboarding_fanout_templates: str = None  # "demo/conf/onboarding_fanout_cars.template",
    # onboarding_file_path: str = None  # "demo/conf/onboarding_cars.json",
    onboarding_fanout_file_path: str = None  # "demo/conf/onboarding_fanout_cars.json",

    # cloudfiles info
    cloudfiles_template: str = "integration_tests/conf/json/cloudfiles-onboarding.template"
    cloudfiles_A2_template: str = (
        "integration_tests/conf/json/cloudfiles-onboarding_A2.template"
    )

    # eventhub info
    eventhub_template: str = "integration_tests/conf/json/eventhub-onboarding.template"
    eventhub_input_data: str = None
    eventhub_append_flow_input_data: str = None
    eventhub_name: str = None
    eventhub_sink_name: str = None
    eventhub_name_append_flow: str = None
    eventhub_producer_accesskey_name: str = None
    eventhub_consumer_accesskey_name: str = None
    eventhub_accesskey_secret_name: str = None
    eventhub_secrets_scope_name: str = None
    eventhub_namespace: str = None
    eventhub_port: str = None

    # kafka info
    kafka_template: str = "integration_tests/conf/json/kafka-onboarding.template"
    kafka_source_topic: str = None
    kafka_source_broker: str = None
    kafka_source_servers_secrets_scope_name: str = None
    kafka_source_servers_secrets_scope_key: str = None
    kafka_sink_topic: str = None
    kafka_sink_servers_secret_scope_name: str = None
    kafka_sink_servers_secret_scope_key: str = None

    # snapshot info
    snapshot_template: str = "integration_tests/conf/json/snapshot-onboarding.template"

    # Release A native Lakeflow quality scenario
    quality_template: str = "integration_tests/conf/json/quality/onboarding.template"
    quality_dqx_template: str = (
        "integration_tests/conf/json/quality_dqx/onboarding.template"
    )

    # multi-source AUTO CDC info (issue #294): three regional bronze CDC
    # sources -> one unified silver SCD-1 ``customers`` table via
    # ``silver_cdc_apply_changes_flows``. No A2 incremental step; no
    # external publish-events step. The template carries both layers'
    # rows so a single onboarding run produces a 4-task workflow
    # (onboard -> bronze -> silver -> validate).
    multi_source_cdc_template: str = (
        "integration_tests/conf/json/multi-source-cdc-onboarding.template"
    )

    def __post_init__(self):
        """Adjust onboarding file paths based on the requested file format."""
        fmt = (self.onboarding_file_format or "json").lower()
        if fmt not in ("json", "yaml", "yml"):
            raise ValueError(
                f"Unsupported onboarding_file_format='{self.onboarding_file_format}'. "
                "Use 'json' or 'yaml'."
            )
        # Normalize "yml" to "yaml" internally; use ".yml" as the on-disk extension.
        self.onboarding_file_format = "yaml" if fmt in ("yaml", "yml") else "json"
        if self.onboarding_file_format == "yaml":
            self.onboarding_file_path = self._to_yaml_variant(self.onboarding_file_path)
            self.onboarding_A2_file_path = self._to_yaml_variant(self.onboarding_A2_file_path)
            if self.onboarding_fanout_file_path:
                self.onboarding_fanout_file_path = self._to_yaml_variant(
                    self.onboarding_fanout_file_path
                )
            self.cloudfiles_template = self._to_yaml_variant(self.cloudfiles_template)
            self.cloudfiles_A2_template = self._to_yaml_variant(self.cloudfiles_A2_template)
            self.eventhub_template = self._to_yaml_variant(self.eventhub_template)
            self.kafka_template = self._to_yaml_variant(self.kafka_template)
            self.snapshot_template = self._to_yaml_variant(self.snapshot_template)
            self.quality_template = self._to_yaml_variant(self.quality_template)
            self.quality_dqx_template = self._to_yaml_variant(
                self.quality_dqx_template
            )
            self.multi_source_cdc_template = self._to_yaml_variant(
                self.multi_source_cdc_template
            )
            if self.onboarding_fanout_templates:
                self.onboarding_fanout_templates = self._to_yaml_variant(
                    self.onboarding_fanout_templates
                )

    @staticmethod
    def _to_yaml_variant(path: str) -> str:
        """Translate a `/json/` conf-bucket path to its `/yml/` sibling.

        Rules:
          * Swap the path segment `/json/` to `/yml/` so the file resolves under
            the YAML conf bucket.
          * For templates ending in `.template`, append `.yml` (e.g.
            `cloudfiles-onboarding.template` -> `cloudfiles-onboarding.template.yml`).
          * For files ending in `.json`, swap the trailing extension to `.yml`.
          * Paths already ending in `.yml`/`.yaml` are returned unchanged.

        The swap is unconditional: this method is also used to compute *output*
        paths (e.g. the runtime-generated `onboarding.yml`) which by definition
        do not exist on disk yet. If a template's YAML sibling is missing, the
        downstream open() will surface a clear `FileNotFoundError` pointing at
        the YAML path the user asked for.
        """
        if not path:
            return path
        if path.endswith((".yml", ".yaml")):
            return path
        candidate = path.replace("/json/", "/yml/")
        if candidate.endswith(".template"):
            return f"{candidate}.yml"
        if candidate.endswith(".json"):
            return f"{candidate[:-5]}.yml"
        return candidate


class SDPMETARunner:
    """
    A class to run integration tests for DLT-Meta.

    Attributes:
    - args: command line arguments
    - workspace_client: Databricks workspace client
    - runner_conf: test information
    """

    def __init__(self, args: dict[str:str], ws, base_dir):
        self.args = args
        self.ws = ws
        self.wsi = WorkspaceInstaller(ws)
        self.base_dir = base_dir

    def init_runner_conf(self) -> SDPMetaRunnerConf:
        """Initialize the runner configuration for running integration tests."""
        run_id = uuid.uuid4().hex
        runner_conf = SDPMetaRunnerConf(
            run_id=run_id,
            username=self.wsi._my_username,
            uc_catalog_name=self.args["uc_catalog_name"],
            sdp_meta_schema=f"sdp_meta_dataflowspecs_it_{run_id}",
            bronze_schema=f"dlt_meta_bronze_it_{run_id}",
            silver_schema=f"sdp_meta_silver_it_{run_id}",
            runners_nb_path=f"/Users/{self.wsi._my_username}/dlt_meta_int_tests/{run_id}",
            source=self.args["source"] if "source" in self.args else None,
            onboarding_file_format=(
                self.args["onboarding_file_format"]
                if self.args.get("onboarding_file_format")
                else "json"
            ),
            # node_type_id=cloud_node_type_id_dict[self.args["cloud_provider_name"]],
            test_output_file_path=(
                f"/Users/{self.wsi._my_username}/dlt_meta_int_tests/"
                f"{run_id}/integration-test-output.csv"
            ),
            # kafka provided args
            kafka_source_topic=self.args["kafka_source_topic"],
            kafka_source_broker=self.args["kafka_source_broker"] if "kafka_source_broker" in self.args else None,
            kafka_source_servers_secrets_scope_name=(
                self.args["kafka_source_servers_secrets_scope_name"]
                if "kafka_source_servers_secrets_scope_name" in self.args
                else None
            ),
            kafka_source_servers_secrets_scope_key=(
                self.args["kafka_source_servers_secrets_scope_key"]
                if "kafka_source_servers_secrets_scope_key" in self.args
                else None
            ),
            kafka_sink_topic=self.args["kafka_sink_topic"] if "kafka_sink_topic" in self.args else None,
            kafka_sink_servers_secret_scope_name=(
                self.args["kafka_sink_servers_secret_scope_name"]
                if "kafka_sink_servers_secret_scope_name" in self.args
                else None
            ),
            kafka_sink_servers_secret_scope_key=(
                self.args["kafka_sink_servers_secret_scope_key"]
                if "kafka_sink_servers_secret_scope_key" in self.args
                else None
            ),

            # eventhub provided args
            eventhub_name=self.args["eventhub_name"],
            eventhub_name_append_flow=self.args["eventhub_name_append_flow"],
            eventhub_producer_accesskey_name=self.args[
                "eventhub_producer_accesskey_name"
            ],
            eventhub_consumer_accesskey_name=self.args[
                "eventhub_consumer_accesskey_name"
            ],
            eventhub_sink_name=self.args["eventhub_sink_name"],
            eventhub_accesskey_secret_name=self.args["eventhub_accesskey_secret_name"],
            eventhub_secrets_scope_name=self.args["eventhub_secrets_scope_name"],
            eventhub_namespace=self.args["eventhub_namespace"],
            eventhub_port=self.args["eventhub_port"],
        )

        # Set the proper directory location for the notebooks that need to be uploaded to run and
        # validate the integration tests
        source_paths = {
            "cloudfiles": "./integration_tests/notebooks/cloudfile_runners/",
            "eventhub": "./integration_tests/notebooks/eventhub_runners/",
            "kafka": "./integration_tests/notebooks/kafka_runners/",
            "snapshot": "./integration_tests/notebooks/snapshot_runners/",
            "quality": "./integration_tests/notebooks/quality_runners/",
            "quality_dqx": "./integration_tests/notebooks/quality_dqx_runners/",
            "multi_source_cdc": (
                "./integration_tests/notebooks/multi_source_cdc_runners/"
            ),
        }
        try:
            runner_conf.runners_full_local_path = source_paths[runner_conf.source]
        except KeyError:
            raise Exception(
                "Given source is not supported. Supported sources are: "
                "cloudfiles, eventhub, kafka, snapshot, quality, quality_dqx, multi_source_cdc"
            )

        return runner_conf

    def _install_folder(self):
        return f"/Users/{self.wsi._my_username}/sdp-meta"

    def _my_username(self, ws):
        if not hasattr(ws, "_me"):
            ws._me = ws.current_user.me()
        return ws._me.user_name

    def create_sdp_meta_pipeline(
        self,
        pipeline_name: str,
        layer: str,
        group: str,
        target_schema: str,
        runner_conf: SDPMetaRunnerConf,
    ) -> str:
        """
        Create a DLT pipeline.

        Parameters:
        ----------
        pipeline_name : str = The name of the pipeline.
        layer : str = The layer of the pipeline.
        target_schema : str = The target schema of the pipeline.
        runner_conf : SDPMetaRunnerConf = The runner configuration.

        Returns:
        -------
        str - The ID of the created pipeline.

        Raises:
        ------
        Exception -  If the pipeline creation fails.
        """
        configuration = {
            "layer": layer,
            "sdp_meta_whl": runner_conf.remote_whl_path,
            "pipelines.externalSink.enabled": "true",
        }
        # For the combined ``bronze_silver`` layer we set BOTH per-layer
        # group + dataflowspec table entries; the runtime expects them
        # prefixed by the individual layer name, not the combined
        # ``bronze_silver``. Mirrors the demo launcher
        # (``demo/launch_multi_source_cdc_demo.py``) and Stage 11 of the
        # interactive demo notebook so the whole multi-source CDC
        # fan-in lives inside one observable DLT flow graph.
        if layer == "bronze_silver":
            for sub_layer in ("bronze", "silver"):
                configuration[f"{sub_layer}.group"] = group
                configuration[f"{sub_layer}.dataflowspecTable"] = (
                    f"{runner_conf.uc_catalog_name}."
                    f"{runner_conf.sdp_meta_schema}."
                    f"{sub_layer}_dataflowspec_cdc"
                )
        else:
            configuration[f"{layer}.group"] = group
            configuration[f"{layer}.dataflowspecTable"] = (
                f"{runner_conf.uc_catalog_name}.{runner_conf.sdp_meta_schema}.{layer}_dataflowspec_cdc"
            )

        created = create_pipeline(
            self.ws,
            catalog=runner_conf.uc_catalog_name,
            name=pipeline_name,
            serverless=True,
            configuration=configuration,
            libraries=[
                PipelineLibrary(
                    notebook=NotebookLibrary(
                        path=f"{runner_conf.runners_nb_path}/runners/init_sdp_meta_pipeline.py"
                    )
                )
            ],
            schema=target_schema,
        )

        if created is None:
            raise Exception("Pipeline creation failed")
        return created.pipeline_id

    def create_workflow_spec(self, runner_conf: SDPMetaRunnerConf):
        """Create the Databricks Workflow Job given the DLT Meta configuration specs"""
        dependencies = [runner_conf.remote_whl_path]
        if runner_conf.source == "quality_dqx":
            dependencies.append("databricks-labs-dqx==0.16.0")
        sdp_meta_environments = [
            jobs.JobEnvironment(
                environment_key="dl_meta_int_env",
                spec=compute.Environment(
                    client="1",
                    dependencies=dependencies,
                ),
            )
        ]
        tasks = [
            jobs.Task(
                task_key="setup_sdp_meta_pipeline_spec",
                environment_key="dl_meta_int_env",
                description="test",
                timeout_seconds=0,
                python_wheel_task=jobs.PythonWheelTask(
                    package_name="databricks_labs_sdp_meta",
                    entry_point="run",
                    named_parameters={
                        "onboard_layer": (
                            "bronze_silver"
                            if runner_conf.source
                            in ["cloudfiles", "snapshot", "multi_source_cdc"]
                            else "bronze"
                        ),
                        "database": f"{runner_conf.uc_catalog_name}.{runner_conf.sdp_meta_schema}",
                        "onboarding_file_path": (
                            f"{runner_conf.uc_volume_path}{runner_conf.onboarding_file_path}"
                        ),
                        "silver_dataflowspec_table": "silver_dataflowspec_cdc",
                        "silver_dataflowspec_path": f"{runner_conf.uc_volume_path}data/dlt_spec/silver",
                        "bronze_dataflowspec_table": "bronze_dataflowspec_cdc",
                        "import_author": "Ravi",
                        "version": "v1",
                        "bronze_dataflowspec_path": f"{runner_conf.uc_volume_path}data/dlt_spec/bronze",
                        "overwrite": "True",
                        "env": runner_conf.env,
                        "uc_enabled": "True",
                    },
                ),
            ),
            jobs.Task(
                # Multi-source AUTO CDC (issue #294) collapses bronze +
                # silver into ONE combined ``bronze_silver`` pipeline
                # (see ``create_bronze_silver_dlt``), so the
                # ``bronze_dlt_pipeline`` task name would be misleading
                # for that source. Use ``sdp-meta-pipeline`` instead so
                # the workflow graph in the UI reflects what the task
                # actually does. Every other source keeps the historical
                # task_key untouched.
                task_key=(
                    "sdp-meta-pipeline"
                    if runner_conf.source == "multi_source_cdc"
                    else "bronze_dlt_pipeline"
                ),
                depends_on=[
                    jobs.TaskDependency(
                        task_key=(
                            # The cloudfiles customers flow declares a
                            # bronze_row_filter that references the
                            # `customer_op_filter` UDF (issue #303); that UDF
                            # must exist before DLT can CREATE TABLE
                            # bronze.customers, so the bronze pipeline depends
                            # on the pre-pipeline UDF setup task specifically
                            # for cloudfiles. snapshot and multi_source_cdc
                            # (issue #294) chain off the standard spec setup;
                            # everything else still chains off publish_events.
                            "setup_row_filter_udf"
                            if runner_conf.source == "cloudfiles"
                            else (
                                "setup_sdp_meta_pipeline_spec"
                                if runner_conf.source
                                in (
                                    "snapshot",
                                    "quality",
                                    "quality_dqx",
                                    "multi_source_cdc",
                                )
                                else "publish_events"
                            )
                        )
                    )
                ],
                pipeline_task=jobs.PipelineTask(
                    pipeline_id=runner_conf.bronze_pipeline_id
                ),
            ),
            jobs.Task(
                task_key="validate_results",
                description="test",
                depends_on=[
                    jobs.TaskDependency(
                        task_key=(
                            self.get_validate_task_key(runner_conf.source)
                        )
                    )
                ],
                notebook_task=jobs.NotebookTask(
                    notebook_path=f"{runner_conf.runners_nb_path}/runners/validate.py",
                    base_parameters={
                        "uc_enabled": "True",
                        "uc_catalog_name": f"{runner_conf.uc_catalog_name}",
                        "bronze_schema": f"{runner_conf.bronze_schema}",
                        "silver_schema": (
                            f"{runner_conf.silver_schema}"
                            if runner_conf.source
                            in ("cloudfiles", "snapshot", "multi_source_cdc")
                            else ""
                        ),
                        "output_file_path": f"/Workspace{runner_conf.test_output_file_path}",
                        "run_id": runner_conf.run_id,
                        "sdp_meta_schema": runner_conf.sdp_meta_schema,
                        "bronze_pipeline_id": runner_conf.bronze_pipeline_id,
                    },
                ),
            ),
        ]

        if runner_conf.source == "cloudfiles":
            tasks.extend(
                [
                    jobs.Task(
                        # Creates `<catalog>.<bronze_schema>.customer_op_filter`
                        # so that the bronze + silver `customers` tables (which
                        # reference it via ROW FILTER in the onboarding spec)
                        # can be created by the bronze DLT pipeline. Predicate:
                        # admins see all rows; non-admins see APPEND+UPDATE only.
                        task_key="setup_row_filter_udf",
                        depends_on=[
                            jobs.TaskDependency(
                                task_key="setup_sdp_meta_pipeline_spec"
                            )
                        ],
                        notebook_task=jobs.NotebookTask(
                            notebook_path=(
                                f"{runner_conf.runners_nb_path}"
                                "/runners/setup_row_filter_udf.py"
                            ),
                            base_parameters={
                                "uc_catalog_name": runner_conf.uc_catalog_name,
                                "bronze_schema": runner_conf.bronze_schema,
                            },
                        ),
                    ),
                    jobs.Task(
                        task_key="onboard_spec_A2",
                        depends_on=[
                            jobs.TaskDependency(task_key="bronze_dlt_pipeline")
                        ],
                        description="test",
                        environment_key="dl_meta_int_env",
                        timeout_seconds=0,
                        python_wheel_task=jobs.PythonWheelTask(
                            package_name="databricks_labs_sdp_meta",
                            entry_point="run",
                            named_parameters={
                                "onboard_layer": "bronze",
                                "database": f"{runner_conf.uc_catalog_name}.{runner_conf.sdp_meta_schema}",
                                "onboarding_file_path": (  # noqa : E501
                                    f"{runner_conf.uc_volume_path}{runner_conf.onboarding_A2_file_path}"
                                ),
                                "bronze_dataflowspec_table": "bronze_dataflowspec_cdc",
                                "import_author": "Ravi",
                                "version": "v1",
                                "overwrite": "False",
                                "env": runner_conf.env,
                                "uc_enabled": "True",
                            },
                        ),
                    ),
                    jobs.Task(
                        task_key="bronze_A2_dlt_pipeline",
                        depends_on=[jobs.TaskDependency(task_key="onboard_spec_A2")],
                        pipeline_task=jobs.PipelineTask(
                            pipeline_id=runner_conf.bronze_pipeline_A2_id
                        ),
                    ),
                    jobs.Task(
                        task_key="silver_dlt_pipeline",
                        depends_on=[
                            jobs.TaskDependency(task_key="bronze_A2_dlt_pipeline")
                        ],
                        pipeline_task=jobs.PipelineTask(
                            pipeline_id=runner_conf.silver_pipeline_id
                        ),
                    ),
                ]
            )
        elif runner_conf.source == "snapshot":
            base_parameters_v1 = {
                "base_path": (
                    f"{runner_conf.uc_volume_path}{self.base_dir}/resources/data/snapshots"
                ),
                "version": "1",
                "source_catalog": runner_conf.uc_catalog_name,
                "source_database": runner_conf.sdp_meta_schema,
                "source_table": "source_products_delta"
            }
            base_parameters_v2 = {
                "base_path": (
                    f"{runner_conf.uc_volume_path}{self.base_dir}/resources/data/snapshots"
                ),
                "version": "2",
                "source_catalog": runner_conf.uc_catalog_name,
                "source_database": runner_conf.sdp_meta_schema,
                "source_table": "source_products_delta"
            }
            base_parameters_v3 = {
                "base_path": (
                    f"{runner_conf.uc_volume_path}{self.base_dir}/resources/data/snapshots"
                ),
                "version": "3",
                "source_catalog": runner_conf.uc_catalog_name,
                "source_database": runner_conf.sdp_meta_schema,
                "source_table": "source_stores_delta"
            }
            tasks[1].depends_on = [jobs.TaskDependency(task_key='create_source_tables')]
            tasks.extend(
                [
                    jobs.Task(
                        task_key="create_source_tables",
                        depends_on=[
                            jobs.TaskDependency(task_key="setup_sdp_meta_pipeline_spec")
                        ],
                        notebook_task=jobs.NotebookTask(
                            notebook_path=f"{runner_conf.runners_nb_path}/runners/upload_snapshots.py",
                            base_parameters=base_parameters_v1,
                        ),
                    ),
                    jobs.Task(
                        task_key="silver_dlt_pipeline",
                        depends_on=[
                            jobs.TaskDependency(task_key="bronze_dlt_pipeline")
                        ],
                        pipeline_task=jobs.PipelineTask(
                            pipeline_id=runner_conf.silver_pipeline_id
                        ),
                    ),
                    jobs.Task(
                        task_key="upload_v2_snapshots",
                        description="test",
                        depends_on=[
                            jobs.TaskDependency(task_key="silver_dlt_pipeline")
                        ],
                        notebook_task=jobs.NotebookTask(
                            notebook_path=f"{runner_conf.runners_nb_path}/runners/upload_snapshots.py",
                            base_parameters=base_parameters_v2,
                        ),
                    ),
                    jobs.Task(
                        task_key="bronze_v2_dlt_pipeline",
                        depends_on=[jobs.TaskDependency(task_key="upload_v2_snapshots")],
                        pipeline_task=jobs.PipelineTask(
                            pipeline_id=runner_conf.bronze_pipeline_id
                        ),
                    ),
                    jobs.Task(
                        task_key="silver_v2_dlt_pipeline",
                        depends_on=[
                            jobs.TaskDependency(task_key="bronze_v2_dlt_pipeline")
                        ],
                        pipeline_task=jobs.PipelineTask(
                            pipeline_id=runner_conf.silver_pipeline_id
                        ),
                    ),
                    jobs.Task(
                        task_key="upload_v3_snapshots",
                        depends_on=[
                            jobs.TaskDependency(task_key="silver_v2_dlt_pipeline")
                        ],
                        notebook_task=jobs.NotebookTask(
                            notebook_path=f"{runner_conf.runners_nb_path}/runners/upload_snapshots.py",
                            base_parameters=base_parameters_v3,
                        ),
                    ),
                    jobs.Task(
                        task_key="bronze_v3_dlt_pipeline",
                        depends_on=[jobs.TaskDependency(task_key="upload_v3_snapshots")],
                        pipeline_task=jobs.PipelineTask(
                            pipeline_id=runner_conf.bronze_pipeline_id
                        ),
                    ),
                    jobs.Task(
                        task_key="silver_v3_dlt_pipeline",
                        depends_on=[
                            jobs.TaskDependency(task_key="bronze_v3_dlt_pipeline")
                        ],
                        pipeline_task=jobs.PipelineTask(
                            pipeline_id=runner_conf.silver_pipeline_id
                        ),
                    )
                ]
            )
        elif runner_conf.source == "multi_source_cdc":
            # Multi-source AUTO CDC (issue #294): the pipeline task
            # above is named ``sdp-meta-pipeline`` and already runs the
            # **combined** ``bronze_silver`` pipeline (3 regional CDC
            # bronze tables AND the unified silver multi-source AUTO
            # CDC merge inside one DLT flow graph). So we don't need a
            # separate ``silver_dlt_pipeline`` task — the validate task
            # just depends on ``sdp-meta-pipeline`` directly (see
            # ``get_validate_task_key``). The workflow is the minimal
            # 3-task shape: setup -> sdp-meta-pipeline -> validate. No
            # A2 step, no publish_events (seed JSON is already on volume).
            pass
        elif runner_conf.source in ("quality", "quality_dqx"):
            # A second update against the same pipeline/checkpoint proves that
            # the deterministic input is not appended again.
            tasks.append(
                jobs.Task(
                    task_key="quality_second_update",
                    depends_on=[
                        jobs.TaskDependency(task_key="bronze_dlt_pipeline")
                    ],
                    pipeline_task=jobs.PipelineTask(
                        pipeline_id=runner_conf.bronze_pipeline_id
                    ),
                )
            )
        else:
            if runner_conf.source == "eventhub":
                base_parameters = {
                    "eventhub_name": runner_conf.eventhub_name,
                    "eventhub_name_append_flow": runner_conf.eventhub_name_append_flow,
                    "eventhub_namespace": runner_conf.eventhub_namespace,
                    "eventhub_secrets_scope_name": runner_conf.eventhub_secrets_scope_name,
                    "eventhub_accesskey_name": runner_conf.eventhub_producer_accesskey_name,
                    "eventhub_input_data": f"{runner_conf.uc_volume_path}{self.base_dir}/resources/data/iot/iot.json",  # noqa : E501
                    "eventhub_append_flow_input_data": f"{runner_conf.uc_volume_path}{self.base_dir}/resources/data/iot_eventhub_af/iot.json",  # noqa : E501
                }
            elif runner_conf.source == "kafka":
                base_parameters = {
                    "kafka_source_topic": runner_conf.kafka_source_topic,
                    "kafka_source_servers_secrets_scope_name": runner_conf.kafka_source_servers_secrets_scope_name,
                    "kafka_source_servers_secrets_scope_key": runner_conf.kafka_source_servers_secrets_scope_key,
                    "kafka_input_data": f"{runner_conf.uc_volume_path}{self.base_dir}/resources/data/iot/iot.json",  # noqa : E501
                }

            tasks.append(
                jobs.Task(
                    task_key="publish_events",
                    description="test",
                    depends_on=[
                        jobs.TaskDependency(task_key="setup_sdp_meta_pipeline_spec")
                    ],
                    notebook_task=jobs.NotebookTask(
                        notebook_path=f"{runner_conf.runners_nb_path}/runners/publish_events.py",
                        base_parameters=base_parameters,
                    ),
                ),
            )

        return self.ws.jobs.create(
            name=f"sdp-meta-{runner_conf.run_id}",
            environments=sdp_meta_environments,
            tasks=tasks,
        )

    def get_validate_task_key(self, source):
        if source == "cloudfiles":
            return "silver_dlt_pipeline"
        elif source == "snapshot":
            return "silver_v3_dlt_pipeline"
        elif source == "multi_source_cdc":
            # The bronze task for MSC runs the COMBINED bronze+silver
            # pipeline (see ``create_bronze_silver_dlt``) and is
            # named ``sdp-meta-pipeline`` (not ``bronze_dlt_pipeline``)
            # to reflect what it actually does, so validate depends
            # on that task directly instead of on a separate silver
            # task.
            return "sdp-meta-pipeline"
        elif source in ("quality", "quality_dqx"):
            return "quality_second_update"
        else:
            return "bronze_dlt_pipeline"

    def initialize_uc_resources(self, runner_conf):
        """Create UC schemas and volumes needed to run the integration tests"""
        SchemasAPI(self.ws.api_client).create(
            catalog_name=runner_conf.uc_catalog_name,
            name=runner_conf.sdp_meta_schema,
            comment="dlt_meta framework schema",
        )
        SchemasAPI(self.ws.api_client).create(
            catalog_name=runner_conf.uc_catalog_name,
            name=runner_conf.bronze_schema,
            comment="bronze_schema",
        )
        if runner_conf.source in ["cloudfiles", "snapshot"]:
            SchemasAPI(self.ws.api_client).create(
                catalog_name=runner_conf.uc_catalog_name,
                name=runner_conf.silver_schema,
                comment="silver_schema",
            )
        volume_info = self.ws.volumes.create(
            catalog_name=runner_conf.uc_catalog_name,
            schema_name=runner_conf.sdp_meta_schema,
            name=runner_conf.uc_volume_name,
            volume_type=VolumeType.MANAGED,
        )
        runner_conf.volume_info = volume_info
        runner_conf.uc_volume_path = (
            f"/Volumes/{runner_conf.volume_info.catalog_name}/"
            f"{runner_conf.volume_info.schema_name}/{runner_conf.volume_info.name}/"
        )

    def generate_onboarding_file(self, runner_conf: SDPMetaRunnerConf):
        """Generate onboarding file from templates."""

        string_subs = {
            "{uc_volume_path}": runner_conf.uc_volume_path,
            "{uc_catalog_name}": runner_conf.uc_catalog_name,
            "{bronze_schema}": runner_conf.bronze_schema,
        }

        if runner_conf.source in ["cloudfiles", "snapshot", "multi_source_cdc"]:
            string_subs.update({
                "{silver_schema}": runner_conf.silver_schema,
                "{source_database}": runner_conf.sdp_meta_schema
            })
        elif runner_conf.source == "eventhub":
            string_subs.update(
                {
                    "{run_id}": runner_conf.run_id,
                    "{eventhub_name}": runner_conf.eventhub_name,
                    "{eventhub_name_append_flow}": runner_conf.eventhub_name_append_flow,
                    "{eventhub_consumer_accesskey_name}": runner_conf.eventhub_consumer_accesskey_name,
                    "{eventhub_producer_accesskey_name}": runner_conf.eventhub_producer_accesskey_name,
                    "{eventhub_sink_name}": runner_conf.eventhub_sink_name,
                    "{eventhub_accesskey_secret_name}": runner_conf.eventhub_accesskey_secret_name,
                    "{eventhub_secrets_scope_name}": runner_conf.eventhub_secrets_scope_name,
                    "{eventhub_namespace}": runner_conf.eventhub_namespace,
                    "{eventhub_port}": runner_conf.eventhub_port,
                }
            )
        elif runner_conf.source == "kafka":
            string_subs.update(
                {
                    "{run_id}": runner_conf.run_id,
                    "{kafka_source_topic}": runner_conf.kafka_source_topic,
                    "{kafka_source_broker}": runner_conf.kafka_source_broker,
                    "{kafka_source_servers_secrets_scope_name}": runner_conf.kafka_source_servers_secrets_scope_name,
                    "{kafka_source_servers_secrets_scope_key}": runner_conf.kafka_source_servers_secrets_scope_key,
                    "{kafka_sink_topic}": runner_conf.kafka_sink_topic,
                    "{kafka_sink_servers_secret_scope_name}": runner_conf.kafka_sink_servers_secret_scope_name,
                    "{kafka_sink_servers_secret_scope_key}": runner_conf.kafka_sink_servers_secret_scope_key,
                }
            )

        # Open the onboarding templates and sub in the proper table locations, paths, etc.
        template_path = None
        if runner_conf.source == "cloudfiles":
            template_path = runner_conf.cloudfiles_template
        elif runner_conf.source == "eventhub":
            template_path = runner_conf.eventhub_template
        elif runner_conf.source == "kafka":
            template_path = runner_conf.kafka_template
        elif runner_conf.source == "snapshot":
            template_path = runner_conf.snapshot_template
        elif runner_conf.source == "quality":
            template_path = runner_conf.quality_template
        elif runner_conf.source == "quality_dqx":
            template_path = runner_conf.quality_dqx_template
        elif runner_conf.source == "multi_source_cdc":
            template_path = runner_conf.multi_source_cdc_template

        if template_path:
            onboard_text = self._read_template_text(template_path)

            if runner_conf.source == "cloudfiles":
                onboard_text_a2 = self._read_template_text(runner_conf.cloudfiles_A2_template)

            for key, val in string_subs.items():
                val = "" if val is None else val  # Ensure val is a string
                onboard_text = onboard_text.replace(key, val)
                if runner_conf.source == "cloudfiles":
                    onboard_text_a2 = onboard_text_a2.replace(key, val)

            self._write_onboarding_file(
                runner_conf.onboarding_file_path,
                self._parse_template_payload(template_path, onboard_text),
                runner_conf.onboarding_file_format,
            )

            if runner_conf.source == "cloudfiles":
                self._write_onboarding_file(
                    runner_conf.onboarding_A2_file_path,
                    self._parse_template_payload(
                        runner_conf.cloudfiles_A2_template, onboard_text_a2
                    ),
                    runner_conf.onboarding_file_format,
                )

        if runner_conf.onboarding_fanout_templates:
            template = runner_conf.onboarding_fanout_templates
            onboard_text = self._read_template_text(template)

            for key, val in string_subs.items():
                onboard_text = onboard_text.replace(key, val)

            self._write_onboarding_file(
                runner_conf.onboarding_fanout_file_path,
                self._parse_template_payload(template, onboard_text),
                runner_conf.onboarding_file_format,
            )

    @staticmethod
    def _read_template_text(template_path: str) -> str:
        with open(template_path, "r") as fh:
            return fh.read()

    @staticmethod
    def _parse_template_payload(template_path: str, text: str):
        """Parse a substituted template body. YAML if the path ends in .yml/.yaml, else JSON."""
        if template_path.endswith((".yml", ".yaml")):
            return yaml.safe_load(text)
        return json.loads(text)

    @staticmethod
    def _write_onboarding_file(path: str, payload, file_format: str):
        """Serialize the onboarding payload as JSON or YAML depending on file_format."""
        with open(path, "w") as fh:
            if file_format == "yaml":
                yaml.safe_dump(payload, fh, sort_keys=False, default_flow_style=False)
            else:
                json.dump(payload, fh, indent=4)

    # Keys in the onboarding spec whose values point at external silver/DQ files.
    # When the user selects YAML format, these files are also converted to YAML so
    # the entire pipeline (onboarding + silver transforms + DQ rules) is YAML.
    _SILVER_DQE_KEY_PREFIXES = (
        "silver_transformation_json_",
        "bronze_data_quality_expectations_json_",
        "silver_data_quality_expectations_json_",
    )

    def _rewrite_silver_and_dqe_paths_to_yml(self, runner_conf: SDPMetaRunnerConf):
        """Rewrite silver/DQ paths in generated onboarding specs from ``.json`` to ``.yml``.

        Assumes dedicated ``.yml`` siblings already exist next to the corresponding
        ``.json`` files in ``integration_tests/conf/`` (they are committed to the
        repo). Walks the just-generated onboarding files, finds every value
        pointing at a ``.json`` silver-transformation or DQ-expectations file, and
        rewrites the path to the ``.yml`` sibling. Raises if the expected sibling
        is missing locally (so we never silently ship a stale spec to the cluster).

        No-op unless ``runner_conf.onboarding_file_format == "yaml"``.
        """
        if runner_conf.onboarding_file_format != "yaml":
            return

        rewritten = set()
        missing_siblings = []
        candidate_paths = [
            runner_conf.onboarding_file_path,
            runner_conf.onboarding_A2_file_path,
        ]
        if runner_conf.onboarding_fanout_file_path:
            candidate_paths.append(runner_conf.onboarding_fanout_file_path)
        for onboarding_path in candidate_paths:
            if not onboarding_path or not os.path.exists(onboarding_path):
                continue
            with open(onboarding_path) as fh:
                spec = yaml.safe_load(fh)
            if not isinstance(spec, list):
                continue
            mutated = False
            for entry in spec:
                if not isinstance(entry, dict):
                    continue
                for key, value in list(entry.items()):
                    if not any(key.startswith(p) for p in self._SILVER_DQE_KEY_PREFIXES):
                        continue
                    if not isinstance(value, str) or not value.endswith(".json"):
                        continue
                    local_src = self._resolve_local_conf_path(value, runner_conf)
                    if local_src is None:
                        missing_siblings.append(value)
                        continue
                    # Mirror the on-disk json/ -> yml/ bucket split when looking
                    # for the YAML sibling, and rewrite both the bucket and the
                    # extension in the spec value. Reuse the same helper that
                    # SDPMetaRunnerConf.__post_init__ uses so the json/->yml/
                    # contract lives in exactly one place.
                    yml_local = SDPMetaRunnerConf._to_yaml_variant(local_src)
                    if not os.path.exists(yml_local):
                        missing_siblings.append(yml_local)
                        continue
                    entry[key] = SDPMetaRunnerConf._to_yaml_variant(value)
                    rewritten.add(yml_local)
                    mutated = True
            if mutated:
                with open(onboarding_path, "w") as fh:
                    yaml.safe_dump(spec, fh, sort_keys=False, default_flow_style=False)

        if missing_siblings:
            raise FileNotFoundError(
                "Missing dedicated .yml sibling(s) for YAML integration run; "
                f"create them next to the .json file(s): {sorted(set(missing_siblings))}"
            )
        if rewritten:
            print(
                f"Rewrote {len(rewritten)} silver/DQ path(s) to dedicated .yml "
                f"siblings for YAML integration run: {sorted(rewritten)}"
            )

    @staticmethod
    def _resolve_local_conf_path(spec_path: str, runner_conf: SDPMetaRunnerConf):
        """Map a spec path (which may be a UC volume path) back to a local conf file.

        The spec path always contains the substring ``/<basename>/conf/`` (e.g.
        ``/integration_tests/conf/...``). We use the basename of
        ``runner_conf.int_tests_dir`` to find that marker, then rebuild the local
        filesystem path under the runner's actual ``int_tests_dir``.

        Returns the local filesystem path if the file exists, else ``None``.
        """
        int_dir_name = os.path.basename(runner_conf.int_tests_dir.rstrip("/"))
        marker = f"/{int_dir_name}/conf/"
        if marker not in spec_path:
            return None
        rel = spec_path.split(marker, 1)[1]
        local = os.path.join(runner_conf.int_tests_dir, "conf", rel)
        return local if os.path.exists(local) else None

    def upload_files_to_databricks(self, runner_conf: SDPMetaRunnerConf):
        """
        Upload all necessary data, configuration files, wheels, and notebooks to run the
        integration tests
        """
        uc_vol_full_path = f"{runner_conf.uc_volume_path}{runner_conf.int_tests_dir}"
        print(f"Integration test file upload to {uc_vol_full_path} starting...")
        # Upload the entire resources directory containing ddl and test data
        for root, dirs, files in os.walk(f"{runner_conf.int_tests_dir}/resources"):
            for file in files:
                with open(os.path.join(root, file), "rb") as content:
                    self.ws.files.upload(
                        file_path=f"{runner_conf.uc_volume_path}{root}/{file}",
                        contents=content,
                        overwrite=True,
                    )

        # Upload generated onboarding files (JSON or YAML) and DQE JSONs from the conf dir.
        for root, dirs, files in os.walk(f"{runner_conf.int_tests_dir}/conf"):
            for file in files:
                if file.endswith((".json", ".yml", ".yaml")):
                    with open(os.path.join(root, file), "rb") as content:
                        self.ws.files.upload(
                            file_path=f"{runner_conf.uc_volume_path}{root}/{file}",
                            contents=content,
                            overwrite=True,
                        )
        print(f"Integration test file upload to {uc_vol_full_path} complete!!!")

        # Upload required notebooks for the given source
        print(f"Notebooks upload to {runner_conf.runners_nb_path} started...")
        self.ws.workspace.mkdirs(f"{runner_conf.runners_nb_path}/runners")

        for notebook in os.listdir(runner_conf.runners_full_local_path):
            local_path = os.path.join(runner_conf.runners_full_local_path, notebook)
            if not os.path.isfile(local_path):
                continue
            with open(local_path, "rb") as nb_file:
                self.ws.workspace.upload(
                    path=f"{runner_conf.runners_nb_path}/runners/{notebook}",
                    format=ImportFormat.SOURCE,
                    language=Language.PYTHON,
                    content=nb_file.read(),
                )
        print(f"Notebooks upload to {runner_conf.runners_nb_path} complete!!!")

        print("Python wheel upload starting...")
        # Upload the wheel to both the workspace and the uc volume
        runner_conf.remote_whl_path = (
            f"{self.wsi._upload_wheel(uc_volume_path=runner_conf.uc_volume_path)}"
        )
        print(f"Python wheel upload to {runner_conf.remote_whl_path} completed!!!")

    def init_sdp_meta_runner_conf(self, runner_conf: SDPMetaRunnerConf):
        """Create testing metadata including schemas, volumes, and uploading necessary notebooks"""

        # Generate uc schemas, volumes and upload onboarding files
        self.initialize_uc_resources(runner_conf)
        self.generate_onboarding_file(runner_conf)
        # When --onboarding_file_format yaml, also convert referenced silver
        # transformation and DQ-rule files so the entire pipeline runs on YAML.
        self._rewrite_silver_and_dqe_paths_to_yml(runner_conf)
        self.upload_files_to_databricks(runner_conf)

    def create_bronze_silver_dlt(self, runner_conf: SDPMetaRunnerConf):
        # Multi-source AUTO CDC (issue #294) uses a SINGLE combined
        # ``bronze_silver`` pipeline so the whole fan-in (N bronze CDC
        # tables → one silver target via ``silver_cdc_apply_changes_flows``)
        # lives inside one observable DLT flow graph. Matches Stage 11
        # of the interactive demo notebook and the standalone demo
        # launcher (``demo/launch_multi_source_cdc_demo.py``). We stash
        # the combined pipeline id on ``bronze_pipeline_id`` so the
        # existing workflow-spec / cleanup wiring can reuse it, and
        # leave ``silver_pipeline_id`` as ``None``.
        if runner_conf.source == "multi_source_cdc":
            # Pipeline resource name matches the workflow task_key
            # (``sdp-meta-pipeline``) so the Pipelines UI and the Jobs
            # UI line up at a glance for this combined bronze+silver
            # MSC run.
            runner_conf.bronze_pipeline_id = self.create_sdp_meta_pipeline(
                f"sdp-meta-pipeline-{runner_conf.run_id}",
                "bronze_silver",
                "A1",
                runner_conf.bronze_schema,
                runner_conf,
            )
            runner_conf.silver_pipeline_id = None
            return

        runner_conf.bronze_pipeline_id = self.create_sdp_meta_pipeline(
            f"sdp-meta-bronze-{runner_conf.run_id}",
            "bronze",
            "A1",
            runner_conf.bronze_schema,
            runner_conf,
        )

        if runner_conf.source == "cloudfiles":
            runner_conf.bronze_pipeline_A2_id = self.create_sdp_meta_pipeline(
                f"sdp-meta-bronze-A2-{runner_conf.run_id}",
                "bronze",
                "A2",
                runner_conf.bronze_schema,
                runner_conf,
            )

        if runner_conf.source in ["cloudfiles", "snapshot"]:
            runner_conf.silver_pipeline_id = self.create_sdp_meta_pipeline(
                f"sdp-meta-silver-{runner_conf.run_id}",
                "silver",
                "A1",
                runner_conf.silver_schema,
                runner_conf,
            )

    def launch_workflow(self, runner_conf: SDPMetaRunnerConf):

        created_job = self.create_workflow_spec(runner_conf)

        runner_conf.job_id = created_job.job_id
        print(f"Job created successfully. job_id={created_job.job_id}, started run...")
        webbrowser.open(
            f"{self.ws.config.host}/jobs/{created_job.job_id}?o={self.ws.get_workspace_id()}"
        )
        print(f"Waiting for job to complete. job_id={created_job.job_id}")
        run_by_id = self.ws.jobs.run_now(job_id=created_job.job_id).result(timeout=timedelta(minutes=20))
        print(f"Job run finished. run_id={run_by_id}")
        return created_job

    def download_test_results(self, runner_conf: SDPMetaRunnerConf):
        ws_output_file = self.ws.workspace.download(runner_conf.test_output_file_path)
        with open(
            f"integration_test_output_{runner_conf.run_id}.csv", "wb"
        ) as output_file:
            output_file.write(ws_output_file.read())

    def open_job_url(self, runner_conf, created_job):
        runner_conf.job_id = created_job.job_id
        url = f"{self.ws.config.host}/jobs/{created_job.job_id}?o={self.ws.get_workspace_id()}"
        self.ws.jobs.run_now(job_id=created_job.job_id)
        webbrowser.open(url)
        print(f"Job created successfully. job_id={created_job.job_id}, url={url}")

    def clean_up(self, runner_conf: SDPMetaRunnerConf):
        print("Cleaning up...")
        if runner_conf.job_id:
            self.ws.jobs.delete(runner_conf.job_id)
        if runner_conf.bronze_pipeline_id:
            self.ws.pipelines.delete(runner_conf.bronze_pipeline_id)
        if runner_conf.bronze_pipeline_A2_id:
            self.ws.pipelines.delete(runner_conf.bronze_pipeline_A2_id)
        if runner_conf.silver_pipeline_id:
            self.ws.pipelines.delete(runner_conf.silver_pipeline_id)
        if runner_conf.uc_catalog_name:
            test_schema_list = [
                runner_conf.sdp_meta_schema,
                runner_conf.bronze_schema,
                runner_conf.silver_schema,
            ]
            schema_list = self.ws.schemas.list(catalog_name=runner_conf.uc_catalog_name)
            for schema in schema_list:
                if schema.name in test_schema_list:
                    print(f"Deleting schema: {schema.name}")
                    vol_list = self.ws.volumes.list(
                        catalog_name=runner_conf.uc_catalog_name,
                        schema_name=schema.name,
                    )
                    for vol in vol_list:
                        print(f"Deleting volume: {vol.full_name}")
                        self.ws.volumes.delete(vol.full_name)
                    tables_list = self.ws.tables.list(
                        catalog_name=runner_conf.uc_catalog_name,
                        schema_name=schema.name,
                    )
                    for table in tables_list:
                        print(f"Deleting table: {table.full_name}")
                        self.ws.tables.delete(table.full_name)
                    self.ws.schemas.delete(schema.full_name)
        print("Cleaning up complete!!!")

    def run(self, runner_conf: SDPMetaRunnerConf):
        try:
            self.init_sdp_meta_runner_conf(runner_conf)
            self.create_bronze_silver_dlt(runner_conf)
            self.launch_workflow(runner_conf)
            self.download_test_results(runner_conf)
        except Exception as e:
            print(e)
            traceback.print_exc()
            raise
        # finally:
        #     self.clean_up(runner_conf)


def process_arguments() -> dict[str:str]:
    """
    Get, process, and validate the command line arguements

    Returns:
        A dictionary where the argument names are the keys and the values aredictionary values
    """

    print("Processing comand line arguments...")

    # Possible input arguments, organized as elements in a list like:
    # [argument, help message, type, required, choices (if applicable)]
    input_args = [
        # Generic arguments
        [
            "profile",
            "Provide databricks cli profile name, if not provide databricks_host and token",
            str,
            False,
            [],
        ],
        [
            "uc_catalog_name",
            "Provide databricks uc_catalog name, this is required to create volume, schema, table",
            str,
            True,
            [],
        ],
        [
            "source",
            "Provide source type: cloudfiles, eventhub, kafka, snapshot, quality, multi_source_cdc",
            str.lower,
            False,
            [
                "cloudfiles",
                "eventhub",
                "kafka",
                "snapshot",
                "quality",
                "quality_dqx",
                "multi_source_cdc",
            ],
        ],
        [
            "onboarding_file_format",
            "Format of the generated onboarding file: 'json' (default) or 'yaml'.",
            str.lower,
            False,
            ["json", "yaml", "yml"],
        ],
        # Eventhub arguments
        ["eventhub_name", "Provide eventhub_name e.g: iot", str.lower, False, []],
        [
            "eventhub_name_append_flow",
            "Provide eventhub_name_append_flow e.g: iot_af",
            str.lower,
            False,
            [],
        ],
        [
            "eventhub_producer_accesskey_name",
            "Provide access key that has write permission on the eventhub",
            str.lower,
            False,
            [],
        ],
        [
            "eventhub_consumer_accesskey_name",
            "Provide access key that has read permission on the eventhub",
            str.lower,
            False,
            [],
        ],
        [
            "eventhub_secrets_scope_name",
            "Provide eventhub_secrets_scope_name e.g: eventhubs_creds",
            str.lower,
            False,
            [],
        ],
        [
            "eventhub_accesskey_secret_name",
            "Provide eventhub_accesskey_secret_name e.g: RootManageSharedAccessKey",
            str.lower,
            False,
            [],
        ],
        [
            "eventhub_namespace",
            "Provide eventhub_namespace e.g: topic-standar",
            str.lower,
            False,
            [],
        ],
        [
            "eventhub_port",
            "Provide eventhub_port e.g: 9093",
            str.lower,
            False,
            [],
        ],
        [
            "eventhub_sink_name",
            "Provide an eventhub sink name to write data",
            str.lower,
            False,
            []
        ],
        # Kafka arguments
        [
            "kafka_source_topic",
            "Provide kafka source topic name e.g: iot",
            str.lower,
            False,
            [],
        ],
        [
            "kafka_source_servers_secrets_scope_name",
            "Provide kafka broker secret scope name e.g: abc",
            str.lower,
            False,
            [],
        ],
        [
            "kafka_source_servers_secrets_scope_key",
            "Provide kafka broker secret scope key e.g: xyz",
            str.lower,
            False,
            [],
        ],
        [
            "kafka_broker",
            "Provide kafka broker e.g 127.0.0.1:9092",
            str.lower,
            False,
            [],
        ],
        [
            "kafka_sink_topic",
            "Provide kafka sink topic e.g: iot_sink",
            str.lower,
            False,
            [],
        ],
        [
            "kafka_sink_servers_secret_scope_name",
            "Provide kafka server for sink secret scope name e.g: abc",
            str.lower,
            False,
            [],
        ],
        [
            "kafka_sink_servers_secret_scope_key",
            "Provide kafka server for sink secret scope key e.g: xyz",
            str.lower,
            False,
            [],
        ],
    ]

    # Build cli parser
    parser = argparse.ArgumentParser()
    for arg in input_args:
        if arg[4]:
            parser.add_argument(
                f"--{arg[0]}", help=arg[1], type=arg[2], required=arg[3], choices=arg[4]
            )
        else:
            parser.add_argument(
                f"--{arg[0]}", help=arg[1], type=arg[2], required=arg[3]
            )
    args = vars(parser.parse_args())

    # Validate UC identifiers up-front so the run fails fast with a clear
    # error message instead of crashing later inside CREATE SCHEMA / CREATE
    # VOLUME / spark.read.table when a hyphenated catalog or other illegal
    # name was passed (issue #261). Strictly regular SQL identifiers only.
    validate_uc_identifier(args["uc_catalog_name"], kind="--uc_catalog_name")

    def check_cond_mandatory_arg(args, mandatory_args):
        """Post argument parsing check for conditionally required arguments"""
        for mand_arg in mandatory_args:
            if args[mand_arg] is None:
                raise Exception(f"Please provide '--{mand_arg}'")

    # Check for arguments that are required depending on the selected source
    if args["source"] == "eventhub":
        check_cond_mandatory_arg(
            args,
            [
                "eventhub_name",
                "eventhub_producer_accesskey_name",
                "eventhub_consumer_accesskey_name",
                "eventhub_secrets_scope_name",
                "eventhub_namespace",
                "eventhub_sink_name",
                "eventhub_port",
            ],
        )
    elif args["source"] == "kafka":
        check_cond_mandatory_arg(
            args,
            [
                "kafka_source_topic",
                "kafka_sink_topic"
            ],
        )

    print(f"Processing comand line arguments Complete: {args}")
    return args


def get_workspace_api_client(profile=None) -> WorkspaceClient:
    """Get a ``WorkspaceClient`` configured for the current runtime.

    Resolution order:

    1. **Databricks Apps container** — when ``DATABRICKS_APP_PORT`` is set
       the platform has injected ``DATABRICKS_HOST`` plus the app's
       service-principal OAuth credentials (``DATABRICKS_CLIENT_ID`` /
       ``DATABRICKS_CLIENT_SECRET``) into the environment. The SDK's
       default credentials provider picks these up automatically when
       ``WorkspaceClient()`` is constructed with no arguments — no
       profile, no interactive prompts. The explicit ``profile`` argument
       is intentionally ignored in this branch: ``~/.databrickscfg``
       does not exist in the container, and falling back to it would
       fail the exact way Apps callers are trying to avoid.
    2. **Local CLI with ``--profile``** — uses the named
       ``~/.databrickscfg`` entry. This is the path
       ``run_integration_tests.py`` and the ``launch_*_demo.py`` scripts
       take when invoked from a developer's terminal.
    3. **Interactive fallback** — prompts for host + token via ``input()``
       so ad-hoc local runs without a configured profile still work.
       Requires a real stdin; do NOT route App / CI traffic through
       this branch (it raises ``EOFError`` when stdin is not a TTY).
    """
    if os.environ.get("DATABRICKS_APP_PORT"):
        return WorkspaceClient()
    if profile:
        return WorkspaceClient(profile=profile)
    return WorkspaceClient(
        host=input("Databricks Workspace URL: "), token=input("Token: ")
    )


def main():
    """Entry method to run integration tests."""
    args = process_arguments()
    workspace_client = get_workspace_api_client(args["profile"])
    integration_test_runner = SDPMETARunner(args, workspace_client, "integration_tests")
    runner_conf = integration_test_runner.init_runner_conf()
    integration_test_runner.run(runner_conf)


if __name__ == "__main__":
    """
    Cloud files tests passing

    Kafka is failling due to 'AttributeError: '_SixMetaPathImporter' object has no attribute 'find_spec''
    that occurs on from kafka import KafkaProducer when using a serverless notebook (it succeeds on
    a classic cluster)

    No eventhub connection to be able to test that
    """
    main()
