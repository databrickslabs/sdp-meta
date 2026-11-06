import uuid
import traceback
from databricks.sdk.service import jobs, compute
from databricks.labs.sdp_meta.install import WorkspaceInstaller
from integration_tests.run_integration_tests import (
    SDPMETARunner,
    SDPMetaRunnerConf,
    get_workspace_api_client,
    process_arguments
)


class SDPMETADAISDemo(SDPMETARunner):
    """
    A class to run SDP-META DAIS DEMO.

    Attributes:
    - args: command line arguments
    - workspace_client: Databricks workspace client
    - base_dir: base directory
    """
    def __init__(self, args, ws, base_dir):
        self.args = args
        self.ws = ws
        self.wsi = WorkspaceInstaller(ws)
        self.base_dir = base_dir

    def init_runner_conf(self) -> SDPMetaRunnerConf:
        """
        Initialize the runner configuration.

        Returns:
        - runner_conf: SDPMetaRunnerConf object
        """
        run_id = uuid.uuid4().hex
        runner_conf = SDPMetaRunnerConf(
            run_id=run_id,
            username=self._my_username(self.ws),
            int_tests_dir="demo",
            sdp_meta_schema=f"sdp_meta_dataflowspecs_demo_{run_id}",
            bronze_schema=f"sdp_meta_bronze_dais_demo_{run_id}",
            silver_schema=f"sdp_meta_silver_dais_demo_{run_id}",
            runners_nb_path=f"/Users/{self.wsi._my_username}/sdp_meta_dais_demo/{run_id}",
            runners_full_local_path="demo/notebooks/dais_runners",
            # node_type_id=cloud_node_type_id_dict[self.args.__dict__['cloud_provider_name']],
            # dbr_version=self.args.__dict__['dbr_version'],
            cloudfiles_template="demo/conf/json/onboarding.template",
            env="prod",
            source="cloudfiles",
            # runners_full_local_path='./demo/dbc/dais_sdp_meta_runners.dbc',
            onboarding_file_path='demo/conf/json/onboarding.json',
            onboarding_file_format=self.args.get("onboarding_file_format") or "json",
        )
        if self.args['uc_catalog_name']:
            runner_conf.uc_catalog_name = self.args['uc_catalog_name']
            runner_conf.uc_volume_name = f"{runner_conf.uc_catalog_name}_dais_demo_{run_id}"

        return runner_conf

    def run(self, runner_conf: SDPMetaRunnerConf):
        """
        Run the SDP-META DAIS DEMO.

        Args:
        - runner_conf: SDPMetaRunnerConf object
        """
        try:
            self.init_sdp_meta_runner_conf(runner_conf)
            self.create_bronze_silver_dlt(runner_conf)
            self.launch_workflow(runner_conf)
        except Exception as e:
            print(e)
            traceback.print_exc()
            raise
        # finally:
        #     self.clean_up(runner_conf)

    def launch_workflow(self, runner_conf: SDPMetaRunnerConf):
        """
        Launch the workflow for SDP-META DAIS DEMO.

        Args:
        - runner_conf: SDPMetaRunnerConf object
        """
        created_job = self.create_daisdemo_workflow(runner_conf)
        self.open_job_url(
            runner_conf,
            created_job,
            wait_for_completion=True,
        )

    def create_daisdemo_workflow(self, runner_conf: SDPMetaRunnerConf):
        """
        Create the workflow for SDP-META DAIS DEMO.

        Args:
        - runner_conf: SDPMetaRunnerConf object

        Returns:
        - created_job: created job object
        """
        sdp_meta_environments = [
            jobs.JobEnvironment(
                environment_key="dl_meta_int_env",
                spec=compute.Environment(
                    client="1",
                    dependencies=[runner_conf.remote_whl_path],
                ),
            )
        ]
        return self.ws.jobs.create(
            name=f"sdp_meta_dais_demo-{runner_conf.run_id}",
            environments=sdp_meta_environments,
            tasks=[
                jobs.Task(
                    task_key="setup_sdp_meta_pipeline_spec",
                    description="test",
                    environment_key="dl_meta_int_env",
                    timeout_seconds=0,
                    python_wheel_task=jobs.PythonWheelTask(
                        package_name="databricks_labs_sdp_meta",
                        entry_point="run",
                        named_parameters={
                            "onboard_layer": "bronze_silver",
                            "database": f"{runner_conf.uc_catalog_name}.{runner_conf.sdp_meta_schema}",
                            "onboarding_file_path": f"{runner_conf.uc_volume_path}/{runner_conf.onboarding_file_path}",
                            "silver_dataflowspec_table": "silver_dataflowspec_cdc",
                            "silver_dataflowspec_path": (
                                f"{runner_conf.uc_volume_path}/demo/resources/data/dlt_spec/silver"
                            ),
                            "bronze_dataflowspec_table": "bronze_dataflowspec_cdc",
                            "import_author": "Ravi",
                            "version": "v1",
                            "bronze_dataflowspec_path": (
                                f"{runner_conf.uc_volume_path}/demo/resources/data/dlt_spec/bronze"
                            ),
                            "overwrite": "True",
                            "env": runner_conf.env,
                            "uc_enabled": "True" if runner_conf.uc_catalog_name else "False"
                        }
                    )
                ),
                jobs.Task(
                    task_key="bronze_initial_run",
                    depends_on=[jobs.TaskDependency(task_key="setup_sdp_meta_pipeline_spec")],
                    pipeline_task=jobs.PipelineTask(
                        pipeline_id=runner_conf.bronze_pipeline_id
                    ),
                ),
                jobs.Task(
                    task_key="silver_initial_run",
                    depends_on=[jobs.TaskDependency(task_key="bronze_initial_run")],
                    pipeline_task=jobs.PipelineTask(
                        pipeline_id=runner_conf.silver_pipeline_id
                    )
                ),
                jobs.Task(
                    task_key="load_incremental_data",
                    description="Load Incremental Data",
                    depends_on=[jobs.TaskDependency(task_key="silver_initial_run")],
                    notebook_task=jobs.NotebookTask(
                        notebook_path=f"{runner_conf.runners_nb_path}/runners/load_incremental_data.py",
                        base_parameters={
                            "dbfs_tmp_path": runner_conf.uc_volume_path
                        }
                    )
                ),

                jobs.Task(
                    task_key="bronze_incremental_run",
                    depends_on=[jobs.TaskDependency(task_key="load_incremental_data")],
                    pipeline_task=jobs.PipelineTask(
                        pipeline_id=runner_conf.bronze_pipeline_id
                    ),
                ),
                jobs.Task(
                    task_key="silver_incremental_run",
                    depends_on=[jobs.TaskDependency(task_key="bronze_incremental_run")],
                    pipeline_task=jobs.PipelineTask(
                        pipeline_id=runner_conf.silver_pipeline_id
                    )
                )
            ]
        )


def main():
    """Entry method to run integration tests."""
    args = process_arguments()
    workspace_client = get_workspace_api_client(args['profile'])
    sdp_meta_dais_demo_runner = SDPMETADAISDemo(args, workspace_client, "demo")
    runner_conf = sdp_meta_dais_demo_runner.init_runner_conf()
    sdp_meta_dais_demo_runner.run(runner_conf)


if __name__ == "__main__":
    main()
