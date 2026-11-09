
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


class SDPMETASilverFanoutDemo(SDPMETARunner):
    """
    Represents the SDP-META Silver Fanout Demo.

    This class is responsible for running the SDP-META Silver Fanout Demo, which includes setting up metadata tables,
    creating clusters, launching workflows, and more.

    Attributes:
    - args: The command-line arguments passed to the script.
    - ws: The Databricks workspace object.
    - base_dir: The base directory of the project.

    Methods:
    - run: Runs the SDP-META Silver Fanout Demo.
    - init_runner_conf: Initializes the runner configuration for running integration tests.
    - launch_workflow: Launches the workflow for the SDP-META Silver Fanout Demo.
    - create_sfo_workflow_spec: Creates the workflow for the SDP-META Silver Fanout Demo by defining the tasks
                                and their dependencies.
    """

    def __init__(self, args, ws, base_dir):
        self.args = args
        self.ws = ws
        self.wsi = WorkspaceInstaller(ws)
        self.base_dir = base_dir

    def run(self, runner_conf: SDPMetaRunnerConf):
        """
        Runs the SDP-META Silver Fanout Demo.

        Parameters:
        - runner_conf: The SDPMetaRunnerConf object containing the runner configuration parameters.
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

    def init_runner_conf(self) -> SDPMetaRunnerConf:
        """
        Initialize the runner configuration for running integration tests.

        Returns:
        -------
        SDPMetaRunnerConf
            The initialized runner configuration.
        """
        run_id = uuid.uuid4().hex
        runner_conf = SDPMetaRunnerConf(
            run_id=run_id,
            username=self.wsi._my_username,
            int_tests_dir="demo",
            sdp_meta_schema=f"sdp_meta_dataflowspecs_demo_{run_id}",
            bronze_schema=f"sdp_meta_bronze_demo_{run_id}",
            silver_schema=f"sdp_meta_silver_demo_{run_id}",
            runners_nb_path=f"/Users/{self.wsi._my_username}/sdp_meta_fout_demo/{run_id}",
            runners_full_local_path="demo/notebooks/silver_fanout_runners",
            source="cloudfiles",
            # Single-file fanout: ``silver-fanout-onboarding.template``
            # carries the cars bronze+silver row PLUS three silver-only
            # fanout rows (cars_germany / cars_uk / cars_japan) in one
            # spec. The bronze pass skips rows lacking
            # ``source_details`` (the fanout consumers) and the silver
            # pass picks them up from the same file \u2014 no second
            # ``onboard_layer=silver, overwrite=False`` task needed.
            cloudfiles_template="demo/conf/json/silver-fanout-onboarding.template",
            onboarding_file_path="demo/conf/json/silver-fanout-onboarding.json",
            onboarding_file_format=self.args.get("onboarding_file_format") or "json",
            env="demo"
        )
        runner_conf.uc_catalog_name = self.args['uc_catalog_name']
        runner_conf.uc_volume_name = f"{runner_conf.uc_catalog_name}_sdp_meta_fout_demo_{run_id}"
        return runner_conf

    def launch_workflow(self, runner_conf: SDPMetaRunnerConf):
        created_job = self.create_sfo_workflow_spec(runner_conf)
        self.open_job_url(
            runner_conf,
            created_job,
            wait_for_completion=True,
        )

    def create_sfo_workflow_spec(self, runner_conf: SDPMetaRunnerConf):
        """
        Creates the workflow for the SDP-META Silver Fanout Demo by defining the tasks and their dependencies.

        Parameters:
        - runner_conf: The SDPMetaRunnerConf object containing the runner configuration parameters.

        Returns:
        - created_job: The created job object.
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
            name=f"dlt-silver-fanout-demo-{runner_conf.run_id}",
            environments=sdp_meta_environments,
            tasks=[
                jobs.Task(
                    task_key="onboarding_job",
                    description="Sets up metadata tables for SDP-META",
                    environment_key="dl_meta_int_env",
                    timeout_seconds=0,
                    python_wheel_task=jobs.PythonWheelTask(
                        package_name="databricks_labs_sdp_meta",
                        entry_point="run",
                        named_parameters={
                            "onboard_layer": "bronze_silver",
                            "database": f"{runner_conf.uc_catalog_name}.{runner_conf.sdp_meta_schema}",
                            "onboarding_file_path":
                            f"{runner_conf.uc_volume_path}/{runner_conf.onboarding_file_path}",
                            "silver_dataflowspec_table": "silver_dataflowspec_cdc",
                            "bronze_dataflowspec_table": "bronze_dataflowspec_cdc",
                            "import_author": "Ravi",
                            "version": "v1",
                            "overwrite": "True",
                            "env": runner_conf.env,
                            "uc_enabled": "True"
                        },
                    ),
                ),
                jobs.Task(
                    task_key="bronze_dlt",
                    depends_on=[jobs.TaskDependency(task_key="onboarding_job")],
                    pipeline_task=jobs.PipelineTask(
                        pipeline_id=runner_conf.bronze_pipeline_id
                    ),
                ),
                jobs.Task(
                    task_key="silver_dlt",
                    depends_on=[jobs.TaskDependency(task_key="bronze_dlt")],
                    pipeline_task=jobs.PipelineTask(
                        pipeline_id=runner_conf.silver_pipeline_id
                    )
                )
            ]
        )


def main():
    args = process_arguments()
    workspace_client = get_workspace_api_client(args['profile'])
    sdp_meta_afam_demo_runner = SDPMETASilverFanoutDemo(args, workspace_client, "demo")
    print("initializing complete")
    runner_conf = sdp_meta_afam_demo_runner.init_runner_conf()
    sdp_meta_afam_demo_runner.run(runner_conf)


if __name__ == "__main__":
    main()
