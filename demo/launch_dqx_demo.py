"""Run an end-to-end SDP-META and Databricks Labs DQX demo.

The adapter keeps the package boundary explicit while execution stays in one
pipeline:

    onboarding -> SDP-META bronze pipeline (DQX adapter) -> validation

The pipeline applies native DQX metadata checks and writes
``customers_valid`` plus ``customers_quarantine``.

Usage:
    python demo/launch_dqx_demo.py \
        --uc_catalog_name <your_catalog> \
        --profile <your_profile>

Optional:
    --onboarding_file_format yaml   (default json)
"""

import os
import sys
import traceback
import uuid

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from databricks.sdk.service import compute, jobs  # noqa: E402

from databricks.labs.sdp_meta.install import WorkspaceInstaller  # noqa: E402
from integration_tests.run_integration_tests import (  # noqa: E402
    SDPMETARunner,
    SDPMetaRunnerConf,
    get_workspace_api_client,
    process_arguments,
)

DQX_VERSION = "0.16.0"
DQX_DEPENDENCY = f"databricks-labs-dqx=={DQX_VERSION}"


class SDPMETADQXDemo(SDPMETARunner):
    """Launch DQX inside the SDP-META bronze pipeline."""

    def __init__(self, args, ws, base_dir):
        self.args = args
        self.ws = ws
        self.wsi = WorkspaceInstaller(ws)
        self.base_dir = base_dir

    def run(self, runner_conf: SDPMetaRunnerConf):
        try:
            self.init_sdp_meta_runner_conf(runner_conf)
            self.create_bronze_silver_dlt(runner_conf)
            self.launch_workflow(runner_conf)
        except Exception as exc:
            print(exc)
            traceback.print_exc()
            raise
        # Resources remain available for inspection. Cleanup commands are
        # printed after job creation and documented in demo/README.md.

    def init_runner_conf(self) -> SDPMetaRunnerConf:
        run_id = uuid.uuid4().hex
        runner_conf = SDPMetaRunnerConf(
            run_id=run_id,
            username=self.wsi._my_username,
            int_tests_dir="demo",
            sdp_meta_schema=f"sdp_meta_dataflowspecs_dqx_demo_{run_id}",
            bronze_schema=f"sdp_meta_bronze_dqx_demo_{run_id}",
            silver_schema=f"sdp_meta_silver_dqx_demo_{run_id}",
            runners_nb_path=(
                f"/Users/{self.wsi._my_username}/sdp_meta_dqx_demo/{run_id}"
            ),
            runners_full_local_path="demo/notebooks/dqx_runners",
            source="quality_dqx",
            quality_dqx_template="demo/conf/json/dqx-onboarding.template",
            # The shared configuration generator expects an A2 template for
            # CloudFiles. This demo runs only A1, so reuse the same template.
            cloudfiles_A2_template=(
                "demo/conf/json/dqx-onboarding.template"
            ),
            onboarding_file_path="demo/conf/json/dqx_onboarding.json",
            onboarding_A2_file_path=(
                "demo/conf/json/dqx_onboarding_A2.json"
            ),
            onboarding_file_format=(
                self.args.get("onboarding_file_format") or "json"
            ),
            env="demo",
        )
        runner_conf.uc_catalog_name = self.args["uc_catalog_name"]
        return runner_conf

    def create_bronze_silver_dlt(self, runner_conf: SDPMetaRunnerConf):
        """Create the single bronze pipeline used by the DQX job task."""
        runner_conf.bronze_pipeline_id = self.create_sdp_meta_pipeline(
            f"sdp-meta-bronze-dqx-demo-{runner_conf.run_id}",
            "bronze",
            "A1",
            runner_conf.bronze_schema,
            runner_conf,
        )
        runner_conf.bronze_pipeline_A2_id = None
        runner_conf.silver_pipeline_id = None

    def launch_workflow(self, runner_conf: SDPMetaRunnerConf):
        created_job = self._create_workflow_spec(runner_conf)
        self.open_job_url(runner_conf, created_job)
        catalog = runner_conf.uc_catalog_name
        print("Inspect the DQX outputs after the workflow completes.")
        print("Cleanup:")
        for schema in (
            runner_conf.sdp_meta_schema,
            runner_conf.bronze_schema,
            runner_conf.silver_schema,
        ):
            print(f"  DROP SCHEMA {catalog}.{schema} CASCADE;")

    def _create_workflow_spec(self, runner_conf: SDPMetaRunnerConf):
        environment_key = "sdp_meta_dqx_env"
        environments = [
            jobs.JobEnvironment(
                environment_key=environment_key,
                spec=compute.Environment(
                    environment_version="4",
                    dependencies=[
                        runner_conf.remote_whl_path,
                        DQX_DEPENDENCY,
                    ],
                ),
            )
        ]

        return self.ws.jobs.create(
            name=f"sdp-meta-dqx-demo-{runner_conf.run_id}",
            environments=environments,
            tasks=[
                jobs.Task(
                    task_key="onboarding_job",
                    description="Populate the bronze SDP-META dataflow spec.",
                    environment_key=environment_key,
                    timeout_seconds=0,
                    python_wheel_task=jobs.PythonWheelTask(
                        package_name="databricks_labs_sdp_meta",
                        entry_point="run",
                        named_parameters={
                            "onboard_layer": "bronze",
                            "database": (
                                f"{runner_conf.uc_catalog_name}."
                                f"{runner_conf.sdp_meta_schema}"
                            ),
                            "onboarding_file_path": (
                                f"{runner_conf.uc_volume_path}"
                                f"{runner_conf.onboarding_file_path}"
                            ),
                            "bronze_dataflowspec_table": (
                                "bronze_dataflowspec_cdc"
                            ),
                            "import_author": "SDP-META DQX demo",
                            "version": "v1",
                            "overwrite": "True",
                            "env": runner_conf.env,
                            "uc_enabled": "True",
                        },
                    ),
                ),
                jobs.Task(
                    task_key="sdp_meta_pipeline",
                    description=(
                        "Apply DQX and materialize valid/quarantine tables."
                    ),
                    depends_on=[
                        jobs.TaskDependency(task_key="onboarding_job")
                    ],
                    pipeline_task=jobs.PipelineTask(
                        pipeline_id=runner_conf.bronze_pipeline_id
                    ),
                ),
                jobs.Task(
                    task_key="validate",
                    description="Assert DQX routing and result counts.",
                    depends_on=[
                        jobs.TaskDependency(task_key="sdp_meta_pipeline")
                    ],
                    environment_key=environment_key,
                    timeout_seconds=0,
                    notebook_task=jobs.NotebookTask(
                        notebook_path=(
                            f"{runner_conf.runners_nb_path}"
                            "/runners/validate.py"
                        ),
                        base_parameters={
                            "uc_catalog_name": runner_conf.uc_catalog_name,
                            "bronze_schema": runner_conf.bronze_schema,
                            "silver_schema": runner_conf.silver_schema,
                        },
                    ),
                ),
            ],
        )


def main():
    args = process_arguments()
    workspace_client = get_workspace_api_client(args["profile"])
    runner = SDPMETADQXDemo(args, workspace_client, "demo")
    print("Initialization complete")
    runner.run(runner.init_runner_conf())


if __name__ == "__main__":
    main()
