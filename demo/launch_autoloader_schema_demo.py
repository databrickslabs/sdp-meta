"""Auto Loader schema inference, hints, and evolution demo.

This standalone demo intentionally omits ``source_schema_path``. It proves:

1. Auto Loader infers a Bronze schema while honoring ``schemaHints``.
2. ``schemaEvolutionMode=addNewColumns`` adds ``device_type`` when the
   second file arrives.
3. A value incompatible with the hinted ``DECIMAL(10,2)`` type is retained
   in ``_rescued_data`` instead of being lost.

Usage:
    python demo/launch_autoloader_schema_demo.py \
        --uc_catalog_name <catalog> \
        --profile <profile>

Optional:
    --onboarding_file_format yaml
"""

import sys
import traceback
import uuid
import webbrowser
from datetime import timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
for _path in (REPO_ROOT, REPO_ROOT / "src"):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from databricks.labs.sdp_meta.install import WorkspaceInstaller  # noqa: E402
from databricks.labs.sdp_meta.identifiers import (  # noqa: E402
    validate_uc_identifier,
)
from databricks.sdk.service import compute, jobs  # noqa: E402
from databricks.sdk.service.pipelines import (  # noqa: E402
    NotebookLibrary,
    PipelineLibrary,
)

from integration_tests.run_integration_tests import (  # noqa: E402
    SDPMETARunner,
    SDPMetaRunnerConf,
    create_pipeline,
    get_workspace_api_client,
    process_arguments,
)


class SDPMetaAutoLoaderSchemaDemo(SDPMETARunner):
    """Run the two-phase Auto Loader schema evolution scenario."""

    GROUP = "SCHEMA_EVOLUTION"

    def __init__(self, args, ws, base_dir="demo"):
        self.args = args
        self.ws = ws
        self.wsi = WorkspaceInstaller(ws)
        self.base_dir = base_dir

    def run(self, runner_conf: SDPMetaRunnerConf):
        try:
            self.init_sdp_meta_runner_conf(runner_conf)
            self.create_bronze_silver_dlt(runner_conf)
            self.launch_workflow(runner_conf)
        except Exception:
            traceback.print_exc()
            raise

    def init_runner_conf(self) -> SDPMetaRunnerConf:
        validate_uc_identifier(
            self.args["uc_catalog_name"], kind="uc_catalog_name"
        )
        run_id = uuid.uuid4().hex
        template = "demo/conf/json/autoloader-schema-onboarding.template"
        runner_conf = SDPMetaRunnerConf(
            run_id=run_id,
            username=self.wsi._my_username,
            uc_catalog_name=self.args["uc_catalog_name"],
            int_tests_dir="demo",
            sdp_meta_schema=f"sdp_meta_dataflowspecs_schema_demo_{run_id}",
            bronze_schema=f"sdp_meta_bronze_schema_demo_{run_id}",
            silver_schema=f"sdp_meta_silver_schema_demo_{run_id}",
            runners_nb_path=(
                f"/Users/{self.wsi._my_username}/"
                f"sdp_meta_autoloader_schema_demo/{run_id}"
            ),
            runners_full_local_path=(
                "demo/notebooks/autoloader_schema_runners"
            ),
            source="cloudfiles",
            cloudfiles_template=template,
            cloudfiles_A2_template=template,
            onboarding_file_path=(
                "demo/conf/json/onboarding_autoloader_schema.json"
            ),
            onboarding_A2_file_path=(
                "demo/conf/json/onboarding_autoloader_schema_A2.json"
            ),
            onboarding_file_format=(
                self.args.get("onboarding_file_format") or "json"
            ),
            env="demo",
        )
        return runner_conf

    def create_bronze_silver_dlt(self, runner_conf: SDPMetaRunnerConf):
        """Create one Bronze pipeline dedicated to schema tracking."""
        configuration = {
            "layer": "bronze",
            "bronze.group": self.GROUP,
            "bronze.dataflowspecTable": (
                f"{runner_conf.uc_catalog_name}."
                f"{runner_conf.sdp_meta_schema}.bronze_dataflowspec_cdc"
            ),
            "sdp_meta_whl": runner_conf.remote_whl_path,
            "pipelines.externalSink.enabled": "true",
        }
        created = create_pipeline(
            self.ws,
            catalog=runner_conf.uc_catalog_name,
            name=f"sdp-meta-autoloader-schema-demo-{runner_conf.run_id}",
            serverless=True,
            configuration=configuration,
            libraries=[
                PipelineLibrary(
                    notebook=NotebookLibrary(
                        path=(
                            f"{runner_conf.runners_nb_path}/runners/"
                            "init_sdp_meta_pipeline.py"
                        )
                    )
                )
            ],
            schema=runner_conf.bronze_schema,
        )
        if created is None:
            raise RuntimeError("Auto Loader schema demo pipeline creation failed")
        runner_conf.bronze_pipeline_id = created.pipeline_id
        runner_conf.bronze_pipeline_A2_id = None
        runner_conf.silver_pipeline_id = None

    def launch_workflow(self, runner_conf: SDPMetaRunnerConf):
        created_job = self.create_schema_workflow_spec(runner_conf)
        runner_conf.job_id = created_job.job_id
        url = (
            f"{self.ws.config.host}/jobs/{created_job.job_id}"
            f"?o={self.ws.get_workspace_id()}"
        )
        waiter = self.ws.jobs.run_now(job_id=created_job.job_id)
        webbrowser.open(url)
        print(
            "Auto Loader schema demo started. "
            f"job_id={created_job.job_id}, run_id={waiter.run_id}, url={url}"
        )
        waiter.result(timeout=timedelta(minutes=30))
        print("Auto Loader schema demo completed successfully.")

    def _stage_task(self, runner_conf, phase, depends_on=None):
        return jobs.Task(
            task_key=f"stage_phase_{phase}",
            description=f"Copy phase-{phase} JSON into the landing directory.",
            depends_on=depends_on or [],
            timeout_seconds=0,
            notebook_task=jobs.NotebookTask(
                notebook_path=(
                    f"{runner_conf.runners_nb_path}/runners/stage_data.py"
                ),
                base_parameters={
                    "uc_volume_path": runner_conf.uc_volume_path,
                    "phase": str(phase),
                },
            ),
        )

    def _validate_task(self, runner_conf, phase, depends_on):
        return jobs.Task(
            task_key=f"validate_phase_{phase}",
            description=f"Validate Auto Loader schema demo phase {phase}.",
            depends_on=[jobs.TaskDependency(task_key=depends_on)],
            timeout_seconds=0,
            notebook_task=jobs.NotebookTask(
                notebook_path=(
                    f"{runner_conf.runners_nb_path}/runners/validate.py"
                ),
                base_parameters={
                    "uc_catalog_name": runner_conf.uc_catalog_name,
                    "sdp_meta_schema": runner_conf.sdp_meta_schema,
                    "bronze_schema": runner_conf.bronze_schema,
                    "phase": str(phase),
                },
            ),
        )

    def create_schema_workflow_spec(self, runner_conf: SDPMetaRunnerConf):
        """Create the staged two-update workflow."""
        environment_key = "sdp_meta_schema_demo_env"
        tasks = [
            self._stage_task(runner_conf, phase=1),
            jobs.Task(
                task_key="onboarding_job",
                description="Persist the inference-only Bronze DataflowSpec.",
                depends_on=[jobs.TaskDependency(task_key="stage_phase_1")],
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
                        "bronze_dataflowspec_path": (
                            f"{runner_conf.uc_volume_path}"
                            "data/autoloader_schema_demo/spec"
                        ),
                        "import_author": "sdp-meta-demo",
                        "version": "v1",
                        "overwrite": "True",
                        "env": runner_conf.env,
                        "uc_enabled": "True",
                    },
                ),
            ),
            jobs.Task(
                task_key="pipeline_phase_1",
                description="Infer the initial schema and apply schema hints.",
                depends_on=[jobs.TaskDependency(task_key="onboarding_job")],
                pipeline_task=jobs.PipelineTask(
                    pipeline_id=runner_conf.bronze_pipeline_id
                ),
            ),
            self._validate_task(
                runner_conf, phase=1, depends_on="pipeline_phase_1"
            ),
            self._stage_task(
                runner_conf,
                phase=2,
                depends_on=[jobs.TaskDependency(task_key="validate_phase_1")],
            ),
            jobs.Task(
                task_key="pipeline_phase_2",
                description=(
                    "Evolve device_type and rescue an incompatible amount."
                ),
                depends_on=[jobs.TaskDependency(task_key="stage_phase_2")],
                max_retries=2,
                min_retry_interval_millis=10000,
                pipeline_task=jobs.PipelineTask(
                    pipeline_id=runner_conf.bronze_pipeline_id
                ),
            ),
            self._validate_task(
                runner_conf, phase=2, depends_on="pipeline_phase_2"
            ),
        ]
        return self.ws.jobs.create(
            name=f"sdp-meta-autoloader-schema-demo-{runner_conf.run_id}",
            environments=[
                jobs.JobEnvironment(
                    environment_key=environment_key,
                    spec=compute.Environment(
                        client="1",
                        dependencies=[runner_conf.remote_whl_path],
                    ),
                )
            ],
            tasks=tasks,
        )


def main():
    args = process_arguments()
    workspace_client = get_workspace_api_client(args["profile"])
    demo = SDPMetaAutoLoaderSchemaDemo(args, workspace_client)
    runner_conf = demo.init_runner_conf()
    demo.run(runner_conf)


if __name__ == "__main__":
    main()
