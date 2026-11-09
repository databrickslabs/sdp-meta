"""Launch the modern at-scale Auto Loader demo.

The demo creates a configurable 100-table workload split across schema
inference, schema hints, and explicit DDL cohorts.  It also validates schema
evolution, rescued data, DQ quarantine, a table-specific Silver callback, and
a post-Silver Gold aggregation.
"""

from __future__ import annotations

import argparse
import sys
import traceback
import uuid
from dataclasses import dataclass
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
from databricks.sdk.errors import NotFound  # noqa: E402
from databricks.sdk.service import compute, jobs  # noqa: E402
from databricks.sdk.service.jobs import RunLifeCycleState  # noqa: E402
from databricks.sdk.service.pipelines import (  # noqa: E402
    NotebookLibrary,
    PipelineLibrary,
)

from demo.at_scale_autoloader_config import (  # noqa: E402
    DEFAULT_TABLE_COUNT,
    GROUP,
    build_onboarding,
    generated_onboarding_path,
    write_onboarding,
)
from integration_tests.run_integration_tests import (  # noqa: E402
    SDPMETARunner,
    SDPMetaRunnerConf,
    create_pipeline,
    get_workspace_api_client,
)


@dataclass
class AtScaleRunnerConf(SDPMetaRunnerConf):
    """Extra state required by the at-scale workflow."""

    gold_schema: str = None
    table_count: int = DEFAULT_TABLE_COUNT
    keep_resources: bool = False
    local_onboarding_path: str = None
    job_run_id: int = None


class SDPMetaAtScaleAutoLoaderDemo(SDPMETARunner):
    """Provision, run, validate, and clean the at-scale scenario."""

    def __init__(self, args, ws, base_dir="demo"):
        self.args = args
        self.ws = ws
        self.wsi = WorkspaceInstaller(ws)
        self.base_dir = base_dir

    def init_runner_conf(self) -> AtScaleRunnerConf:
        run_id = uuid.uuid4().hex
        file_format = self.args.get("onboarding_file_format") or "json"
        file_format = "yaml" if file_format in {"yaml", "yml"} else "json"
        output_path = generated_onboarding_path(file_format, run_id)
        return AtScaleRunnerConf(
            run_id=run_id,
            username=self.wsi._my_username,
            uc_catalog_name=self.args["uc_catalog_name"],
            int_tests_dir="demo",
            sdp_meta_schema=f"sdp_meta_at_scale_specs_{run_id}",
            bronze_schema=f"sdp_meta_at_scale_bronze_{run_id}",
            silver_schema=f"sdp_meta_at_scale_silver_{run_id}",
            gold_schema=f"sdp_meta_at_scale_gold_{run_id}",
            runners_nb_path=(
                f"/Users/{self.wsi._my_username}/"
                f"sdp_meta_at_scale_autoloader/{run_id}"
            ),
            runners_full_local_path=(
                "demo/notebooks/at_scale_autoloader_runners"
            ),
            source="cloudfiles",
            onboarding_file_format=file_format,
            onboarding_file_path=str(output_path),
            onboarding_A2_file_path=str(output_path),
            cloudfiles_template="",
            cloudfiles_A2_template="",
            env="demo",
            table_count=int(self.args.get("table_count") or DEFAULT_TABLE_COUNT),
            keep_resources=bool(self.args.get("keep_resources")),
            local_onboarding_path=str(output_path),
        )

    def initialize_uc_resources(self, runner_conf: AtScaleRunnerConf):
        super().initialize_uc_resources(runner_conf)
        self.ws.schemas.create(
            catalog_name=runner_conf.uc_catalog_name,
            name=runner_conf.gold_schema,
            comment="Gold outputs for the at-scale Auto Loader demo",
        )

    def generate_onboarding_file(self, runner_conf: AtScaleRunnerConf):
        payload = build_onboarding(
            table_count=runner_conf.table_count,
            uc_volume_path=runner_conf.uc_volume_path,
            uc_catalog_name=runner_conf.uc_catalog_name,
            bronze_schema=runner_conf.bronze_schema,
            silver_schema=runner_conf.silver_schema,
        )
        path = Path(runner_conf.onboarding_file_path)
        write_onboarding(payload, path, runner_conf.onboarding_file_format)
        runner_conf.local_onboarding_path = str(path)

    def init_sdp_meta_runner_conf(self, runner_conf: AtScaleRunnerConf):
        self.initialize_uc_resources(runner_conf)
        self.generate_onboarding_file(runner_conf)
        self.upload_files_to_databricks(runner_conf)

    def _create_pipeline(
        self,
        runner_conf: AtScaleRunnerConf,
        *,
        layer: str,
        schema: str,
    ) -> str:
        configuration = {
            "layer": layer,
            f"{layer}.group": GROUP,
            f"{layer}.dataflowspecTable": (
                f"{runner_conf.uc_catalog_name}."
                f"{runner_conf.sdp_meta_schema}."
                f"{layer}_dataflowspec_cdc"
            ),
            "sdp_meta_whl": runner_conf.remote_whl_path,
            "at_scale.dimensionTable": (
                f"{runner_conf.uc_catalog_name}."
                f"{runner_conf.sdp_meta_schema}.customer_dimension"
            ),
            "pipelines.externalSink.enabled": "true",
        }
        created = create_pipeline(
            self.ws,
            catalog=runner_conf.uc_catalog_name,
            name=f"sdp-meta-at-scale-{layer}-{runner_conf.run_id}",
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
            schema=schema,
        )
        if created is None:
            raise RuntimeError(f"Failed to create the {layer} pipeline")
        return created.pipeline_id

    def create_bronze_silver_dlt(self, runner_conf: AtScaleRunnerConf):
        runner_conf.bronze_pipeline_id = self._create_pipeline(
            runner_conf,
            layer="bronze",
            schema=runner_conf.bronze_schema,
        )
        runner_conf.bronze_pipeline_A2_id = None
        runner_conf.silver_pipeline_id = self._create_pipeline(
            runner_conf,
            layer="silver",
            schema=runner_conf.silver_schema,
        )

    @staticmethod
    def _dependency(task_key: str) -> list[jobs.TaskDependency]:
        return [jobs.TaskDependency(task_key=task_key)]

    def _notebook_task(
        self,
        runner_conf: AtScaleRunnerConf,
        *,
        task_key: str,
        notebook: str,
        parameters: dict[str, str],
        depends_on: str | None = None,
    ) -> jobs.Task:
        return jobs.Task(
            task_key=task_key,
            depends_on=(
                self._dependency(depends_on) if depends_on is not None else []
            ),
            timeout_seconds=0,
            notebook_task=jobs.NotebookTask(
                notebook_path=(
                    f"{runner_conf.runners_nb_path}/runners/{notebook}"
                ),
                base_parameters=parameters,
            ),
        )

    def create_workflow_spec(self, runner_conf: AtScaleRunnerConf):
        common = {
            "uc_catalog_name": runner_conf.uc_catalog_name,
            "sdp_meta_schema": runner_conf.sdp_meta_schema,
            "bronze_schema": runner_conf.bronze_schema,
            "silver_schema": runner_conf.silver_schema,
            "gold_schema": runner_conf.gold_schema,
            "uc_volume_path": runner_conf.uc_volume_path,
            "table_count": str(runner_conf.table_count),
        }
        environment_key = "sdp_meta_at_scale_env"
        tasks = [
            self._notebook_task(
                runner_conf,
                task_key="generate_phase_1",
                notebook="generate_data.py",
                parameters={**common, "phase": "1"},
            ),
            jobs.Task(
                task_key="onboard_specs",
                depends_on=self._dependency("generate_phase_1"),
                environment_key=environment_key,
                timeout_seconds=0,
                python_wheel_task=jobs.PythonWheelTask(
                    package_name="databricks_labs_sdp_meta",
                    entry_point="run",
                    named_parameters={
                        "onboard_layer": "bronze_silver",
                        "database": (
                            f"{runner_conf.uc_catalog_name}."
                            f"{runner_conf.sdp_meta_schema}"
                        ),
                        "onboarding_file_path": (
                            f"{runner_conf.uc_volume_path}"
                            f"{runner_conf.onboarding_file_path}"
                        ),
                        "silver_dataflowspec_table": (
                            "silver_dataflowspec_cdc"
                        ),
                        "silver_dataflowspec_path": (
                            f"{runner_conf.uc_volume_path}"
                            "data/at_scale_autoloader/spec/silver"
                        ),
                        "bronze_dataflowspec_table": (
                            "bronze_dataflowspec_cdc"
                        ),
                        "bronze_dataflowspec_path": (
                            f"{runner_conf.uc_volume_path}"
                            "data/at_scale_autoloader/spec/bronze"
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
                task_key="bronze_phase_1",
                depends_on=self._dependency("onboard_specs"),
                pipeline_task=jobs.PipelineTask(
                    pipeline_id=runner_conf.bronze_pipeline_id
                ),
            ),
            jobs.Task(
                task_key="silver_phase_1",
                depends_on=self._dependency("bronze_phase_1"),
                pipeline_task=jobs.PipelineTask(
                    pipeline_id=runner_conf.silver_pipeline_id
                ),
            ),
            self._notebook_task(
                runner_conf,
                task_key="validate_phase_1",
                notebook="validate.py",
                parameters={**common, "phase": "1"},
                depends_on="silver_phase_1",
            ),
            self._notebook_task(
                runner_conf,
                task_key="stage_phase_2",
                notebook="generate_data.py",
                parameters={**common, "phase": "2"},
                depends_on="validate_phase_1",
            ),
            jobs.Task(
                task_key="bronze_phase_2",
                depends_on=self._dependency("stage_phase_2"),
                max_retries=2,
                min_retry_interval_millis=10000,
                pipeline_task=jobs.PipelineTask(
                    pipeline_id=runner_conf.bronze_pipeline_id
                ),
            ),
            jobs.Task(
                task_key="silver_phase_2",
                depends_on=self._dependency("bronze_phase_2"),
                pipeline_task=jobs.PipelineTask(
                    pipeline_id=runner_conf.silver_pipeline_id
                ),
            ),
            self._notebook_task(
                runner_conf,
                task_key="build_gold",
                notebook="build_gold.py",
                parameters=common,
                depends_on="silver_phase_2",
            ),
            self._notebook_task(
                runner_conf,
                task_key="validate_final",
                notebook="validate.py",
                parameters={**common, "phase": "2"},
                depends_on="build_gold",
            ),
        ]
        return self.ws.jobs.create(
            name=f"sdp-meta-at-scale-autoloader-{runner_conf.run_id}",
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

    def launch_workflow(self, runner_conf: AtScaleRunnerConf):
        created_job = self.create_workflow_spec(runner_conf)
        runner_conf.job_id = created_job.job_id
        url = (
            f"{self.ws.config.host}/jobs/{created_job.job_id}"
            f"?o={self.ws.get_workspace_id()}"
        )
        print(f"At-scale demo job: {url}")
        waiter = self.ws.jobs.run_now(job_id=created_job.job_id)
        runner_conf.job_run_id = waiter.run_id
        waiter.result(
            timeout=timedelta(minutes=60)
        )

    def _stop_active_run(self, runner_conf: AtScaleRunnerConf):
        if not runner_conf.job_run_id:
            return
        try:
            run = self.ws.jobs.get_run(run_id=runner_conf.job_run_id)
        except NotFound:
            return
        state = run.state.life_cycle_state
        terminal_states = {
            RunLifeCycleState.TERMINATED,
            RunLifeCycleState.SKIPPED,
            RunLifeCycleState.INTERNAL_ERROR,
        }
        if state in terminal_states:
            return
        print(
            f"Cancelling active job run {runner_conf.job_run_id} "
            "before resource cleanup..."
        )
        self.ws.jobs.cancel_run(
            run_id=runner_conf.job_run_id
        ).result(timeout=timedelta(minutes=10))

    def clean_up(self, runner_conf: AtScaleRunnerConf):
        """Remove per-run resources while tolerating concurrent LDP cleanup."""
        errors = []

        def attempt(action):
            try:
                action()
            except NotFound:
                return
            except Exception as err:  # keep deleting independent resources
                message = str(err).lower()
                if "does not exist" in message or "not found" in message:
                    return
                errors.append(err)

        attempt(lambda: self._stop_active_run(runner_conf))
        if runner_conf.job_id:
            attempt(lambda: self.ws.jobs.delete(runner_conf.job_id))
        for pipeline_id in (
            runner_conf.bronze_pipeline_id,
            runner_conf.bronze_pipeline_A2_id,
            runner_conf.silver_pipeline_id,
        ):
            if pipeline_id:
                attempt(lambda value=pipeline_id: self.ws.pipelines.delete(value))

        schema_names = (
            runner_conf.bronze_schema,
            runner_conf.silver_schema,
            runner_conf.sdp_meta_schema,
            runner_conf.gold_schema,
        )
        for schema_name in schema_names:
            if not schema_name:
                continue
            try:
                tables = list(
                    self.ws.tables.list(
                        catalog_name=runner_conf.uc_catalog_name,
                        schema_name=schema_name,
                    )
                )
            except NotFound:
                tables = []
            except Exception as err:
                errors.append(err)
                tables = []
            for table in tables:
                attempt(lambda value=table.full_name: self.ws.tables.delete(value))

            try:
                volumes = list(
                    self.ws.volumes.list(
                        catalog_name=runner_conf.uc_catalog_name,
                        schema_name=schema_name,
                    )
                )
            except NotFound:
                volumes = []
            except Exception as err:
                errors.append(err)
                volumes = []
            for volume in volumes:
                attempt(
                    lambda value=volume.full_name: self.ws.volumes.delete(value)
                )

            attempt(
                lambda value=(
                    f"{runner_conf.uc_catalog_name}.{schema_name}"
                ): self.ws.schemas.delete(value, force=True)
            )

        attempt(
            lambda: self.ws.workspace.delete(
                runner_conf.runners_nb_path, recursive=True
            )
        )
        if errors:
            details = "; ".join(str(error) for error in errors)
            raise RuntimeError(f"At-scale demo cleanup was incomplete: {details}")

    def run(self, runner_conf: AtScaleRunnerConf):
        try:
            try:
                self.init_sdp_meta_runner_conf(runner_conf)
                self.create_bronze_silver_dlt(runner_conf)
                self.launch_workflow(runner_conf)
            except Exception:
                traceback.print_exc()
                try:
                    if not runner_conf.keep_resources:
                        self.clean_up(runner_conf)
                    else:
                        print("Keeping remote resources for inspection.")
                except Exception as cleanup_error:
                    print(
                        "Cleanup also failed; preserving the original demo "
                        f"failure. Cleanup error: {cleanup_error}",
                        file=sys.stderr,
                    )
                    traceback.print_exception(cleanup_error)
                raise
            else:
                if not runner_conf.keep_resources:
                    self.clean_up(runner_conf)
                else:
                    print("Keeping remote resources for inspection.")
        finally:
            if runner_conf.local_onboarding_path:
                Path(runner_conf.local_onboarding_path).unlink(
                    missing_ok=True
                )


def parse_arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uc_catalog_name", required=True)
    parser.add_argument("--profile")
    parser.add_argument(
        "--onboarding_file_format",
        choices=("json", "yaml", "yml"),
        default="json",
    )
    parser.add_argument(
        "--table_count",
        type=int,
        default=DEFAULT_TABLE_COUNT,
        help="Number of generated source tables (minimum: 3).",
    )
    parser.add_argument(
        "--keep-resources",
        dest="keep_resources",
        action="store_true",
        help="Keep the job, pipelines, schemas, and volume after validation.",
    )
    args = vars(parser.parse_args(argv))
    if args["table_count"] < 3:
        parser.error("--table_count must be at least 3")
    try:
        validate_uc_identifier(
            args["uc_catalog_name"], kind="uc_catalog_name"
        )
    except ValueError as err:
        parser.error(str(err))
    return args


def main():
    args = parse_arguments()
    ws = get_workspace_api_client(args["profile"])
    demo = SDPMetaAtScaleAutoLoaderDemo(args, ws)
    demo.run(demo.init_runner_conf())


if __name__ == "__main__":
    main()
