
import uuid
import traceback
from databricks.labs.sdp_meta.install import WorkspaceInstaller
from integration_tests.run_integration_tests import (
    SDPMETARunner,
    SDPMetaRunnerConf,
    get_workspace_api_client,
    process_arguments
)


class ApplyChangesFromSnapshotDemo(SDPMETARunner):
    def __init__(self, args, ws, base_dir):
        self.args = args
        self.ws = ws
        self.wsi = WorkspaceInstaller(ws)
        self.base_dir = base_dir

    def run(self, runner_conf: SDPMetaRunnerConf):
        """
        Runs the SDP-META Apply Changes from Snapshot Demo by calling the necessary methods in the correct order.

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
        run_id = uuid.uuid4().hex
        runner_conf = SDPMetaRunnerConf(
            run_id=run_id,
            username=self.wsi._my_username,
            int_tests_dir="demo",
            sdp_meta_schema=f"sdp_meta_dataflowspecs_demo_{run_id}",
            bronze_schema=f"sdp_meta_bronze_demo_{run_id}",
            silver_schema=f"sdp_meta_silver_demo_{run_id}",
            runners_nb_path=f"/Users/{self.wsi._my_username}/sdp_meta_demo/{run_id}",
            runners_full_local_path="demo/notebooks/snapshot_runners",
            test_output_file_path=(
                f"/Users/{self.wsi._my_username}/sdp_meta_demo/"
                f"{run_id}/sdp_meta_acfs_demo-output.csv"
            ),
            source="snapshot",
            snapshot_template="demo/conf/json/snapshot-onboarding.template",
            onboarding_file_path="demo/conf/json/onboarding.json",
            onboarding_file_format=self.args.get("onboarding_file_format") or "json",
            env="demo"
        )
        runner_conf.uc_catalog_name = self.args['uc_catalog_name']
        return runner_conf

    def launch_workflow(self, runner_conf: SDPMetaRunnerConf):
        created_job = self.create_workflow_spec(runner_conf)
        self.open_job_url(
            runner_conf,
            created_job,
            wait_for_completion=True,
        )


def main():
    args = process_arguments()
    workspace_client = get_workspace_api_client(args["profile"])
    sdp_meta_afam_demo_runner = ApplyChangesFromSnapshotDemo(args, workspace_client, "demo")
    print("initializing complete")
    runner_conf = sdp_meta_afam_demo_runner.init_runner_conf()
    sdp_meta_afam_demo_runner.run(runner_conf)


if __name__ == "__main__":
    main()
