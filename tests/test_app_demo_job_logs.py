"""Tests for ``GET /api/job/<token>/logs`` request validation.

The polling endpoint in ``databricks_app/routes/demo.py`` parses an
``offset`` query parameter via ``int(...)``. A malformed value (non-
numeric string, negative integer) used to bubble out of ``int()`` as a
generic 500 via the global ``handle_exception`` hook in ``app.py`` — the
client got no actionable signal. These tests pin the 400-on-bad-offset
contract in place so future refactors of ``get_job_logs()`` don't
accidentally reintroduce the 500.

They mirror the validation contract enforced for ``limit`` in
``test_app_metadata_browse.py`` so the two endpoints stay consistent.
"""

from __future__ import annotations

import os
import sys
import unittest
from unittest import mock

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_APP_DIR = os.path.join(_REPO_ROOT, "databricks_app")
if _APP_DIR not in sys.path:
    sys.path.insert(0, _APP_DIR)

import app as app_mod  # noqa: E402  (deliberate post-sys.path-insert import)
import _jobs as _jobs_module  # noqa: E402
from routes import demo as demo_routes  # noqa: E402
from uc_preflight import PreflightResult  # noqa: E402
from demo.launch_acfs_demo import ApplyChangesFromSnapshotDemo  # noqa: E402
from demo.launch_af_cloudfiles_demo import SDPMETAFCFDemo  # noqa: E402
from demo.launch_dais_demo import SDPMETADAISDemo  # noqa: E402
from demo.launch_silver_fanout_demo import SDPMETASilverFanoutDemo  # noqa: E402
from integration_tests.run_integration_tests import (  # noqa: E402
    SDPMETARunner,
    get_workspace_api_client,
)


class JobLogsOffsetValidationTests(unittest.TestCase):
    """``offset`` validation runs before any log slicing."""

    def setUp(self):
        app_mod.app.testing = True
        self.client = app_mod.app.test_client()
        # Seed a real job so the 404-on-missing-token short-circuit
        # isn't what's masking the validation behavior.
        self.token = _jobs_module._new_job_token()
        job = _jobs_module._get_job(self.token)
        job['logs'] = [
            {'stream': 'stdout', 'line': 'line-0'},
            {'stream': 'stdout', 'line': 'line-1'},
            {'stream': 'stderr', 'line': 'line-2'},
        ]

    def tearDown(self):
        _jobs_module._jobs.pop(self.token, None)

    def _get(self, query):
        return self.client.get(f"/api/job/{self.token}/logs{query}")

    def test_non_numeric_offset_returns_400(self):
        """A non-numeric ``offset`` must produce a 400, not a 500."""
        resp = self._get("?offset=abc")
        self.assertEqual(resp.status_code, 400)
        payload = resp.get_json()
        self.assertIn("offset", payload["error"].lower())
        self.assertIn("integer", payload["error"].lower())

    def test_negative_offset_returns_400(self):
        """Negative ``offset`` would tail-slice the log buffer instead
        of returning the requested forward window."""
        resp = self._get("?offset=-1")
        self.assertEqual(resp.status_code, 400)
        self.assertIn("non-negative", resp.get_json()["error"].lower())

    def test_missing_offset_defaults_to_zero(self):
        """``offset`` is optional; absent means start of the buffer."""
        resp = self._get("")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(resp.get_json()["logs"]), 3)

    def test_valid_offset_slices_from_position(self):
        """A well-formed numeric offset returns the trailing slice."""
        resp = self._get("?offset=1")
        self.assertEqual(resp.status_code, 200)
        logs = resp.get_json()["logs"]
        self.assertEqual(len(logs), 2)
        self.assertEqual(logs[0]["line"], "line-1")

    def test_offset_beyond_end_returns_empty(self):
        """An offset past the end of the buffer returns an empty list,
        not an error. This is the steady-state case for polling once
        the client has caught up with the subprocess."""
        resp = self._get("?offset=99")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["logs"], [])

    def test_missing_token_returns_404_not_400(self):
        """``token`` lookup runs BEFORE offset validation — a missing
        token should surface as 404, not as an offset-validation 400."""
        resp = self.client.get("/api/job/does-not-exist/logs?offset=abc")
        self.assertEqual(resp.status_code, 404)
        self.assertIn("not found", resp.get_json()["error"].lower())


class DemoCapacityConfigurationTests(unittest.TestCase):
    def test_positive_environment_value_overrides_default(self):
        with mock.patch.dict(
            os.environ,
            {"SDP_META_MAX_ACTIVE_DEMOS": "12"},
        ):
            self.assertEqual(
                _jobs_module._positive_int_env(
                    "SDP_META_MAX_ACTIVE_DEMOS",
                    8,
                ),
                12,
            )

    def test_invalid_environment_values_use_default(self):
        for value in ("invalid", "0", "-1"):
            with self.subTest(value=value), mock.patch.dict(
                os.environ,
                {"SDP_META_MAX_ACTIVE_DEMOS": value},
            ):
                self.assertEqual(
                    _jobs_module._positive_int_env(
                        "SDP_META_MAX_ACTIVE_DEMOS",
                        8,
                    ),
                    8,
                )


class DemoLauncherAuthenticationTests(unittest.TestCase):
    """Demo subprocesses must never fall through to interactive auth."""

    def setUp(self):
        app_mod.app.testing = True
        self.client = app_mod.app.test_client()

    def test_interactive_demo_keeps_real_completion_timeout(self):
        interactive = demo_routes._DEMO_REGISTRY["demo_interactive"]
        self.assertNotIn("--timeout-minutes", interactive["extra_args"])

    def test_featured_demo_preserves_remote_resources(self):
        featured = demo_routes._DEMO_REGISTRY["demo_at_scale_autoloader"]
        self.assertIn("--keep-resources", featured["extra_args"])

    @mock.patch("integration_tests.run_integration_tests.WorkspaceClient")
    def test_default_profile_uses_sdk_credential_chain(self, mock_client):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("DATABRICKS_APP_PORT", None)
            get_workspace_api_client(None)
        mock_client.assert_called_once_with()

    @mock.patch("integration_tests.run_integration_tests.WorkspaceClient")
    def test_pat_environment_uses_sdk_credential_chain(self, mock_client):
        with mock.patch.dict(
            os.environ,
            {
                "DATABRICKS_HOST": "https://example.cloud.databricks.com",
                "DATABRICKS_TOKEN": "test-token",
            },
            clear=False,
        ):
            os.environ.pop("DATABRICKS_APP_PORT", None)
            get_workspace_api_client(None)
        mock_client.assert_called_once_with()

    @mock.patch("integration_tests.run_integration_tests.WorkspaceClient")
    def test_named_profile_is_forwarded_to_sdk(self, mock_client):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("DATABRICKS_APP_PORT", None)
            get_workspace_api_client("fevm")
        mock_client.assert_called_once_with(profile="fevm")

    @mock.patch("integration_tests.run_integration_tests.WorkspaceClient")
    def test_app_auth_ignores_local_profile(self, mock_client):
        with mock.patch.dict(
            os.environ,
            {"DATABRICKS_APP_PORT": "8000"},
            clear=False,
        ):
            get_workspace_api_client("fevm")
        mock_client.assert_called_once_with()

    @mock.patch("integration_tests.run_integration_tests.webbrowser.open")
    def test_open_job_url_waits_when_requested(self, _mock_browser):
        runner = SDPMETARunner.__new__(SDPMETARunner)
        runner.ws = mock.Mock()
        runner.ws.config.host = "https://example.cloud.databricks.com"
        runner.ws.get_workspace_id.return_value = "123"
        waiter = runner.ws.jobs.run_now.return_value
        runner_conf = mock.Mock()
        created_job = mock.Mock(job_id=456)

        runner.open_job_url(
            runner_conf,
            created_job,
            wait_for_completion=True,
        )

        waiter.result.assert_called_once()

    def test_registered_legacy_launchers_wait_for_remote_completion(self):
        cases = (
            (SDPMETAFCFDemo, "create_workflow_spec"),
            (ApplyChangesFromSnapshotDemo, "create_workflow_spec"),
            (SDPMETASilverFanoutDemo, "create_sfo_workflow_spec"),
            (SDPMETADAISDemo, "create_daisdemo_workflow"),
        )
        for demo_class, create_method in cases:
            with self.subTest(demo_class=demo_class.__name__):
                demo = demo_class.__new__(demo_class)
                created_job = mock.Mock()
                setattr(demo, create_method, mock.Mock(return_value=created_job))
                demo.open_job_url = mock.Mock()
                runner_conf = mock.Mock()

                demo.launch_workflow(runner_conf)

                demo.open_job_url.assert_called_once_with(
                    runner_conf,
                    created_job,
                    wait_for_completion=True,
                )

    @mock.patch("routes.demo.check_app_sp_grants_on_catalog")
    def test_demo_capacity_returns_429(self, mock_preflight):
        mock_preflight.return_value = PreflightResult(
            ok=True,
            uc_name="main",
            sp_principal="test-user",
            sp_display_name="Test User",
        )
        token = _jobs_module._new_job_token(kind="demo")
        try:
            with mock.patch.object(
                _jobs_module,
                "_MAX_ACTIVE_DEMO_JOBS",
                1,
            ):
                resp = self.client.post(
                    "/rundemo",
                    json={
                        "demo_name": "demo_at_scale_autoloader",
                        "uc_name": "main",
                    },
                )
            self.assertEqual(resp.status_code, 429)
            self.assertEqual(resp.headers.get("Retry-After"), "30")
            error = resp.get_json()["error"]
            self.assertIn("active demo", error.lower())
            self.assertNotIn("1/1", error)
        finally:
            _jobs_module._jobs.pop(token, None)

    @mock.patch("routes.demo._run_command_in_background")
    @mock.patch(
        "routes.demo._jobs_module._new_job_token",
        return_value="demo-token",
    )
    @mock.patch("routes.demo.check_app_sp_grants_on_catalog")
    def test_every_registered_demo_returns_background_token(
        self,
        mock_preflight,
        _mock_new_token,
        mock_run,
    ):
        mock_preflight.return_value = PreflightResult(
            ok=True,
            uc_name="main",
            sp_principal="test-user",
            sp_display_name="Test User",
        )

        for demo_name in demo_routes._DEMO_REGISTRY:
            with self.subTest(demo_name=demo_name):
                resp = self.client.post(
                    "/rundemo",
                    json={"demo_name": demo_name, "uc_name": "main"},
                )
                self.assertEqual(resp.status_code, 202)
                self.assertEqual(resp.get_json()["token"], "demo-token")

        self.assertEqual(mock_run.call_count, len(demo_routes._DEMO_REGISTRY))
        self.assertTrue(
            all(
                call.kwargs["idle_timeout_seconds"] == 2 * 60 * 60
                for call in mock_run.call_args_list
            )
        )

    @mock.patch("routes.demo._run_command_in_background")
    @mock.patch(
        "routes.demo._jobs_module._new_job_token",
        return_value="demo-token",
    )
    @mock.patch("routes.demo.check_app_sp_grants_on_catalog")
    def test_local_app_forwards_profile_without_enabling_app_path_shim(
        self,
        mock_preflight,
        _mock_new_token,
        mock_run,
    ):
        mock_preflight.return_value = PreflightResult(
            ok=True,
            uc_name="main",
            sp_principal="test-user",
            sp_display_name="Test User",
        )
        with mock.patch.dict(
            os.environ,
            {"DATABRICKS_CONFIG_PROFILE": "fevm"},
            clear=False,
        ):
            os.environ.pop("DATABRICKS_APP_PORT", None)
            resp = self.client.post(
                "/rundemo",
                json={
                    "demo_name": "demo_at_scale_autoloader",
                    "uc_name": "main",
                },
            )

        self.assertEqual(resp.status_code, 202, resp.get_data(as_text=True))
        self.assertEqual(resp.get_json()["token"], "demo-token")
        child_command = mock_run.call_args.kwargs["command"]
        child_env = mock_run.call_args.kwargs["env"]
        self.assertEqual(child_command[-2:], ["--profile", "fevm"])
        self.assertNotIn("DATABRICKS_APP_PORT", child_env)


class JobRetentionTests(unittest.TestCase):
    """Completed jobs and verbose logs remain bounded."""

    def test_log_cap_preserves_absolute_polling_offset(self):
        token = _jobs_module._new_job_token()
        job = _jobs_module._jobs[token]
        try:
            with mock.patch.object(_jobs_module, "_MAX_LOG_LINES", 2):
                for line in ("one", "two", "three"):
                    _jobs_module._append_job_log(
                        job,
                        {"stream": "stdout", "line": line},
                    )

            client = app_mod.app.test_client()
            payload = client.get(
                f"/api/job/{token}/logs?offset=0"
            ).get_json()
            self.assertEqual(
                [entry["line"] for entry in payload["logs"]],
                ["two", "three"],
            )
            self.assertEqual(payload["next_offset"], 3)
        finally:
            _jobs_module._jobs.pop(token, None)

    def test_expired_completed_job_is_evicted(self):
        token = _jobs_module._new_job_token()
        job = _jobs_module._jobs[token]
        job["done"] = True
        job["finished_at"] = 10

        _jobs_module._prune_jobs(
            now=10 + _jobs_module._COMPLETED_JOB_TTL_SECONDS + 1
        )

        self.assertNotIn(token, _jobs_module._jobs)


class LegacyDemoFailurePropagationTests(unittest.TestCase):
    """Launcher failures must produce a nonzero subprocess exit."""

    def test_registered_legacy_launchers_reraise_failures(self):
        for demo_class in (
            SDPMETAFCFDemo,
            ApplyChangesFromSnapshotDemo,
            SDPMETASilverFanoutDemo,
            SDPMETADAISDemo,
        ):
            with self.subTest(demo_class=demo_class.__name__):
                demo = demo_class.__new__(demo_class)
                demo.init_sdp_meta_runner_conf = mock.Mock(
                    side_effect=RuntimeError("simulated failure")
                )
                with self.assertRaisesRegex(
                    RuntimeError,
                    "simulated failure",
                ):
                    demo.run(mock.Mock())


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
