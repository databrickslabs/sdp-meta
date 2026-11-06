"""Tests for the App's landing-page rendering.

Currently scoped to the version-string injection contract: the landing
page footer must render the *installed* ``databricks.labs.sdp_meta``
package version, not a hardcoded literal. This pins the regression
where the footer string drifted from ``__about__.__version__`` and
shipped as a stale ``v0.1.0`` long after the package version moved on.

The mechanism under test:

  * ``app.py`` imports ``__version__`` from
    ``databricks.labs.sdp_meta.__about__`` at startup and registers a
    Jinja ``context_processor`` exposing it as ``{{ app_version }}``.
  * ``templates/landingPage.html`` renders ``SDP-META v{{ app_version }}``
    in the sidebar footer.

If either end of that contract regresses (the context processor goes
away, the template re-hardcodes the version, or the import path
changes) these tests fail loudly.
"""

from __future__ import annotations

import os
import sys
import unittest
from types import SimpleNamespace
from unittest import mock

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_APP_DIR = os.path.join(_REPO_ROOT, "databricks_app")
if _APP_DIR not in sys.path:
    sys.path.insert(0, _APP_DIR)

import app as app_mod  # noqa: E402


def _raise_test_exception():
    raise RuntimeError("raw SDK failure: principal lacks USE CATALOG")


if "_test_unhandled_error" not in app_mod.app.view_functions:
    app_mod.app.add_url_rule(
        "/__test_unhandled_error",
        endpoint="_test_unhandled_error",
        view_func=_raise_test_exception,
    )


class LandingPageVersionInjectionTests(unittest.TestCase):
    """``GET /`` must render the installed package's version in the
    sidebar footer."""

    def setUp(self):
        self.client = app_mod.app.test_client()

    def test_footer_renders_installed_package_version(self):
        from databricks.labs.sdp_meta.__about__ import __version__

        resp = self.client.get("/")
        self.assertEqual(resp.status_code, 200)
        html = resp.get_data(as_text=True)
        # The footer string is human-readable, so this assertion
        # doubles as a smoke check that the page actually rendered
        # (and didn't fall back to a Flask 500 HTML error page).
        self.assertIn(f"SDP-META v{__version__}", html)

    def test_footer_does_not_carry_stale_hardcoded_version(self):
        """Regression guard: the previous footer hardcoded
        ``SDP-META v0.1.0``. If the package's real version ever moves
        and someone re-hardcodes the literal, we want to know."""
        from databricks.labs.sdp_meta.__about__ import __version__

        resp = self.client.get("/")
        html = resp.get_data(as_text=True)
        # Only flag the legacy literal when it WOULDN'T match the
        # installed version. If the package happens to still be at
        # 0.1.0 today, both literals coincide and this assertion is
        # trivially true; once the package version moves on, this
        # test fails fast on any future re-hardcoding.
        if __version__ != "0.1.0":
            self.assertNotIn("SDP-META v0.1.0", html)

    def test_context_processor_exposes_app_version(self):
        """The context processor is the single source of truth that
        the template depends on. Reach into it directly so a future
        refactor that breaks the wiring (e.g. removing the processor
        but leaving the template variable) is caught even if the
        landing page itself happens to keep rendering."""
        from databricks.labs.sdp_meta.__about__ import __version__

        with app_mod.app.app_context():
            # Flask exposes context-processor output via
            # ``app.update_template_context`` on an empty dict.
            ctx: dict = {}
            app_mod.app.update_template_context(ctx)
            self.assertIn("app_version", ctx)
            self.assertEqual(ctx["app_version"], __version__)


class LandingPageFeaturedDemoTests(unittest.TestCase):
    """The Demos panel exposes one compact launcher dropdown."""

    def setUp(self):
        self.client = app_mod.app.test_client()

    def test_featured_demo_dropdown_lists_every_runnable_demo(self):
        html = self.client.get("/").get_data(as_text=True)

        self.assertIn('id="featured_demo_select"', html)
        for demo_name in (
            "demo_at_scale_autoloader",
            "demo_interactive",
            "demo_cloudfiles",
            "demo_acf",
            "demo_silverfanout",
            "demo_dias",
        ):
            self.assertIn(f'value="{demo_name}"', html)

    def test_at_scale_autoloader_is_the_default_featured_demo(self):
        html = self.client.get("/").get_data(as_text=True)

        self.assertIn(
            '<option value="demo_at_scale_autoloader" selected>'
            'At-scale Auto Loader (100 tables)</option>',
            html,
        )
        self.assertIn('id="runFeaturedDemo"', html)

    def test_onboarding_picker_routes_launcher_entries_by_metadata(self):
        html = self.client.get("/").get_data(as_text=True)

        self.assertIn("__featured_at_scale_autoloader__", html)
        self.assertIn("Featured \\u2014 At-scale Auto Loader (100 tables)", html)
        self.assertIn("launcher: 'demo_at_scale_autoloader'", html)
        self.assertIn("bundledSelect.selectedIndex = 0;", html)
        self.assertNotIn(
            "var preferenceOrder = ['onboarding', 'onboarding_cars'];",
            html,
        )
        self.assertIn('id="onboardingSubmitBtn"', html)
        self.assertIn("var bundledLauncher = form.dataset.bundledLauncher;", html)
        self.assertIn("runDemo(bundledLauncher);", html)
        self.assertIn("entry.launcher || ''", html)
        self.assertNotIn("bundledPicker.value === '__featured_at_scale_autoloader__'", html)
        self.assertIn("'&#9654; Run Featured Demo'", html)
        self.assertIn("'&#9654; Launch Demo Workflow'", html)

    def test_progress_modal_can_close_while_demo_runs(self):
        html = self.client.get("/").get_data(as_text=True)

        self.assertIn(
            'id="progressClose" onclick="closeProgressModal()">Close</button>',
            html,
        )
        self.assertNotIn('id="progressClose" disabled', html)
        self.assertIn(
            "document.getElementById('progressClose').disabled = false;",
            html,
        )
        close_body = html.split(
            "function closeProgressModal() {",
            1,
        )[1].split(
            "function _startDots()",
            1,
        )[0]
        self.assertNotIn("_cancelProgressSession()", close_body)
        self.assertNotIn("clearTimeout", close_body)

    def test_progress_polling_owns_session_and_retries_transient_errors(self):
        html = self.client.get("/").get_data(as_text=True)

        self.assertIn("new AbortController()", html)
        self.assertIn("_progressSession === session", html)
        self.assertNotIn("_cancelProgressSession()", html)
        self.assertIn("var maxRetries = 4;", html)
        self.assertIn("err.status >= 500", html)
        self.assertIn("Math.pow(2, session.retryCount - 1) * 1000", html)
        self.assertIn("err.name === 'AbortError'", html)
        fail_body = html.split(
            "function failPolling(err) {",
            1,
        )[1].split(
            "function poll()",
            1,
        )[0]
        self.assertIn("result: {", fail_body)
        self.assertIn("details: {", fail_body)
        self.assertIn("status: err.status || null", fail_body)
        self.assertIn("retries: session.retryCount", fail_body)

    def test_new_operation_tracks_multiple_active_progress_sessions(self):
        html = self.client.get("/").get_data(as_text=True)

        claim_body = html.split(
            "function _claimProgressOperation() {",
            1,
        )[1].split(
            "function _releaseProgressClaim()",
            1,
        )[0]
        self.assertIn("_progressPending = true;", claim_body)
        self.assertIn("return true;", claim_body)
        self.assertNotIn("return false;", claim_body)
        self.assertIn("var _progressSessions = {};", html)
        self.assertIn("function _renderActiveRuns()", html)
        self.assertIn('id="activeRuns"', html)
        self.assertGreaterEqual(
            html.count("if (!_claimProgressOperation()) return;"),
            3,
        )

    def test_completed_background_runs_remain_available_in_progress_panel(self):
        html = self.client.get("/").get_data(as_text=True)

        finish_body = html.split(
            "function finishOnce(data) {",
            1,
        )[1].split(
            "function schedulePoll(delay)",
            1,
        )[0]
        self.assertNotIn("delete _progressSessions[token];", finish_body)
        self.assertIn("session.result = data.result || null;", finish_body)
        self.assertIn("session.status = session.success", finish_body)
        self.assertIn("function _showProgressSession(session)", html)
        self.assertIn("_showProgressSession(session);", html)


class FriendlyErrorTests(unittest.TestCase):
    def setUp(self):
        self.client = app_mod.app.test_client()

    def test_unhandled_error_has_safe_headline_and_expandable_details(self):
        response = self.client.get("/__test_unhandled_error")

        self.assertEqual(response.status_code, 500)
        body = response.get_json()
        self.assertNotIn("raw SDK failure", response.get_data(as_text=True))
        self.assertNotIn("principal lacks USE CATALOG", response.get_data(as_text=True))
        self.assertTrue(body["details"]["request_id"])
        self.assertEqual(body["details"]["status"], 500)
        self.assertEqual(set(body["details"]), {"request_id", "status"})

    @mock.patch.object(app_mod, "logger")
    def test_caught_server_error_is_normalized_and_logged(self, mock_logger):
        with app_mod.app.test_request_context("/api/test"):
            response = app_mod.app.make_response(
                (
                    app_mod.jsonify(
                        {
                            "error": "raw SDK permission failure",
                            "details": {"message": "raw SDK permission failure"},
                            "stderr": "raw SDK permission failure",
                        }
                    ),
                    500,
                )
            )
            response = app_mod.add_security_headers(response)

        body = response.get_json()
        self.assertNotIn("raw SDK permission failure", response.get_data(as_text=True))
        self.assertEqual(body["details"]["status"], 500)
        self.assertTrue(body["details"]["request_id"])
        self.assertNotIn("stderr", body)
        mock_logger.error.assert_any_call(
            mock.ANY,
            body["details"]["request_id"],
            "raw SDK permission failure",
        )
        mock_logger.error.assert_any_call(
            mock.ANY,
            body["details"]["request_id"],
            None,
            "raw SDK permission failure",
        )

    @mock.patch("routes.warehouse._get_warehouse_id", return_value="warehouse-id")
    @mock.patch(
        "databricks.sdk.WorkspaceClient",
        side_effect=RuntimeError("sensitive warehouse SDK detail"),
    )
    def test_warehouse_status_does_not_leak_caught_exception(
        self,
        _mock_workspace_client,
        _mock_warehouse_id,
    ):
        response = self.client.get("/api/warehouse/status")

        self.assertEqual(response.status_code, 200)
        self.assertNotIn("sensitive warehouse SDK detail", response.get_data(as_text=True))
        self.assertNotIn("details", response.get_json())

    @mock.patch("routes.dataflowspecs._get_warehouse_id", return_value="warehouse-id")
    @mock.patch(
        "databricks.sdk.WorkspaceClient",
        side_effect=RuntimeError("sensitive DataflowSpec SDK detail"),
    )
    def test_dataflowspec_query_does_not_leak_caught_exception(
        self,
        _mock_workspace_client,
        _mock_warehouse_id,
    ):
        response = self.client.get("/api/dataflowspecs?catalog=main&schema=sdp_meta")

        self.assertEqual(response.status_code, 200)
        self.assertNotIn("sensitive DataflowSpec SDK detail", response.get_data(as_text=True))
        self.assertNotIn("details", response.get_json()["bronze"])
        self.assertNotIn("details", response.get_json()["silver"])

    @mock.patch("routes.dataflowspecs._get_warehouse_id", return_value="warehouse-id")
    @mock.patch("databricks.sdk.WorkspaceClient")
    def test_dataflowspec_query_does_not_leak_failed_statement_text(
        self,
        mock_workspace_client,
        _mock_warehouse_id,
    ):
        failed_result = SimpleNamespace(
            status=SimpleNamespace(
                state="FAILED",
                error=SimpleNamespace(message="sensitive warehouse SQL error"),
            )
        )
        workspace_client = mock_workspace_client.return_value
        workspace_client.statement_execution.execute_statement.return_value = failed_result

        response = self.client.get("/api/dataflowspecs?catalog=main&schema=sdp_meta")

        self.assertEqual(response.status_code, 200)
        self.assertNotIn("sensitive warehouse SQL error", response.get_data(as_text=True))
        self.assertIn("could not read this DataflowSpec table", response.get_json()["bronze"]["error"])

    @mock.patch(
        "databricks.sdk.WorkspaceClient",
        side_effect=RuntimeError("sensitive SDK credential detail"),
    )
    def test_warehouse_configuration_does_not_expose_sdk_exception(
        self,
        _mock_client,
    ):
        response = self.client.post(
            "/api/warehouse/configure",
            json={"mode": "existing", "warehouse_id": "warehouse-id"},
        )

        self.assertEqual(response.status_code, 400)
        response_text = response.get_data(as_text=True)
        self.assertNotIn("sensitive SDK credential detail", response_text)
        self.assertIn(
            "could not use that SQL warehouse",
            response.get_json()["error"],
        )

    def test_landing_page_renders_collapsed_technical_details(self):
        html = self.client.get("/").get_data(as_text=True)

        self.assertIn("function showErrorResponse(data, title)", html)
        self.assertIn("<summary", html)
        self.assertIn("Technical details", html)
        self.assertIn("escapeHtml(text)", html)
        self.assertIn("safeDetails.request_id", html)
        self.assertIn("safeDetails.status", html)
        self.assertIn(
            "error: 'The operation failed before it could complete.'",
            html,
        )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
