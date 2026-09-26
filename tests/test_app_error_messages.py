"""Errors keep the next step readable while retaining SDK diagnostics."""

from __future__ import annotations

import os
import sys
import unittest
from unittest.mock import patch

_APP_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'databricks_app')
if _APP_DIR not in sys.path:
    sys.path.insert(0, _APP_DIR)

import app as app_mod  # noqa: E402


class AppErrorMessageTests(unittest.TestCase):
    def setUp(self):
        self.client = app_mod.app.test_client()

    def test_catalog_permission_failure_has_action_and_expandable_diagnostic(self):
        with patch('databricks.sdk.WorkspaceClient', side_effect=PermissionError('Permission denied: CREATE SCHEMA on catalog sales')):
            response = self.client.get('/api/metadata/catalogs')

        self.assertEqual(response.status_code, 500)
        payload = response.get_json()
        self.assertIn('App service principal', payload['error'])
        self.assertNotIn('CREATE SCHEMA on catalog sales', payload['error'])
        self.assertIn('CREATE SCHEMA on catalog sales', payload['details'])

    def test_global_exception_handler_preserves_diagnostics(self):
        with app_mod.app.test_request_context('/api/unknown-operation'):
            response, status = app_mod.handle_exception(RuntimeError('internal trace context'))
        self.assertEqual(status, 500)
        self.assertIn('try again', response.get_json()['error'])
        self.assertIn('RuntimeError: internal trace context', response.get_json()['details'])

    def test_response_modal_has_collapsed_technical_details(self):
        html = self.client.get('/').get_data(as_text=True)
        self.assertIn('<summary>Details</summary>', html)
        self.assertIn("escapeHtml(details)", html)
        self.assertIn("commandFailureMessage(data)", html)


if __name__ == '__main__':  # pragma: no cover
    unittest.main()
