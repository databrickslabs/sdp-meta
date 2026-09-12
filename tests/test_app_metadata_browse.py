"""Tests for ``/api/metadata/table-data`` request validation.

Background: the handler in ``databricks_app/routes/metadata_browse.py``
parses the request body up front (catalog / schema / table / limit) and
then opens a try/except around the SQL execution. The ``int(limit)``
conversion lives OUTSIDE that try/except, so a malformed ``limit``
(non-numeric string, ``null``, zero, negative) used to surface as a
generic 500 via the global ``handle_exception`` hook in ``app.py``
instead of a proper 400 with a client-readable message.

These tests pin the 400-on-bad-limit contract in place so future
refactors of ``table_data()`` don't accidentally reintroduce the 500.
"""

from __future__ import annotations

import json
import os
import sys
import unittest

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_APP_DIR = os.path.join(_REPO_ROOT, "databricks_app")
if _APP_DIR not in sys.path:
    sys.path.insert(0, _APP_DIR)

import app as app_mod  # noqa: E402  (deliberate post-sys.path-insert import)


class QualitySpecEditorValidationTests(unittest.TestCase):
    def setUp(self):
        app_mod.app.testing = True
        self.client = app_mod.app.test_client()

    def _parse(self, row):
        response = self.client.post(
            "/api/metadata/parse-spec",
            data=json.dumps({
                "content": json.dumps([row]),
                "format": "json",
                "spec_type": "onboarding",
                "env": "dev",
            }),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        return response.get_json()

    def test_lakeflow_fields_are_exposed_and_rules_path_is_warned(self):
        body = self._parse({
            "data_flow_id": "100",
            "bronze_quality_engine": "lakeflow",
            "bronze_quality_rules_path_dev": "/Volumes/c/s/v/rules.yml",
            "bronze_database_quarantine_dev": "bronze",
            "bronze_quarantine_table": "invalid_rows",
        })
        self.assertEqual(body["errors"], [])
        self.assertTrue(
            any("bronze_quality_rules_path_dev" in warning
                for warning in body["warnings"])
        )

    def test_lakeflow_rejects_legacy_conflict(self):
        body = self._parse({
            "data_flow_id": "100",
            "bronze_quality_engine": "lakeflow",
            "bronze_quality_rules_path_dev": "/Volumes/c/s/v/rules.yml",
            "bronze_data_quality_expectations_json_dev": "/Volumes/c/s/v/legacy.yml",
            "bronze_database_quarantine_dev": "bronze",
            "bronze_quarantine_table": "invalid_rows",
        })
        self.assertTrue(
            any("cannot combine" in error for error in body["errors"])
        )


class TableDataLimitValidationTests(unittest.TestCase):
    """``limit`` validation runs before any SDK / SQL work."""

    def setUp(self):
        app_mod.app.testing = True
        self.client = app_mod.app.test_client()

    def _post(self, body):
        return self.client.post(
            "/api/metadata/table-data",
            data=json.dumps(body),
            content_type="application/json",
        )

    def test_non_numeric_limit_returns_400(self):
        """A non-numeric ``limit`` must produce a 400, not a 500."""
        resp = self._post({
            "catalog": "c", "schema": "s", "table": "t", "limit": "abc",
        })
        self.assertEqual(resp.status_code, 400)
        payload = resp.get_json()
        self.assertIn("limit", payload["error"].lower())
        self.assertIn("integer", payload["error"].lower())

    def test_null_limit_returns_400(self):
        """``limit: null`` (TypeError on ``int(None)``) must produce a 400."""
        resp = self._post({
            "catalog": "c", "schema": "s", "table": "t", "limit": None,
        })
        self.assertEqual(resp.status_code, 400)
        self.assertIn("limit", resp.get_json()["error"].lower())

    def test_zero_limit_returns_400(self):
        """``LIMIT 0`` is a degenerate query; reject with 400."""
        resp = self._post({
            "catalog": "c", "schema": "s", "table": "t", "limit": 0,
        })
        self.assertEqual(resp.status_code, 400)
        self.assertIn("positive", resp.get_json()["error"].lower())

    def test_negative_limit_returns_400(self):
        """Negative ``limit`` would yield invalid SQL; reject with 400."""
        resp = self._post({
            "catalog": "c", "schema": "s", "table": "t", "limit": -5,
        })
        self.assertEqual(resp.status_code, 400)
        self.assertIn("positive", resp.get_json()["error"].lower())

    def test_valid_limit_passes_validation(self):
        """A well-formed numeric limit must NOT be rejected by limit
        validation. We deliberately omit a configured warehouse so the
        handler short-circuits at the warehouse check (also a 400, but
        with a different error string) — that's how we know the limit
        check passed without having to mock the SDK."""
        resp = self._post({
            "catalog": "c", "schema": "s", "table": "t", "limit": 50,
        })
        self.assertEqual(resp.status_code, 400)
        err = resp.get_json()["error"].lower()
        self.assertNotIn("limit must", err)
        self.assertIn("warehouse", err)

    def test_string_numeric_limit_is_accepted(self):
        """JSON clients sometimes send numbers as strings; ``int('50')``
        works fine, so this is NOT a limit-validation failure. Same
        warehouse-short-circuit assertion as the valid-limit case."""
        resp = self._post({
            "catalog": "c", "schema": "s", "table": "t", "limit": "50",
        })
        self.assertEqual(resp.status_code, 400)
        err = resp.get_json()["error"].lower()
        self.assertNotIn("limit must", err)
        self.assertIn("warehouse", err)


class TableDataWhereClauseRejectionTests(unittest.TestCase):
    """``where_clause`` denylist rejects SQL-injection vectors at the
    App boundary BEFORE any SDK / warehouse work.

    The Databricks Statement Execution API cannot bind a structural
    WHERE expression as a parameter, so validation is the only line of
    defence \u2014 see ``validate_sql_where_clause``. These tests pin the
    rejected token set in place so a future refactor of
    ``table_data()`` cannot accidentally remove the check or weaken it
    into a passthrough.
    """

    def setUp(self):
        app_mod.app.testing = True
        self.client = app_mod.app.test_client()

    def _post(self, where):
        return self.client.post(
            "/api/metadata/table-data",
            data=json.dumps({
                "catalog": "c", "schema": "s", "table": "t",
                "limit": 50, "where_clause": where,
            }),
            content_type="application/json",
        )

    def test_semicolon_is_rejected(self):
        """Statement separator: classic stacked-query injection."""
        resp = self._post("1=1; DROP TABLE x")
        self.assertEqual(resp.status_code, 400)
        self.assertIn(";", resp.get_json()["error"])

    def test_double_dash_comment_is_rejected(self):
        """Line comment would comment out the trailing LIMIT."""
        resp = self._post("1=1 --")
        self.assertEqual(resp.status_code, 400)
        self.assertIn("--", resp.get_json()["error"])

    def test_block_comment_open_is_rejected(self):
        resp = self._post("1=1 /* hi */")
        self.assertEqual(resp.status_code, 400)
        err = resp.get_json()["error"]
        self.assertTrue("/*" in err or "*/" in err)

    def test_backtick_is_rejected(self):
        """Identifier delimiter would let caller escape to a different
        table reference."""
        resp = self._post("1=1 AND `evil`.`tbl`.col=1")
        self.assertEqual(resp.status_code, 400)
        self.assertIn("`", resp.get_json()["error"])

    def test_union_select_is_rejected(self):
        """The poster-child data-exfiltration vector from the review."""
        resp = self._post(
            "1=1 UNION SELECT * FROM system.information_schema.columns"
        )
        self.assertEqual(resp.status_code, 400)
        self.assertIn("UNION", resp.get_json()["error"].upper())

    def test_drop_keyword_is_rejected(self):
        resp = self._post("1=1 OR DROP TABLE x")
        self.assertEqual(resp.status_code, 400)
        self.assertIn("DROP", resp.get_json()["error"].upper())

    def test_insert_keyword_is_rejected(self):
        resp = self._post("INSERT INTO x VALUES (1)")
        self.assertEqual(resp.status_code, 400)
        self.assertIn("INSERT", resp.get_json()["error"].upper())

    def test_keyword_match_is_word_boundary(self):
        """``unionized_state`` is a legitimate column name. Word-boundary
        regex must not flag it as the ``UNION`` keyword."""
        resp = self._post("unionized_state = 'CA'")
        # We expect to short-circuit at the warehouse check (no
        # warehouse configured in tests), NOT at the WHERE-clause
        # validator.
        self.assertEqual(resp.status_code, 400)
        err = resp.get_json()["error"].lower()
        self.assertNotIn("disallowed", err)
        self.assertIn("warehouse", err)

    def test_simple_comparison_passes_validation(self):
        """A legitimate WHERE clause is allowed through to the
        warehouse-not-configured short-circuit."""
        resp = self._post("col1 = 'CA' AND col2 > 10")
        self.assertEqual(resp.status_code, 400)
        err = resp.get_json()["error"].lower()
        self.assertNotIn("disallowed", err)
        self.assertIn("warehouse", err)

    def test_empty_where_clause_is_allowed(self):
        """An empty WHERE clause means "no filter" \u2014 still has to
        reach the warehouse-config short-circuit."""
        resp = self._post("")
        self.assertEqual(resp.status_code, 400)
        self.assertIn("warehouse", resp.get_json()["error"].lower())


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
