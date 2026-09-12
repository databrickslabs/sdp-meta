"""Flask-client tests for the App's onboarding-template surface:

* ``/onboarding/preview`` — server-side dry-run rendering.
* The pre-flight parse path in ``/onboarding`` — bad templates return 400
  immediately instead of vanishing into the background subprocess.

The Flask ``app`` object lives in ``databricks_app/app.py``. That module
imports ``uc_preflight`` as a sibling, so the test bootstrap puts
``databricks_app/`` on ``sys.path`` before importing.
"""

from __future__ import annotations

import io
import json
import os
import shutil
import sys
import tempfile
import time
import unittest
from unittest import mock

# Bootstrap: make ``databricks_app/`` importable so ``import app`` resolves
# the Flask module and its ``uc_preflight`` sibling. This mirrors how
# Gunicorn launches the App in the container (``gunicorn app:app`` from
# inside ``databricks_app/``).
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_APP_DIR = os.path.join(_REPO_ROOT, "databricks_app")
if _APP_DIR not in sys.path:
    sys.path.insert(0, _APP_DIR)

import app as app_mod  # noqa: E402  (deliberate post-sys.path-insert import)


class OnboardingPreviewRouteTests(unittest.TestCase):
    """End-to-end exercises of ``POST /onboarding/preview`` via the Flask
    test client. Each test writes a real template to a tempdir and feeds
    the path through the form, so the resolve + preflight + render chain
    runs in production wiring (no monkeypatching)."""

    def setUp(self):
        self.client = app_mod.app.test_client()
        # Anchor the tempdir INSIDE the repo so the path resolver's
        # boundary check (S-2 hardening) accepts the absolute paths
        # the tests pass to ``onboarding_file_path``. A system
        # tempdir (``/var/folders/...``) would now be rejected as
        # "escapes the repo root".
        self.tmpdir = tempfile.mkdtemp(dir=_REPO_ROOT)
        self.addCleanup(shutil.rmtree, self.tmpdir, True)

    def tearDown(self):
        # Leave the tempfile cleanup to the OS — tests are fast and the
        # tempdir layout (one per test) means there's no overlap risk.
        pass

    # ── helpers ─────────────────────────────────────────────────────────────

    def _write(self, name: str, content: str) -> str:
        path = os.path.join(self.tmpdir, name)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(content)
        return path

    # Field names match the HTML form on landingPage.html 1:1 (both the
    # form fields and the CLI envelope use the ``sdp_meta_*`` namespace).
    _BASE_FORM = {
        "unity_catalog_enabled": "1",
        "unity_catalog_name": "my_cat",
        "sdp_meta_schema": "sch",
        "bronze_schema": "br",
        "silver_schema": "sv",
    }

    def _form(self, path: str, **overrides):
        data = dict(self._BASE_FORM, onboarding_file_path=path, **overrides)
        return data

    # ── happy path ──────────────────────────────────────────────────────────

    def test_yaml_preview_returns_rendered_text_with_substitutions(self):
        path = self._write(
            "onboarding.template.yml",
            "- data_flow_id: '100'\n"
            "  bronze_catalog: '{uc_catalog_name}'\n"
            "  bronze_database: '{bronze_schema}'\n"
            "  silver_database: '{silver_schema}'\n"
            "  bronze_path: '{uc_volume_path}/data/bronze/customers'\n",
        )
        resp = self.client.post("/onboarding/preview", data=self._form(path))
        self.assertEqual(resp.status_code, 200, resp.get_data(as_text=True))
        body = resp.get_json()
        self.assertEqual(body["source_extension"], ".yml")
        self.assertEqual(
            body["uc_volume_path_used"],
            "/Volumes/my_cat/sch/sch/sdp_meta_conf/",
        )
        # ``rendered`` must round-trip through YAML cleanly and carry the
        # substituted values — no placeholders, no literal ``None``s.
        import yaml as _yaml
        rendered_doc = _yaml.safe_load(body["rendered"])
        self.assertEqual(rendered_doc[0]["bronze_catalog"], "my_cat")
        self.assertEqual(rendered_doc[0]["bronze_database"], "br")
        self.assertEqual(rendered_doc[0]["silver_database"], "sv")
        self.assertIn("/Volumes/my_cat/sch/sch/sdp_meta_conf/",
                      rendered_doc[0]["bronze_path"])
        for token in ("{uc_catalog_name}", "{bronze_schema}",
                      "{silver_schema}", "{uc_volume_path}"):
            self.assertNotIn(token, body["rendered"])

    def test_json_preview_returns_rendered_text(self):
        path = self._write(
            "onboarding.json",
            json.dumps([{
                "data_flow_id": "100",
                "bronze_catalog": "{uc_catalog_name}",
                "bronze_database": "{bronze_schema}",
            }]),
        )
        resp = self.client.post("/onboarding/preview", data=self._form(path))
        self.assertEqual(resp.status_code, 200, resp.get_data(as_text=True))
        body = resp.get_json()
        self.assertEqual(body["source_extension"], ".json")
        rendered_doc = json.loads(body["rendered"])
        self.assertEqual(rendered_doc[0]["bronze_catalog"], "my_cat")
        self.assertEqual(rendered_doc[0]["bronze_database"], "br")

    def test_preview_does_not_call_workspace_client(self):
        """Preview must be side-effect-free — in particular it must not
        invoke ``WorkspaceClient`` (no UC volume creation, no SDK auth).
        We assert by monkey-patching the SDK import target to raise; if
        the preview path ever touches the SDK, this test catches it
        immediately. Uses a local-disk template path so ``_resolve_local_*``
        never enters the remote-download branch."""
        path = self._write(
            "onboarding.yml",
            "- bronze_catalog: '{uc_catalog_name}'\n",
        )

        from databricks.sdk import WorkspaceClient as _RealWC
        boobytrap_hits = []

        class _Boobytrap:
            def __init__(self, *a, **k):
                boobytrap_hits.append((a, k))
                raise AssertionError(
                    "preview must not instantiate WorkspaceClient")

        import databricks.sdk as _sdk_mod
        _sdk_mod.WorkspaceClient = _Boobytrap
        try:
            resp = self.client.post("/onboarding/preview", data=self._form(path))
        finally:
            _sdk_mod.WorkspaceClient = _RealWC

        self.assertEqual(resp.status_code, 200, resp.get_data(as_text=True))
        self.assertEqual(boobytrap_hits, [])

    # ── validation ─────────────────────────────────────────────────────────

    def test_missing_required_fields_returns_400(self):
        path = self._write("onboarding.yml", "- a: 1\n")
        resp = self.client.post("/onboarding/preview", data={
            "onboarding_file_path": path,
            # Omitting unity_catalog_name, schemas → should 400.
            "unity_catalog_enabled": "1",
        })
        self.assertEqual(resp.status_code, 400)
        body = resp.get_json()
        self.assertIn("Required fields missing", body["error"])
        for field in ("Unity Catalog Name", "SDP Meta Schema",
                      "Bronze Schema", "Silver Schema"):
            self.assertIn(field, body["error"])

    def test_missing_file_path_returns_400(self):
        resp = self.client.post("/onboarding/preview",
                                data=dict(self._BASE_FORM, onboarding_file_path=""))
        self.assertEqual(resp.status_code, 400)
        self.assertIn("Onboarding File Path", resp.get_json()["error"])

    def test_malformed_schema_identifiers_rejected(self):
        """Schema identifiers are validated too, not just the catalog: a
        hyphen in sdp_meta_schema / bronze_schema / silver_schema (all
        spliced into the volume path + substitutions) must return 400."""
        path = self._write("onboarding.yml", "- data_flow_id: '100'\n")
        for field in ("unity_catalog_name", "sdp_meta_schema",
                      "bronze_schema", "silver_schema"):
            with self.subTest(field=field):
                data = self._form(path, **{field: "bad-name"})
                resp = self.client.post("/onboarding/preview", data=data)
                self.assertEqual(
                    resp.status_code, 400, resp.get_data(as_text=True)
                )
                self.assertIn("identifier", resp.get_json()["error"])

    def test_file_not_found_returns_400(self):
        resp = self.client.post(
            "/onboarding/preview",
            data=self._form(os.path.join(self.tmpdir, "does_not_exist.yml")),
        )
        self.assertEqual(resp.status_code, 400)
        self.assertIn("not found", resp.get_json()["error"])

    def test_malformed_yaml_returns_400(self):
        path = self._write(
            "broken.yml",
            "- key: '{uc_catalog_name}\n  unbalanced: \"oops\n",
        )
        resp = self.client.post("/onboarding/preview", data=self._form(path))
        self.assertEqual(resp.status_code, 400)
        self.assertIn("parse", resp.get_json()["error"].lower())

    def test_malformed_json_returns_400(self):
        path = self._write("broken.json", "{not valid json")
        resp = self.client.post("/onboarding/preview", data=self._form(path))
        self.assertEqual(resp.status_code, 400)
        self.assertIn("parse", resp.get_json()["error"].lower())

    def test_empty_yaml_returns_400(self):
        path = self._write("empty.yml", "")
        resp = self.client.post("/onboarding/preview", data=self._form(path))
        self.assertEqual(resp.status_code, 400)
        self.assertIn("empty", resp.get_json()["error"].lower())

    def test_top_level_scalar_returns_400(self):
        path = self._write("scalar.yml", "just_a_string\n")
        resp = self.client.post("/onboarding/preview", data=self._form(path))
        self.assertEqual(resp.status_code, 400)
        self.assertIn("top level", resp.get_json()["error"].lower())

    def test_uc_disabled_uses_placeholder_volume_path(self):
        path = self._write(
            "onboarding.yml",
            "- bronze_catalog: '{uc_catalog_name}'\n"
            "  bronze_path: '{uc_volume_path}/bronze'\n",
        )
        resp = self.client.post("/onboarding/preview", data={
            "onboarding_file_path": path,
            "unity_catalog_enabled": "0",   # ← UC disabled
            "sdp_meta_schema": "sch",
            "bronze_schema": "br",
            "silver_schema": "sv",
        })
        self.assertEqual(resp.status_code, 200, resp.get_data(as_text=True))
        body = resp.get_json()
        self.assertIn("not-applicable-without-uc", body["uc_volume_path_used"])

    def test_sdp_meta_schema_field_is_actually_used(self):
        """Regression for #313-style symptom: the user types a schema name
        into ``sdp_meta_schema`` on the form, but the CLI receives a random
        UUID because the App envelope dropped the value. Preview must
        substitute the *user-supplied* schema into ``{uc_volume_path}``,
        not a random ``sdp_meta_dataflowspecs_<hex>`` placeholder."""
        path = self._write(
            "onboarding.yml",
            "- bronze_path: '{uc_volume_path}/bronze/customers'\n",
        )
        resp = self.client.post("/onboarding/preview", data=self._form(
            path,
            **{"sdp_meta_schema": "my_chosen_schema_name"},
        ))
        self.assertEqual(resp.status_code, 200, resp.get_data(as_text=True))
        body = resp.get_json()
        # The volume path the preview reports must use the user's value,
        # not a UUID-mangled default.
        self.assertEqual(
            body["uc_volume_path_used"],
            "/Volumes/my_cat/my_chosen_schema_name/my_chosen_schema_name/sdp_meta_conf/",
        )
        self.assertIn("my_chosen_schema_name", body["rendered"])
        self.assertNotIn("sdp_meta_dataflowspecs_", body["rendered"])

    def test_preview_accepts_merged_bundled_spec_path(self):
        """Regression for the production-reported failure:

            Onboarding path escapes the repo root: /tmp/sdp_meta_app_bundled_merged/
            cloudfiles-onboarding.merged.template.yml

        Repro: user picks "Cloud Files Autoloader (YAML)" from the
        bundled-demo dropdown. ``bundled_specs._materialise_merged_spec``
        writes the flattened spec under ``BUNDLED_SPEC_MERGED_DIR``
        (deliberately outside the repo root \u2014 the Apps container's
        repo tree is read-only). The frontend pre-fills the path into
        the form. Preview submits, and the S-2 traversal guard rejects
        the absolute path.

        Fix: ``BUNDLED_SPEC_MERGED_DIR`` is now in the resolver's
        trusted-prefix allow-list. This test confirms the end-to-end
        flow: a real file written under the trusted dir survives the
        full preview pipeline (resolve \u2192 pre-flight parse \u2192 substitute
        \u2192 200).
        """
        from services.onboarding.path_resolver import BUNDLED_SPEC_MERGED_DIR
        os.makedirs(BUNDLED_SPEC_MERGED_DIR, exist_ok=True)
        merged_path = os.path.join(
            BUNDLED_SPEC_MERGED_DIR,
            "cloudfiles-onboarding.merged.template.yml",
        )
        with open(merged_path, "w", encoding="utf-8") as fh:
            fh.write(
                "- data_flow_id: '100'\n"
                "  bronze_catalog: '{uc_catalog_name}'\n"
                "  bronze_path: '{uc_volume_path}/cloudfiles/bronze'\n"
            )
        self.addCleanup(
            lambda: os.path.exists(merged_path) and os.unlink(merged_path)
        )

        resp = self.client.post(
            "/onboarding/preview", data=self._form(merged_path)
        )
        self.assertEqual(resp.status_code, 200, resp.get_data(as_text=True))
        body = resp.get_json()
        # Render must have substituted both placeholders \u2014 same contract
        # as the in-repo preview tests.
        self.assertIn("my_cat", body["rendered"])
        self.assertIn("/Volumes/my_cat/sch/sch/sdp_meta_conf", body["rendered"])
        self.assertNotIn("{uc_catalog_name}", body["rendered"])
        self.assertNotIn("{uc_volume_path}", body["rendered"])


class OnboardingPreflightParseTests(unittest.TestCase):
    """Pre-flight parse on ``POST /onboarding`` — the real endpoint must
    reject malformed templates with a 400 BEFORE the background subprocess
    is spawned (i.e. without returning a job token)."""

    def setUp(self):
        self.client = app_mod.app.test_client()
        # Anchor the tempdir INSIDE the repo so the path resolver's
        # boundary check (S-2 hardening) accepts the absolute paths
        # the tests pass to ``onboarding_file_path``. A system
        # tempdir (``/var/folders/...``) would now be rejected as
        # "escapes the repo root".
        self.tmpdir = tempfile.mkdtemp(dir=_REPO_ROOT)
        self.addCleanup(shutil.rmtree, self.tmpdir, True)

    def _write(self, name: str, content: str) -> str:
        path = os.path.join(self.tmpdir, name)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(content)
        return path

    _ONBOARD_FORM = {
        "unity_catalog_enabled": "1",
        "unity_catalog_name": "my_cat",
        "sdp_meta_schema": "sch",
        "bronze_schema": "br",
        "silver_schema": "sv",
        "sdp_meta_layer": "1",
    }

    def test_onboarding_rejects_malformed_yaml_with_400(self):
        path = self._write(
            "broken.yml",
            "- key: '{uc_catalog_name}\n  unbalanced: \"oops\n",
        )
        resp = self.client.post("/onboarding", data=dict(
            self._ONBOARD_FORM, onboarding_file_path=path,
        ))
        self.assertEqual(resp.status_code, 400, resp.get_data(as_text=True))
        body = resp.get_json()
        # Must NOT have started a background job — no token in response.
        self.assertNotIn("token", body)
        self.assertNotIn("started", body)
        self.assertIn("parse", body["error"].lower())

    def test_onboarding_rejects_empty_template_with_400(self):
        path = self._write("empty.yml", "")
        resp = self.client.post("/onboarding", data=dict(
            self._ONBOARD_FORM, onboarding_file_path=path,
        ))
        self.assertEqual(resp.status_code, 400)
        body = resp.get_json()
        self.assertNotIn("token", body)
        self.assertIn("empty", body["error"].lower())

    def test_onboarding_rejects_malformed_schema_identifiers(self):
        """The /onboarding endpoint validates schema + table identifiers,
        not just the catalog: a hyphen in any of them must 400 before a
        background job is started (issue #261)."""
        path = self._write("ok.yml", "- data_flow_id: '100'\n")
        for field in ("unity_catalog_name", "sdp_meta_schema",
                      "bronze_schema", "silver_schema",
                      "bronze_table", "silver_table"):
            with self.subTest(field=field):
                resp = self.client.post("/onboarding", data=dict(
                    self._ONBOARD_FORM, onboarding_file_path=path,
                    **{field: "bad-name"},
                ))
                self.assertEqual(
                    resp.status_code, 400, resp.get_data(as_text=True)
                )
                body = resp.get_json()
                self.assertNotIn("token", body)
                self.assertIn("identifier", body["error"])


class DetectEnvSuffixesTests(unittest.TestCase):
    """Unit-test the helper that scans a parsed onboarding template for
    the env suffix(es) actually used on env-aware field names. The
    upstream onboarding parser (onboard_dataflowspec.py:1139) silently
    ``continue``s past rows whose ``bronze_database_<env>`` doesn't match
    — surfaces as a SUCCESS job with empty dataflowspec tables, the
    worst failure mode for demo onboarding. This helper is the App's
    defense against that."""

    def test_returns_empty_list_for_non_list_top_level(self):
        # ``parsed`` can be a top-level dict (e.g. some legacy templates)
        # but the env suffix lives on per-row dicts inside a list. A
        # bare dict has nothing to scan.
        self.assertEqual(app_mod._detect_env_suffixes({"key": "value"}), [])
        self.assertEqual(app_mod._detect_env_suffixes(None), [])
        self.assertEqual(app_mod._detect_env_suffixes("scalar"), [])

    def test_returns_empty_list_for_no_env_aware_fields(self):
        # Multi-source-CDC silver-only specs legitimately have no
        # env-aware bronze fields — must not flag those.
        parsed = [{"data_flow_id": "100", "data_flow_group": "g1"}]
        self.assertEqual(app_mod._detect_env_suffixes(parsed), [])

    def test_detects_single_env_suffix_demo(self):
        parsed = [{
            "data_flow_id": "100",
            "bronze_database_demo": "cat.br",
            "bronze_catalog_demo": "cat",
            "source_path_demo": "/Volumes/cat/sch/vol/data",
            "silver_database_demo": "cat.sv",
            "silver_transformation_json_demo": "/Volumes/cat/sch/vol/transforms.json",
        }]
        self.assertEqual(app_mod._detect_env_suffixes(parsed), ["demo"])

    def test_detects_single_env_suffix_prod(self):
        parsed = [{
            "bronze_database_prod": "cat.br",
            "source_path_prod": "/path/data",
        }]
        self.assertEqual(app_mod._detect_env_suffixes(parsed), ["prod"])

    def test_detects_quality_rules_path_suffix(self):
        parsed = [{
            "bronze_quality_engine": "lakeflow",
            "bronze_quality_rules_path_dev": "/Volumes/cat/sch/vol/rules.yml",
        }]
        self.assertEqual(app_mod._detect_env_suffixes(parsed), ["dev"])

    def test_detects_mixed_env_suffixes_returns_sorted_unique_list(self):
        # A user error worth surfacing: half the rows use ``_demo`` and
        # half use ``_prod``. Whichever env the form picks, the other
        # half are silently skipped. Detection returns both so the
        # error message can tell them what's actually in the file.
        parsed = [
            {"bronze_database_demo": "cat.br"},
            {"bronze_database_prod": "cat.br"},
        ]
        self.assertEqual(app_mod._detect_env_suffixes(parsed), ["demo", "prod"])

    def test_quarantine_fields_do_not_leak_into_suffix(self):
        # ``bronze_database_quarantine_demo`` must resolve to suffix
        # ``demo`` (matched against the ``bronze_database_quarantine``
        # template), NOT ``quarantine_demo`` (which would happen if we
        # tried to match the shorter ``bronze_database`` template
        # first). This is the reason _ENV_REQUIRED_FIELD_PREFIXES is
        # sorted by length-descending in the helper.
        parsed = [{
            "bronze_database_quarantine_demo": "cat.br_q",
            "bronze_catalog_quarantine_demo": "cat",
            "bronze_quarantine_table_path_demo": "/path/quarantine",
        }]
        self.assertEqual(app_mod._detect_env_suffixes(parsed), ["demo"])

    def test_ignores_non_dict_rows(self):
        # Robustness — a row that's a string or None shouldn't crash
        # detection; just skip and look at the dict rows.
        parsed = [None, "string-row", {"bronze_database_demo": "cat.br"}]
        self.assertEqual(app_mod._detect_env_suffixes(parsed), ["demo"])

    def test_ignores_keys_with_multi_word_remainders(self):
        # If a key matches a prefix but the remainder contains another
        # underscore, it's almost certainly a different env-aware
        # field we haven't enumerated (or noise) — better to skip than
        # mis-classify.
        parsed = [{"bronze_database_special_case_demo": "cat.br"}]
        # ``special_case_demo`` contains ``_`` so it's not a valid env
        # suffix candidate. Detection returns empty.
        self.assertEqual(app_mod._detect_env_suffixes(parsed), [])


class QualityConfigurationPreflightTests(unittest.TestCase):
    def test_accepts_complete_lakeflow_configuration(self):
        app_mod._verify_quality_configuration([{
            "data_flow_id": "100",
            "source_format": "cloudFiles",
            "bronze_quality_engine": "lakeflow",
            "bronze_quality_rules_path_demo": "/Volumes/c/s/v/rules.yml",
            "bronze_database_quarantine_demo": "c.quarantine",
            "bronze_quarantine_table": "invalid_rows",
        }], "demo")

    def test_rejects_unsupported_mode_and_incomplete_target(self):
        with self.assertRaisesRegex(
            Exception,
            "(?s)snapshot source.*bronze_database_quarantine_demo",
        ):
            app_mod._verify_quality_configuration([{
                "data_flow_id": "100",
                "source_format": "snapshot",
                "bronze_quality_engine": "lakeflow",
                "bronze_quality_rules_path_demo": "/Volumes/c/s/v/rules.yml",
            }], "demo")

    def test_accepts_snapshot_source_with_standard_silver_quality(self):
        app_mod._verify_quality_configuration([{
            "data_flow_id": "100",
            "source_format": "snapshot",
            "silver_quality_engine": "lakeflow",
            "silver_quality_rules_path_demo": "/Volumes/c/s/v/rules.yml",
            "silver_database_quarantine_demo": "c.quarantine",
            "silver_quarantine_table": "invalid_rows",
        }], "demo")

    def test_rejects_legacy_and_lakeflow_mix(self):
        with self.assertRaisesRegex(Exception, "cannot combine"):
            app_mod._verify_quality_configuration([{
                "data_flow_id": "100",
                "bronze_quality_engine": "lakeflow",
                "bronze_quality_rules_path_demo": "/Volumes/c/s/v/rules.yml",
                "bronze_data_quality_expectations_json_demo": "/legacy.yml",
                "bronze_database_quarantine_demo": "c.quarantine",
                "bronze_quarantine_table": "invalid_rows",
            }], "demo")


class OnboardingEnvMismatchRejectionTests(unittest.TestCase):
    """The actual bug-fix surface: POST /onboarding must reject when the
    form's Environment value doesn't match the env suffix(es) in the
    template. Without this check, the onboarding job runs to SUCCESS
    with empty dataflowspec tables, which is exactly what motivated
    these tests."""

    def setUp(self):
        self.client = app_mod.app.test_client()
        # Anchor the tempdir INSIDE the repo so the path resolver's
        # boundary check (S-2 hardening) accepts the absolute paths
        # the tests pass to ``onboarding_file_path``. A system
        # tempdir (``/var/folders/...``) would now be rejected as
        # "escapes the repo root".
        self.tmpdir = tempfile.mkdtemp(dir=_REPO_ROOT)
        self.addCleanup(shutil.rmtree, self.tmpdir, True)

    def _write(self, name: str, content: str) -> str:
        path = os.path.join(self.tmpdir, name)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(content)
        return path

    _BASE_FORM = {
        "unity_catalog_enabled": "1",
        "unity_catalog_name": "my_cat",
        "sdp_meta_schema": "sch",
        "bronze_schema": "br",
        "silver_schema": "sv",
        "sdp_meta_layer": "1",
    }

    def test_onboarding_rejects_env_mismatch_with_400(self):
        # Template uses ``_demo`` suffix; form passes ``environment=prod``.
        # The parser would silently skip every row. Must 400 here instead.
        path = self._write(
            "demo_suffix.yml",
            "- data_flow_id: '100'\n"
            "  bronze_database_demo: 'cat.br'\n"
            "  bronze_catalog_demo: 'cat'\n"
            "  source_path_demo: '/Volumes/cat/sch/vol/data'\n",
        )
        resp = self.client.post("/onboarding", data=dict(
            self._BASE_FORM,
            onboarding_file_path=path,
            environment="prod",
        ))
        self.assertEqual(resp.status_code, 400, resp.get_data(as_text=True))
        body = resp.get_json()
        self.assertNotIn("token", body)
        self.assertNotIn("started", body)
        # Error must name the detected suffix so the user knows what to fix.
        self.assertIn("demo", body["error"])
        self.assertIn("prod", body["error"])

    def test_onboarding_accepts_matching_env(self):
        # Template uses ``_demo`` suffix; form passes ``environment=demo``.
        # Env check passes — the CLI subprocess is then spawned (we get
        # a token back), which we let run async without waiting on it.
        path = self._write(
            "demo_match.yml",
            "- data_flow_id: '100'\n"
            "  bronze_database_demo: 'cat.br'\n"
            "  bronze_catalog_demo: 'cat'\n"
            "  source_path_demo: '/Volumes/cat/sch/vol/data'\n",
        )
        # Stub out the subprocess so the test doesn't actually exec.
        with mock.patch.object(app_mod.subprocess, "Popen") as mock_popen:
            mock_popen.return_value.stdout = io.StringIO("")
            mock_popen.return_value.stderr = io.StringIO("")
            mock_popen.return_value.wait.return_value = 0
            mock_popen.return_value.returncode = 0
            resp = self.client.post("/onboarding", data=dict(
                self._BASE_FORM,
                onboarding_file_path=path,
                environment="demo",
            ))
        self.assertEqual(resp.status_code, 200, resp.get_data(as_text=True))
        body = resp.get_json()
        self.assertIn("token", body)
        self.assertTrue(body.get("started"))

    def test_onboarding_accepts_template_with_no_env_aware_fields(self):
        # Multi-source-CDC silver-only spec: no env-aware bronze fields,
        # nothing to validate against. Must NOT reject — those templates
        # legitimately have no env suffixes.
        path = self._write(
            "no_env.yml",
            "- data_flow_id: '100'\n"
            "  data_flow_group: 'g1'\n"
            "  silver_cdc_apply_changes_flows:\n"
            "  - name: 'f1'\n",
        )
        with mock.patch.object(app_mod.subprocess, "Popen") as mock_popen:
            mock_popen.return_value.stdout = io.StringIO("")
            mock_popen.return_value.stderr = io.StringIO("")
            mock_popen.return_value.wait.return_value = 0
            mock_popen.return_value.returncode = 0
            resp = self.client.post("/onboarding", data=dict(
                self._BASE_FORM,
                onboarding_file_path=path,
                environment="prod",
            ))
        self.assertEqual(resp.status_code, 200, resp.get_data(as_text=True))
        body = resp.get_json()
        self.assertIn("token", body)

    def test_onboarding_default_environment_is_demo(self):
        # Regression: the App's form default for Environment used to be
        # ``prod`` while every demo template ships with ``_demo`` suffix.
        # Default must now be ``demo`` to match the demo flow.
        path = self._write(
            "demo_suffix.yml",
            "- bronze_database_demo: 'cat.br'\n"
            "  source_path_demo: '/path'\n",
        )
        with mock.patch.object(app_mod.subprocess, "Popen") as mock_popen:
            mock_popen.return_value.stdout = io.StringIO("")
            mock_popen.return_value.stderr = io.StringIO("")
            mock_popen.return_value.wait.return_value = 0
            mock_popen.return_value.returncode = 0
            # Note: no ``environment`` key in form — uses the App-side default.
            resp = self.client.post("/onboarding", data=dict(
                self._BASE_FORM, onboarding_file_path=path,
            ))
        self.assertEqual(resp.status_code, 200, resp.get_data(as_text=True))


class OnboardingPreviewEnvWarningTests(unittest.TestCase):
    """``/onboarding/preview`` surfaces ``detected_envs`` + an
    ``env_warning`` field so the UI can warn before submit without
    failing the dry-run itself (the preview is informational; the real
    POST /onboarding rejects)."""

    def setUp(self):
        self.client = app_mod.app.test_client()
        # Anchor the tempdir INSIDE the repo so the path resolver's
        # boundary check (S-2 hardening) accepts the absolute paths
        # the tests pass to ``onboarding_file_path``. A system
        # tempdir (``/var/folders/...``) would now be rejected as
        # "escapes the repo root".
        self.tmpdir = tempfile.mkdtemp(dir=_REPO_ROOT)
        self.addCleanup(shutil.rmtree, self.tmpdir, True)

    def _write(self, name, content):
        path = os.path.join(self.tmpdir, name)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(content)
        return path

    _BASE_FORM = {
        "unity_catalog_enabled": "1",
        "unity_catalog_name": "my_cat",
        "sdp_meta_schema": "sch",
        "bronze_schema": "br",
        "silver_schema": "sv",
    }

    def test_preview_returns_detected_envs(self):
        path = self._write(
            "demo_suffix.yml",
            "- bronze_database_demo: '{uc_catalog_name}.{bronze_schema}'\n"
            "  bronze_catalog_demo: '{uc_catalog_name}'\n",
        )
        resp = self.client.post("/onboarding/preview", data=dict(
            self._BASE_FORM,
            onboarding_file_path=path,
            environment="demo",
        ))
        self.assertEqual(resp.status_code, 200, resp.get_data(as_text=True))
        body = resp.get_json()
        self.assertEqual(body["detected_envs"], ["demo"])
        self.assertIsNone(body["env_warning"])

    def test_preview_returns_warning_on_env_mismatch(self):
        path = self._write(
            "demo_suffix.yml",
            "- bronze_database_demo: '{uc_catalog_name}.{bronze_schema}'\n",
        )
        resp = self.client.post("/onboarding/preview", data=dict(
            self._BASE_FORM,
            onboarding_file_path=path,
            environment="prod",  # mismatch
        ))
        # Preview returns 200 (it's a dry-run that just informs) but
        # surfaces the warning so the UI can show it before the user
        # clicks the real Run button.
        self.assertEqual(resp.status_code, 200, resp.get_data(as_text=True))
        body = resp.get_json()
        self.assertEqual(body["detected_envs"], ["demo"])
        self.assertIsNotNone(body["env_warning"])
        self.assertIn("demo", body["env_warning"])
        self.assertIn("prod", body["env_warning"])


class OnboardingPayloadKeyMappingTests(unittest.TestCase):
    """Regression: the JSON envelope the App sends to the CLI subprocess
    MUST use ``sdp_meta_schema`` / ``sdp_meta_layer`` populated with the
    user's actual form values, and MUST NOT carry any legacy
    ``dlt_meta_*`` aliases (a guard against accidental re-introduction
    \u2014 the App once translated form keys at this boundary). Otherwise
    the CLI's ``_load_onboard_config_ui`` looks up the wrong key, falls
    through to its random-UUID default, and the user's value is
    silently discarded.

    We assert on the JSON envelope rather than the subprocess outcome so
    the test stays fast and hermetic (no actual subprocess spawn)."""

    def setUp(self):
        self.client = app_mod.app.test_client()
        # Anchor the tempdir INSIDE the repo so the path resolver's
        # boundary check (S-2 hardening) accepts the absolute paths
        # the tests pass to ``onboarding_file_path``. A system
        # tempdir (``/var/folders/...``) would now be rejected as
        # "escapes the repo root".
        self.tmpdir = tempfile.mkdtemp(dir=_REPO_ROOT)
        self.addCleanup(shutil.rmtree, self.tmpdir, True)
        self.template = os.path.join(self.tmpdir, "onboarding.yml")
        with open(self.template, "w", encoding="utf-8") as fh:
            fh.write("- bronze_catalog: '{uc_catalog_name}'\n")

    def test_onboarding_payload_uses_sdp_meta_keys(self):
        # Capture the JSON envelope by monkey-patching ``subprocess.Popen``
        # before the background thread can spawn the real CLI.
        captured = {}

        class _FakeProc:
            def __init__(self):
                # Empty stream readers; the reader threads in
                # ``handle_onboard_form`` iterate them and immediately hit
                # EOF, so the background routine exits cleanly without us
                # having to plumb realistic CLI output.
                self.stdout = io.StringIO("")
                self.stderr = io.StringIO("")
                self.returncode = 0

            def wait(self, timeout=None):
                return 0

            def poll(self):
                # H-1: the runner's reap-on-cleanup path now calls
                # ``proc.poll()`` to decide whether to terminate.
                # Returning a non-None value mimics "already exited".
                return 0

        def _fake_popen(args, **kwargs):
            captured["args"] = args
            # Subprocess receives the JSON as args[-1] (``argv[1]`` of the
            # child python interpreter).
            captured["json_payload"] = json.loads(args[-1])
            return _FakeProc()

        with mock.patch.object(app_mod.subprocess, "Popen",
                               side_effect=_fake_popen):
            resp = self.client.post("/onboarding", data={
                "unity_catalog_enabled": "1",
                "unity_catalog_name": "my_cat",
                "sdp_meta_schema": "my_chosen_schema",   # ← user's value
                "bronze_schema": "br",
                "silver_schema": "sv",
                "sdp_meta_layer": "0",                   # ← user picks "bronze only"
                "onboarding_file_path": self.template,
            })

            # The route returns immediately and the subprocess fires from
            # a daemon thread — wait for it to call Popen before asserting.
            for _ in range(50):
                if "json_payload" in captured:
                    break
                time.sleep(0.02)

        self.assertEqual(resp.status_code, 200, resp.get_data(as_text=True))
        payload = captured.get("json_payload")
        self.assertIsNotNone(payload, "subprocess.Popen was not invoked")

        # The whole point of the regression: CLI-canonical key names,
        # populated with the user's actual values.
        self.assertEqual(payload.get("sdp_meta_schema"), "my_chosen_schema")
        self.assertEqual(payload.get("sdp_meta_layer"), "0")

        # And the wrong names must NOT appear — leaving them in would let
        # the CLI silently see both and pick the wrong one.
        self.assertNotIn("dlt_meta_schema", payload)
        self.assertNotIn("dlt_meta_layer", payload)


class BundledSpecsEndpointTests(unittest.TestCase):
    """``GET /onboarding/bundled-specs`` exposes the curated list of demo
    onboarding specs the App container ships. The UI uses this to render
    a "pick a demo" dropdown instead of forcing the user to know each
    relative path. Filesystem-backed (the registry filters out entries
    whose files don't exist) so we hit the real ``demo/conf/`` tree."""

    def setUp(self):
        self.client = app_mod.app.test_client()

    def test_endpoint_returns_200_with_spec_list(self):
        resp = self.client.get("/onboarding/bundled-specs")
        self.assertEqual(resp.status_code, 200)
        body = resp.get_json()
        self.assertIn("specs", body)
        self.assertIsInstance(body["specs"], list)
        # The repo ships at least one bundled demo; the flagship
        # ``onboarding`` spec must always be present.
        ids = [s["id"] for s in body["specs"]]
        self.assertIn("onboarding", ids)

    def test_each_entry_has_required_keys(self):
        resp = self.client.get("/onboarding/bundled-specs")
        body = resp.get_json()
        for spec in body["specs"]:
            self.assertIn("id", spec)
            self.assertIn("label", spec)
            self.assertIn("description", spec)
            self.assertIn("formats", spec)
            self.assertIn("default_local_directory", spec)
            # At least one format (JSON or YAML) must be present \u2014
            # the registry would filter the entry out otherwise.
            self.assertTrue(spec["formats"], f"Spec {spec['id']} has no formats")
            for fmt, path in spec["formats"].items():
                self.assertIn(fmt, ("json", "yaml"))
                # Merge-with entries return absolute paths pointing
                # at the materialised merged file in /tmp; everything
                # else is repo-relative. Either shape is valid \u2014
                # the path resolver accepts both. For absolute paths,
                # confirm the file actually exists so a broken merge
                # at registry-load time fails here rather than silently
                # producing a 400 at onboarding submit time.
                if os.path.isabs(path):
                    self.assertTrue(
                        os.path.isfile(path),
                        f"Absolute path {path} for spec {spec['id']} "
                        f"({fmt}) must exist on disk",
                    )

    def test_picker_includes_simple_and_multisource_entries(self):
        # The "out-of-the-box" registry promises a click-and-it-works
        # first-run demo and a multi-source representative; both must
        # be discoverable through the endpoint.
        resp = self.client.get("/onboarding/bundled-specs")
        ids = [s["id"] for s in resp.get_json()["specs"]]
        self.assertIn("onboarding_cars", ids)
        self.assertIn("multi-source-cdc-onboarding", ids)

    def test_picker_includes_silver_fanout_single_file_entry(self):
        # Silver Fanout returned to the picker as a single-file entry
        # (no ``merge_with``, no ``env_override``). It exercises the
        # post-fix path where the bronze pass skips rows lacking
        # ``source_details``, so a 1 bronze + 4 silver fanout file
        # onboards in a single ``layer=bronze_silver`` pass.
        resp = self.client.get("/onboarding/bundled-specs")
        by_id = {s["id"]: s for s in resp.get_json()["specs"]}
        self.assertIn("silver-fanout-onboarding", by_id)
        fanout = by_id["silver-fanout-onboarding"]
        self.assertEqual(
            fanout.get("merge_with", []), [],
            "Silver Fanout must NOT use merge_with \u2014 the single-file "
            "shape works directly after the bronze-pass skip fix",
        )
        self.assertIsNone(
            fanout.get("env_override"),
            "Silver Fanout uses the default ``demo`` env suffix; no override",
        )

    def test_silver_fanout_bundled_file_has_one_bronze_and_three_consumers(self):
        # Spot-check the merged silver-fanout template ships in the
        # repo and has the expected shape: 1 producer row (full
        # bronze + silver) + 3 fanout consumer rows (silver-only, no
        # source_details). If a future template edit breaks that
        # shape, the picker would silently surface a regressed demo.
        resp = self.client.get("/onboarding/bundled-specs")
        fanout = next(
            s for s in resp.get_json()["specs"]
            if s["id"] == "silver-fanout-onboarding"
        )
        json_path = fanout["formats"]["json"]
        # The picker returns a REPO-RELATIVE path for non-merge entries.
        # This test file lives at ``<repo>/tests/test_app_onboarding_preview.py``
        # so the repo root is ONE ``dirname`` above the test file.
        if not os.path.isabs(json_path):
            repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            json_path = os.path.join(repo_root, json_path)
        with open(json_path, "r", encoding="utf-8") as fh:
            rows = json.load(fh)
        self.assertEqual(
            len(rows), 4,
            f"Expected 4 rows (1 producer + 3 fanout consumers), got "
            f"{len(rows)}",
        )
        producers = [r for r in rows if r.get("source_details")]
        consumers = [r for r in rows if not r.get("source_details")]
        self.assertEqual(
            len(producers), 1,
            "Exactly one row must define source_details (the bronze producer)",
        )
        self.assertEqual(
            len(consumers), 3,
            "Exactly three rows must be silver-only fanout consumers",
        )
        # Every consumer must reference the same bronze the producer creates.
        producer_bronze = producers[0].get("bronze_table")
        for c in consumers:
            self.assertEqual(
                c.get("bronze_table"), producer_bronze,
                f"Fanout consumer {c.get('data_flow_id')} must read from "
                f"the same bronze table ({producer_bronze}) the producer "
                f"creates; got {c.get('bronze_table')}",
            )

    def test_snapshot_demo_is_not_in_picker(self):
        # The snapshot-onboarding template requires a pre-existing
        # source_products_delta UC table that the App can't create as
        # part of the single-step onboarding flow, so it was removed
        # from the picker. If it ever sneaks back in, the picker would
        # surface a broken demo to first-time users \u2014 fail loudly.
        resp = self.client.get("/onboarding/bundled-specs")
        ids = [s["id"] for s in resp.get_json()["specs"]]
        self.assertNotIn("snapshot-onboarding", ids)

    def test_cloudfiles_declares_merge_with_a2(self):
        # The cloudfiles A1 spec's silver append-flow reads a
        # ``customers_delta`` table that the A2 companion produces. The
        # picker resolves this by transparently merging A2's rows into
        # the same onboarding pass. After the fix:
        #   - ``merge_with`` must list ``cloudfiles-onboarding_A2``
        #   - ``companion`` must be empty (the merge made it transparent)
        resp = self.client.get("/onboarding/bundled-specs")
        by_id = {s["id"]: s for s in resp.get_json()["specs"]}
        self.assertIn("cloudfiles-onboarding", by_id)
        cf = by_id["cloudfiles-onboarding"]
        self.assertIn("cloudfiles-onboarding_A2", cf.get("merge_with", []))
        self.assertEqual(
            cf.get("companion", []), [],
            "companion should be empty for merge_with entries \u2014 "
            "the merge is transparent so the UI must NOT show the "
            "yellow companion-required warning bar",
        )

    def test_cloudfiles_merged_file_contains_both_a1_and_a2_rows(self):
        # The merged JSON file the picker hands to the onboarding
        # pipeline must contain the A1 rows PLUS the A2 rows so the
        # resulting DataflowSpec includes the customers_delta producer.
        resp = self.client.get("/onboarding/bundled-specs")
        cf = next(
            s for s in resp.get_json()["specs"]
            if s["id"] == "cloudfiles-onboarding"
        )
        merged_json_path = cf["formats"]["json"]
        self.assertTrue(
            os.path.isabs(merged_json_path),
            "merge_with entry must return an absolute path to the "
            "materialised merged file",
        )
        with open(merged_json_path, "r", encoding="utf-8") as fh:
            merged_rows = json.load(fh)
        # A1 ships 2 rows (CUSTOMERS + TRANSACTIONS); A2 ships 1 row
        # (customers_delta producer). Merged file must have all 3.
        self.assertEqual(
            len(merged_rows), 3,
            f"Expected 3 rows in merged spec (A1=2 + A2=1), got "
            f"{len(merged_rows)}: {[r.get('data_flow_id') for r in merged_rows]}",
        )
        # The A2 row's bronze target IS the customers_delta producer
        # the A1 silver append-flow reads from \u2014 surface that as
        # a positive assertion so a future merge regression fails here.
        bronze_tables = {row.get("bronze_table") for row in merged_rows}
        self.assertIn(
            "customers_delta", bronze_tables,
            f"customers_delta must be one of the merged bronze targets "
            f"so the A1 silver append-flow can resolve it at runtime; "
            f"got {bronze_tables}",
        )

    def test_dais_demo_declares_env_override_prod(self):
        # The DAIS template ships with the ``_prod`` env suffix on
        # every env-aware field. The picker must surface
        # ``env_override="prod"`` so the UI can auto-set the
        # Environment field; otherwise the preview's env-mismatch
        # guard rejects the onboarding with the default ``demo`` value.
        resp = self.client.get("/onboarding/bundled-specs")
        by_id = {s["id"]: s for s in resp.get_json()["specs"]}
        self.assertIn("onboarding", by_id)
        self.assertEqual(by_id["onboarding"].get("env_override"), "prod")
        self.assertTrue(
            by_id["onboarding"].get("note"),
            "DAIS entry should carry a ``note`` explaining the env override",
        )

    def test_missing_disk_files_are_filtered_silently(self):
        # If the registry has an entry whose backing files don't exist
        # on disk (partial container layout), it must be omitted from
        # the response rather than blowing up the endpoint.
        with mock.patch.dict(app_mod._BUNDLED_DEMO_SPECS,
                             {"this_definitely_does_not_exist": {
                                 "label": "x", "description": "y",
                                 "tags": []}},
                             clear=False):
            resp = self.client.get("/onboarding/bundled-specs")
        body = resp.get_json()
        ids = [s["id"] for s in body["specs"]]
        self.assertNotIn("this_definitely_does_not_exist", ids)


class ExtractRequiredFilesTests(unittest.TestCase):
    """Unit tests for ``_extract_required_files`` \u2014 the path-shape
    detector that drives the UI's "required files" preflight panel.

    The detector must catch every field name pattern that lands a path
    in front of the cluster: ``source_path*``, ``source_schema_path``,
    DQE / silver-transformation JSON pointers, and the same set nested
    inside ``bronze_append_flows`` / ``silver_append_flows.source_details``."""

    SUBS = {
        "{uc_volume_path}": "/Volumes/cat/sch/sch/sdp_meta_conf/",
        "{uc_catalog_name}": "cat",
        "{bronze_schema}": "br",
        "{silver_schema}": "sv",
    }

    def test_detects_source_path_with_env_suffix(self):
        spec = [{
            "data_flow_id": "100",
            "source_details": {"source_path_demo": "{uc_volume_path}/data/x"},
        }]
        out = app_mod._extract_required_files(spec, self.SUBS)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["field"], "source_path_demo")
        self.assertEqual(out[0]["path"],
                         "/Volumes/cat/sch/sch/sdp_meta_conf/data/x")
        self.assertEqual(out[0]["entity"], "data_flow_id=100")

    def test_detects_source_schema_path_unsuffixed(self):
        spec = [{
            "data_flow_id": "100",
            "source_details": {"source_schema_path": "{uc_volume_path}/ddl/c.ddl"},
        }]
        out = app_mod._extract_required_files(spec, self.SUBS)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["field"], "source_schema_path")

    def test_detects_dqe_and_transformation_json_pointers(self):
        spec = [{
            "data_flow_id": "100",
            "bronze_data_quality_expectations_json_demo": "{uc_volume_path}/dqe/b.json",
            "silver_data_quality_expectations_json_demo": "{uc_volume_path}/dqe/s.json",
            "bronze_quality_rules_path_demo": "{uc_volume_path}/quality/b.yml",
            "silver_quality_rules_path_demo": "{uc_volume_path}/quality/s.yml",
            "silver_transformation_json_demo": "{uc_volume_path}/sxf.json",
        }]
        out = app_mod._extract_required_files(spec, self.SUBS)
        fields = [e["field"] for e in out]
        self.assertEqual(set(fields), {
            "bronze_data_quality_expectations_json_demo",
            "silver_data_quality_expectations_json_demo",
            "bronze_quality_rules_path_demo",
            "silver_quality_rules_path_demo",
            "silver_transformation_json_demo",
        })

    def test_detects_paths_nested_in_append_flows(self):
        spec = [{
            "data_flow_id": "100",
            "bronze_append_flows": [{
                "source_details": {
                    "source_path_demo": "{uc_volume_path}/data/af",
                    "source_schema_path": "{uc_volume_path}/ddl/c.ddl",
                },
            }],
        }]
        out = app_mod._extract_required_files(spec, self.SUBS)
        # Both nested paths must be detected, both attributed to the
        # parent row's data_flow_id (not the append-flow's anon name).
        self.assertEqual(len(out), 2)
        for entry in out:
            self.assertEqual(entry["entity"], "data_flow_id=100")

    def test_skips_unsubstituted_placeholders(self):
        # A field whose value still contains ``{...}`` after subs (e.g.
        # references a token we don't know how to resolve at preview
        # time) is dropped silently \u2014 we can't existence-check it.
        spec = [{
            "data_flow_id": "100",
            "source_details": {"source_path_demo": "{some_unknown_token}/data"},
        }]
        out = app_mod._extract_required_files(spec, self.SUBS)
        self.assertEqual(out, [])

    def test_ignores_non_path_fields(self):
        spec = [{
            "data_flow_id": "100",
            "bronze_table": "customers",
            "source_format": "cloudFiles",
            "bronze_cluster_by": ["id"],
        }]
        out = app_mod._extract_required_files(spec, self.SUBS)
        self.assertEqual(out, [])

    def test_returns_empty_for_non_list_top_level(self):
        self.assertEqual(app_mod._extract_required_files({}, self.SUBS), [])
        self.assertEqual(app_mod._extract_required_files(None, self.SUBS), [])


class CheckRequiredFilesExistenceTests(unittest.TestCase):
    """Existence-resolution tests for ``_check_required_files_existence``.

    Covers four cases:
      * Local supporting dir + file exists \u2192 ``exists: True``.
      * Local supporting dir + file missing \u2192 ``exists: False``.
      * UC Volume supporting dir + SDK find \u2192 ``exists: True``.
      * UC Volume supporting dir + SDK miss \u2192 ``exists: False``.
    """

    UC_VOL_PATH = "/Volumes/cat/sch/sch/sdp_meta_conf/"

    def test_local_dir_file_exists(self):
        tmpdir = tempfile.mkdtemp()
        # Layout: <tmpdir>/demo/resources/data/customers.json
        nested = os.path.join(tmpdir, "demo", "resources", "data")
        os.makedirs(nested)
        with open(os.path.join(nested, "customers.json"), "w") as fh:
            fh.write("{}")
        required = [{
            "entity": "data_flow_id=100", "field": "source_path_demo",
            "path": f"{self.UC_VOL_PATH.rstrip('/')}/demo/resources/data/customers.json",
        }]
        local_dir = os.path.join(tmpdir, "demo")
        out = app_mod._check_required_files_existence(
            required, self.UC_VOL_PATH, local_dir
        )
        self.assertEqual(len(out), 1)
        self.assertIs(out[0]["exists"], True)
        self.assertTrue(out[0]["check_path"].endswith("customers.json"))

    def test_local_dir_file_missing(self):
        tmpdir = tempfile.mkdtemp()
        os.makedirs(os.path.join(tmpdir, "demo"))
        required = [{
            "entity": "data_flow_id=100", "field": "source_path_demo",
            "path": f"{self.UC_VOL_PATH.rstrip('/')}/demo/resources/data/missing.json",
        }]
        out = app_mod._check_required_files_existence(
            required, self.UC_VOL_PATH, os.path.join(tmpdir, "demo")
        )
        self.assertEqual(len(out), 1)
        self.assertIs(out[0]["exists"], False)

    def test_uc_volume_dir_file_exists_via_sdk(self):
        required = [{
            "entity": "data_flow_id=100", "field": "source_path_demo",
            "path": f"{self.UC_VOL_PATH.rstrip('/')}/demo/resources/data/customers.json",
        }]
        # Mock the SDK \u2014 successful get_metadata means "file exists".
        fake_ws = mock.MagicMock()
        fake_ws.files.get_metadata.return_value = mock.MagicMock()
        out = app_mod._check_required_files_existence(
            required, self.UC_VOL_PATH,
            local_supporting_dir="/Volumes/cat/sch/sch/user_uploaded",
            ws_factory=lambda: fake_ws,
        )
        self.assertEqual(len(out), 1)
        self.assertIs(out[0]["exists"], True)
        fake_ws.files.get_metadata.assert_called_once()

    def test_uc_volume_dir_file_missing_via_sdk(self):
        required = [{
            "entity": "data_flow_id=100", "field": "source_path_demo",
            "path": f"{self.UC_VOL_PATH.rstrip('/')}/demo/resources/data/missing.json",
        }]
        fake_ws = mock.MagicMock()
        fake_ws.files.get_metadata.side_effect = Exception("not found")
        out = app_mod._check_required_files_existence(
            required, self.UC_VOL_PATH,
            local_supporting_dir="/Volumes/cat/sch/sch/user_uploaded",
            ws_factory=lambda: fake_ws,
        )
        self.assertEqual(len(out), 1)
        self.assertIs(out[0]["exists"], False)

    def test_path_outside_uc_volume_marked_unknown(self):
        # A file path that doesn't live under {uc_volume_path} can't be
        # preflighted \u2014 e.g. a literal DBFS path or absolute local
        # path the user hand-wrote. ``exists`` is ``None`` with a reason.
        required = [{
            "entity": "data_flow_id=100", "field": "source_path",
            "path": "dbfs:/some/legacy/path",
        }]
        out = app_mod._check_required_files_existence(
            required, self.UC_VOL_PATH, "/tmp/whatever"
        )
        self.assertEqual(len(out), 1)
        self.assertIsNone(out[0]["exists"])
        self.assertIn("can't preflight", out[0]["reason"])


class OnboardingPreviewRequiredFilesIntegrationTests(unittest.TestCase):
    """End-to-end exercise of the preview endpoint with the new
    ``required_files`` field. Writes a real spec + supporting tree to
    a tempdir and confirms the response correctly classifies each
    referenced file as exists/missing."""

    def setUp(self):
        self.client = app_mod.app.test_client()
        # Anchor the tempdir INSIDE the repo so the path resolver's
        # boundary check (S-2 hardening) accepts the absolute paths
        # the tests pass to ``onboarding_file_path``. A system
        # tempdir (``/var/folders/...``) would now be rejected as
        # "escapes the repo root".
        self.tmpdir = tempfile.mkdtemp(dir=_REPO_ROOT)
        self.addCleanup(shutil.rmtree, self.tmpdir, True)
        # Spec on disk references three files under {uc_volume_path}/demo/...
        # Create two of them on the local supporting tree, leave one
        # missing \u2014 the response must classify accordingly.
        self.spec_path = os.path.join(self.tmpdir, "spec.yml")
        with open(self.spec_path, "w") as fh:
            fh.write(
                "- data_flow_id: '100'\n"
                "  source_details:\n"
                "    source_path_demo: '{uc_volume_path}/demo/resources/data/customers'\n"
                "    source_schema_path: '{uc_volume_path}/demo/resources/ddl/customers.ddl'\n"
                "  silver_transformation_json_demo: '{uc_volume_path}/demo/conf/silver_transformations.json'\n"
            )
        # Build the supporting tree. Two files exist, one is missing
        # on purpose.
        demo_dir = os.path.join(self.tmpdir, "demo")
        os.makedirs(os.path.join(demo_dir, "resources", "data"))
        with open(os.path.join(demo_dir, "resources", "data", "customers"), "w") as fh:
            fh.write("")
        os.makedirs(os.path.join(demo_dir, "resources", "ddl"))
        with open(os.path.join(demo_dir, "resources", "ddl", "customers.ddl"), "w") as fh:
            fh.write("")
        # silver_transformations.json is deliberately NOT created.

    def test_preview_classifies_each_referenced_file(self):
        resp = self.client.post("/onboarding/preview", data={
            "unity_catalog_enabled": "1",
            "unity_catalog_name": "my_cat",
            "sdp_meta_schema": "sch",
            "bronze_schema": "br",
            "silver_schema": "sv",
            "onboarding_file_path": self.spec_path,
            "local_directory": self.tmpdir + "/demo",
        })
        self.assertEqual(resp.status_code, 200, resp.get_data(as_text=True))
        body = resp.get_json()
        self.assertIn("required_files", body)
        files = body["required_files"]
        self.assertEqual(len(files), 3)
        by_field = {f["field"]: f for f in files}
        self.assertIs(by_field["source_path_demo"]["exists"], True)
        self.assertIs(by_field["source_schema_path"]["exists"], True)
        self.assertIs(by_field["silver_transformation_json_demo"]["exists"], False)


if __name__ == "__main__":
    unittest.main()
