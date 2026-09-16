"""Tests for the DAB template and `databricks.labs.sdp_meta.bundle` module.

Two kinds of coverage:

1. Unit tests for the three CLI handler functions (``bundle_init``,
   ``bundle_prepare_wheel``, ``bundle_validate``). These mock out
   ``subprocess.run`` and the ``WorkspaceClient`` so they are fast and do not
   require a ``databricks`` CLI or workspace credentials.

2. End-to-end render tests that invoke the real ``databricks bundle init``
   against the packaged template and assert the rendered bundle is valid
   YAML/JSON, exercises the layer/source/format branches, and passes the
   sdp-meta sanity checks. These are skipped automatically if the
   ``databricks`` CLI is not on PATH (so contributors without it can still run
   ``pytest``). They render offline against throwaway credentials, so they
   never touch a workspace or read the developer's ``~/.databrickscfg``.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import unittest
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, patch

import yaml

from databricks.labs.sdp_meta.bundle import (
    QUICKSTART_BUNDLE_INIT_DEFAULTS,
    TEMPLATE_DIR,
    BundleAddFlowCommand,
    BundleAddGoldCommand,
    BundleInitCommand,
    BundlePrepareWheelCommand,
    BundleValidateCommand,
    FlowSpec,
    _GOLD_PIPELINE_BLOCK,
    _discover_bundle_dir,
    _sdp_meta_sanity_checks,
    _stamp_sdp_meta_version,
    bundle_add_flow,
    bundle_add_gold,
    bundle_init,
    bundle_prepare_wheel,
    bundle_validate,
    write_quickstart_config_file,
)
from databricks.labs.sdp_meta.__about__ import __version__


REPO_ROOT = Path(__file__).resolve().parents[1]


class TemplateLayoutTests(unittest.TestCase):
    """Shape assertions on the packaged template. Offline."""

    def test_schema_file_is_valid_json_with_required_keys(self):
        schema_path = TEMPLATE_DIR / "databricks_template_schema.json"
        self.assertTrue(schema_path.is_file(), f"missing: {schema_path}")
        schema = json.loads(schema_path.read_text())
        self.assertIn("properties", schema)
        for required in (
            "bundle_name",
            "uc_catalog_name",
            "sdp_meta_schema",
            "bronze_target_schema",
            "silver_target_schema",
            "gold_enabled",
            "gold_target_schema",
            "gold_models_path",
            "layer",
            "pipeline_mode",
            "source_format",
            "onboarding_file_format",
            "dataflow_group",
            "wheel_source",
            "sdp_meta_dependency",
            "author",
        ):
            self.assertIn(required, schema["properties"], f"missing prompt: {required}")
        self.assertEqual(
            schema["properties"]["wheel_source"]["enum"], ["pypi", "volume_path"]
        )
        self.assertEqual(schema["properties"]["sdp_meta_dependency"]["default"], "__SET_ME__")

    def test_template_root_exists(self):
        root = TEMPLATE_DIR / "template" / "{{.bundle_name}}"
        self.assertTrue(root.is_dir())
        self.assertTrue((root / "databricks.yml.tmpl").is_file())
        self.assertTrue((root / "resources" / "variables.yml.tmpl").is_file())
        self.assertTrue((root / "resources" / "sdp_meta_onboarding_job.yml.tmpl").is_file())
        self.assertTrue((root / "resources" / "sdp_meta_pipelines.yml.tmpl").is_file())
        self.assertTrue((root / "notebooks" / "init_sdp_meta_pipeline.py.tmpl").is_file())
        recipes_root = root / "recipes"
        self.assertTrue(recipes_root.is_dir(), "recipes/ template dir missing")
        for name in (
            "README.md.tmpl",
            "from_uc.py.tmpl",
            "from_volume.py.tmpl",
            "from_topics.py.tmpl",
            "from_inventory.py.tmpl",
        ):
            self.assertTrue((recipes_root / name).is_file(), f"recipes/{name} missing")

    def test_databricks_yml_ships_run_as_block_commented(self):
        """The prod target ships a guidance comment showing how to set
        ``run_as: { service_principal_name: ... }``. It must stay commented
        so ad-hoc / manual prod deploys don't break, but the comment must
        exist so CI/CD users have a copy-pasteable starting point and the
        ``bundle-validate`` placeholder check has a placeholder to react to
        once the user uncomments it."""
        tmpl = TEMPLATE_DIR / "template" / "{{.bundle_name}}" / "databricks.yml.tmpl"
        text = tmpl.read_text()
        self.assertIn(
            "# run_as:", text,
            "prod target must ship a commented `run_as:` guidance block",
        )
        self.assertIn(
            "<your-prod-service-principal-application-id>", text,
            "commented run_as block must use the <your-...> placeholder "
            "convention so bundle-validate can flag it after uncomment",
        )
        # The block must live under the prod target, not dev. Check by
        # finding the run_as line and walking backwards to the nearest
        # target header.
        lines = text.splitlines()
        run_as_line = next(i for i, l in enumerate(lines) if "# run_as:" in l)
        prior = "\n".join(lines[:run_as_line])
        self.assertIn("prod:", prior)
        # And no live `run_as:` (uncommented) — the shipped template must
        # not opt users into SP execution by default.
        self.assertNotRegex(
            text,
            r"^\s{2,}run_as:",
            "shipped template must NOT have an uncommented run_as block; "
            "users opt in by uncommenting.",
        )

    def test_quickstart_defaults_cover_every_schema_prompt(self):
        """`bundle-init --quickstart` writes a config file that pre-answers
        every schema prompt. If a new prompt is added to the schema and
        nobody updates QUICKSTART_BUNDLE_INIT_DEFAULTS, `databricks bundle
        init --config-file` will silently fall back to the prompt's default
        for that key (or interactively prompt, defeating the point of
        quickstart). This test fails loudly in that case."""
        schema_path = TEMPLATE_DIR / "databricks_template_schema.json"
        schema = json.loads(schema_path.read_text())
        schema_keys = set(schema["properties"].keys())
        quickstart_keys = set(QUICKSTART_BUNDLE_INIT_DEFAULTS.keys())
        missing = schema_keys - quickstart_keys
        self.assertFalse(
            missing,
            f"QUICKSTART_BUNDLE_INIT_DEFAULTS is missing keys present in "
            f"the schema: {sorted(missing)}. Add a default for each new "
            f"prompt in bundle.py.",
        )
        extra = quickstart_keys - schema_keys
        self.assertFalse(
            extra,
            f"QUICKSTART_BUNDLE_INIT_DEFAULTS has keys not present in the "
            f"schema: {sorted(extra)}. Remove or rename them.",
        )
        # Each quickstart default must satisfy the schema's enum (when
        # declared) so `databricks bundle init --config-file` doesn't
        # reject the file at parse time.
        for key, value in QUICKSTART_BUNDLE_INIT_DEFAULTS.items():
            enum = schema["properties"][key].get("enum")
            if enum:
                self.assertIn(
                    value, enum,
                    f"QUICKSTART_BUNDLE_INIT_DEFAULTS[{key!r}]={value!r} "
                    f"is not in schema enum {enum}",
                )

    def test_onboarding_job_uses_correct_wheel_entry_point(self):
        """The onboarding job must call `databricks_labs_sdp_meta:run`."""
        job_tmpl = (
            TEMPLATE_DIR
            / "template" / "{{.bundle_name}}"
            / "resources" / "sdp_meta_onboarding_job.yml.tmpl"
        ).read_text()
        self.assertIn("package_name: databricks_labs_sdp_meta", job_tmpl)
        self.assertIn("entry_point: run", job_tmpl)

    def test_add_gold_pipeline_block_matches_bundle_template(self):
        pipeline_template = (
            TEMPLATE_DIR
            / "template"
            / "{{.bundle_name}}"
            / "resources"
            / "sdp_meta_pipelines.yml.tmpl"
        ).read_text()
        match = re.search(
            r"\{\{- if \.gold_enabled\}\}\n"
            r"(?P<block>    gold:\n.*?\n)"
            r"\{\{- end\}\}\n\n  jobs:",
            pipeline_template,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(match)
        template_gold = yaml.safe_load(
            "resources:\n  pipelines:\n" + match.group("block")
        )["resources"]["pipelines"]["gold"]
        command_gold = yaml.safe_load(
            "resources:\n  pipelines:\n" + _GOLD_PIPELINE_BLOCK
        )["resources"]["pipelines"]["gold"]
        self.assertEqual(command_gold, template_gold)

    def test_bundle_module_does_not_use_path_is_relative_to(self):
        bundle_module = (
            REPO_ROOT / "src" / "databricks" / "labs" / "sdp_meta" / "bundle.py"
        ).read_text()
        self.assertNotIn(
            ".is_relative_to(",
            bundle_module,
            "Path.is_relative_to is unavailable at the declared Python 3.8 floor",
        )


class SanityChecksTests(unittest.TestCase):
    """Unit tests for `_sdp_meta_sanity_checks` with synthetic bundle trees."""

    def _write(self, path: Path, body: str):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body)

    def _make_bundle(
        self,
        tmp: Path,
        *,
        layer: str,
        pipeline_mode: str = "split",
        with_bronze: bool = True,
        with_silver: bool = True,
        with_combined: bool = False,
        gold_enabled: bool = False,
    ):
        """Create a minimal bundle tree on disk matching `layer` + pipeline flags."""
        self._write(tmp / "databricks.yml", "bundle: {name: t}\n")
        self._write(
            tmp / "resources" / "variables.yml",
            yaml.safe_dump({
                "variables": {
                    "layer": {"default": layer},
                    "pipeline_mode": {"default": pipeline_mode},
                    "uc_catalog_name": {"default": "main"},
                    "silver_target_schema": {"default": "sdp_meta_silver"},
                    "gold_enabled": {"default": gold_enabled},
                    "gold_target_schema": {"default": "sdp_meta_gold"},
                    "gold_models_path": {"default": "gold/models"},
                    "dataflow_group": {"default": "g"},
                    "onboarding_file_name": {"default": "onboarding.yml"},
                    "wheel_source": {"default": "pypi"},
                    "sdp_meta_dependency": {"default": "databricks-labs-sdp-meta==0.1.0"},
                }
            }),
        )
        self._write(
            tmp / "conf" / "onboarding.yml",
            yaml.safe_dump([{"data_flow_id": "1", "data_flow_group": "g"}]),
        )
        pipelines = {
            "resources": {
                "pipelines": {},
                "jobs": {"pipelines": {"tasks": []}},
            }
        }
        if with_bronze:
            pipelines["resources"]["pipelines"]["bronze"] = {}
        if with_silver:
            pipelines["resources"]["pipelines"]["silver"] = {}
        if with_combined:
            pipelines["resources"]["pipelines"]["bronze_silver"] = {}
        if gold_enabled:
            pipelines["resources"]["pipelines"]["gold"] = {
                "catalog": "${var.uc_catalog_name}",
                "schema": "${var.gold_target_schema}",
                "root_path": ".",
                "libraries": [
                    {
                        "glob": {
                            "include": "../${var.gold_models_path}/**",
                        }
                    }
                ],
                "configuration": {
                    "silver_catalog": "${var.uc_catalog_name}",
                    "silver_schema": "${var.silver_target_schema}",
                },
            }
            upstream = (
                "bronze_silver"
                if layer == "bronze_silver" and pipeline_mode == "combined"
                else "silver"
            )
            pipelines["resources"]["jobs"]["pipelines"]["tasks"].append(
                {
                    "task_key": "gold",
                    "depends_on": [{"task_key": upstream}],
                    "pipeline_task": {
                        "pipeline_id": "${resources.pipelines.gold.id}",
                    },
                }
            )
            self._write(
                tmp / "gold" / "models" / "model.sql",
                "CREATE OR REFRESH MATERIALIZED VIEW gold_model AS SELECT 1;\n",
            )
        self._write(tmp / "resources" / "sdp_meta_pipelines.yml", yaml.safe_dump(pipelines))

    def test_happy_path_bronze_silver(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp, layer="bronze_silver", with_bronze=True, with_silver=True)
            self.assertEqual(_sdp_meta_sanity_checks(tmp), [])

    def test_layer_mismatch_detected(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp, layer="bronze_silver", with_bronze=True, with_silver=False)
            errors = _sdp_meta_sanity_checks(tmp)
            self.assertTrue(any("layer=bronze_silver" in e for e in errors), errors)

    def test_bronze_only_rejects_silver_pipeline(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp, layer="bronze", with_bronze=True, with_silver=True)
            errors = _sdp_meta_sanity_checks(tmp)
            self.assertTrue(any("layer=bronze" in e for e in errors), errors)

    def test_missing_onboarding_file_detected(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp, layer="bronze")
            (tmp / "conf" / "onboarding.yml").unlink()
            errors = _sdp_meta_sanity_checks(tmp)
            self.assertTrue(any("onboarding.yml" in e for e in errors), errors)

    def test_dataflow_group_mismatch_detected(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp, layer="bronze", with_bronze=True, with_silver=False)
            self._write(
                tmp / "conf" / "onboarding.yml",
                yaml.safe_dump([{"data_flow_id": "1", "data_flow_group": "other"}]),
            )
            errors = _sdp_meta_sanity_checks(tmp)
            self.assertTrue(any("dataflow_group" in e for e in errors), errors)

    def test_combined_mode_happy_path(self):
        with _tempdir() as tmp:
            self._make_bundle(
                tmp,
                layer="bronze_silver",
                pipeline_mode="combined",
                with_bronze=False,
                with_silver=False,
                with_combined=True,
            )
            self.assertEqual(_sdp_meta_sanity_checks(tmp), [])

    def test_combined_mode_rejects_split_pipelines(self):
        with _tempdir() as tmp:
            self._make_bundle(
                tmp,
                layer="bronze_silver",
                pipeline_mode="combined",
                with_bronze=True,
                with_silver=True,
                with_combined=False,
            )
            errors = _sdp_meta_sanity_checks(tmp)
            self.assertTrue(any("pipeline_mode=combined" in e for e in errors), errors)

    def test_split_mode_rejects_combined_pipeline(self):
        with _tempdir() as tmp:
            self._make_bundle(
                tmp,
                layer="bronze_silver",
                pipeline_mode="split",
                with_bronze=True,
                with_silver=True,
                with_combined=True,
            )
            errors = _sdp_meta_sanity_checks(tmp)
            self.assertTrue(any("pipeline_mode=split" in e for e in errors), errors)

    def test_gold_accepts_supported_dataset_declaration_forms(self):
        declarations = (
            "CREATE MATERIALIZED VIEW gold_model AS SELECT * FROM staged;",
            "CREATE PRIVATE MATERIALIZED VIEW gold_model AS SELECT * FROM staged;",
            "CREATE OR REFRESH PRIVATE MATERIALIZED VIEW gold_model AS SELECT * FROM staged;",
            "CREATE STREAMING TABLE gold_model AS SELECT * FROM STREAM staged;",
            "CREATE OR REFRESH STREAMING TABLE gold_model AS SELECT * FROM STREAM staged;",
        )
        for declaration in declarations:
            with self.subTest(declaration=declaration), _tempdir() as tmp:
                self._make_bundle(
                    tmp,
                    layer="bronze_silver",
                    with_bronze=True,
                    with_silver=True,
                    gold_enabled=True,
                )
                (tmp / "gold" / "models" / "model.sql").write_text(
                    "CREATE TEMPORARY VIEW staged AS SELECT 1;\n"
                    f"{declaration}\n"
                )
                self.assertEqual(_sdp_meta_sanity_checks(tmp), [])

    def test_gold_rejects_non_sql_files_in_model_directory(self):
        for name in ("README.md", "helper.py", ".DS_Store"):
            with self.subTest(name=name), _tempdir() as tmp:
                self._make_bundle(
                    tmp,
                    layer="bronze_silver",
                    with_bronze=True,
                    with_silver=True,
                    gold_enabled=True,
                )
                self._write(tmp / "gold" / "models" / name, "not SQL\n")
                errors = _sdp_meta_sanity_checks(tmp)
                self.assertTrue(
                    any(
                        name in error
                        and "only `.sql` source files are allowed" in error
                        for error in errors
                    ),
                    errors,
                )

    def test_gold_rejects_non_pipeline_workflow_task(self):
        with _tempdir() as tmp:
            self._make_bundle(
                tmp,
                layer="bronze_silver",
                with_bronze=True,
                with_silver=True,
                gold_enabled=True,
            )
            pipelines_path = tmp / "resources" / "sdp_meta_pipelines.yml"
            pipelines = yaml.safe_load(pipelines_path.read_text())
            gold_task = pipelines["resources"]["jobs"]["pipelines"]["tasks"][0]
            gold_task.pop("pipeline_task")
            gold_task["notebook_task"] = {"notebook_path": "/Shared/not-gold"}
            pipelines_path.write_text(yaml.safe_dump(pipelines))

            errors = _sdp_meta_sanity_checks(tmp)

            self.assertTrue(
                any(
                    "must be a pipeline task targeting "
                    "`${resources.pipelines.gold.id}`" in error
                    for error in errors
                ),
                errors,
            )

    def test_gold_rejects_mutated_or_target_overridden_scaffold_marker(self):
        with _tempdir() as tmp:
            self._make_bundle(
                tmp,
                layer="bronze_silver",
                with_bronze=True,
                with_silver=True,
                gold_enabled=True,
            )
            variables_path = tmp / "resources" / "variables.yml"
            variables = yaml.safe_load(variables_path.read_text())
            variables["variables"]["gold_enabled"]["default"] = False
            variables_path.write_text(yaml.safe_dump(variables))
            (tmp / "databricks.yml").write_text(
                "bundle: {name: t}\n"
                "targets:\n"
                "  dev:\n"
                "    variables:\n"
                "      gold_enabled: true\n"
            )

            errors = _sdp_meta_sanity_checks(tmp)

            self.assertTrue(
                any("gold_enabled=false` conflicts" in error for error in errors),
                errors,
            )
            self.assertTrue(
                any("overrides immutable `gold_enabled`" in error for error in errors),
                errors,
            )

    def test_gold_rejects_bronze_only_bundle(self):
        with _tempdir() as tmp:
            self._make_bundle(
                tmp,
                layer="bronze",
                with_bronze=True,
                with_silver=False,
                gold_enabled=True,
            )
            errors = _sdp_meta_sanity_checks(tmp)
            self.assertTrue(any("cannot be true when layer=bronze" in e for e in errors), errors)

    def test_gold_rejects_unsafe_or_missing_model_paths(self):
        with _tempdir() as tmp:
            self._make_bundle(
                tmp,
                layer="silver",
                with_bronze=False,
                with_silver=True,
                gold_enabled=True,
            )
            variables_path = tmp / "resources" / "variables.yml"
            variables = yaml.safe_load(variables_path.read_text())
            variables["variables"]["gold_models_path"]["default"] = "../outside"
            variables_path.write_text(yaml.safe_dump(variables))
            errors = _sdp_meta_sanity_checks(tmp)
            self.assertTrue(any("cannot escape" in e for e in errors), errors)

    def test_gold_rejects_invalid_schema_and_pipeline_shape(self):
        with _tempdir() as tmp:
            self._make_bundle(
                tmp,
                layer="silver",
                with_bronze=False,
                with_silver=True,
                gold_enabled=True,
            )
            variables_path = tmp / "resources" / "variables.yml"
            variables = yaml.safe_load(variables_path.read_text())
            variables["variables"]["gold_target_schema"]["default"] = "bad-schema"
            variables_path.write_text(yaml.safe_dump(variables))
            pipelines_path = tmp / "resources" / "sdp_meta_pipelines.yml"
            pipelines = yaml.safe_load(pipelines_path.read_text())
            pipelines["resources"]["pipelines"]["gold"].pop("root_path")
            pipelines["resources"]["jobs"]["pipelines"]["tasks"][0]["depends_on"] = [
                {"task_key": "bronze"}
            ]
            pipelines_path.write_text(yaml.safe_dump(pipelines))
            errors = _sdp_meta_sanity_checks(tmp)
            self.assertTrue(any("gold_target_schema" in e for e in errors), errors)
            self.assertTrue(any("root_path" in e for e in errors), errors)
            self.assertTrue(any("depend only on `silver`" in e for e in errors), errors)

    def test_gold_accepts_non_default_root_path(self):
        with _tempdir() as tmp:
            self._make_bundle(
                tmp,
                layer="silver",
                with_bronze=False,
                with_silver=True,
                gold_enabled=True,
            )
            pipelines_path = tmp / "resources" / "sdp_meta_pipelines.yml"
            pipelines = yaml.safe_load(pipelines_path.read_text())
            pipelines["resources"]["pipelines"]["gold"]["root_path"] = (
                "${workspace.file_path}"
            )
            pipelines_path.write_text(yaml.safe_dump(pipelines))
            self.assertEqual(_sdp_meta_sanity_checks(tmp), [])

    def test_gold_rejects_invalid_sql_shapes_and_duplicate_names(self):
        with _tempdir() as tmp:
            self._make_bundle(
                tmp,
                layer="silver",
                with_bronze=False,
                with_silver=True,
                gold_enabled=True,
            )
            models = tmp / "gold" / "models"
            (models / "model.sql").write_text(
                "CREATE OR REFRESH MATERIALIZED VIEW duplicate_name AS SELECT 1;\n"
            )
            (models / "duplicate.sql").write_text(
                "CREATE OR REFRESH MATERIALIZED VIEW DUPLICATE_NAME AS SELECT 2;\n"
            )
            (models / "multiple.sql").write_text(
                "CREATE OR REFRESH MATERIALIZED VIEW first_model AS SELECT 1;\n"
                "CREATE OR REFRESH MATERIALIZED VIEW second_model AS SELECT 2;\n"
            )
            (models / "missing.sql").write_text("CREATE TEMPORARY VIEW v AS SELECT 1;\n")
            (models / "deprecated.sql").write_text(
                "CREATE OR REFRESH LIVE TABLE old_model AS SELECT 1;\n"
            )
            (models / "deprecated_streaming.sql").write_text(
                "CREATE OR REFRESH STREAMING LIVE TABLE old_stream AS SELECT 1;\n"
            )
            (models / "template.sql").write_text(
                "CREATE OR REFRESH MATERIALIZED VIEW templated AS "
                "SELECT * FROM {{ source }};\n"
            )
            errors = _sdp_meta_sanity_checks(tmp)
            self.assertTrue(any("also declared" in e for e in errors), errors)
            self.assertTrue(any("found 2" in e for e in errors), errors)
            self.assertTrue(any("found 0" in e for e in errors), errors)
            self.assertTrue(any("LIVE TABLE" in e for e in errors), errors)
            self.assertTrue(
                any(
                    "deprecated_streaming.sql" in e and "LIVE TABLE" in e
                    for e in errors
                ),
                errors,
            )
            self.assertTrue(any("template marker" in e for e in errors), errors)

    def test_gold_warns_when_onboarding_silver_schema_differs(self):
        with _tempdir() as tmp:
            self._make_bundle(
                tmp,
                layer="silver",
                with_bronze=False,
                with_silver=True,
                gold_enabled=True,
            )
            (tmp / "conf" / "onboarding.yml").write_text(
                yaml.safe_dump(
                    [{
                        "data_flow_id": "1",
                        "data_flow_group": "g",
                        "silver_database_dev": "other.curated",
                    }]
                )
            )
            warnings = []
            errors = _sdp_meta_sanity_checks(tmp, warnings=warnings)
            self.assertEqual(errors, [])
            self.assertTrue(any("Gold may not see" in warning for warning in warnings), warnings)
            self.assertTrue(
                any("variables.yml defaults" in warning for warning in warnings),
                warnings,
            )

    def test_gold_silver_warning_handles_split_catalog_and_ignores_quarantine(self):
        with _tempdir() as tmp:
            self._make_bundle(
                tmp,
                layer="silver",
                with_bronze=False,
                with_silver=True,
                gold_enabled=True,
            )
            (tmp / "conf" / "onboarding.yml").write_text(
                yaml.safe_dump(
                    [{
                        "data_flow_id": "1",
                        "data_flow_group": "g",
                        "silver_catalog_dev": "main",
                        "silver_database_dev": "sdp_meta_silver",
                        "silver_database_quarantine_dev": "other.quarantine",
                    }]
                )
            )
            warnings = []
            errors = _sdp_meta_sanity_checks(tmp, warnings=warnings)
            self.assertEqual(errors, [])
            self.assertEqual(warnings, [])

    def test_sentinel_dependency_is_flagged(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp, layer="bronze", with_bronze=True, with_silver=False)
            vars_yml = tmp / "resources" / "variables.yml"
            doc = yaml.safe_load(vars_yml.read_text())
            doc["variables"]["sdp_meta_dependency"]["default"] = "__SET_ME__"
            vars_yml.write_text(yaml.safe_dump(doc))
            errors = _sdp_meta_sanity_checks(tmp)
            self.assertTrue(any("__SET_ME__" in e for e in errors), errors)

    def test_volume_path_with_pypi_string_is_flagged(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp, layer="bronze", with_bronze=True, with_silver=False)
            vars_yml = tmp / "resources" / "variables.yml"
            doc = yaml.safe_load(vars_yml.read_text())
            doc["variables"]["wheel_source"]["default"] = "volume_path"
            doc["variables"]["sdp_meta_dependency"]["default"] = "databricks-labs-sdp-meta==0.1.0"
            vars_yml.write_text(yaml.safe_dump(doc))
            errors = _sdp_meta_sanity_checks(tmp)
            self.assertTrue(
                any("volume_path" in e and "/Volumes/" in e for e in errors), errors
            )

    def test_pypi_with_volume_path_is_flagged(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp, layer="bronze", with_bronze=True, with_silver=False)
            vars_yml = tmp / "resources" / "variables.yml"
            doc = yaml.safe_load(vars_yml.read_text())
            doc["variables"]["wheel_source"]["default"] = "pypi"
            doc["variables"]["sdp_meta_dependency"]["default"] = "/Volumes/c/s/v/whl.whl"
            vars_yml.write_text(yaml.safe_dump(doc))
            errors = _sdp_meta_sanity_checks(tmp)
            self.assertTrue(any("wheel_source=pypi" in e for e in errors), errors)

    def test_volume_path_happy_path(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp, layer="bronze", with_bronze=True, with_silver=False)
            vars_yml = tmp / "resources" / "variables.yml"
            doc = yaml.safe_load(vars_yml.read_text())
            doc["variables"]["wheel_source"]["default"] = "volume_path"
            doc["variables"]["sdp_meta_dependency"]["default"] = (
                "/Volumes/main/sdp/wheels/databricks_labs_sdp_meta-0.1.0-py3-none-any.whl"
            )
            vars_yml.write_text(yaml.safe_dump(doc))
            self.assertEqual(_sdp_meta_sanity_checks(tmp), [])

    def test_missing_variables_yml_short_circuits(self):
        with _tempdir() as tmp:
            (tmp / "databricks.yml").write_text("bundle: {name: t}\n")
            errors = _sdp_meta_sanity_checks(tmp)
            self.assertEqual(len(errors), 1)
            self.assertIn("variables.yml", errors[0])

    def _seed_eventhub_flow(self, tmp: Path, **overrides):
        """Replace the default onboarding stub with an Event Hubs flow whose
        source_details defaults match the seeded template (so we can prove the
        placeholder check fires on the same shape the template emits)."""
        details = {
            "eventhub.namespace": "<your-eventhub-namespace>",
            "eventhub.name": "example-hub",
            "eventhub.port": "9093",
            "eventhub.accessKeyName": "<your-sas-policy-name>",
            "eventhub.accessKeySecretName": "<your-secret-name>",
            "eventhub.secretsScopeName": "<your-secret-scope>",
        }
        details.update(overrides)
        flow = {
            "data_flow_id": "100",
            "data_flow_group": "g",
            "source_format": "eventhub",
            "source_details": details,
        }
        (tmp / "conf" / "onboarding.yml").write_text(yaml.safe_dump([flow]))

    def test_unedited_placeholder_in_source_details_is_flagged(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp, layer="bronze", with_bronze=True, with_silver=False)
            self._seed_eventhub_flow(tmp)
            errors = _sdp_meta_sanity_checks(tmp)
            placeholder_errors = [e for e in errors if "placeholder" in e]
            # All four <your-...> values should be reported by name.
            self.assertEqual(len(placeholder_errors), 4, placeholder_errors)
            joined = "\n".join(placeholder_errors)
            for field_name in (
                "source_details.eventhub.namespace",
                "source_details.eventhub.accessKeyName",
                "source_details.eventhub.accessKeySecretName",
                "source_details.eventhub.secretsScopeName",
            ):
                self.assertIn(field_name, joined)
            self.assertTrue(
                all("data_flow_id='100'" in e for e in placeholder_errors),
                placeholder_errors,
            )

    def test_real_values_in_source_details_pass(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp, layer="bronze", with_bronze=True, with_silver=False)
            self._seed_eventhub_flow(
                tmp,
                **{
                    "eventhub.namespace": "prod-ns",
                    "eventhub.accessKeyName": "ProdSendListenKey",
                    "eventhub.accessKeySecretName": "prod-eh-key",
                    "eventhub.secretsScopeName": "prod-secrets",
                },
            )
            self.assertEqual(_sdp_meta_sanity_checks(tmp), [])

    def test_placeholder_at_top_level_flow_field_is_flagged(self):
        # A <your-...> in any flow field — not just source_details — should
        # also fire. Use bronze_table to prove the recursion works on the
        # outermost dict too.
        with _tempdir() as tmp:
            self._make_bundle(tmp, layer="bronze", with_bronze=True, with_silver=False)
            (tmp / "conf" / "onboarding.yml").write_text(
                yaml.safe_dump([{
                    "data_flow_id": "200",
                    "data_flow_group": "g",
                    "source_format": "delta",
                    "bronze_table": "<your-table-name>",
                }])
            )
            errors = _sdp_meta_sanity_checks(tmp)
            placeholder_errors = [e for e in errors if "placeholder" in e]
            self.assertEqual(len(placeholder_errors), 1, placeholder_errors)
            self.assertIn("`bronze_table`", placeholder_errors[0])
            self.assertIn("<your-table-name>", placeholder_errors[0])

    def test_placeholder_check_ignores_bare_angle_brackets(self):
        # The check is intentionally narrow (`<your-...>` only) so that
        # legitimate angle-bracket content (eg. an XML-shaped column comment)
        # doesn't trigger false positives.
        with _tempdir() as tmp:
            self._make_bundle(tmp, layer="bronze", with_bronze=True, with_silver=False)
            (tmp / "conf" / "onboarding.yml").write_text(
                yaml.safe_dump([{
                    "data_flow_id": "300",
                    "data_flow_group": "g",
                    "bronze_table_comment": "<not-a-placeholder> just punctuation",
                }])
            )
            errors = _sdp_meta_sanity_checks(tmp)
            self.assertEqual([e for e in errors if "placeholder" in e], [])

    # --- databricks.yml placeholder coverage --------------------------------
    # The same `<your-...>` rule that polices the onboarding file is now
    # extended to the bundle config itself, so users who uncomment the
    # production `run_as` block (or any other guidance comment we ship)
    # without filling in the value are caught at validate time instead of
    # discovering it on a failed `bundle deploy --target prod`.

    def test_commented_databricks_yml_placeholder_is_not_flagged(self):
        # Comments are stripped at YAML parse time, so a placeholder that
        # only appears inside a `# ...` line must not produce an error.
        # This is the default state of the shipped template -- regressing
        # it would create a "you must edit databricks.yml" error on every
        # freshly-rendered bundle.
        with _tempdir() as tmp:
            self._make_bundle(tmp, layer="bronze", with_bronze=True, with_silver=False)
            (tmp / "databricks.yml").write_text(
                "bundle: {name: t}\n"
                "targets:\n"
                "  prod:\n"
                "    mode: production\n"
                "    # run_as:\n"
                "    #   service_principal_name: <your-prod-service-principal-application-id>\n"
            )
            errors = _sdp_meta_sanity_checks(tmp)
            self.assertEqual(
                [e for e in errors if "databricks.yml" in e], [],
                "commented placeholders must not be flagged",
            )

    def test_uncommented_databricks_yml_placeholder_is_flagged(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp, layer="bronze", with_bronze=True, with_silver=False)
            (tmp / "databricks.yml").write_text(
                "bundle: {name: t}\n"
                "targets:\n"
                "  prod:\n"
                "    mode: production\n"
                "    run_as:\n"
                "      service_principal_name: <your-prod-service-principal-application-id>\n"
            )
            errors = _sdp_meta_sanity_checks(tmp)
            placeholder_errors = [e for e in errors if "databricks.yml" in e]
            self.assertEqual(len(placeholder_errors), 1, placeholder_errors)
            self.assertIn(
                "targets.prod.run_as.service_principal_name",
                placeholder_errors[0],
            )
            self.assertIn(
                "<your-prod-service-principal-application-id>",
                placeholder_errors[0],
            )

    def test_databricks_yml_placeholder_check_recurses_into_lists(self):
        # `permissions:` is a list of dicts in real bundles; make sure the
        # walker doesn't silently skip placeholders nested inside list items.
        with _tempdir() as tmp:
            self._make_bundle(tmp, layer="bronze", with_bronze=True, with_silver=False)
            (tmp / "databricks.yml").write_text(
                "bundle: {name: t}\n"
                "targets:\n"
                "  prod:\n"
                "    mode: production\n"
                "    permissions:\n"
                "      - user_name: <your-admin-user-email>\n"
                "        level: CAN_MANAGE\n"
            )
            errors = _sdp_meta_sanity_checks(tmp)
            placeholder_errors = [e for e in errors if "databricks.yml" in e]
            self.assertEqual(len(placeholder_errors), 1, placeholder_errors)
            self.assertIn(
                "targets.prod.permissions[0].user_name",
                placeholder_errors[0],
            )

    def test_real_databricks_yml_values_pass(self):
        # An uncommented run_as block with a *real* SP application id must
        # not produce a placeholder error -- only the literal `<your-...>`
        # marker should fire.
        with _tempdir() as tmp:
            self._make_bundle(tmp, layer="bronze", with_bronze=True, with_silver=False)
            (tmp / "databricks.yml").write_text(
                "bundle: {name: t}\n"
                "targets:\n"
                "  prod:\n"
                "    mode: production\n"
                "    run_as:\n"
                "      service_principal_name: 12345678-1234-1234-1234-123456789012\n"
            )
            errors = _sdp_meta_sanity_checks(tmp)
            self.assertEqual([e for e in errors if "databricks.yml" in e], [])


class BundleInitUnitTests(unittest.TestCase):
    """bundle_init wraps `databricks bundle init`; mock the subprocess."""

    @patch("databricks.labs.sdp_meta.bundle._run")
    @patch("databricks.labs.sdp_meta.bundle._resolve_databricks_cli", return_value="/fake/databricks")
    def test_bundle_init_invokes_databricks_cli(self, _cli, run):
        run.return_value = subprocess.CompletedProcess(args=[], returncode=0)
        with _tempdir() as tmp:
            rc = bundle_init(BundleInitCommand(output_dir=str(tmp)))
            self.assertEqual(rc, 0)
            run.assert_called_once()
            argv = run.call_args[0][0]
            self.assertEqual(argv[0], "/fake/databricks")
            self.assertEqual(argv[1:4], ["bundle", "init", str(TEMPLATE_DIR)])
            self.assertIn("--output-dir", argv)

    @patch("databricks.labs.sdp_meta.bundle._run")
    @patch("databricks.labs.sdp_meta.bundle._resolve_databricks_cli", return_value="/fake/databricks")
    def test_bundle_init_forwards_config_and_profile(self, _cli, run):
        run.return_value = subprocess.CompletedProcess(args=[], returncode=0)
        with _tempdir() as tmp:
            cfg = tmp / "cfg.json"
            cfg.write_text("{}")
            rc = bundle_init(
                BundleInitCommand(output_dir=str(tmp), config_file=str(cfg), profile="DEFAULT")
            )
            self.assertEqual(rc, 0)
            argv = run.call_args[0][0]
            self.assertIn("--config-file", argv)
            # Resolve both sides — macOS maps /var → /private/var.
            resolved_argv = [str(Path(a).resolve()) if Path(a).exists() else a for a in argv]
            self.assertIn(str(cfg.resolve()), resolved_argv)
            self.assertIn("--profile", argv)
            self.assertIn("DEFAULT", argv)

    @patch("databricks.labs.sdp_meta.bundle._run")
    @patch("databricks.labs.sdp_meta.bundle._resolve_databricks_cli", return_value="/fake/databricks")
    def test_bundle_init_returns_nonzero_on_failure(self, _cli, run):
        run.return_value = subprocess.CompletedProcess(args=[], returncode=7)
        with _tempdir() as tmp:
            rc = bundle_init(BundleInitCommand(output_dir=str(tmp)))
            self.assertEqual(rc, 7)

    @patch("databricks.labs.sdp_meta.bundle._run")
    @patch("databricks.labs.sdp_meta.bundle._resolve_databricks_cli", return_value="/fake/databricks")
    def test_success_message_points_at_nested_bundle_dir(self, _cli, run):
        """`--output-dir` is the *parent*; the bundle lands in
        <output-dir>/<bundle_name>. The wrapper's own message must print that
        real nested path (the template's own `cd <bundle_name>` hint ignores
        --output-dir and is misleading)."""
        import io
        from contextlib import redirect_stdout

        run.return_value = subprocess.CompletedProcess(args=[], returncode=0)
        with _tempdir() as tmp:
            # Simulate what `databricks bundle init` creates: a project folder
            # holding a databricks.yml under the output (parent) dir.
            bundle = Path(tmp) / "my_sdp_meta_pipeline"
            bundle.mkdir()
            (bundle / "databricks.yml").write_text("bundle:\n  name: x\n")
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = bundle_init(BundleInitCommand(output_dir=str(tmp)))
            self.assertEqual(rc, 0)
            out = buf.getvalue()
            self.assertIn(str(bundle.resolve()), out)
            self.assertIn("cd ", out)

    @patch("databricks.labs.sdp_meta.bundle._run")
    @patch("databricks.labs.sdp_meta.bundle._resolve_databricks_cli", return_value="/fake/databricks")
    def test_bundle_init_stamps_installed_version_into_variables(self, _cli, run):
        """After a successful init, the scaffolded resources/variables.yml has
        its sdp_meta_version `true` sentinel replaced with the installed
        version so bundle-deployed pipelines tag sdp_meta=<version>."""
        run.return_value = subprocess.CompletedProcess(args=[], returncode=0)
        with _tempdir() as tmp:
            bundle = Path(tmp) / "my_sdp_meta_pipeline"
            (bundle / "resources").mkdir(parents=True)
            (bundle / "databricks.yml").write_text("bundle:\n  name: x\n")
            (bundle / "resources" / "variables.yml").write_text(
                "variables:\n"
                "  sdp_meta_version:\n"
                "    description: |\n"
                "      multi-line description\n"
                "      second line\n"
                '    default: "true"\n'
                "  serverless:\n"
                "    default: true\n"
            )
            rc = bundle_init(BundleInitCommand(output_dir=str(tmp)))
            self.assertEqual(rc, 0)
            doc = yaml.safe_load((bundle / "resources" / "variables.yml").read_text())
            self.assertEqual(doc["variables"]["sdp_meta_version"]["default"], __version__)
            # Unrelated variables are untouched.
            self.assertEqual(doc["variables"]["serverless"]["default"], True)


class StampSdpMetaVersionTests(unittest.TestCase):
    """Unit tests for the best-effort version stamp helper."""

    def _write_vars(self, root: Path, default_value: str) -> Path:
        (root / "resources").mkdir(parents=True)
        vy = root / "resources" / "variables.yml"
        vy.write_text(
            "variables:\n"
            "  uc_catalog_name:\n"
            "    default: main\n"
            "  sdp_meta_version:\n"
            "    description: tag value\n"
            f'    default: "{default_value}"\n'
        )
        return vy

    def test_replaces_true_sentinel_with_version(self):
        with _tempdir() as tmp:
            vy = self._write_vars(Path(tmp), "true")
            _stamp_sdp_meta_version(Path(tmp))
            doc = yaml.safe_load(vy.read_text())
            self.assertEqual(doc["variables"]["sdp_meta_version"]["default"], __version__)
            self.assertEqual(doc["variables"]["uc_catalog_name"]["default"], "main")

    def test_noop_when_default_not_sentinel(self):
        """A user-customised default is left alone (only the shipped `true`
        sentinel is stamped)."""
        with _tempdir() as tmp:
            vy = self._write_vars(Path(tmp), "0.9.9")
            _stamp_sdp_meta_version(Path(tmp))
            doc = yaml.safe_load(vy.read_text())
            self.assertEqual(doc["variables"]["sdp_meta_version"]["default"], "0.9.9")

    def test_noop_when_variables_file_missing(self):
        with _tempdir() as tmp:
            # No resources/variables.yml — must not raise.
            _stamp_sdp_meta_version(Path(tmp))


class DiscoverBundleDirTests(unittest.TestCase):
    """`_discover_bundle_dir` recovers the real bundle folder under the
    output (parent) dir so callers can print an accurate `cd`."""

    def test_prefers_bundle_name_from_config_file(self):
        with _tempdir() as tmp:
            out = Path(tmp)
            bundle = out / "chosen_name"
            bundle.mkdir()
            (bundle / "databricks.yml").write_text("bundle:\n  name: x\n")
            cfg = out / "cfg.json"
            cfg.write_text(json.dumps({"bundle_name": "chosen_name"}))
            found = _discover_bundle_dir(out, str(cfg))
            self.assertEqual(found, bundle)

    def test_falls_back_to_newest_subdir_with_databricks_yml(self):
        with _tempdir() as tmp:
            out = Path(tmp)
            bundle = out / "some_bundle"
            bundle.mkdir()
            (bundle / "databricks.yml").write_text("bundle:\n  name: x\n")
            # A sibling dir without databricks.yml must be ignored.
            (out / "not_a_bundle").mkdir()
            found = _discover_bundle_dir(out, None)
            self.assertEqual(found, bundle)

    def test_returns_none_when_no_bundle_dir(self):
        with _tempdir() as tmp:
            self.assertIsNone(_discover_bundle_dir(Path(tmp), None))


class QuickstartConfigFileTests(unittest.TestCase):
    """Pure-Python coverage for the `bundle-init --quickstart` config builder.

    Doesn't touch the `databricks` CLI -- that path is exercised by the
    `EndToEndRenderTests.test_quickstart_renders_runnable_bundle` smoke test."""

    def test_writes_json_with_every_default_key(self):
        with _tempdir() as tmp:
            path = write_quickstart_config_file(tmp)
            self.assertTrue(path.is_file())
            self.assertEqual(path.parent.resolve(), Path(tmp).resolve())
            written = json.loads(path.read_text())
            for key, expected in QUICKSTART_BUNDLE_INIT_DEFAULTS.items():
                self.assertEqual(written[key], expected, f"key {key!r}")

    def test_quickstart_keeps_set_me_sentinel_for_dependency(self):
        # Quickstart deliberately leaves `sdp_meta_dependency` as the
        # sentinel so users see the (intended) `bundle-validate` failure
        # and consciously pick PyPI vs UC-volume wheel before deploy.
        # If somebody "helpfully" replaces this with a real value, both
        # the bundle-validate sentinel-check error and the docs flow
        # become incoherent.
        self.assertEqual(
            QUICKSTART_BUNDLE_INIT_DEFAULTS["sdp_meta_dependency"], "__SET_ME__"
        )

    def test_quickstart_creates_dest_dir_if_missing(self):
        with _tempdir() as tmp:
            nested = Path(tmp) / "does" / "not" / "exist"
            path = write_quickstart_config_file(nested)
            self.assertTrue(path.is_file())

    def test_overrides_change_uc_catalog_name_only(self):
        with _tempdir() as tmp:
            path = write_quickstart_config_file(
                tmp, overrides={"uc_catalog_name": "acme_prod"}
            )
            written = json.loads(path.read_text())
            # The override took effect...
            self.assertEqual(written["uc_catalog_name"], "acme_prod")
            # ...and every other default is untouched.
            for key, expected in QUICKSTART_BUNDLE_INIT_DEFAULTS.items():
                if key == "uc_catalog_name":
                    continue
                self.assertEqual(written[key], expected, f"key {key!r}")

    def test_overrides_accept_multiple_fields(self):
        with _tempdir() as tmp:
            path = write_quickstart_config_file(
                tmp,
                overrides={
                    "uc_catalog_name": "acme_prod",
                    "sdp_meta_schema": "meta",
                    "sdp_meta_dependency": "databricks-labs-sdp-meta==0.1.0",
                    "source_format": "delta",
                    "layer": "bronze",
                },
            )
            written = json.loads(path.read_text())
            self.assertEqual(written["uc_catalog_name"], "acme_prod")
            self.assertEqual(written["sdp_meta_schema"], "meta")
            self.assertEqual(
                written["sdp_meta_dependency"], "databricks-labs-sdp-meta==0.1.0"
            )
            self.assertEqual(written["source_format"], "delta")
            self.assertEqual(written["layer"], "bronze")

    def test_overrides_reject_unknown_key(self):
        with _tempdir() as tmp:
            with self.assertRaisesRegex(ValueError, "unknown quickstart override"):
                write_quickstart_config_file(tmp, overrides={"uc_catalog": "x"})

    def test_overrides_reject_hyphenated_catalog(self):
        with _tempdir() as tmp:
            with self.assertRaisesRegex(ValueError, "uc_catalog_name"):
                write_quickstart_config_file(
                    tmp, overrides={"uc_catalog_name": "bad-catalog"}
                )

    def test_overrides_reject_invalid_enum(self):
        with _tempdir() as tmp:
            with self.assertRaisesRegex(ValueError, "layer"):
                write_quickstart_config_file(tmp, overrides={"layer": "platinum"})

    def test_overrides_reject_plugin_not_supported_by_template(self):
        with _tempdir() as tmp:
            with self.assertRaisesRegex(ValueError, "quality_engine"):
                write_quickstart_config_file(
                    tmp, overrides={"quality_engine": "synthetic"}
                )

    def test_overrides_reject_invalid_source_format(self):
        with _tempdir() as tmp:
            with self.assertRaises(ValueError):
                write_quickstart_config_file(
                    tmp, overrides={"source_format": "parquet"}
                )

    def test_gold_overrides_accept_valid_values(self):
        with _tempdir() as tmp:
            path = write_quickstart_config_file(
                tmp,
                overrides={
                    "gold_enabled": True,
                    "gold_target_schema": "analytics_gold",
                    "gold_models_path": "sql/gold_models",
                },
            )
            written = json.loads(path.read_text())
            self.assertIs(written["gold_enabled"], True)
            self.assertEqual(written["gold_target_schema"], "analytics_gold")
            self.assertEqual(written["gold_models_path"], "sql/gold_models")

    def test_gold_overrides_reject_invalid_values(self):
        invalid_overrides = (
            ({"gold_enabled": "true"}, "gold_enabled"),
            ({"gold_target_schema": "bad-schema"}, "gold_target_schema"),
            ({"gold_models_path": "../gold"}, "gold_models_path"),
            ({"gold_models_path": "/gold/models"}, "gold_models_path"),
        )
        with _tempdir() as tmp:
            for override, field in invalid_overrides:
                with self.subTest(override=override):
                    with self.assertRaisesRegex(ValueError, field):
                        write_quickstart_config_file(tmp, overrides=override)

    def test_none_overrides_is_pure_defaults(self):
        with _tempdir() as tmp:
            path = write_quickstart_config_file(tmp, overrides=None)
            written = json.loads(path.read_text())
            self.assertEqual(written, dict(QUICKSTART_BUNDLE_INIT_DEFAULTS))


class BundlePrepareWheelUnitTests(unittest.TestCase):
    """bundle_prepare_wheel builds a wheel and uploads it via WorkspaceClient.

    We mock the wheel-build step (``_run``) and the ``WorkspaceClient`` so the
    test neither invokes ``pip wheel`` nor talks to a real workspace. The test
    pre-seeds the expected wheel in the repo's ``dist/`` directory so the glob
    that picks the wheel finds something.
    """

    def test_rejects_missing_required_args(self):
        with self.assertRaises(ValueError):
            bundle_prepare_wheel(
                BundlePrepareWheelCommand(uc_catalog="", uc_schema="s", uc_volume="v")
            )

    @patch("databricks.labs.sdp_meta.bundle._run")
    def test_uploads_to_expected_volume_path(self, run):
        run.return_value = subprocess.CompletedProcess(args=[], returncode=0)

        from databricks.labs.sdp_meta.__about__ import __version__
        dist = REPO_ROOT / "dist"
        dist.mkdir(exist_ok=True)
        wheel_name = f"databricks_labs_sdp_meta-{__version__}-py3-none-any.whl"
        wheel_path = dist / wheel_name
        pre_existed = wheel_path.exists()
        if not pre_existed:
            wheel_path.write_bytes(b"fake wheel bytes for test")

        try:
            with patch("databricks.sdk.WorkspaceClient") as WS:
                ws_instance = MagicMock()
                WS.return_value = ws_instance
                result = bundle_prepare_wheel(
                    BundlePrepareWheelCommand(
                        uc_catalog="cat", uc_schema="sch", uc_volume="vol"
                    )
                )
            expected = f"/Volumes/cat/sch/vol/{wheel_name}"
            self.assertEqual(result, expected)
            ws_instance.files.upload.assert_called_once()
            upload_kwargs = ws_instance.files.upload.call_args.kwargs
            self.assertEqual(upload_kwargs["file_path"], expected)
            self.assertTrue(upload_kwargs["overwrite"])
        finally:
            if not pre_existed and wheel_path.exists():
                wheel_path.unlink()

    @patch("databricks.labs.sdp_meta.bundle._run")
    def test_pip_index_url_is_forwarded_to_pip_wheel(self, run):
        """pip_index_url + pip_extra_index_urls land as flags in the pip argv."""
        run.return_value = subprocess.CompletedProcess(args=[], returncode=0)

        from databricks.labs.sdp_meta.__about__ import __version__
        dist = REPO_ROOT / "dist"
        dist.mkdir(exist_ok=True)
        wheel_name = f"databricks_labs_sdp_meta-{__version__}-py3-none-any.whl"
        wheel_path = dist / wheel_name
        pre_existed = wheel_path.exists()
        if not pre_existed:
            wheel_path.write_bytes(b"fake wheel bytes for test")
        try:
            with patch("databricks.sdk.WorkspaceClient") as WS:
                WS.return_value = MagicMock()
                bundle_prepare_wheel(
                    BundlePrepareWheelCommand(
                        uc_catalog="cat", uc_schema="sch", uc_volume="vol",
                        pip_index_url="https://pypi-proxy.dev.databricks.com/simple",
                        pip_extra_index_urls=[
                            "https://internal.example.com/simple",
                            "https://other.example.com/simple",
                        ],
                    )
                )
            argv = run.call_args.args[0]
            self.assertIn("--index-url", argv)
            self.assertEqual(
                argv[argv.index("--index-url") + 1],
                "https://pypi-proxy.dev.databricks.com/simple",
            )
            self.assertEqual(argv.count("--extra-index-url"), 2)
            self.assertIn("https://internal.example.com/simple", argv)
            self.assertIn("https://other.example.com/simple", argv)
            # Sanity: the pip wheel command still ends with the repo root.
            self.assertEqual(argv[-1], str(REPO_ROOT))
        finally:
            if not pre_existed and wheel_path.exists():
                wheel_path.unlink()

    def _seed_wheel(self):
        """Seed dist/<expected wheel> so the file glob in bundle_prepare_wheel
        picks it up. Returns (wheel_path, pre_existed) for cleanup."""
        from databricks.labs.sdp_meta.__about__ import __version__
        dist = REPO_ROOT / "dist"
        dist.mkdir(exist_ok=True)
        wheel_path = dist / f"databricks_labs_sdp_meta-{__version__}-py3-none-any.whl"
        pre_existed = wheel_path.exists()
        if not pre_existed:
            wheel_path.write_bytes(b"fake wheel bytes for test")
        return wheel_path, pre_existed

    @patch("databricks.labs.sdp_meta.bundle._run")
    def test_creates_schema_when_missing_and_create_if_missing_true(self, run):
        """Default create_if_missing=True should auto-create a missing schema."""
        run.return_value = subprocess.CompletedProcess(args=[], returncode=0)
        wheel_path, pre_existed = self._seed_wheel()
        try:
            with patch("databricks.sdk.WorkspaceClient") as WS:
                ws = MagicMock()
                WS.return_value = ws
                ws.catalogs.get.return_value = MagicMock()  # catalog exists
                ws.schemas.get.side_effect = Exception("Schema 'cat.sch' does not exist.")
                bundle_prepare_wheel(
                    BundlePrepareWheelCommand(uc_catalog="cat", uc_schema="sch", uc_volume="vol")
                )
            ws.catalogs.get.assert_called_once_with(name="cat")
            ws.schemas.create.assert_called_once_with(name="sch", catalog_name="cat")
            ws.volumes.create.assert_called_once()
            ws.files.upload.assert_called_once()
        finally:
            if not pre_existed and wheel_path.exists():
                wheel_path.unlink()

    @patch("databricks.labs.sdp_meta.bundle._run")
    def test_does_not_create_schema_when_create_if_missing_false(self, run):
        """create_if_missing=False keeps the old hard-fail behavior."""
        run.return_value = subprocess.CompletedProcess(args=[], returncode=0)
        wheel_path, pre_existed = self._seed_wheel()
        try:
            with patch("databricks.sdk.WorkspaceClient") as WS:
                ws = MagicMock()
                WS.return_value = ws
                ws.catalogs.get.return_value = MagicMock()
                ws.schemas.get.side_effect = Exception("Schema 'cat.sch' does not exist.")
                with self.assertRaises(RuntimeError) as ctx:
                    bundle_prepare_wheel(
                        BundlePrepareWheelCommand(
                            uc_catalog="cat", uc_schema="sch", uc_volume="vol",
                            create_if_missing=False,
                        )
                    )
            self.assertIn("create_if_missing=False", str(ctx.exception))
            ws.schemas.create.assert_not_called()
            ws.volumes.create.assert_not_called()
        finally:
            if not pre_existed and wheel_path.exists():
                wheel_path.unlink()

    @patch("databricks.labs.sdp_meta.bundle._run")
    def test_missing_catalog_is_actionable_error(self, run):
        """A missing catalog is never auto-created; we raise a clear message."""
        run.return_value = subprocess.CompletedProcess(args=[], returncode=0)
        wheel_path, pre_existed = self._seed_wheel()
        try:
            with patch("databricks.sdk.WorkspaceClient") as WS:
                ws = MagicMock()
                WS.return_value = ws
                ws.catalogs.get.side_effect = Exception("Catalog 'nope' does not exist.")
                with self.assertRaises(RuntimeError) as ctx:
                    bundle_prepare_wheel(
                        BundlePrepareWheelCommand(
                            uc_catalog="nope", uc_schema="sch", uc_volume="vol"
                        )
                    )
            msg = str(ctx.exception)
            self.assertIn("Catalogs are not auto-created", msg)
            self.assertIn("'nope'", msg)
            ws.schemas.get.assert_not_called()
            ws.schemas.create.assert_not_called()
        finally:
            if not pre_existed and wheel_path.exists():
                wheel_path.unlink()

    @patch("databricks.labs.sdp_meta.bundle._run")
    def test_volumes_create_uses_volume_type_enum(self, run):
        """Regression: SDK >=0.40 requires VolumeType enum, not the string "MANAGED"."""
        from databricks.sdk.service.catalog import VolumeType

        run.return_value = subprocess.CompletedProcess(args=[], returncode=0)
        wheel_path, pre_existed = self._seed_wheel()
        try:
            with patch("databricks.sdk.WorkspaceClient") as WS:
                ws = MagicMock()
                WS.return_value = ws
                ws.catalogs.get.return_value = MagicMock()
                ws.schemas.get.return_value = MagicMock()
                bundle_prepare_wheel(
                    BundlePrepareWheelCommand(uc_catalog="cat", uc_schema="sch", uc_volume="vol")
                )
            kwargs = ws.volumes.create.call_args.kwargs
            self.assertIs(kwargs["volume_type"], VolumeType.MANAGED,
                          "volume_type must be the VolumeType enum, not a string")
            self.assertEqual(kwargs["catalog_name"], "cat")
            self.assertEqual(kwargs["schema_name"], "sch")
            self.assertEqual(kwargs["name"], "vol")
        finally:
            if not pre_existed and wheel_path.exists():
                wheel_path.unlink()

    @patch("databricks.labs.sdp_meta.bundle._run")
    def test_existing_volume_is_reused_silently(self, run):
        """ALREADY_EXISTS on volumes.create is a no-op (re-run path)."""
        run.return_value = subprocess.CompletedProcess(args=[], returncode=0)
        wheel_path, pre_existed = self._seed_wheel()
        try:
            with patch("databricks.sdk.WorkspaceClient") as WS:
                ws = MagicMock()
                WS.return_value = ws
                ws.catalogs.get.return_value = MagicMock()
                ws.schemas.get.return_value = MagicMock()
                ws.volumes.create.side_effect = Exception(
                    "ALREADY_EXISTS: Volume cat.sch.vol already exists"
                )
                bundle_prepare_wheel(
                    BundlePrepareWheelCommand(uc_catalog="cat", uc_schema="sch", uc_volume="vol")
                )
            ws.files.upload.assert_called_once()
        finally:
            if not pre_existed and wheel_path.exists():
                wheel_path.unlink()

    @patch("databricks.labs.sdp_meta.bundle._run")
    def test_volume_create_other_error_surfaces(self, run):
        """Non-ALREADY_EXISTS errors from volumes.create must NOT be swallowed."""
        run.return_value = subprocess.CompletedProcess(args=[], returncode=0)
        wheel_path, pre_existed = self._seed_wheel()
        try:
            with patch("databricks.sdk.WorkspaceClient") as WS:
                ws = MagicMock()
                WS.return_value = ws
                ws.catalogs.get.return_value = MagicMock()
                ws.schemas.get.return_value = MagicMock()
                ws.volumes.create.side_effect = Exception(
                    "PERMISSION_DENIED: missing CREATE_VOLUME on cat.sch"
                )
                with self.assertRaises(RuntimeError) as ctx:
                    bundle_prepare_wheel(
                        BundlePrepareWheelCommand(
                            uc_catalog="cat", uc_schema="sch", uc_volume="vol"
                        )
                    )
            self.assertIn("PERMISSION_DENIED", str(ctx.exception))
            ws.files.upload.assert_not_called()
        finally:
            if not pre_existed and wheel_path.exists():
                wheel_path.unlink()

    @patch.dict(os.environ, {"PIP_INDEX_URL": "https://env-fallback.example.com/simple"}, clear=False)
    @patch("databricks.labs.sdp_meta.bundle._run")
    def test_pip_index_url_falls_back_to_env(self, run):
        run.return_value = subprocess.CompletedProcess(args=[], returncode=0)

        from databricks.labs.sdp_meta.__about__ import __version__
        dist = REPO_ROOT / "dist"
        dist.mkdir(exist_ok=True)
        wheel_name = f"databricks_labs_sdp_meta-{__version__}-py3-none-any.whl"
        wheel_path = dist / wheel_name
        pre_existed = wheel_path.exists()
        if not pre_existed:
            wheel_path.write_bytes(b"fake wheel bytes for test")
        try:
            with patch("databricks.sdk.WorkspaceClient") as WS:
                WS.return_value = MagicMock()
                bundle_prepare_wheel(
                    BundlePrepareWheelCommand(
                        uc_catalog="cat", uc_schema="sch", uc_volume="vol"
                    )
                )
            argv = run.call_args.args[0]
            self.assertIn("--index-url", argv)
            self.assertEqual(
                argv[argv.index("--index-url") + 1],
                "https://env-fallback.example.com/simple",
            )
        finally:
            if not pre_existed and wheel_path.exists():
                wheel_path.unlink()


class BundleIdentifierValidationTests(unittest.TestCase):
    """Strict UC-identifier validation at every bundle input boundary.

    The bundle module accepts UC names from three places: the
    ``BundlePrepareWheelCommand`` constructor, the bundle's own
    ``variables.yml`` (read by ``_flow_to_dict``), and the per-flow
    ``FlowSpec`` (bronze/silver table). We must reject anything that
    isn't a regular SQL identifier at every one of them so a hyphenated
    catalog or other illegal name can't sneak through to the deployed
    pipeline (issue #261).
    """

    def test_prepare_wheel_rejects_hyphenated_catalog(self):
        with self.assertRaisesRegex(ValueError, r"regular identifier"):
            BundlePrepareWheelCommand(
                uc_catalog="my-cat", uc_schema="schema", uc_volume="vol",
            )

    def test_prepare_wheel_rejects_dotted_schema(self):
        with self.assertRaisesRegex(ValueError, r"regular identifier"):
            BundlePrepareWheelCommand(
                uc_catalog="cat", uc_schema="bad.name", uc_volume="vol",
            )

    def test_prepare_wheel_rejects_leading_digit_volume(self):
        with self.assertRaisesRegex(ValueError, r"regular identifier"):
            BundlePrepareWheelCommand(
                uc_catalog="cat", uc_schema="schema", uc_volume="9vol",
            )

    def test_prepare_wheel_accepts_regular_identifiers(self):
        # Sanity: the validation is strict but not over-eager. Plain
        # underscore-separated names continue to work.
        cmd = BundlePrepareWheelCommand(
            uc_catalog="my_cat", uc_schema="my_schema", uc_volume="my_volume",
        )
        self.assertEqual(cmd.uc_catalog, "my_cat")

    def test_flow_to_dict_rejects_hyphenated_variables(self):
        # Hand-edited variables.yml could carry a hyphenated catalog from
        # an older sdp-meta version; we must reject when reading too.
        from databricks.labs.sdp_meta.bundle import _flow_to_dict
        variables = {
            "uc_catalog_name": {"default": "my-cat"},
            "bronze_target_schema": {"default": "bronze"},
            "silver_target_schema": {"default": "silver"},
            "layer": {"default": "bronze"},
            "onboarding_file_format": {"default": "yaml"},
        }
        with self.assertRaisesRegex(ValueError, r"regular identifier"):
            _flow_to_dict(
                FlowSpec(source_format="cloudFiles", bronze_table="t"),
                variables,
                assigned_id="100",
            )

    def test_flow_to_dict_rejects_hyphenated_bronze_table(self):
        from databricks.labs.sdp_meta.bundle import _flow_to_dict
        variables = {
            "uc_catalog_name": {"default": "main"},
            "bronze_target_schema": {"default": "bronze"},
            "silver_target_schema": {"default": "silver"},
            "layer": {"default": "bronze"},
            "onboarding_file_format": {"default": "yaml"},
        }
        with self.assertRaisesRegex(ValueError, r"regular identifier"):
            _flow_to_dict(
                FlowSpec(source_format="cloudFiles", bronze_table="bad-name"),
                variables,
                assigned_id="100",
            )

    def test_flow_to_dict_rejects_unknown_source_format(self):
        # The five-format set is pinned in identifiers.SUPPORTED_SOURCE_FORMATS;
        # _flow_to_dict must reject anything outside it (e.g. "parquet")
        # so a hand-edited bundle can't ship a flow that the bronze
        # readers in dataflow_pipeline.py have no branch for.
        from databricks.labs.sdp_meta.bundle import _flow_to_dict
        variables = {
            "uc_catalog_name": {"default": "main"},
            "bronze_target_schema": {"default": "bronze"},
            "silver_target_schema": {"default": "silver"},
            "layer": {"default": "bronze"},
            "onboarding_file_format": {"default": "yaml"},
        }
        with self.assertRaises(ValueError) as ctx:
            _flow_to_dict(
                FlowSpec(source_format="parquet", bronze_table="t"),
                variables,
                assigned_id="100",
            )
        msg = str(ctx.exception)
        # Allowed values must be inlined for the user.
        self.assertIn("parquet", msg)
        self.assertIn("cloudFiles", msg)


class BundleValidateUnitTests(unittest.TestCase):
    """bundle_validate shells out to `databricks bundle validate` plus sanity."""

    @patch("databricks.labs.sdp_meta.bundle._run")
    @patch("databricks.labs.sdp_meta.bundle._resolve_databricks_cli", return_value="/fake/databricks")
    def test_returns_zero_on_clean_bundle(self, _cli, run):
        run.return_value = subprocess.CompletedProcess(args=[], returncode=0)
        with _tempdir() as tmp:
            self._make_bundle(tmp)
            rc = bundle_validate(BundleValidateCommand(bundle_dir=str(tmp), target="dev"))
            self.assertEqual(rc, 0)

    @patch("databricks.labs.sdp_meta.bundle._run")
    @patch("databricks.labs.sdp_meta.bundle._resolve_databricks_cli", return_value="/fake/databricks")
    def test_propagates_databricks_validate_failure(self, _cli, run):
        run.return_value = subprocess.CompletedProcess(args=[], returncode=5)
        with _tempdir() as tmp:
            self._make_bundle(tmp)
            rc = bundle_validate(BundleValidateCommand(bundle_dir=str(tmp)))
            self.assertEqual(rc, 5)

    @patch("databricks.labs.sdp_meta.bundle._run")
    @patch("databricks.labs.sdp_meta.bundle._resolve_databricks_cli", return_value="/fake/databricks")
    def test_sanity_check_failure_returns_nonzero(self, _cli, run):
        run.return_value = subprocess.CompletedProcess(args=[], returncode=0)
        with _tempdir() as tmp:
            self._make_bundle(tmp)
            (tmp / "conf" / "onboarding.yml").unlink()  # break it
            rc = bundle_validate(BundleValidateCommand(bundle_dir=str(tmp)))
            self.assertEqual(rc, 1)

    def test_rejects_non_bundle_directory(self):
        with _tempdir() as tmp:
            rc = bundle_validate(BundleValidateCommand(bundle_dir=str(tmp)))
            self.assertEqual(rc, 2)

    def _make_bundle(self, tmp: Path):
        (tmp / "databricks.yml").write_text("bundle: {name: t}\n")
        (tmp / "resources").mkdir()
        (tmp / "conf").mkdir()
        (tmp / "resources" / "variables.yml").write_text(
            yaml.safe_dump({
                "variables": {
                    "layer": {"default": "bronze"},
                    "dataflow_group": {"default": "g"},
                    "onboarding_file_name": {"default": "onboarding.yml"},
                    "wheel_source": {"default": "pypi"},
                    "sdp_meta_dependency": {"default": "databricks-labs-sdp-meta==0.1.0"},
                }
            })
        )
        (tmp / "resources" / "sdp_meta_pipelines.yml").write_text(
            yaml.safe_dump({"resources": {"pipelines": {"bronze": {}}}})
        )
        (tmp / "conf" / "onboarding.yml").write_text(
            yaml.safe_dump([{"data_flow_id": "1", "data_flow_group": "g"}])
        )


@unittest.skipUnless(shutil.which("databricks"), "databricks CLI not on PATH")
class EndToEndRenderTests(unittest.TestCase):
    """Render the real template via `databricks bundle init` and inspect the
    output. Skipped if the databricks CLI is not installed (keeps CI/dev fast
    when it isn't available)."""

    def _render(self, answers: dict, out: Path) -> Path:
        cfg = out / "answers.json"
        cfg.write_text(json.dumps(answers))
        # ``bundle init`` refuses to start without default auth ("cannot
        # configure default credentials") even though this template renders
        # purely locally and calls no workspace API. Supply throwaway
        # credentials so the render never depends on the developer's
        # ``~/.databrickscfg`` -- otherwise it passes on a configured laptop
        # and fails anywhere clean, CI included.
        env = {
            **os.environ,
            "DATABRICKS_HOST": "https://sdp-meta-render-test.invalid",
            "DATABRICKS_TOKEN": "not-a-real-token",
        }
        proc = subprocess.run(
            [
                "databricks", "bundle", "init", str(TEMPLATE_DIR),
                "--output-dir", str(out),
                "--config-file", str(cfg),
            ],
            env=env,
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            self.fail(
                "`databricks bundle init` failed with exit "
                f"{proc.returncode}\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
            )
        rendered = out / answers["bundle_name"]
        self.assertTrue(rendered.is_dir(), f"rendered bundle not found: {rendered}")
        return rendered

    def _strip_placeholders(self, onboarding: Path, ext: str) -> None:
        """Replace every `<your-...>` value in the rendered onboarding file
        with a real-looking string so the placeholder validator stops
        firing. Used by render tests whose intent is shape, not deploy
        readiness."""
        text = onboarding.read_text()
        # JSON and YAML both render `<your-...>` as bare characters inside a
        # quoted scalar, so a flat str.replace is sufficient and
        # round-trip-safe (no need to parse + re-serialize).
        cleaned = re.sub(r"<your-([^>]+)>", r"placeholder-\1", text)
        if cleaned != text:
            onboarding.write_text(cleaned)

    def _common_answers(self, **overrides) -> dict:
        base = {
            "bundle_name": "demo_bundle",
            "uc_catalog_name": "main",
            "sdp_meta_schema": "sdp_meta_dataflowspecs",
            "bronze_target_schema": "sdp_meta_bronze",
            "silver_target_schema": "sdp_meta_silver",
            "layer": "bronze_silver",
            "pipeline_mode": "split",
            "source_format": "cloudFiles",
            "onboarding_file_format": "yaml",
            "quality_engine": "none",
            "managed_quality_migrations": False,
            "gold_enabled": False,
            "gold_target_schema": "sdp_meta_gold",
            "gold_models_path": "gold/models",
            "dataflow_group": "demo_group",
            "wheel_source": "pypi",
            "sdp_meta_dependency": "databricks-labs-sdp-meta==0.1.0",
            "author": "maintainer",
        }
        base.update(overrides)
        return base

    def _assert_rendered_bundle_is_valid(
        self, rendered: Path, *, expect_ext: str, expect_layer: str, expect_pipeline_mode: str = "split",
    ):
        # Every YAML resource file is parseable.
        for yml in (rendered / "resources").glob("*.yml"):
            yaml.safe_load(yml.read_text())

        # Onboarding file has the expected extension and parses.
        onboarding = rendered / "conf" / f"onboarding.{expect_ext}"
        self.assertTrue(onboarding.is_file(), f"missing: {onboarding}")
        text = onboarding.read_text()
        if expect_ext == "yml":
            parsed = yaml.safe_load(text)
        else:
            parsed = json.loads(text)
        self.assertIsInstance(parsed, list)
        self.assertGreaterEqual(len(parsed), 1)
        self.assertEqual(parsed[0]["data_flow_group"], "demo_group")

        # Strip seeded `<your-...>` placeholders before the sanity-check
        # comparison: the helper's job is "rendered bundle is structurally
        # sound", not "ready to deploy". Tests that explicitly want to lock
        # in the placeholder-detection behavior call _sdp_meta_sanity_checks
        # directly on a non-pre-cleaned bundle.
        self._strip_placeholders(onboarding, expect_ext)

        # Pipeline file matches the requested layer + mode.
        pipelines_doc = yaml.safe_load((rendered / "resources" / "sdp_meta_pipelines.yml").read_text())
        pipes = pipelines_doc["resources"]["pipelines"]
        if expect_layer == "bronze":
            self.assertEqual(set(pipes), {"bronze"})
        elif expect_layer == "silver":
            self.assertEqual(set(pipes), {"silver"})
        elif expect_pipeline_mode == "combined":
            self.assertEqual(set(pipes), {"bronze_silver"})
            cfg = pipes["bronze_silver"]["configuration"]
            self.assertEqual(cfg["layer"], "bronze_silver")
            self.assertIn("bronze.dataflowspecTable", cfg)
            self.assertIn("silver.dataflowspecTable", cfg)
        else:
            self.assertEqual(set(pipes), {"bronze", "silver"})

        runner = pipelines_doc["resources"]["jobs"]["pipelines"]
        for task in runner["tasks"]:
            self.assertIn("pipeline_task", task)
            self.assertNotIn("python_wheel_task", task)
        self.assertNotIn("environments", runner)

        # Every pipeline carries the sdp_meta tag (wired to the
        # sdp_meta_version bundle variable) so the SDP-META App and the
        # workspace UI tag search find bundle-deployed pipelines the same
        # way they find ones created by `sdp-meta deploy`.
        for pname, pdef in pipes.items():
            self.assertEqual(
                (pdef.get("tags") or {}).get("sdp_meta"),
                "${var.sdp_meta_version}",
                f"pipeline {pname!r} is missing the sdp_meta tag",
            )

        # Sanity checks must pass on the rendered bundle.
        self.assertEqual(_sdp_meta_sanity_checks(rendered), [])

        # Recipes ship pre-rendered (no .tmpl), reference the real bundle
        # primitives, and have the bundle's catalog substituted in.
        recipes_dir = rendered / "recipes"
        self.assertTrue(recipes_dir.is_dir(), f"missing: {recipes_dir}")
        for name in (
            "README.md", "from_uc.py", "from_volume.py", "from_topics.py", "from_inventory.py",
        ):
            self.assertTrue((recipes_dir / name).is_file(), f"missing: {recipes_dir / name}")
        for py_recipe in ("from_uc.py", "from_volume.py", "from_topics.py", "from_inventory.py"):
            text = (recipes_dir / py_recipe).read_text()
            self.assertIn("from databricks.labs.sdp_meta.bundle import", text)
            self.assertIn("bundle_add_flow", text)
            self.assertIn("FlowSpec", text)
            # Catalog has been Go-templated in (no `{{...}}` left over).
            self.assertNotIn("{{", text, f"unrendered Go template in {py_recipe}")
            # Each recipe should also be syntactically valid Python.
            compile(text, py_recipe, "exec")

    def test_bronze_silver_yaml_cloudfiles(self):
        with _tempdir() as tmp:
            rendered = self._render(self._common_answers(), tmp)
            self._assert_rendered_bundle_is_valid(rendered, expect_ext="yml", expect_layer="bronze_silver")

    def test_bronze_silver_yaml_lakeflow_quality_engine(self):
        with _tempdir() as tmp:
            rendered = self._render(
                self._common_answers(
                    layer="bronze_silver",
                    quality_engine="lakeflow",
                ),
                tmp,
            )
            onboarding = yaml.safe_load(
                (rendered / "conf" / "onboarding.yml").read_text()
            )[0]
            self.assertEqual(onboarding["bronze_quality_engine"], "lakeflow")
            self.assertEqual(
                onboarding["bronze_quarantine_table"],
                "example_table_quarantine",
            )
            self.assertNotIn(
                "bronze_data_quality_expectations_json_dev", onboarding
            )
            self.assertEqual(onboarding["silver_quality_engine"], "lakeflow")
            self.assertEqual(
                onboarding["silver_quarantine_table"],
                "example_table_quarantine",
            )
            for layer in ("bronze", "silver"):
                rules = (
                    rendered / "conf" / "quality" / "example_table"
                    / f"{layer}_rules.yml"
                )
                self.assertTrue(rules.is_file())
                self.assertIn(
                    "expect_or_quarantine", yaml.safe_load(rules.read_text())
                )

    def test_managed_quality_migration_runner_is_explicit(self):
        with _tempdir() as tmp:
            rendered = self._render(
                self._common_answers(
                    layer="bronze",
                    quality_engine="lakeflow",
                    managed_quality_migrations=True,
                ),
                tmp,
            )
            pipelines_doc = yaml.safe_load(
                (
                    rendered
                    / "resources"
                    / "sdp_meta_pipelines.yml"
                ).read_text()
            )
            runner = pipelines_doc["resources"]["jobs"]["pipelines"]
            task = runner["tasks"][0]
            self.assertIn("python_wheel_task", task)
            self.assertNotIn("pipeline_task", task)
            self.assertEqual(
                task["python_wheel_task"]["entry_point"],
                "quality_migrate",
            )
            self.assertEqual(
                task["python_wheel_task"]["named_parameters"][
                    "timeout_seconds"
                ],
                "${var.quality_migration_timeout_seconds}",
            )
            self.assertEqual(
                task["timeout_seconds"],
                "${var.quality_migration_timeout_seconds}",
            )
            variables = yaml.safe_load(
                (rendered / "resources" / "variables.yml").read_text()
            )["variables"]
            self.assertEqual(
                variables["quality_migration_timeout_seconds"]["default"],
                10800,
            )
            self.assertIn("environments", runner)

    def test_lakeflow_snapshot_bundle_fails_static_validation(self):
        with _tempdir() as tmp:
            rendered = self._render(
                self._common_answers(
                    layer="bronze",
                    source_format="snapshot",
                    quality_engine="lakeflow",
                ),
                tmp,
            )
            errors = _sdp_meta_sanity_checks(rendered)
            self.assertTrue(
                any("with snapshot sources" in error for error in errors),
                errors,
            )

    def test_snapshot_bronze_with_standard_silver_quality_is_valid(self):
        with _tempdir() as tmp:
            rendered = self._render(
                self._common_answers(
                    layer="bronze_silver",
                    source_format="snapshot",
                    quality_engine="lakeflow",
                ),
                tmp,
            )
            onboarding_path = rendered / "conf" / "onboarding.yml"
            onboarding = yaml.safe_load(onboarding_path.read_text())
            flow = onboarding[0]
            flow.pop("bronze_quality_engine", None)
            for key in list(flow):
                if key.startswith("bronze_quality_rules_path_"):
                    flow.pop(key)
            onboarding_path.write_text(
                yaml.safe_dump(onboarding, sort_keys=False)
            )

            errors = _sdp_meta_sanity_checks(rendered)

            self.assertFalse(
                any("with snapshot sources" in error for error in errors),
                errors,
            )

    def test_delta_lakeflow_rules_do_not_reference_rescued_data(self):
        with _tempdir() as tmp:
            rendered = self._render(
                self._common_answers(
                    layer="bronze",
                    source_format="delta",
                    quality_engine="lakeflow",
                ),
                tmp,
            )
            rules = yaml.safe_load(
                (
                    rendered / "conf" / "quality" / "example_table"
                    / "bronze_rules.yml"
                ).read_text()
            )
            self.assertEqual(rules["expect"], {})

    def test_rendered_databricks_yml_keeps_run_as_block_commented(self):
        """E2E lock-in: the run_as guidance lives in the rendered bundle so
        users see it; it stays commented so a fresh `bundle deploy` works
        without an SP set up; and it uses the `<your-...>` placeholder
        convention so `bundle-validate` flags it the moment a user
        uncomments without filling it in."""
        with _tempdir() as tmp:
            rendered = self._render(self._common_answers(), tmp)
            db_yml = (rendered / "databricks.yml").read_text()
            self.assertIn("# run_as:", db_yml)
            self.assertRegex(
                db_yml,
                r"#\s+service_principal_name:\s*<your-prod-service-principal-application-id>",
            )
            # Belt-and-suspenders: parsed YAML (which drops comments) must
            # NOT contain a live run_as block under prod -- the shipped
            # template opts users in by uncomment, not by default.
            parsed = yaml.safe_load(db_yml)
            prod_target = parsed.get("targets", {}).get("prod", {})
            self.assertNotIn(
                "run_as", prod_target,
                "shipped prod target must not have a parsed (live) "
                "run_as block; users opt in by uncommenting",
            )
            # Sanity-check still passes (commented placeholder = no error).
            self._strip_placeholders(rendered / "conf" / "onboarding.yml", "yml")
            self.assertEqual(_sdp_meta_sanity_checks(rendered), [])

    def test_quickstart_renders_runnable_bundle(self):
        """Smoke test: feeding the QUICKSTART_BUNDLE_INIT_DEFAULTS dict to
        `databricks bundle init --config-file` produces a bundle that
        passes the sanity checks (modulo the deliberately-unfilled
        `__SET_ME__` sdp_meta_dependency, which we patch in here)."""
        with _tempdir() as tmp:
            rendered = self._render(QUICKSTART_BUNDLE_INIT_DEFAULTS, tmp)

            # Quickstart deliberately ships __SET_ME__; the validator must
            # flag it. That's the WHOLE POINT of leaving it as the sentinel.
            errors_before = _sdp_meta_sanity_checks(rendered)
            self.assertTrue(
                any("__SET_ME__" in e for e in errors_before),
                f"quickstart bundle must flag __SET_ME__; got: {errors_before}",
            )

            # Fix the sentinel + strip onboarding placeholders, then prove
            # the rest of the bundle is structurally sound.
            vars_yml = rendered / "resources" / "variables.yml"
            doc = yaml.safe_load(vars_yml.read_text())
            doc["variables"]["sdp_meta_dependency"]["default"] = (
                "databricks-labs-sdp-meta==0.1.0"
            )
            vars_yml.write_text(yaml.safe_dump(doc))
            self._strip_placeholders(rendered / "conf" / "onboarding.yml", "yml")
            self.assertEqual(_sdp_meta_sanity_checks(rendered), [])

    def test_bronze_only_json_kafka(self):
        with _tempdir() as tmp:
            rendered = self._render(
                self._common_answers(
                    layer="bronze",
                    source_format="kafka",
                    onboarding_file_format="json",
                ),
                tmp,
            )
            self._assert_rendered_bundle_is_valid(rendered, expect_ext="json", expect_layer="bronze")

    def test_silver_only_yaml_delta(self):
        with _tempdir() as tmp:
            rendered = self._render(
                self._common_answers(layer="silver", source_format="delta"),
                tmp,
            )
            self._assert_rendered_bundle_is_valid(rendered, expect_ext="yml", expect_layer="silver")

    def test_runner_notebook_cell_1_layout_matches_working_demo_runners(self):
        """Regression for two SDP-startup failures we hit on serverless:

          1) ``SyntaxError: incomplete input`` / ``NOTEBOOK_PIP_INSTALL_ERROR``
             when cell 1 has multi-line Python BEFORE ``%pip install``.
          2) ``Whl libraries are not supported`` when we tried to move the
             install into the pipeline's ``libraries:`` field.

        The only layout that works on serverless SDP is the same one the
        existing ``demo/notebooks/*_runners/init_sdp_meta_pipeline.py`` uses:
        cell 1 is exactly ``<varname> = spark.conf.get(...)`` followed
        immediately by ``%pip install $varname``, and a ``# COMMAND ----------``
        cell break before any other Python.
        """
        with _tempdir() as tmp:
            rendered = self._render(self._common_answers(), tmp)
            notebook = rendered / "notebooks" / "init_sdp_meta_pipeline.py"
            text = notebook.read_text()

            # Drop comment / blank lines to expose the actual code shape.
            code_lines = [ln for ln in text.splitlines()
                          if ln.strip() and not ln.lstrip().startswith("#")]

            # Find the cell break.
            try:
                cell_break = next(i for i, ln in enumerate(text.splitlines())
                                  if ln.strip() == "# COMMAND ----------")
            except StopIteration:
                self.fail("runner notebook has no `# COMMAND ----------` cell break")

            # Cell 1 (above the break) should have exactly two code lines:
            # the spark.conf.get assignment and the %pip install magic.
            cell1_code = [ln for ln in text.splitlines()[:cell_break]
                          if ln.strip() and not ln.lstrip().startswith("#")]
            self.assertEqual(len(cell1_code), 2,
                             f"cell 1 must be exactly <var = spark.conf.get(...)> + "
                             f"`%pip install $var`; got: {cell1_code}")
            self.assertIn("spark.conf.get", cell1_code[0])
            self.assertTrue(cell1_code[1].lstrip().startswith("%pip install"),
                            f"cell 1 line 2 must be `%pip install ...`; got: {cell1_code[1]!r}")
            self.assertIn("$", cell1_code[1],
                          "the `%pip install` line must reference the python "
                          "variable from the line above (e.g. `$sdp_meta_dependency`)")
            del code_lines  # silence unused

    def test_pipelines_yml_does_not_use_whl_or_pypi_libraries(self):
        """Serverless SDP rejects both `whl:` and `pypi:` library entries
        ("Whl libraries are not supported"). The pipeline must only declare
        the runner notebook; install happens inside the notebook."""
        for src in ("pypi", "volume_path"):
            with _tempdir() as tmp:
                rendered = self._render(
                    self._common_answers(
                        wheel_source=src,
                        sdp_meta_dependency=(
                            "databricks-labs-sdp-meta==0.1.0" if src == "pypi"
                            else "/Volumes/cat/sch/vol/databricks_labs_sdp_meta-0.1.0-py3-none-any.whl"
                        ),
                    ),
                    tmp,
                )
                doc = yaml.safe_load(
                    (rendered / "resources" / "sdp_meta_pipelines.yml").read_text()
                )
                pipes = doc["resources"]["pipelines"]
                self.assertGreater(len(pipes), 0)
                for name, spec in pipes.items():
                    libs = spec.get("libraries") or []
                    for lib in libs:
                        self.assertNotIn("whl", lib,
                                         f"pipeline {name!r} (wheel_source={src}): `whl:` "
                                         f"library entries break serverless SDP")
                        self.assertNotIn("pypi", lib,
                                         f"pipeline {name!r} (wheel_source={src}): `pypi:` "
                                         f"library entries break serverless SDP")
                    # The notebook entry MUST be present -- it's our only install path.
                    self.assertTrue(
                        any(isinstance(lib.get("notebook"), dict) for lib in libs),
                        f"pipeline {name!r}: missing notebook library entry",
                    )

    def test_freshly_rendered_kafka_bundle_flags_placeholders(self):
        """Lock-in: a kafka render contains `<your-kafka-host>:9092`, and
        bundle-validate's sanity check refuses to deploy until the user
        edits it. Same guarantee for the four `<your-...>` values in the
        eventhub branch — covered separately to keep failure messages
        scoped."""
        with _tempdir() as tmp:
            rendered = self._render(
                self._common_answers(
                    layer="bronze",
                    source_format="kafka",
                    onboarding_file_format="json",
                ),
                tmp,
            )
            errors = _sdp_meta_sanity_checks(rendered)
            placeholder_errors = [e for e in errors if "placeholder" in e]
            self.assertEqual(len(placeholder_errors), 1, placeholder_errors)
            self.assertIn("kafka.bootstrap.servers", placeholder_errors[0])
            self.assertIn("<your-kafka-host>", placeholder_errors[0])

    def test_freshly_rendered_eventhub_bundle_flags_placeholders(self):
        with _tempdir() as tmp:
            rendered = self._render(
                self._common_answers(
                    layer="bronze",
                    source_format="eventhub",
                ),
                tmp,
            )
            errors = _sdp_meta_sanity_checks(rendered)
            placeholder_errors = [e for e in errors if "placeholder" in e]
            # All four <your-...> defaults in the eventhub branch fire.
            self.assertEqual(len(placeholder_errors), 4, placeholder_errors)
            joined = "\n".join(placeholder_errors)
            for field_name in (
                "eventhub.namespace",
                "eventhub.accessKeyName",
                "eventhub.accessKeySecretName",
                "eventhub.secretsScopeName",
            ):
                self.assertIn(field_name, joined)

    def test_volume_path_sentinel_renders_and_is_flagged(self):
        """wheel_source=volume_path with __SET_ME__ default scaffolds cleanly,
        the runner notebook contains the fail-fast guard, and bundle-validate's
        sanity checks reject the placeholder until the user pastes a real path."""
        with _tempdir() as tmp:
            rendered = self._render(
                self._common_answers(
                    wheel_source="volume_path",
                    sdp_meta_dependency="__SET_ME__",
                ),
                tmp,
            )
            notebook = (rendered / "notebooks" / "init_sdp_meta_pipeline.py").read_text()
            self.assertIn("__SET_ME__", notebook)
            self.assertIn("raise ValueError", notebook)

            errors = _sdp_meta_sanity_checks(rendered)
            self.assertTrue(any("__SET_ME__" in e for e in errors), errors)

            vars_yml = rendered / "resources" / "variables.yml"
            doc = yaml.safe_load(vars_yml.read_text())
            doc["variables"]["sdp_meta_dependency"]["default"] = (
                "/Volumes/main/sdp_meta_dataflowspecs/sdp_meta_wheels/"
                "databricks_labs_sdp_meta-0.1.0-py3-none-any.whl"
            )
            vars_yml.write_text(yaml.safe_dump(doc))
            self.assertEqual(_sdp_meta_sanity_checks(rendered), [])

    def test_topics_recipe_writes_kafka_flows_to_rendered_bundle(self):
        """Render a bronze-only kafka bundle, import the rendered
        `recipes/from_topics.py` as a module, build 5 kafka FlowSpecs via its
        `build_flows` helper, and confirm they land in the onboarding file
        with auto-incremented data_flow_ids and pass post-add sanity checks."""
        import importlib.util
        from databricks.labs.sdp_meta.bundle import (
            BundleAddFlowCommand, bundle_add_flow,
        )

        with _tempdir() as tmp:
            rendered = self._render(
                self._common_answers(layer="bronze", source_format="kafka"),
                tmp,
            )

            recipe_path = rendered / "recipes" / "from_topics.py"
            spec = importlib.util.spec_from_file_location("rendered_from_topics", recipe_path)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)

            topics = ["orders", "customers", "events.v1", "payments-prod", "shipments"]
            flows = mod.build_flows(
                topics,
                source_format="kafka",
                bootstrap_servers="broker1:9092,broker2:9092",
            )
            self.assertEqual(len(flows), 5)
            # Topic names with `.` and `-` get sanitized for the table name.
            self.assertEqual({f.bronze_table for f in flows},
                             {"orders", "customers", "events_v1", "payments_prod", "shipments"})
            # Original topic strings preserved for the kafka subscribe field.
            self.assertIn("events.v1", {f.kafka_topic for f in flows})

            rc = bundle_add_flow(BundleAddFlowCommand(
                bundle_dir=str(rendered),
                flows=flows,
                dry_run=False,
            ))
            self.assertEqual(rc, 0)

            onboarding = yaml.safe_load(
                (rendered / "conf" / "onboarding.yml").read_text()
            )
            self.assertEqual(len(onboarding), 6)  # 1 seeded + 5 appended
            for entry in onboarding[1:]:
                self.assertEqual(entry["source_format"], "kafka")
                self.assertIn("subscribe", entry["source_details"])
                self.assertEqual(entry["source_details"]["kafka.bootstrap.servers"],
                                 "broker1:9092,broker2:9092")
                self.assertNotIn("silver_table", entry,
                                 "bronze-only bundle should not emit silver keys")
            # data_flow_id auto-incremented across the appended block.
            appended_ids = [e["data_flow_id"] for e in onboarding[1:]]
            self.assertEqual(appended_ids, sorted(appended_ids))
            self.assertEqual(len(set(appended_ids)), 5)
            # The kafka render ships the seeded flow with a `<your-...>`
            # placeholder; that's the new placeholder-validator behavior
            # exercised explicitly elsewhere. Strip it before asserting the
            # rest of the bundle is clean.
            self._strip_placeholders(rendered / "conf" / "onboarding.yml", "yml")
            self.assertEqual(_sdp_meta_sanity_checks(rendered), [])

    def test_bronze_silver_combined_single_pipeline(self):
        with _tempdir() as tmp:
            rendered = self._render(
                self._common_answers(layer="bronze_silver", pipeline_mode="combined"),
                tmp,
            )
            self._assert_rendered_bundle_is_valid(
                rendered, expect_ext="yml", expect_layer="bronze_silver", expect_pipeline_mode="combined"
            )

            jobs = yaml.safe_load(
                (rendered / "resources" / "sdp_meta_pipelines.yml").read_text()
            )["resources"]["jobs"]["pipelines"]
            self.assertEqual(len(jobs["tasks"]), 1)
            self.assertEqual(jobs["tasks"][0]["task_key"], "bronze_silver")

    def test_gold_is_absent_by_default(self):
        with _tempdir() as tmp:
            rendered = self._render(self._common_answers(), tmp)
            self.assertFalse(list((rendered / "gold").rglob("*.sql")))
            doc = yaml.safe_load(
                (rendered / "resources" / "sdp_meta_pipelines.yml").read_text()
            )
            self.assertNotIn("gold", doc["resources"]["pipelines"])
            self.assertNotIn(
                "gold",
                {
                    task["task_key"]
                    for task in doc["resources"]["jobs"]["pipelines"]["tasks"]
                },
            )

    def test_gold_renders_model_guidance_pipeline_and_split_dependency(self):
        with _tempdir() as tmp:
            rendered = self._render(
                self._common_answers(gold_enabled=True),
                tmp,
            )
            models = sorted((rendered / "gold" / "models").glob("*.sql"))
            self.assertEqual(models, [])
            self.assertIn(
                "Create `${var.gold_models_path}` and add native SDP SQL files",
                (rendered / "README.md").read_text(),
            )
            doc = yaml.safe_load(
                (rendered / "resources" / "sdp_meta_pipelines.yml").read_text()
            )
            gold_pipeline = doc["resources"]["pipelines"]["gold"]
            self.assertEqual(gold_pipeline["root_path"], "..")
            self.assertEqual(
                gold_pipeline["libraries"][0]["glob"]["include"],
                "../${var.gold_models_path}/**",
            )
            self.assertEqual(
                gold_pipeline["configuration"],
                {
                    "silver_catalog": "${var.uc_catalog_name}",
                    "silver_schema": "${var.silver_target_schema}",
                },
            )
            gold_task = next(
                task
                for task in doc["resources"]["jobs"]["pipelines"]["tasks"]
                if task["task_key"] == "gold"
            )
            self.assertEqual(gold_task["depends_on"], [{"task_key": "silver"}])
            self._strip_placeholders(rendered / "conf" / "onboarding.yml", "yml")
            errors = _sdp_meta_sanity_checks(rendered)
            self.assertEqual(len(errors), 1)
            self.assertIn(
                "add at least one Silver-compatible native SDP SQL model",
                errors[0],
            )
            (rendered / "gold" / "models" / "business_summary.sql").write_text(
                "CREATE OR REFRESH MATERIALIZED VIEW business_summary AS SELECT 1;\n"
            )
            self.assertEqual(_sdp_meta_sanity_checks(rendered), [])

    def test_gold_dependency_tracks_silver_producer_and_quality_wrapper(self):
        cases = (
            ({"layer": "silver"}, "silver"),
            (
                {"layer": "bronze_silver", "pipeline_mode": "combined"},
                "bronze_silver",
            ),
            (
                {
                    "layer": "bronze_silver",
                    "pipeline_mode": "split",
                    "managed_quality_migrations": True,
                },
                "silver",
            ),
        )
        for overrides, expected in cases:
            with self.subTest(overrides=overrides), _tempdir() as tmp:
                rendered = self._render(
                    self._common_answers(gold_enabled=True, **overrides),
                    tmp,
                )
                doc = yaml.safe_load(
                    (rendered / "resources" / "sdp_meta_pipelines.yml").read_text()
                )
                gold_task = next(
                    task
                    for task in doc["resources"]["jobs"]["pipelines"]["tasks"]
                    if task["task_key"] == "gold"
                )
                self.assertEqual(
                    gold_task["depends_on"], [{"task_key": expected}]
                )


# ---------------------------------------------------------------------------
# utilities
# ---------------------------------------------------------------------------

class _tempdir:
    """Tiny context manager — creates a TemporaryDirectory and yields a Path.
    Kept local to avoid adding a pytest dependency surface."""

    def __enter__(self) -> Path:
        import tempfile
        self._tmp = tempfile.TemporaryDirectory()
        return Path(self._tmp.name)

    def __exit__(self, *exc):
        self._tmp.cleanup()
        return False


class BundleAddGoldTests(unittest.TestCase):
    """Pure file-I/O tests for idempotent existing-bundle enablement."""

    def _make_bundle(
        self, tmp: Path, *, layer: str = "bronze_silver", mode: str = "split"
    ) -> Path:
        (tmp / "databricks.yml").write_text("bundle: {name: t}\n")
        resources = tmp / "resources"
        resources.mkdir()
        (resources / "variables.yml").write_text(
            "variables:\n"
            "  # preserve this user comment\n"
            f"  layer:\n    default: {layer}\n"
            f"  pipeline_mode:\n    default: {mode}\n"
        )
        upstream_pipelines = (
            "    bronze_silver: {}\n"
            if mode == "combined"
            else "    bronze: {}\n    silver: {}\n"
        )
        upstream_tasks = (
            "        - task_key: bronze_silver\n"
            if mode == "combined"
            else
            "        - task_key: bronze\n"
            "        - task_key: silver\n"
            "          depends_on:\n"
            "            - task_key: bronze\n"
        )
        (resources / "sdp_meta_pipelines.yml").write_text(
            "resources:\n"
            "  pipelines:\n"
            f"{upstream_pipelines}"
            "  jobs:\n"
            "    pipelines:\n"
            "      tasks:\n"
            f"{upstream_tasks}"
        )
        return tmp

    def test_add_gold_is_idempotent_and_preserves_existing_content(self):
        with _tempdir() as tmp:
            bundle = self._make_bundle(tmp)
            self.assertEqual(bundle_add_gold(BundleAddGoldCommand(str(bundle))), 0)
            first_variables = (bundle / "resources" / "variables.yml").read_text()
            first_pipelines = (
                bundle / "resources" / "sdp_meta_pipelines.yml"
            ).read_text()
            self.assertIn("# preserve this user comment", first_variables)
            self.assertFalse(list((bundle / "gold" / "models").glob("*.sql")))
            self.assertEqual(first_pipelines.count("    gold:"), 1)
            self.assertEqual(first_pipelines.count("- task_key: gold"), 1)

            self.assertEqual(bundle_add_gold(BundleAddGoldCommand(str(bundle))), 0)
            self.assertEqual(
                (bundle / "resources" / "variables.yml").read_text(),
                first_variables,
            )
            self.assertEqual(
                (bundle / "resources" / "sdp_meta_pipelines.yml").read_text(),
                first_pipelines,
            )

    def test_add_gold_rejects_existing_non_pipeline_gold_task(self):
        with _tempdir() as tmp:
            bundle = self._make_bundle(tmp)
            self.assertEqual(
                bundle_add_gold(BundleAddGoldCommand(str(bundle))),
                0,
            )
            variables_path = bundle / "resources" / "variables.yml"
            pipelines_path = bundle / "resources" / "sdp_meta_pipelines.yml"
            pipelines = yaml.safe_load(pipelines_path.read_text())
            gold_task = next(
                task
                for task in pipelines["resources"]["jobs"]["pipelines"]["tasks"]
                if task["task_key"] == "gold"
            )
            gold_task.pop("pipeline_task")
            gold_task["notebook_task"] = {"notebook_path": "/Shared/not-gold"}
            pipelines_path.write_text(yaml.safe_dump(pipelines, sort_keys=False))
            original_variables = variables_path.read_text()
            original_pipelines = pipelines_path.read_text()
            output = StringIO()

            self.assertEqual(
                bundle_add_gold(
                    BundleAddGoldCommand(str(bundle)),
                    output=output,
                ),
                2,
            )
            self.assertEqual(variables_path.read_text(), original_variables)
            self.assertEqual(pipelines_path.read_text(), original_pipelines)
            self.assertIn(
                "existing Gold resources do not match",
                output.getvalue(),
            )
            self.assertNotIn("generated edits did not produce", output.getvalue())

    def test_add_gold_supports_custom_models_path(self):
        with _tempdir() as tmp:
            bundle = self._make_bundle(tmp)
            variables_path = bundle / "resources" / "variables.yml"
            variables_path.write_text(
                variables_path.read_text()
                + "  gold_models_path:\n"
                + "    default: sql/gold_models\n"
            )

            self.assertEqual(
                bundle_add_gold(BundleAddGoldCommand(str(bundle))),
                0,
            )

            self.assertFalse((bundle / "sql" / "gold_models").exists())
            variables = yaml.safe_load(variables_path.read_text())["variables"]
            self.assertEqual(
                variables["gold_models_path"]["default"],
                "sql/gold_models",
            )
            pipelines = yaml.safe_load(
                (bundle / "resources" / "sdp_meta_pipelines.yml").read_text()
            )["resources"]
            self.assertIn("gold", pipelines["pipelines"])
            self.assertEqual(
                [
                    task["task_key"]
                    for task in pipelines["jobs"]["pipelines"]["tasks"]
                ].count("gold"),
                1,
            )

    def test_add_gold_supports_gold_as_models_path_without_guidance_collision(self):
        with _tempdir() as tmp:
            bundle = self._make_bundle(tmp)
            variables_path = bundle / "resources" / "variables.yml"
            variables_path.write_text(
                variables_path.read_text()
                + "  gold_models_path:\n"
                + "    default: gold\n"
            )

            self.assertEqual(
                bundle_add_gold(BundleAddGoldCommand(str(bundle))),
                0,
            )

            self.assertFalse((bundle / "gold").exists())

    def test_add_gold_rolls_back_when_a_replacement_fails(self):
        with _tempdir() as tmp:
            bundle = self._make_bundle(tmp)
            variables_path = bundle / "resources" / "variables.yml"
            pipelines_path = bundle / "resources" / "sdp_meta_pipelines.yml"
            original_variables = variables_path.read_text()
            original_pipelines = pipelines_path.read_text()
            real_replace = os.replace
            replace_calls = 0

            def flaky_replace(source, destination):
                nonlocal replace_calls
                replace_calls += 1
                if replace_calls == 2:
                    raise OSError("injected replacement failure")
                return real_replace(source, destination)

            with patch(
                "databricks.labs.sdp_meta.bundle.os.replace",
                side_effect=flaky_replace,
            ):
                self.assertEqual(
                    bundle_add_gold(BundleAddGoldCommand(str(bundle))),
                    2,
                )

            self.assertEqual(variables_path.read_text(), original_variables)
            self.assertEqual(pipelines_path.read_text(), original_pipelines)
            self.assertFalse((bundle / "gold" / "models").exists())
            self.assertFalse(list(bundle.rglob("*.tmp")))

    def test_add_gold_uses_combined_upstream(self):
        with _tempdir() as tmp:
            bundle = self._make_bundle(tmp, mode="combined")
            self.assertEqual(bundle_add_gold(BundleAddGoldCommand(str(bundle))), 0)
            doc = yaml.safe_load(
                (bundle / "resources" / "sdp_meta_pipelines.yml").read_text()
            )
            gold_task = doc["resources"]["jobs"]["pipelines"]["tasks"][-1]
            self.assertEqual(
                gold_task["depends_on"], [{"task_key": "bronze_silver"}]
            )

    def test_add_gold_rejects_bronze_only_bundle(self):
        with _tempdir() as tmp:
            bundle = self._make_bundle(tmp, layer="bronze")
            self.assertEqual(bundle_add_gold(BundleAddGoldCommand(str(bundle))), 2)
            self.assertFalse((bundle / "gold").exists())

    def test_add_gold_places_task_in_pipelines_job_when_another_job_follows(self):
        with _tempdir() as tmp:
            bundle = self._make_bundle(tmp)
            pipelines_path = bundle / "resources" / "sdp_meta_pipelines.yml"
            pipelines_path.write_text(
                pipelines_path.read_text()
                .replace("  jobs:\n", "  jobs:   \n")
                .replace("    pipelines:\n", "    pipelines:  \n")
                .replace("      tasks:\n", "      tasks: \n", 1)
                + "    reporting:\n"
                + "      tasks:\n"
                + "        - task_key: report\n"
            )
            self.assertEqual(bundle_add_gold(BundleAddGoldCommand(str(bundle))), 0)
            jobs = yaml.safe_load(pipelines_path.read_text())["resources"]["jobs"]
            self.assertEqual(
                [task["task_key"] for task in jobs["pipelines"]["tasks"]],
                ["bronze", "silver", "gold"],
            )
            self.assertEqual(
                [task["task_key"] for task in jobs["reporting"]["tasks"]],
                ["report"],
            )

    def test_add_gold_invalid_job_shape_leaves_bundle_unchanged(self):
        with _tempdir() as tmp:
            bundle = self._make_bundle(tmp)
            variables_path = bundle / "resources" / "variables.yml"
            pipelines_path = bundle / "resources" / "sdp_meta_pipelines.yml"
            pipelines_path.write_text(
                "resources:\n"
                "  pipelines:\n"
                "    silver: {}\n"
                "  jobs:\n"
                "    reporting:\n"
                "      tasks: []\n"
            )
            original_variables = variables_path.read_text()
            original_pipelines = pipelines_path.read_text()
            self.assertEqual(bundle_add_gold(BundleAddGoldCommand(str(bundle))), 2)
            self.assertEqual(variables_path.read_text(), original_variables)
            self.assertEqual(pipelines_path.read_text(), original_pipelines)
            self.assertFalse((bundle / "gold").exists())


class BundleAddFlowTests(unittest.TestCase):
    """Unit tests for the `bundle-add-flow` engine. Pure file I/O — no CLI."""

    def _make_bundle(
        self,
        tmp: Path,
        *,
        layer: str = "bronze_silver",
        onboarding_format: str = "yaml",
        seed_flows=None,
    ) -> Path:
        (tmp / "databricks.yml").write_text("bundle: {name: t}\n")
        (tmp / "resources").mkdir()
        (tmp / "conf").mkdir()
        ext = "yml" if onboarding_format == "yaml" else "json"
        onboarding_name = f"onboarding.{ext}"
        (tmp / "resources" / "variables.yml").write_text(
            yaml.safe_dump({
                "variables": {
                    "layer": {"default": layer},
                    "pipeline_mode": {"default": "split"},
                    "uc_catalog_name": {"default": "test_cat"},
                    "bronze_target_schema": {"default": "test_bronze"},
                    "silver_target_schema": {"default": "test_silver"},
                    "dataflow_group": {"default": "G1"},
                    "onboarding_file_name": {"default": onboarding_name},
                    "onboarding_file_format": {"default": onboarding_format},
                    "wheel_source": {"default": "pypi"},
                    "sdp_meta_dependency": {"default": "databricks-labs-sdp-meta==0.1.0"},
                }
            })
        )
        seed_flows = seed_flows if seed_flows is not None else []
        path = tmp / "conf" / onboarding_name
        if onboarding_format == "yaml":
            path.write_text(yaml.safe_dump(seed_flows))
        else:
            path.write_text(json.dumps(seed_flows))

        # Mirror the real template, which ships with a starter silver
        # transformations file containing one `example_table` row. Our
        # auto-seed helper appends to this file when new silver tables are
        # added; without it, the helper is a no-op.
        st_path = tmp / "conf" / f"silver_transformations.{ext}"
        starter = [{"target_table": "example_table", "select_exp": ["*"]}]
        if onboarding_format == "yaml":
            st_path.write_text(yaml.safe_dump(starter, sort_keys=False))
        else:
            st_path.write_text(json.dumps(starter, indent=2))
        return path

    def test_appends_single_cloudfiles_flow_to_yaml(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp, seed_flows=[{"data_flow_id": "100", "data_flow_group": "G1"}])
            cmd = BundleAddFlowCommand(
                bundle_dir=str(tmp),
                flows=[FlowSpec(
                    source_format="cloudFiles",
                    source_path="/Volumes/raw/landing/orders/",
                    bronze_table="orders",
                    silver_table="orders",
                )],
            )
            self.assertEqual(bundle_add_flow(cmd), 0)
            doc = yaml.safe_load((tmp / "conf" / "onboarding.yml").read_text())
            self.assertEqual(len(doc), 2)
            new = doc[1]
            self.assertEqual(new["data_flow_id"], "101")
            self.assertEqual(new["data_flow_group"], "G1")
            self.assertEqual(new["source_format"], "cloudFiles")
            self.assertEqual(new["source_details"]["source_path_dev"], "/Volumes/raw/landing/orders/")
            self.assertEqual(new["bronze_database_dev"], "test_cat.test_bronze")
            self.assertEqual(new["silver_database_dev"], "test_cat.test_silver")
            self.assertIn(
                "${workspace.file_path}/conf/dqe/orders/bronze_expectations.yml",
                new["bronze_data_quality_expectations_json_dev"],
            )

    def test_appends_to_json_onboarding_file(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp, onboarding_format="json", seed_flows=[])
            cmd = BundleAddFlowCommand(
                bundle_dir=str(tmp),
                flows=[FlowSpec(source_format="delta", bronze_table="t", silver_table="t")],
            )
            self.assertEqual(bundle_add_flow(cmd), 0)
            doc = json.loads((tmp / "conf" / "onboarding.json").read_text())
            self.assertEqual(len(doc), 1)
            self.assertEqual(doc[0]["data_flow_id"], "100")
            self.assertEqual(doc[0]["source_details"]["source_database"], "test_cat.landing")
            self.assertIn(
                "silver_transformations.json", doc[0]["silver_transformation_json_dev"]
            )

    def test_auto_id_increments_from_max_existing(self):
        with _tempdir() as tmp:
            self._make_bundle(
                tmp,
                seed_flows=[
                    {"data_flow_id": "100", "data_flow_group": "G1"},
                    {"data_flow_id": "203", "data_flow_group": "G1"},
                    {"data_flow_id": "non_numeric", "data_flow_group": "G1"},
                ],
            )
            cmd = BundleAddFlowCommand(
                bundle_dir=str(tmp),
                flows=[
                    FlowSpec(source_format="cloudFiles", bronze_table="a", silver_table="a"),
                    FlowSpec(source_format="cloudFiles", bronze_table="b", silver_table="b"),
                ],
            )
            self.assertEqual(bundle_add_flow(cmd), 0)
            doc = yaml.safe_load((tmp / "conf" / "onboarding.yml").read_text())
            new_ids = [d["data_flow_id"] for d in doc[3:]]
            self.assertEqual(new_ids, ["204", "205"])

    def test_explicit_duplicate_id_is_rejected(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp, seed_flows=[{"data_flow_id": "100", "data_flow_group": "G1"}])
            cmd = BundleAddFlowCommand(
                bundle_dir=str(tmp),
                flows=[FlowSpec(
                    source_format="cloudFiles", bronze_table="o", silver_table="o",
                    data_flow_id="100",
                )],
            )
            self.assertEqual(bundle_add_flow(cmd), 2)
            doc = yaml.safe_load((tmp / "conf" / "onboarding.yml").read_text())
            self.assertEqual(len(doc), 1)

    def test_dry_run_does_not_write(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp, seed_flows=[])
            before = (tmp / "conf" / "onboarding.yml").read_text()
            cmd = BundleAddFlowCommand(
                bundle_dir=str(tmp),
                flows=[FlowSpec(source_format="cloudFiles", bronze_table="x", silver_table="x")],
                dry_run=True,
            )
            self.assertEqual(bundle_add_flow(cmd), 0)
            self.assertEqual((tmp / "conf" / "onboarding.yml").read_text(), before)

    def test_kafka_requires_bootstrap_and_topic(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp, seed_flows=[])
            cmd = BundleAddFlowCommand(
                bundle_dir=str(tmp),
                flows=[FlowSpec(source_format="kafka", bronze_table="t", silver_table="t")],
            )
            with self.assertRaises(ValueError) as ctx:
                bundle_add_flow(cmd)
            self.assertIn("kafka_bootstrap_servers", str(ctx.exception))

    def test_silver_falls_back_to_bronze_table(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp, seed_flows=[])
            cmd = BundleAddFlowCommand(
                bundle_dir=str(tmp),
                flows=[FlowSpec(source_format="cloudFiles", bronze_table="orders")],
            )
            self.assertEqual(bundle_add_flow(cmd), 0)
            doc = yaml.safe_load((tmp / "conf" / "onboarding.yml").read_text())
            self.assertEqual(doc[0]["silver_table"], "orders")

    def test_layer_bronze_only_omits_silver_keys(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp, layer="bronze", seed_flows=[])
            cmd = BundleAddFlowCommand(
                bundle_dir=str(tmp),
                flows=[FlowSpec(source_format="cloudFiles", bronze_table="o")],
            )
            self.assertEqual(bundle_add_flow(cmd), 0)
            doc = yaml.safe_load((tmp / "conf" / "onboarding.yml").read_text())
            self.assertIn("bronze_table", doc[0])
            self.assertNotIn("silver_table", doc[0])

    def test_csv_batch_mode(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp, seed_flows=[])
            csv_path = tmp / "flows.csv"
            csv_path.write_text(
                "source_format,bronze_table,silver_table,source_path,data_flow_id\n"
                "cloudFiles,orders,orders,/V/raw/orders/,auto\n"
                "cloudFiles,customers,customers,/V/raw/customers/,auto\n"
                "delta,line_items,line_items,,500\n"
            )
            cmd = BundleAddFlowCommand(bundle_dir=str(tmp), from_csv=str(csv_path))
            self.assertEqual(bundle_add_flow(cmd), 0)
            doc = yaml.safe_load((tmp / "conf" / "onboarding.yml").read_text())
            self.assertEqual([d["data_flow_id"] for d in doc], ["100", "101", "500"])
            self.assertEqual([d["bronze_table"] for d in doc], ["orders", "customers", "line_items"])

    def test_csv_skips_comment_and_blank_lines(self):
        """Leading `#` comment lines and blanks are ignored; the first surviving
        line is the header. This is what makes the shipped self-documenting
        `conf/samples/flows.csv` parse correctly."""
        with _tempdir() as tmp:
            self._make_bundle(tmp, seed_flows=[])
            csv_path = tmp / "flows.csv"
            csv_path.write_text(
                "# Sample CSV for --from-csv.\n"
                "#   edit me\n"
                "\n"
                "source_format,bronze_table,silver_table,source_path,data_flow_id\n"
                "cloudFiles,orders,orders,/V/raw/orders/,auto\n"
                "  # an indented comment between rows is also skipped\n"
                "delta,line_items,line_items,,500\n"
            )
            cmd = BundleAddFlowCommand(bundle_dir=str(tmp), from_csv=str(csv_path))
            self.assertEqual(bundle_add_flow(cmd), 0)
            doc = yaml.safe_load((tmp / "conf" / "onboarding.yml").read_text())
            self.assertEqual([d["bronze_table"] for d in doc], ["orders", "line_items"])
            self.assertEqual([d["data_flow_id"] for d in doc], ["100", "500"])

    def test_rejects_non_bundle_dir(self):
        with _tempdir() as tmp:
            cmd = BundleAddFlowCommand(
                bundle_dir=str(tmp),
                flows=[FlowSpec(source_format="cloudFiles", bronze_table="t", silver_table="t")],
            )
            self.assertEqual(bundle_add_flow(cmd), 2)

    def test_csv_per_row_cloudfiles_format_override(self):
        """The `cloudfiles_format` column overrides the default `cloudFiles.format=json`."""
        with _tempdir() as tmp:
            self._make_bundle(tmp, seed_flows=[])
            csv_path = tmp / "flows.csv"
            # Three rows: two CSV (override), one JSON (omitted, should default).
            csv_path.write_text(
                "source_format,bronze_table,silver_table,source_path,cloudfiles_format,data_flow_id\n"
                "cloudFiles,customers,customers,/V/raw/customers/,csv,auto\n"
                "cloudFiles,products,products,/V/raw/products/,csv,auto\n"
                "cloudFiles,events,events,/V/raw/events/,,auto\n"
            )
            cmd = BundleAddFlowCommand(bundle_dir=str(tmp), from_csv=str(csv_path))
            self.assertEqual(bundle_add_flow(cmd), 0)
            doc = yaml.safe_load((tmp / "conf" / "onboarding.yml").read_text())
            formats = [d["bronze_reader_options"]["cloudFiles.format"] for d in doc]
            self.assertEqual(formats, ["csv", "csv", "json"])

    def test_csv_format_alias_accepts_dotted_and_short_forms(self):
        """`cloudFiles.format` and `format` columns map to cloudfiles_format too."""
        with _tempdir() as tmp:
            self._make_bundle(tmp, seed_flows=[])
            csv_path = tmp / "flows.csv"
            csv_path.write_text(
                "source_format,bronze_table,source_path,cloudFiles.format\n"
                "cloudFiles,a,/V/raw/a/,parquet\n"
            )
            self.assertEqual(
                bundle_add_flow(BundleAddFlowCommand(bundle_dir=str(tmp), from_csv=str(csv_path))),
                0,
            )
            doc = yaml.safe_load((tmp / "conf" / "onboarding.yml").read_text())
            self.assertEqual(doc[0]["bronze_reader_options"]["cloudFiles.format"], "parquet")

    def test_flowspec_cloudfiles_format_only_applies_to_cloudfiles_source(self):
        """Setting cloudfiles_format on a non-cloudFiles flow is a no-op."""
        with _tempdir() as tmp:
            self._make_bundle(tmp, seed_flows=[])
            cmd = BundleAddFlowCommand(
                bundle_dir=str(tmp),
                flows=[FlowSpec(
                    source_format="delta", bronze_table="x", silver_table="x",
                    cloudfiles_format="csv",  # ignored for delta
                )],
            )
            self.assertEqual(bundle_add_flow(cmd), 0)
            doc = yaml.safe_load((tmp / "conf" / "onboarding.yml").read_text())
            self.assertEqual(doc[0]["bronze_reader_options"], {})

    def test_post_add_flow_passes_sanity_checks(self):
        """End-to-end: appending a flow keeps the bundle valid."""
        with _tempdir() as tmp:
            self._make_bundle(tmp, seed_flows=[{"data_flow_id": "100", "data_flow_group": "G1"}])
            (tmp / "resources" / "sdp_meta_pipelines.yml").write_text(
                yaml.safe_dump({"resources": {"pipelines": {"bronze": {}, "silver": {}}}})
            )
            cmd = BundleAddFlowCommand(
                bundle_dir=str(tmp),
                flows=[FlowSpec(source_format="cloudFiles", bronze_table="o", silver_table="o")],
            )
            self.assertEqual(bundle_add_flow(cmd), 0)
            self.assertEqual(_sdp_meta_sanity_checks(tmp), [])


class SilverTransformationsAutoSeedTests(BundleAddFlowTests):
    """Regression for `[NO_TABLES_IN_PIPELINE]` on the silver SDP Pipeline.

    The onboarding job builds the silver dataflowspec via an INNER JOIN on
    `silver_transformation_json_df.target_table == silverDataflowSpec
    .targetDetails['table']`. If the silver_transformations file has no
    matching `target_table` for an onboarding row's `silver_table`, the row
    is dropped, the silver dataflowspec ends up empty, and the silver
    pipeline fails at startup with `Pipelines are expected to have at
    least one table defined`. `bundle_add_flow` must therefore auto-seed
    a default `target_table: <silver_table>, select_exp: ["*"]` row in the
    transformations file for every newly-added silver flow."""

    def _read_transformations(self, tmp: Path, ext: str = "yml"):
        path = tmp / "conf" / f"silver_transformations.{ext}"
        text = path.read_text()
        return yaml.safe_load(text) if ext == "yml" else json.loads(text)

    def test_appending_bronze_silver_flow_seeds_matching_silver_row(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp, seed_flows=[])
            cmd = BundleAddFlowCommand(
                bundle_dir=str(tmp),
                flows=[FlowSpec(source_format="cloudFiles",
                                source_path="/V/raw/orders/",
                                bronze_table="orders", silver_table="orders")],
            )
            self.assertEqual(bundle_add_flow(cmd), 0)
            rows = self._read_transformations(tmp)
            target_tables = {r["target_table"] for r in rows}
            # Starter `example_table` is preserved, our new `orders` row is added.
            self.assertIn("example_table", target_tables)
            self.assertIn("orders", target_tables)
            orders_row = next(r for r in rows if r["target_table"] == "orders")
            self.assertEqual(orders_row["select_exp"], ["*"])

    def test_does_not_duplicate_existing_target_table(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp, seed_flows=[])
            # Pre-populate transformations with a real `orders` row that the
            # user already customized; auto-seed must not overwrite it.
            (tmp / "conf" / "silver_transformations.yml").write_text(
                yaml.safe_dump([
                    {"target_table": "example_table", "select_exp": ["*"]},
                    {"target_table": "orders", "select_exp": ["id", "amount"]},
                ], sort_keys=False)
            )
            cmd = BundleAddFlowCommand(
                bundle_dir=str(tmp),
                flows=[FlowSpec(source_format="cloudFiles",
                                source_path="/V/raw/orders/",
                                bronze_table="orders", silver_table="orders")],
            )
            self.assertEqual(bundle_add_flow(cmd), 0)
            rows = self._read_transformations(tmp)
            orders_rows = [r for r in rows if r["target_table"] == "orders"]
            self.assertEqual(len(orders_rows), 1, "must not duplicate target_table")
            self.assertEqual(orders_rows[0]["select_exp"], ["id", "amount"],
                             "must preserve user-customized select_exp")

    def test_csv_with_many_silver_tables_seeds_all(self):
        """Mirrors the cloudfiles demo: 4 flows added via CSV must result in
        4 new silver_transformations rows so the silver pipeline isn't empty."""
        with _tempdir() as tmp:
            self._make_bundle(tmp, seed_flows=[])
            csv_path = tmp / "flows.csv"
            csv_path.write_text(
                "source_format,bronze_table,silver_table,source_path\n"
                "cloudFiles,customers,customers,/V/raw/customers/\n"
                "cloudFiles,transactions,transactions,/V/raw/transactions/\n"
                "cloudFiles,products,products,/V/raw/products/\n"
                "cloudFiles,stores,stores,/V/raw/stores/\n"
            )
            self.assertEqual(
                bundle_add_flow(BundleAddFlowCommand(bundle_dir=str(tmp), from_csv=str(csv_path))),
                0,
            )
            target_tables = {r["target_table"] for r in self._read_transformations(tmp)}
            for tbl in ("customers", "transactions", "products", "stores"):
                self.assertIn(tbl, target_tables, f"missing {tbl}; got {target_tables}")

    def test_bronze_only_layer_does_not_touch_transformations_file(self):
        """When the bundle is bronze-only there's no silver pipeline / file
        to keep in sync; the helper must be a strict no-op."""
        with _tempdir() as tmp:
            self._make_bundle(tmp, layer="bronze", seed_flows=[])
            before = (tmp / "conf" / "silver_transformations.yml").read_text()
            cmd = BundleAddFlowCommand(
                bundle_dir=str(tmp),
                flows=[FlowSpec(source_format="cloudFiles",
                                source_path="/V/raw/orders/",
                                bronze_table="orders")],
            )
            self.assertEqual(bundle_add_flow(cmd), 0)
            after = (tmp / "conf" / "silver_transformations.yml").read_text()
            self.assertEqual(before, after, "bronze-only must not modify transformations file")

    def test_json_onboarding_format_writes_json_transformations(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp, onboarding_format="json", seed_flows=[])
            cmd = BundleAddFlowCommand(
                bundle_dir=str(tmp),
                flows=[FlowSpec(source_format="cloudFiles",
                                source_path="/V/raw/orders/",
                                bronze_table="orders", silver_table="orders")],
            )
            self.assertEqual(bundle_add_flow(cmd), 0)
            rows = self._read_transformations(tmp, ext="json")
            self.assertIn("orders", {r["target_table"] for r in rows})

    def test_missing_transformations_file_is_a_quiet_noop(self):
        """If the user removed the transformations file, the helper should
        not crash -- it just skips seeding."""
        with _tempdir() as tmp:
            self._make_bundle(tmp, seed_flows=[])
            (tmp / "conf" / "silver_transformations.yml").unlink()
            cmd = BundleAddFlowCommand(
                bundle_dir=str(tmp),
                flows=[FlowSpec(source_format="cloudFiles",
                                source_path="/V/raw/orders/",
                                bronze_table="orders", silver_table="orders")],
            )
            self.assertEqual(bundle_add_flow(cmd), 0)
            self.assertFalse((tmp / "conf" / "silver_transformations.yml").exists())


if __name__ == "__main__":
    unittest.main()
