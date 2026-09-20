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
    BundleAddPipelineCommand,
    BundleInitCommand,
    BundlePrepareWheelCommand,
    BundleValidateCommand,
    FlowSpec,
    PipelineSpec,
    _effective_pipeline_resources,
    _effective_pipeline_tasks,
    _discover_bundle_dir,
    _pipeline_ownership_errors,
    _resolved_variable,
    _sdp_meta_sanity_checks,
    _split_dependency_errors,
    _wire_split_pipeline_dependencies,
    _stamp_sdp_meta_version,
    bundle_add_flow,
    bundle_add_pipeline,
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
    ):
        """Create a minimal bundle tree on disk matching `layer` + pipeline flags."""
        self._write(tmp / "databricks.yml", "bundle: {name: t}\n")
        self._write(
            tmp / "resources" / "variables.yml",
            yaml.safe_dump({
                "variables": {
                    "layer": {"default": layer},
                    "pipeline_mode": {"default": pipeline_mode},
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
        pipelines = {"resources": {"pipelines": {}}}
        if with_bronze:
            pipelines["resources"]["pipelines"]["bronze"] = {}
        if with_silver:
            pipelines["resources"]["pipelines"]["silver"] = {}
        if with_combined:
            pipelines["resources"]["pipelines"]["bronze_silver"] = {}
        self._write(tmp / "resources" / "sdp_meta_pipelines.yml", yaml.safe_dump(pipelines))
        onboarding_parameters = {"onboard_layer": "${var.layer}"}
        if layer in ("bronze", "bronze_silver"):
            onboarding_parameters["bronze_dataflowspec_table"] = (
                "${var.bronze_dataflowspec_table}"
            )
        if layer in ("silver", "bronze_silver"):
            onboarding_parameters["silver_dataflowspec_table"] = (
                "${var.silver_dataflowspec_table}"
            )
        self._write(
            tmp / "resources" / "sdp_meta_onboarding_job.yml",
            yaml.safe_dump({
                "resources": {
                    "jobs": {
                        "onboarding": {
                            "tasks": [{
                                "task_key": "onboard_dataflowspecs",
                                "python_wheel_task": {
                                    "named_parameters": onboarding_parameters
                                },
                            }]
                        }
                    }
                }
            }),
        )

    @staticmethod
    def _configured_pipeline(layer: str, group: str):
        config = {
            "layer": layer,
            "sdp_meta_dependency": "${var.sdp_meta_dependency}",
        }
        if layer in ("bronze", "bronze_silver"):
            config.update({
                "bronze.dataflowspecTable": "${var.uc_catalog_name}.meta.bronze_specs",
                "bronze.group": group,
            })
        if layer in ("silver", "bronze_silver"):
            config.update({
                "silver.dataflowspecTable": "${var.uc_catalog_name}.meta.silver_specs",
                "silver.group": group,
            })
        return {"schema": "${var.bronze_target_schema}", "configuration": config}

    def _write_configured_topologies(self, tmp: Path, *, omit_dependency=False):
        pipelines = {
            "orders_bronze": self._configured_pipeline("bronze", "orders"),
            "orders_silver": self._configured_pipeline("silver", "orders"),
            "customers_bronze_silver": self._configured_pipeline(
                "bronze_silver", "customers"
            ),
            "audit_bronze": self._configured_pipeline("bronze", "audit"),
        }
        silver_task = {
            "task_key": "orders_silver",
            "pipeline_task": {
                "pipeline_id": "${resources.pipelines.orders_silver.id}"
            },
        }
        if not omit_dependency:
            silver_task["depends_on"] = [{"task_key": "orders_bronze"}]
        tasks = [
            {
                "task_key": "orders_bronze",
                "pipeline_task": {
                    "pipeline_id": "${resources.pipelines.orders_bronze.id}"
                },
            },
            silver_task,
            {
                "task_key": "customers_bronze_silver",
                "pipeline_task": {
                    "pipeline_id": (
                        "${resources.pipelines.customers_bronze_silver.id}"
                    )
                },
            },
            {
                "task_key": "audit_bronze",
                "pipeline_task": {
                    "pipeline_id": "${resources.pipelines.audit_bronze.id}"
                },
            },
        ]
        self._write(
            tmp / "resources" / "sdp_meta_pipelines.yml",
            yaml.safe_dump({
                "resources": {
                    "pipelines": pipelines,
                    "jobs": {"pipelines": {"tasks": tasks}},
                }
            }),
        )

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

    def test_quarantine_database_does_not_cover_primary_layer_target(self):
        with _tempdir() as tmp:
            self._make_bundle(
                tmp,
                layer="bronze",
                with_bronze=True,
                with_silver=False,
            )
            self._write(
                tmp / "conf" / "onboarding.yml",
                yaml.safe_dump([
                    {
                        "data_flow_id": "1",
                        "data_flow_group": "g",
                        "bronze_database_quarantine_dev": (
                            "cat.bronze_quarantine"
                        ),
                        "bronze_table": "orders",
                    },
                ]),
            )
            self._write(
                tmp / "resources" / "sdp_meta_pipelines.yml",
                yaml.safe_dump({
                    "resources": {
                        "pipelines": {
                            "orders_bronze": self._configured_pipeline(
                                "bronze",
                                "g",
                            ),
                        },
                        "jobs": {
                            "pipelines": {
                                "tasks": [
                                    {
                                        "task_key": "orders_bronze",
                                        "pipeline_task": {
                                            "pipeline_id": (
                                                "${resources.pipelines."
                                                "orders_bronze.id}"
                                            ),
                                        },
                                    },
                                ],
                            },
                        },
                    },
                }),
            )

            errors = _sdp_meta_sanity_checks(tmp)

            self.assertTrue(
                any(
                    "has no bronze onboarding row" in error
                    for error in errors
                ),
                errors,
            )

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

    def test_multiple_configured_topologies_are_validated_independently(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp, layer="bronze_silver")
            self._write(
                tmp / "conf" / "onboarding.yml",
                yaml.safe_dump([
                    {
                        "data_flow_id": "1", "data_flow_group": "orders",
                        "bronze_database_dev": "cat.bronze",
                        "bronze_table": "orders",
                        "silver_database_dev": "cat.silver",
                        "silver_table": "orders",
                    },
                    {
                        "data_flow_id": "2", "data_flow_group": "customers",
                        "bronze_database_dev": "cat.bronze",
                        "bronze_table": "customers",
                        "silver_database_dev": "cat.silver",
                        "silver_table": "customers",
                    },
                    {
                        "data_flow_id": "3", "data_flow_group": "audit",
                        "bronze_database_dev": "cat.bronze",
                        "bronze_table": "audit",
                    },
                ]),
            )
            self._write_configured_topologies(tmp)

            self.assertEqual(_sdp_meta_sanity_checks(tmp), [])

    def test_multi_topology_rejects_unwired_pipeline(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp, layer="bronze_silver")
            self._write(
                tmp / "conf" / "onboarding.yml",
                yaml.safe_dump([
                    {
                        "data_flow_id": "1", "data_flow_group": "orders",
                        "bronze_database_dev": "cat.bronze",
                        "bronze_table": "orders",
                        "silver_database_dev": "cat.silver",
                        "silver_table": "orders",
                    },
                    {
                        "data_flow_id": "2", "data_flow_group": "customers",
                        "bronze_database_dev": "cat.bronze",
                        "bronze_table": "customers",
                        "silver_database_dev": "cat.silver",
                        "silver_table": "customers",
                    },
                    {
                        "data_flow_id": "3", "data_flow_group": "audit",
                        "bronze_database_dev": "cat.bronze",
                        "bronze_table": "audit",
                    },
                ]),
            )
            self._write_configured_topologies(tmp)
            path = tmp / "resources" / "sdp_meta_pipelines.yml"
            doc = yaml.safe_load(path.read_text())
            doc["resources"]["jobs"]["pipelines"]["tasks"].pop()
            path.write_text(yaml.safe_dump(doc))

            errors = _sdp_meta_sanity_checks(tmp)
            self.assertTrue(
                any("audit_bronze" in error and "found 0" in error for error in errors),
                errors,
            )

    def test_multi_topology_accepts_transitive_split_dependency(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp, layer="bronze_silver")
            self._write(
                tmp / "conf" / "onboarding.yml",
                yaml.safe_dump([
                    {
                        "data_flow_id": "1",
                        "data_flow_group": "orders",
                        "bronze_database_dev": "cat.bronze",
                        "bronze_table": "orders",
                        "silver_database_dev": "cat.silver",
                        "silver_table": "orders",
                    },
                    {
                        "data_flow_id": "2",
                        "data_flow_group": "customers",
                        "bronze_database_dev": "cat.bronze",
                        "bronze_table": "customers",
                        "silver_database_dev": "cat.silver",
                        "silver_table": "customers",
                    },
                    {
                        "data_flow_id": "3",
                        "data_flow_group": "audit",
                        "bronze_database_dev": "cat.bronze",
                        "bronze_table": "audit",
                    },
                ]),
            )
            self._write_configured_topologies(tmp)

            path = tmp / "resources" / "sdp_meta_pipelines.yml"
            doc = yaml.safe_load(path.read_text())
            tasks = doc["resources"]["jobs"]["pipelines"]["tasks"]

            silver_task = next(
                task for task in tasks
                if task["task_key"] == "orders_silver"
            )
            silver_task["depends_on"] = [{"task_key": "validation"}]

            tasks.append({
                "task_key": "validation",
                "depends_on": [{"task_key": "orders_bronze"}],
            })

            path.write_text(yaml.safe_dump(doc))

            self.assertEqual(_sdp_meta_sanity_checks(tmp), [])

    def test_multi_topology_rejects_missing_split_dependency(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp, layer="bronze_silver")
            self._write(
                tmp / "conf" / "onboarding.yml",
                yaml.safe_dump([
                    {
                        "data_flow_id": "1", "data_flow_group": "orders",
                        "bronze_database_dev": "cat.bronze",
                        "bronze_table": "orders",
                        "silver_database_dev": "cat.silver",
                        "silver_table": "orders",
                    },
                    {
                        "data_flow_id": "2", "data_flow_group": "customers",
                        "bronze_database_dev": "cat.bronze",
                        "bronze_table": "customers",
                        "silver_database_dev": "cat.silver",
                        "silver_table": "customers",
                    },
                    {
                        "data_flow_id": "3", "data_flow_group": "audit",
                        "bronze_database_dev": "cat.bronze",
                        "bronze_table": "audit",
                    },
                ]),
            )
            self._write_configured_topologies(tmp, omit_dependency=True)

            errors = _sdp_meta_sanity_checks(tmp)
            self.assertTrue(
                any("does not depend" in error for error in errors),
                errors,
            )

    def test_target_override_is_used_for_pipeline_group_validation(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp, layer="bronze", with_silver=False)
            self._write(
                tmp / "databricks.yml",
                yaml.safe_dump({
                    "bundle": {"name": "t"},
                    "targets": {
                        "dev": {"variables": {"dataflow_group": "dev_group"}}
                    },
                }),
            )
            self._write(
                tmp / "conf" / "onboarding.yml",
                yaml.safe_dump([
                    {
                        "data_flow_id": "1", "data_flow_group": "dev_group",
                        "bronze_database_dev": "cat.bronze",
                        "bronze_table": "orders",
                    }
                ]),
            )
            self._write(
                tmp / "resources" / "sdp_meta_pipelines.yml",
                yaml.safe_dump({
                    "resources": {
                        "pipelines": {
                            "bronze": self._configured_pipeline(
                                "bronze", "${var.dataflow_group}"
                            )
                        },
                        "jobs": {
                            "pipelines": {
                                "tasks": [{
                                    "task_key": "bronze",
                                    "pipeline_task": {
                                        "pipeline_id": (
                                            "${resources.pipelines.bronze.id}"
                                        )
                                    },
                                }]
                            }
                        },
                    }
                }),
            )

            self.assertEqual(_sdp_meta_sanity_checks(tmp, target="dev"), [])
            errors = _sdp_meta_sanity_checks(tmp)
            self.assertTrue(any("bronze.group='g'" in error for error in errors))

    def test_mapping_target_override_accepts_default_key(self):
        self.assertEqual(
            _resolved_variable(
                {"dataflow_group": {"default": "base"}},
                {
                    "targets": {
                        "prod": {
                            "variables": {
                                "dataflow_group": {
                                    "default": "orders",
                                },
                            },
                        },
                    },
                },
                "dataflow_group",
                "prod",
            ),
            "orders",
        )

    def test_default_target_override_is_used_when_target_is_omitted(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp, layer="bronze", with_silver=False)
            self._write(
                tmp / "databricks.yml",
                yaml.safe_dump({
                    "bundle": {"name": "t"},
                    "targets": {
                        "dev": {
                            "default": True,
                            "variables": {"dataflow_group": "dev_group"},
                        }
                    },
                }),
            )
            self._write(
                tmp / "conf" / "onboarding.yml",
                yaml.safe_dump([{
                    "data_flow_id": "1",
                    "data_flow_group": "dev_group",
                    "bronze_database_dev": "cat.bronze",
                    "bronze_table": "orders",
                }]),
            )
            self._write(
                tmp / "resources" / "sdp_meta_pipelines.yml",
                yaml.safe_dump({
                    "resources": {
                        "pipelines": {
                            "bronze": self._configured_pipeline(
                                "bronze", "${var.dataflow_group}"
                            )
                        },
                        "jobs": {
                            "pipelines": {
                                "tasks": [{
                                    "task_key": "bronze",
                                    "pipeline_task": {
                                        "pipeline_id": (
                                            "${resources.pipelines.bronze.id}"
                                        )
                                    },
                                }]
                            }
                        },
                    }
                }),
            )

            self.assertEqual(_sdp_meta_sanity_checks(tmp), [])

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

    def test_overrides_reject_invalid_source_format(self):
        with _tempdir() as tmp:
            with self.assertRaises(ValueError):
                write_quickstart_config_file(
                    tmp, overrides={"source_format": "parquet"}
                )

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
        if expect_layer in ("bronze", "bronze_silver"):
            flow = parsed[0]
            self.assertIn("bronze_quarantine_table", flow)
            self.assertEqual(flow["bronze_quarantine_table"], "")
            self.assertEqual(
                flow["bronze_database_quarantine_dev"],
                "main.sdp_meta_bronze",
            )
            self.assertEqual(flow["bronze_quarantine_table_path_dev"], "")
            self.assertEqual(flow["bronze_quarantine_table_properties"], {})
            self.assertEqual(flow["bronze_quarantine_table_cluster_by"], [])

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

    def test_rendered_bundle_supports_multiple_independent_topologies(self):
        """Real template render + add-flow + add-pipeline + validation."""
        with _tempdir() as tmp:
            rendered = self._render(self._common_answers(), tmp)
            self._strip_placeholders(
                rendered / "conf" / "onboarding.yml", "yml"
            )

            additions = [
                (
                    FlowSpec(
                        source_format="delta",
                        source_database="main.raw",
                        source_table="audit_events",
                        bronze_table="audit_events",
                        layer="bronze",
                        data_flow_group="audit_group",
                        bronze_target_schema="audit_bronze",
                    ),
                    PipelineSpec(
                        name="audit",
                        layer="bronze",
                        dataflow_group="audit_group",
                        bronze_target_schema="audit_bronze",
                    ),
                ),
                (
                    FlowSpec(
                        source_format="delta",
                        source_database="main.raw",
                        source_table="payments",
                        bronze_table="payments",
                        silver_table="payments",
                        layer="bronze_silver",
                        data_flow_group="finance_group",
                        bronze_target_schema="finance_bronze",
                        silver_target_schema="finance_silver",
                    ),
                    PipelineSpec(
                        name="finance",
                        layer="bronze_silver",
                        pipeline_mode="combined",
                        dataflow_group="finance_group",
                        bronze_target_schema="finance_bronze",
                        silver_target_schema="finance_silver",
                    ),
                ),
            ]
            for flow, pipeline in additions:
                self.assertEqual(
                    bundle_add_flow(BundleAddFlowCommand(
                        bundle_dir=str(rendered), flows=[flow]
                    )),
                    0,
                )
                self.assertEqual(
                    bundle_add_pipeline(BundleAddPipelineCommand(
                        bundle_dir=str(rendered), pipeline=pipeline
                    )),
                    0,
                )

            self.assertEqual(_sdp_meta_sanity_checks(rendered), [])
            resources = yaml.safe_load(
                (rendered / "resources" / "sdp_meta_pipelines.yml").read_text()
            )["resources"]
            self.assertEqual(
                set(resources["pipelines"]),
                {"bronze", "silver", "audit_bronze", "finance_bronze_silver"},
            )
            tasks = {
                task["task_key"]: task
                for task in resources["jobs"]["pipelines"]["tasks"]
            }
            self.assertEqual(set(tasks), set(resources["pipelines"]))
            rows = yaml.safe_load(
                (rendered / "conf" / "onboarding.yml").read_text()
            )
            finance = next(
                row for row in rows
                if row["data_flow_group"] == "finance_group"
            )
            self.assertEqual(
                finance["bronze_database_dev"], "main.finance_bronze"
            )
            self.assertEqual(
                finance["silver_database_dev"], "main.finance_silver"
            )

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
            self.assertIn(
                "--force-reinstall",
                cell1_code[1],
                "serverless pipeline installs must not reuse stale environment metadata",
            )
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


class PipelineOwnershipHelperTests(unittest.TestCase):
    """Branch coverage for defensive target/topology resolution."""

    def test_ownership_skips_invalid_layers_and_blank_groups(self):
        pipelines = {
            "invalid": {
                "configuration": {
                    "layer": "gold",
                    "bronze.group": "g",
                },
            },
            "blank": {
                "schema": "bronze",
                "configuration": {
                    "layer": "bronze",
                    "bronze.group": " ",
                },
            },
            "valid": {
                "schema": "bronze",
                "configuration": {
                    "layer": "bronze",
                    "bronze.group": "valid",
                },
            },
        }

        self.assertEqual(
            _pipeline_ownership_errors(pipelines, {}, {}),
            [],
        )

    def test_effective_resources_handles_new_and_malformed_overrides(self):
        base = {"bronze": {"configuration": {"layer": "bronze"}}}
        self.assertIs(
            _effective_pipeline_resources(base, {}, None),
            base,
        )
        self.assertIs(
            _effective_pipeline_resources(
                base,
                {"targets": {"prod": {"resources": "invalid"}}},
                "prod",
            ),
            base,
        )

        effective = _effective_pipeline_resources(
            base,
            {
                "targets": {
                    "prod": {
                        "resources": {
                            "pipelines": {
                                "silver": {
                                    "configuration": {
                                        "layer": "silver",
                                    },
                                },
                                "disabled": False,
                            },
                        },
                    },
                },
            },
            "prod",
        )

        self.assertEqual(
            effective["silver"]["configuration"]["layer"],
            "silver",
        )
        self.assertFalse(effective["disabled"])

    def test_effective_tasks_merges_and_appends_target_tasks(self):
        base = [
            {
                "task_key": "bronze",
                "pipeline_task": {"pipeline_id": "bronze-id"},
            },
            "ignored",
        ]
        self.assertIs(_effective_pipeline_tasks(base, {}, None), base)
        self.assertIs(
            _effective_pipeline_tasks(
                base,
                {"targets": {"prod": {"resources": []}}},
                "prod",
            ),
            base,
        )

        effective = _effective_pipeline_tasks(
            base,
            {
                "targets": {
                    "prod": {
                        "resources": {
                            "jobs": {
                                "pipelines": {
                                    "tasks": [
                                        {
                                            "task_key": "bronze",
                                            "depends_on": [],
                                        },
                                        {
                                            "task_key": "silver",
                                            "pipeline_task": {
                                                "pipeline_id": "silver-id",
                                            },
                                        },
                                        "ignored-target-task",
                                    ],
                                },
                            },
                        },
                    },
                },
            },
            "prod",
        )

        self.assertEqual(
            effective[0]["pipeline_task"]["pipeline_id"],
            "bronze-id",
        )
        self.assertEqual(effective[1], "ignored")
        self.assertEqual(effective[2]["task_key"], "silver")
        self.assertEqual(effective[3], "ignored-target-task")

    def test_split_validation_skips_unowned_and_unreferenced_specs(self):
        pipelines = {
            "not-a-mapping": "invalid",
            "combined": {
                "configuration": {
                    "layer": "bronze_silver",
                },
            },
            "bronze": {
                "configuration": {
                    "layer": "bronze",
                    "bronze.group": "g",
                },
            },
            "silver_other": {
                "configuration": {
                    "layer": "silver",
                    "silver.group": "other",
                },
            },
            "silver_without_task": {
                "configuration": {
                    "layer": "silver",
                    "silver.group": "g",
                },
            },
            "silver_without_group": {
                "configuration": {
                    "layer": "silver",
                },
            },
        }
        tasks = [
            "invalid",
            {"task_key": "no_pipeline_reference"},
            {
                "task_key": "bronze",
                "pipeline_task": {
                    "pipeline_id": (
                        "${resources.pipelines.bronze.id}"
                    ),
                },
            },
        ]

        self.assertEqual(
            _split_dependency_errors(
                pipelines,
                tasks,
                {},
                {},
                None,
            ),
            [],
        )

    def test_split_wiring_skips_malformed_specs_and_missing_tasks(self):
        pipelines = {
            "invalid": "not-a-mapping",
            "ungrouped": {
                "configuration": {
                    "layer": "bronze",
                },
            },
            "bronze": {
                "configuration": {
                    "layer": "bronze",
                    "bronze.group": "g",
                },
            },
            "silver": {
                "configuration": {
                    "layer": "silver",
                    "silver.group": "g",
                },
            },
        }
        tasks = [
            "not-a-task",
            {"task_key": "no-pipeline-reference"},
            {
                "task_key": "bronze",
                "pipeline_task": {
                    "pipeline_id": (
                        "${resources.pipelines.bronze.id}"
                    ),
                },
            },
        ]

        _wire_split_pipeline_dependencies(
            pipelines,
            tasks,
            {"silver"},
            {},
            {},
        )

        self.assertEqual(len(tasks), 3)


class BundleAddPipelineTests(unittest.TestCase):
    """Unit tests for adding independent pipeline topology resources."""

    def _make_bundle(self, tmp: Path):
        (tmp / "resources").mkdir()
        (tmp / "conf").mkdir()
        (tmp / "databricks.yml").write_text("bundle: {name: test}\n")
        (tmp / "resources" / "variables.yml").write_text(
            yaml.safe_dump({
                "variables": {
                    "layer": {"default": "bronze"},
                    "pipeline_mode": {"default": "split"},
                    "dataflow_group": {"default": "base"},
                    "onboarding_file_name": {"default": "onboarding.yml"},
                    "uc_catalog_name": {"default": "test_cat"},
                    "bronze_target_schema": {"default": "test_bronze"},
                    "silver_target_schema": {"default": "test_silver"},
                    "wheel_source": {"default": "pypi"},
                    "sdp_meta_dependency": {
                        "default": "databricks-labs-sdp-meta==0.1.0"
                    },
                }
            })
        )
        (tmp / "conf" / "onboarding.yml").write_text(
            yaml.safe_dump([
                {
                    "data_flow_id": "1",
                    "data_flow_group": "base",
                    "bronze_database_dev": "test_cat.test_bronze",
                    "bronze_table": "base",
                },
                {
                    "data_flow_id": "2",
                    "data_flow_group": "orders",
                    "bronze_database_dev": "test_cat.test_bronze",
                    "bronze_table": "orders",
                },
            ])
        )
        (tmp / "resources" / "sdp_meta_onboarding_job.yml").write_text(
            yaml.safe_dump({
                "resources": {
                    "jobs": {
                        "onboarding": {
                            "tasks": [{
                                "task_key": "onboard_dataflowspecs",
                                "python_wheel_task": {
                                    "named_parameters": {
                                        "onboard_layer": "${var.layer}",
                                        "bronze_dataflowspec_table": (
                                            "${var.bronze_dataflowspec_table}"
                                        ),
                                    }
                                },
                            }]
                        }
                    }
                }
            }, sort_keys=False)
        )
        (tmp / "resources" / "sdp_meta_pipelines.yml").write_text(
            yaml.safe_dump({
                "resources": {
                    "pipelines": {
                        "bronze": {
                            "schema": "${var.bronze_target_schema}",
                            "configuration": {
                                "layer": "bronze",
                                "sdp_meta_dependency": (
                                    "${var.sdp_meta_dependency}"
                                ),
                                "bronze.dataflowspecTable": (
                                    "${var.uc_catalog_name}.${var.sdp_meta_schema}."
                                    "${var.bronze_dataflowspec_table}"
                                ),
                                "bronze.group": "${var.dataflow_group}",
                            }
                        }
                    },
                    "jobs": {
                        "pipelines": {
                            "tasks": [{
                                "task_key": "bronze",
                                "pipeline_task": {
                                    "pipeline_id": (
                                        "${resources.pipelines.bronze.id}"
                                    )
                                },
                            }]
                        }
                    },
                }
            }, sort_keys=False)
        )

    def test_adds_split_topology_and_job_dependency(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp)
            rc = bundle_add_pipeline(BundleAddPipelineCommand(
                bundle_dir=str(tmp),
                pipeline=PipelineSpec(
                    name="orders",
                    layer="bronze_silver",
                    pipeline_mode="split",
                    dataflow_group="orders",
                    bronze_target_schema="orders_bronze",
                    silver_target_schema="orders_silver",
                ),
            ))
            self.assertEqual(rc, 0)

            doc = yaml.safe_load(
                (tmp / "resources" / "sdp_meta_pipelines.yml").read_text()
            )
            pipelines = doc["resources"]["pipelines"]
            self.assertIn("orders_bronze", pipelines)
            self.assertIn("orders_silver", pipelines)
            self.assertEqual(pipelines["orders_bronze"]["schema"], "orders_bronze")
            self.assertEqual(
                pipelines["orders_silver"]["configuration"]["silver.group"],
                "orders",
            )
            tasks = {
                task["task_key"]: task
                for task in doc["resources"]["jobs"]["pipelines"]["tasks"]
            }
            self.assertEqual(
                tasks["orders_silver"]["depends_on"],
                [{"task_key": "orders_bronze"}],
            )
            variables = yaml.safe_load(
                (tmp / "resources" / "variables.yml").read_text()
            )["variables"]
            self.assertEqual(variables["layer"]["default"], "bronze_silver")
            onboarding_job = yaml.safe_load(
                (tmp / "resources" / "sdp_meta_onboarding_job.yml").read_text()
            )
            named_parameters = onboarding_job["resources"]["jobs"]["onboarding"][
                "tasks"
            ][0]["python_wheel_task"]["named_parameters"]
            self.assertIn("silver_dataflowspec_table", named_parameters)
            onboarding_rows = yaml.safe_load(
                (tmp / "conf" / "onboarding.yml").read_text()
            )
            orders_row = next(
                row for row in onboarding_rows
                if row["data_flow_group"] == "orders"
            )
            self.assertEqual(orders_row["silver_table"], "orders")
            self.assertEqual(
                orders_row["silver_database_dev"], "test_cat.orders_silver"
            )
            transformations = yaml.safe_load(
                (tmp / "conf" / "silver_transformations.yml").read_text()
            )
            self.assertIn(
                {"target_table": "orders", "select_exp": ["*"]},
                transformations,
            )
            self.assertEqual(_sdp_meta_sanity_checks(tmp), [])

            flow = FlowSpec(
                source_format="delta",
                source_database="raw",
                source_table="order_lines",
                bronze_table="order_lines",
                data_flow_group="orders",
            )
            self.assertEqual(
                bundle_add_flow(BundleAddFlowCommand(
                    bundle_dir=str(tmp), flows=[flow]
                )),
                0,
            )
            added_row = yaml.safe_load(
                (tmp / "conf" / "onboarding.yml").read_text()
            )[-1]
            self.assertEqual(
                added_row["bronze_database_dev"], "test_cat.orders_bronze"
            )
            self.assertEqual(
                added_row["silver_database_dev"], "test_cat.orders_silver"
            )

    def test_adds_combined_topology(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp)
            self.assertEqual(
                bundle_add_pipeline(BundleAddPipelineCommand(
                    bundle_dir=str(tmp),
                    pipeline=PipelineSpec(
                        name="orders",
                        layer="bronze_silver",
                        pipeline_mode="combined",
                        dataflow_group="orders",
                    ),
                )),
                0,
            )
            doc = yaml.safe_load(
                (tmp / "resources" / "sdp_meta_pipelines.yml").read_text()
            )
            self.assertIn(
                "orders_bronze_silver", doc["resources"]["pipelines"]
            )
            self.assertEqual(_sdp_meta_sanity_checks(tmp), [])

    def test_same_group_different_layers_create_supported_split(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp)

            rc = bundle_add_pipeline(BundleAddPipelineCommand(
                bundle_dir=str(tmp),
                pipeline=PipelineSpec(
                    name="base_silver_owner",
                    layer="silver",
                    dataflow_group="base",
                    silver_target_schema="base_silver",
                ),
            ))

            self.assertEqual(rc, 0)
            doc = yaml.safe_load(
                (tmp / "resources" / "sdp_meta_pipelines.yml").read_text()
            )
            tasks = {
                task["task_key"]: task
                for task in doc["resources"]["jobs"]["pipelines"]["tasks"]
            }
            self.assertEqual(
                tasks["base_silver_owner_silver"]["depends_on"],
                [{"task_key": "bronze"}],
            )
            self.assertEqual(_sdp_meta_sanity_checks(tmp), [])

    def test_target_only_split_ownership_wires_dependency(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp)
            (tmp / "databricks.yml").write_text(yaml.safe_dump({
                "bundle": {"name": "test"},
                "targets": {
                    "prod": {
                        "resources": {
                            "pipelines": {
                                "bronze": {
                                    "configuration": {
                                        "bronze.group": "orders",
                                    },
                                },
                            },
                        },
                    },
                },
            }, sort_keys=False))

            rc = bundle_add_pipeline(BundleAddPipelineCommand(
                bundle_dir=str(tmp),
                pipeline=PipelineSpec(
                    name="orders_silver_owner",
                    layer="silver",
                    dataflow_group="orders",
                ),
            ))

            self.assertEqual(rc, 0)
            doc = yaml.safe_load(
                (tmp / "resources" / "sdp_meta_pipelines.yml").read_text()
            )
            tasks = {
                task["task_key"]: task
                for task in doc["resources"]["jobs"]["pipelines"]["tasks"]
            }
            self.assertEqual(
                tasks["orders_silver_owner_silver"]["depends_on"],
                [{"task_key": "bronze"}],
            )
            self.assertEqual(
                _sdp_meta_sanity_checks(tmp, target="prod"),
                [],
            )

            tasks["orders_silver_owner_silver"].pop("depends_on")
            (tmp / "resources" / "sdp_meta_pipelines.yml").write_text(
                yaml.safe_dump(doc, sort_keys=False)
            )
            errors = _sdp_meta_sanity_checks(tmp)
            dependency_errors = [
                error for error in errors
                if "does not depend" in error
            ]
            self.assertEqual(len(dependency_errors), 1)
            self.assertIn("Target(s) 'prod'", dependency_errors[0])

    def test_target_task_override_merges_by_task_key(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp)
            self.assertEqual(
                bundle_add_pipeline(BundleAddPipelineCommand(
                    bundle_dir=str(tmp),
                    pipeline=PipelineSpec(
                        name="base_silver_owner",
                        layer="silver",
                        dataflow_group="base",
                    ),
                )),
                0,
            )
            (tmp / "databricks.yml").write_text(yaml.safe_dump({
                "bundle": {"name": "test"},
                "targets": {
                    "prod": {
                        "resources": {
                            "jobs": {
                                "pipelines": {
                                    "tasks": [{
                                        "task_key": (
                                            "base_silver_owner_silver"
                                        ),
                                        "depends_on": [],
                                    }],
                                },
                            },
                        },
                    },
                },
            }, sort_keys=False))

            errors = _sdp_meta_sanity_checks(tmp, target="prod")

            dependency_errors = [
                error for error in errors
                if "does not depend" in error
            ]
            self.assertEqual(len(dependency_errors), 1)
            self.assertIn(
                "base_silver_owner_silver",
                dependency_errors[0],
            )

    def test_target_task_override_cannot_remove_new_split_dependency(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp)
            path = tmp / "resources" / "sdp_meta_pipelines.yml"
            before = path.read_text()
            (tmp / "databricks.yml").write_text(yaml.safe_dump({
                "bundle": {"name": "test"},
                "targets": {
                    "prod": {
                        "resources": {
                            "jobs": {
                                "pipelines": {
                                    "tasks": [{
                                        "task_key": (
                                            "base_silver_owner_silver"
                                        ),
                                        "depends_on": [],
                                    }],
                                },
                            },
                        },
                    },
                },
            }, sort_keys=False))
            output = StringIO()

            rc = bundle_add_pipeline(
                BundleAddPipelineCommand(
                    bundle_dir=str(tmp),
                    pipeline=PipelineSpec(
                        name="base_silver_owner",
                        layer="silver",
                        dataflow_group="base",
                    ),
                ),
                output=output,
            )

            self.assertEqual(rc, 2)
            self.assertEqual(path.read_text(), before)
            self.assertIn("Target(s) 'prod'", output.getvalue())
            self.assertIn("does not depend", output.getvalue())

    def test_duplicate_group_layer_is_rejected_before_any_write(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp)
            before = {
                path.relative_to(tmp): path.read_text()
                for path in tmp.rglob("*")
                if path.is_file()
            }
            output = StringIO()

            rc = bundle_add_pipeline(
                BundleAddPipelineCommand(
                    bundle_dir=str(tmp),
                    pipeline=PipelineSpec(
                        name="duplicate_bronze",
                        layer="bronze",
                        dataflow_group="base",
                        bronze_target_schema="retargeted_bronze",
                    ),
                ),
                output=output,
            )

            after = {
                path.relative_to(tmp): path.read_text()
                for path in tmp.rglob("*")
                if path.is_file()
            }
            self.assertEqual(rc, 2)
            self.assertEqual(after, before)
            self.assertIn(
                "Duplicate bronze ownership for dataflow group 'base'",
                output.getvalue(),
            )
            self.assertIn("retargeted_bronze", output.getvalue())

    def test_combined_pipeline_conflicting_with_split_owner_is_rejected(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp)
            output = StringIO()

            rc = bundle_add_pipeline(
                BundleAddPipelineCommand(
                    bundle_dir=str(tmp),
                    pipeline=PipelineSpec(
                        name="duplicate_combined",
                        layer="bronze_silver",
                        pipeline_mode="combined",
                        dataflow_group="base",
                    ),
                ),
                output=output,
            )

            self.assertEqual(rc, 2)
            self.assertIn("Duplicate bronze ownership", output.getvalue())

    def test_target_override_ownership_conflict_is_rejected(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp)
            (tmp / "databricks.yml").write_text(yaml.safe_dump({
                "bundle": {"name": "test"},
                "targets": {
                    "prod": {
                        "resources": {
                            "pipelines": {
                                "bronze": {
                                    "configuration": {
                                        "bronze.group": "orders",
                                    },
                                },
                            },
                        },
                    },
                },
            }, sort_keys=False))
            output = StringIO()

            rc = bundle_add_pipeline(
                BundleAddPipelineCommand(
                    bundle_dir=str(tmp),
                    pipeline=PipelineSpec(
                        name="orders",
                        layer="bronze",
                        dataflow_group="orders",
                    ),
                ),
                output=output,
            )

            self.assertEqual(rc, 2)
            self.assertIn("Target(s) 'prod'", output.getvalue())
            self.assertIn("Duplicate bronze ownership", output.getvalue())

    def test_sanity_checks_all_targets_and_deduplicates_conflicts(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp)
            (tmp / "databricks.yml").write_text(yaml.safe_dump({
                "bundle": {"name": "test"},
                "targets": {
                    name: {
                        "variables": {
                            "dataflow_group": "orders",
                        },
                    }
                    for name in ("dev", "prod")
                },
            }, sort_keys=False))
            path = tmp / "resources" / "sdp_meta_pipelines.yml"
            doc = yaml.safe_load(path.read_text())
            resources = doc["resources"]
            duplicate = json.loads(json.dumps(
                resources["pipelines"]["bronze"]
            ))
            duplicate["schema"] = "other_bronze"
            duplicate["configuration"]["bronze.group"] = "orders"
            resources["pipelines"]["manual_duplicate"] = duplicate
            resources["jobs"]["pipelines"]["tasks"].append({
                "task_key": "manual_duplicate",
                "pipeline_task": {
                    "pipeline_id": (
                        "${resources.pipelines.manual_duplicate.id}"
                    ),
                },
            })
            path.write_text(yaml.safe_dump(doc, sort_keys=False))

            errors = _sdp_meta_sanity_checks(tmp)

            ownership_errors = [
                error for error in errors
                if "Duplicate bronze ownership" in error
            ]
            self.assertEqual(len(ownership_errors), 1)
            self.assertIn("Target(s) 'dev', 'prod'", ownership_errors[0])

    def test_malformed_split_dependency_is_reported_without_traceback(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp)
            path = tmp / "resources" / "sdp_meta_pipelines.yml"
            doc = yaml.safe_load(path.read_text())
            resources = doc["resources"]
            pipeline = resources["pipelines"]["bronze"]
            pipeline["schema"] = "${var.silver_target_schema}"
            pipeline["configuration"] = {
                "layer": "silver",
                "sdp_meta_dependency": "${var.sdp_meta_dependency}",
                "silver.dataflowspecTable": (
                    "${var.uc_catalog_name}.${var.sdp_meta_schema}."
                    "${var.silver_dataflowspec_table}"
                ),
                "silver.group": "${var.dataflow_group}",
            }
            task = resources["jobs"]["pipelines"]["tasks"][0]
            task["depends_on"] = "not-a-list"
            path.write_text(yaml.safe_dump(doc, sort_keys=False))
            output = StringIO()

            rc = bundle_add_pipeline(
                BundleAddPipelineCommand(
                    bundle_dir=str(tmp),
                    pipeline=PipelineSpec(
                        name="base_bronze_owner",
                        layer="bronze",
                        dataflow_group="base",
                    ),
                ),
                output=output,
            )

            self.assertEqual(rc, 2)
            self.assertIn(
                "malformed `depends_on`; expected a list",
                output.getvalue(),
            )

    def test_preexisting_ownership_conflict_blocks_unrelated_addition(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp)
            path = tmp / "resources" / "sdp_meta_pipelines.yml"
            doc = yaml.safe_load(path.read_text())
            resources = doc["resources"]
            duplicate = json.loads(json.dumps(
                resources["pipelines"]["bronze"]
            ))
            resources["pipelines"]["preexisting_duplicate"] = duplicate
            resources["jobs"]["pipelines"]["tasks"].append({
                "task_key": "preexisting_duplicate",
                "pipeline_task": {
                    "pipeline_id": (
                        "${resources.pipelines.preexisting_duplicate.id}"
                    ),
                },
            })
            path.write_text(yaml.safe_dump(doc, sort_keys=False))
            before = path.read_text()
            output = StringIO()

            rc = bundle_add_pipeline(
                BundleAddPipelineCommand(
                    bundle_dir=str(tmp),
                    pipeline=PipelineSpec(
                        name="unrelated",
                        layer="bronze",
                        dataflow_group="orders",
                    ),
                ),
                output=output,
            )

            self.assertEqual(rc, 2)
            self.assertEqual(path.read_text(), before)
            self.assertIn(
                "merged pipeline topology is invalid",
                output.getvalue(),
            )

    def test_malformed_pipeline_yaml_returns_clean_error(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp)
            path = tmp / "resources" / "sdp_meta_pipelines.yml"
            path.write_text("resources: [unterminated\n")
            output = StringIO()

            rc = bundle_add_pipeline(
                BundleAddPipelineCommand(
                    bundle_dir=str(tmp),
                    pipeline=PipelineSpec(
                        name="orders",
                        layer="bronze",
                        dataflow_group="orders",
                    ),
                ),
                output=output,
            )

            self.assertEqual(rc, 2)
            self.assertIn("ERROR:", output.getvalue())

    def test_invalid_pipeline_yaml_shape_returns_clean_error(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp)
            path = tmp / "resources" / "sdp_meta_pipelines.yml"
            path.write_text("- not\n- a\n- mapping\n")
            output = StringIO()

            rc = bundle_add_pipeline(
                BundleAddPipelineCommand(
                    bundle_dir=str(tmp),
                    pipeline=PipelineSpec(
                        name="orders",
                        layer="bronze",
                        dataflow_group="orders",
                    ),
                ),
                output=output,
            )

            self.assertEqual(rc, 2)
            self.assertIn(
                "expected a top-level mapping",
                output.getvalue(),
            )

    def test_nested_pipeline_yaml_shape_errors_are_clean(self):
        cases = {
            "resources": (
                {"resources": []},
                "`resources` must be a mapping",
            ),
            "pipelines": (
                {"resources": {"pipelines": [], "jobs": {}}},
                "`resources.pipelines` and `resources.jobs`",
            ),
            "pipeline_job": (
                {
                    "resources": {
                        "pipelines": {},
                        "jobs": {"pipelines": []},
                    },
                },
                "`resources.jobs.pipelines` must be a mapping",
            ),
            "tasks": (
                {
                    "resources": {
                        "pipelines": {},
                        "jobs": {
                            "pipelines": {"tasks": {}},
                        },
                    },
                },
                "`resources.jobs.pipelines.tasks` must be a list",
            ),
        }
        for name, (doc, expected) in cases.items():
            with self.subTest(name=name), _tempdir() as tmp:
                self._make_bundle(tmp)
                (
                    tmp / "resources" / "sdp_meta_pipelines.yml"
                ).write_text(yaml.safe_dump(doc))
                output = StringIO()

                rc = bundle_add_pipeline(
                    BundleAddPipelineCommand(
                        bundle_dir=str(tmp),
                        pipeline=PipelineSpec(
                            name="orders",
                            layer="bronze",
                            dataflow_group="orders",
                        ),
                    ),
                    output=output,
                )

                self.assertEqual(rc, 2)
                self.assertIn(expected, output.getvalue())

    def test_invalid_variables_shape_returns_clean_error(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp)
            (tmp / "resources" / "variables.yml").write_text(
                yaml.safe_dump({"variables": ["invalid"]})
            )
            output = StringIO()

            rc = bundle_add_pipeline(
                BundleAddPipelineCommand(
                    bundle_dir=str(tmp),
                    pipeline=PipelineSpec(
                        name="orders",
                        layer="bronze",
                        dataflow_group="orders",
                    ),
                ),
                output=output,
            )

            self.assertEqual(rc, 2)
            self.assertIn(
                "`variables` must be a mapping",
                output.getvalue(),
            )

    def test_sanity_checks_detect_manually_added_duplicate_owner(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp)
            path = tmp / "resources" / "sdp_meta_pipelines.yml"
            doc = yaml.safe_load(path.read_text())
            resources = doc["resources"]
            duplicate = json.loads(json.dumps(
                resources["pipelines"]["bronze"]
            ))
            duplicate["schema"] = "other_bronze"
            resources["pipelines"]["manual_duplicate"] = duplicate
            resources["jobs"]["pipelines"]["tasks"].append({
                "task_key": "manual_duplicate",
                "pipeline_task": {
                    "pipeline_id": (
                        "${resources.pipelines.manual_duplicate.id}"
                    ),
                    "full_refresh": False,
                },
            })
            path.write_text(yaml.safe_dump(doc, sort_keys=False))

            errors = _sdp_meta_sanity_checks(tmp)

            ownership_errors = [
                error for error in errors
                if "Duplicate bronze ownership" in error
            ]
            self.assertEqual(len(ownership_errors), 1)
            self.assertIn("other_bronze", ownership_errors[0])

    def test_collision_is_rejected_without_writing(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp)
            cmd = BundleAddPipelineCommand(
                bundle_dir=str(tmp),
                pipeline=PipelineSpec(
                    name="orders",
                    layer="bronze",
                    dataflow_group="orders",
                ),
            )
            self.assertEqual(bundle_add_pipeline(cmd), 0)
            before = (
                tmp / "resources" / "sdp_meta_pipelines.yml"
            ).read_text()
            self.assertEqual(bundle_add_pipeline(cmd), 2)
            self.assertEqual(
                (tmp / "resources" / "sdp_meta_pipelines.yml").read_text(),
                before,
            )


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

    def test_flow_can_override_layer_group_and_target_schemas(self):
        with _tempdir() as tmp:
            self._make_bundle(tmp, layer="bronze", seed_flows=[])
            cmd = BundleAddFlowCommand(
                bundle_dir=str(tmp),
                flows=[FlowSpec(
                    source_format="delta",
                    source_database="raw",
                    source_table="orders",
                    bronze_table="orders",
                    silver_table="orders",
                    layer="bronze_silver",
                    data_flow_group="orders_group",
                    bronze_target_schema="orders_bronze",
                    silver_target_schema="orders_silver",
                )],
            )
            self.assertEqual(bundle_add_flow(cmd), 0)
            row = yaml.safe_load(
                (tmp / "conf" / "onboarding.yml").read_text()
            )[0]
            self.assertEqual(row["data_flow_group"], "orders_group")
            self.assertEqual(
                row["bronze_database_dev"], "test_cat.orders_bronze"
            )
            self.assertEqual(
                row["silver_database_dev"], "test_cat.orders_silver"
            )
            self.assertTrue(
                (tmp / "conf" / "silver_transformations.yml").is_file()
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

    def test_missing_transformations_file_is_created_for_silver_flow(self):
        """A silver flow added to a bronze-origin bundle creates its config."""
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
            path = tmp / "conf" / "silver_transformations.yml"
            self.assertTrue(path.exists())
            self.assertEqual(
                yaml.safe_load(path.read_text()),
                [{"target_table": "orders", "select_exp": ["*"]}],
            )


if __name__ == "__main__":
    unittest.main()
