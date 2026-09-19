"""Static / offline tests for the DAB template demo.

Validates that:

  - Each ``demo/dab_template_demo/answers/<scenario>.json`` file is parseable
    and contains every prompt declared by the template's
    ``databricks_template_schema.json`` (so ``databricks bundle init
    --config-file`` can consume it without missing-prompt errors).
  - Each scenario uses an enum value the schema actually accepts.
  - Each ``flows/<scenario>_extra.csv`` parses via ``_flows_from_csv`` and
    produces the right ``source_format`` for each row.
  - The launcher script's ``SCENARIOS`` registry references files that
    actually exist on disk.

These tests are pure-Python (no databricks CLI, no workspace, no Spark) and
run in well under a second.
"""

from __future__ import annotations

import csv
import importlib.util
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
DEMO_DIR = REPO_ROOT / "demo" / "dab_template_demo"
SCHEMA_PATH = (
    REPO_ROOT
    / "src"
    / "databricks"
    / "labs"
    / "sdp_meta"
    / "templates"
    / "dab"
    / "databricks_template_schema.json"
)


def _load_schema() -> dict:
    return json.loads(SCHEMA_PATH.read_text())


def _materialize_csv_text(csv_path: Path, catalog: str = "main",
                          demo_data_volume_path: str = "/Volumes/main/demo/wheels/demo_data",
                          uc_source_catalog: str = "main",
                          uc_source_schema: str = "staging") -> str:
    return (
        csv_path.read_text()
        .replace("{uc_catalog_name}", catalog)
        .replace("{uc_source_catalog}", uc_source_catalog)
        .replace("{uc_source_schema}", uc_source_schema)
        .replace("{demo_data_volume_path}", demo_data_volume_path)
    )


class AnswersFileTests(unittest.TestCase):
    """Each answers file must answer every prompt the schema declares."""

    @classmethod
    def setUpClass(cls):
        cls.schema = _load_schema()
        cls.required_prompts = set(cls.schema["properties"].keys())

    def _load_answers(self, name: str) -> dict:
        path = DEMO_DIR / "answers" / name
        self.assertTrue(path.is_file(), f"missing answers file: {path}")
        # Substitute the placeholder so json.loads does not choke on the raw
        # `{uc_catalog_name}` (which is JSON-legal as a string value, but the
        # demo launcher always substitutes it before feeding the CLI).
        text = path.read_text().replace("{uc_catalog_name}", "main")
        doc = json.loads(text)
        # The leading `_comment` key is harmless to the CLI; strip it for
        # validation purposes so we don't false-positive on it being "extra".
        doc.pop("_comment", None)
        return doc

    def _assert_covers_all_prompts(self, doc: dict, scenario_name: str):
        missing = self.required_prompts - set(doc.keys())
        self.assertFalse(
            missing,
            f"{scenario_name}: missing prompts {sorted(missing)}",
        )

    def _assert_enum_values_valid(self, doc: dict, scenario_name: str):
        for prompt, schema in self.schema["properties"].items():
            if "enum" not in schema:
                continue
            self.assertIn(
                doc[prompt],
                schema["enum"],
                f"{scenario_name}: {prompt}={doc[prompt]!r} is not in "
                f"schema enum {schema['enum']}",
            )

    def test_cloudfiles_split(self):
        doc = self._load_answers("cloudfiles_split.json")
        self._assert_covers_all_prompts(doc, "cloudfiles_split")
        self._assert_enum_values_valid(doc, "cloudfiles_split")
        self.assertEqual(doc["layer"], "bronze_silver")
        self.assertEqual(doc["pipeline_mode"], "split")
        self.assertEqual(doc["source_format"], "cloudFiles")
        self.assertEqual(doc["onboarding_file_format"], "yaml")

    def test_kafka_bronze(self):
        doc = self._load_answers("kafka_bronze.json")
        self._assert_covers_all_prompts(doc, "kafka_bronze")
        self._assert_enum_values_valid(doc, "kafka_bronze")
        self.assertEqual(doc["layer"], "bronze")
        self.assertEqual(doc["source_format"], "kafka")
        self.assertEqual(doc["onboarding_file_format"], "json")

    def test_eventhub_combined(self):
        doc = self._load_answers("eventhub_combined.json")
        self._assert_covers_all_prompts(doc, "eventhub_combined")
        self._assert_enum_values_valid(doc, "eventhub_combined")
        self.assertEqual(doc["layer"], "bronze_silver")
        self.assertEqual(doc["pipeline_mode"], "combined")
        self.assertEqual(doc["source_format"], "eventhub")

    def test_delta_split(self):
        doc = self._load_answers("delta_split.json")
        self._assert_covers_all_prompts(doc, "delta_split")
        self._assert_enum_values_valid(doc, "delta_split")
        self.assertEqual(doc["layer"], "bronze_silver")
        self.assertEqual(doc["pipeline_mode"], "split")
        self.assertEqual(doc["source_format"], "delta")
        self.assertEqual(doc["onboarding_file_format"], "yaml")

    def test_all_scenarios_use_volume_path_wheel_source(self):
        """Demos use volume_path so the __SET_ME__ guard rails get exercised."""
        for name in (
            "cloudfiles_split.json",
            "kafka_bronze.json",
            "eventhub_combined.json",
            "delta_split.json",
        ):
            doc = self._load_answers(name)
            self.assertEqual(
                doc["wheel_source"], "volume_path",
                f"{name}: demos pin wheel_source=volume_path so prepare-wheel applies"
            )
            self.assertEqual(
                doc["sdp_meta_dependency"], "__SET_ME__",
                f"{name}: demos default to the sentinel and let STAGE 2 fill it in"
            )


class FlowCsvTests(unittest.TestCase):
    """Each flow CSV must parse cleanly via the engine's CSV loader and
    produce flows with the expected source_format."""

    @classmethod
    def setUpClass(cls):
        # Importing here keeps test discovery cheap.
        from databricks.labs.sdp_meta.bundle import _flows_from_csv  # noqa: WPS433
        cls._flows_from_csv = staticmethod(_flows_from_csv)

    def _load_csv_into_tmp(self, name: str, tmp: Path) -> Path:
        src = DEMO_DIR / "flows" / name
        self.assertTrue(src.is_file(), f"missing flow CSV: {src}")
        rendered = tmp / name
        rendered.write_text(_materialize_csv_text(src))
        return rendered

    def test_cloudfiles_extra(self):
        """The cloudfiles CSV reuses the four demo datasets shipped under
        demo/resources/data/{customers,transactions,products,stores} and
        expects per-row cloudFiles.format=csv."""
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            csv_path = self._load_csv_into_tmp("cloudfiles_extra.csv", tmp)
            flows = self._flows_from_csv(csv_path)
            self.assertEqual(len(flows), 4)
            self.assertEqual({f.source_format for f in flows}, {"cloudFiles"})
            tables = {f.bronze_table for f in flows}
            self.assertEqual(tables, {"customers", "transactions", "products", "stores"})
            for f in flows:
                # All four point at the substituted UC volume base.
                self.assertTrue(
                    f.source_path.startswith("/Volumes/main/demo/wheels/demo_data/"),
                    f"unexpected source_path: {f.source_path}",
                )
                self.assertEqual(f.bronze_table, f.silver_table)
                # The shipped datasets are CSV, not JSON, so each row pins format=csv.
                self.assertEqual(f.cloudfiles_format, "csv")

    def test_kafka_extra(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            csv_path = self._load_csv_into_tmp("kafka_extra.csv", tmp)
            flows = self._flows_from_csv(csv_path)
            self.assertEqual(len(flows), 3)
            self.assertEqual({f.source_format for f in flows}, {"kafka"})
            for f in flows:
                # The CSV used double-quotes around the comma-separated brokers,
                # so the loader should preserve both brokers in one field.
                self.assertIn(",", f.kafka_bootstrap_servers)
                self.assertTrue(f.kafka_topic)

    def test_eventhub_extra(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            csv_path = self._load_csv_into_tmp("eventhub_extra.csv", tmp)
            flows = self._flows_from_csv(csv_path)
            self.assertEqual(len(flows), 3)
            self.assertEqual({f.source_format for f in flows}, {"eventhub"})

    def test_delta_extra(self):
        # delta_extra.csv ships exactly the 4 tables the launcher's
        # `_seed_demo_delta_source` materializes from demo/resources/data/
        # (customers/transactions/stores/products). Keeping the CSV row
        # count and the seeder's `_DELTA_SOURCE_SEEDS` dict in lock-step
        # is enforced here so a future divergence -- which would deploy
        # flows pointing at non-existent source tables -- fails fast in CI.
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            csv_path = self._load_csv_into_tmp("delta_extra.csv", tmp)
            flows = self._flows_from_csv(csv_path)
            self.assertEqual(len(flows), 4)
            self.assertEqual({f.source_format for f in flows}, {"delta"})
            tables = {f.bronze_table for f in flows}
            self.assertEqual(tables, {"customers", "transactions", "stores", "products"})
            for f in flows:
                # Defaults: --uc-source-catalog falls back to --uc-catalog-name
                # ("main" in tests) and --uc-source-schema defaults to "staging".
                self.assertEqual(f.source_database, "main.staging")
                self.assertTrue(f.source_table)
                self.assertEqual(f.bronze_table, f.silver_table)

    def test_delta_extra_with_overridden_source_schema(self):
        """When --uc-source-catalog/--uc-source-schema are passed, the
        rendered CSV must reflect them so STAGE 3's onboarding entries
        agree with STAGE 4's recipe args (and the source tables seeded
        by `_seed_demo_delta_source`)."""
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            src = DEMO_DIR / "flows" / "delta_extra.csv"
            rendered = tmp / "delta_extra.csv"
            rendered.write_text(_materialize_csv_text(
                src,
                uc_source_catalog="my_catalog",
                uc_source_schema="my_source_schema",
            ))
            flows = self._flows_from_csv(rendered)
            for f in flows:
                self.assertEqual(f.source_database, "my_catalog.my_source_schema")
                self.assertEqual(f.source_system, "my_source_schema")

    def test_all_csvs_have_consistent_columns(self):
        """Every CSV must declare its columns in the header row."""
        for name in (
            "cloudfiles_extra.csv",
            "kafka_extra.csv",
            "eventhub_extra.csv",
            "delta_extra.csv",
        ):
            csv_path = DEMO_DIR / "flows" / name
            with csv_path.open() as f:
                reader = csv.reader(f)
                header = next(reader)
                rows = list(reader)
            self.assertGreater(len(header), 0, f"{name}: empty header")
            for row in rows:
                self.assertEqual(
                    len(row), len(header),
                    f"{name}: row {row} has {len(row)} columns vs header's {len(header)}",
                )


class TopicsFileTests(unittest.TestCase):
    """The topic lists fed to from_topics.py must be non-empty and well-formed."""

    def _load_topics(self, name: str):
        path = DEMO_DIR / "topics" / name
        self.assertTrue(path.is_file(), f"missing topics file: {path}")
        return [
            line.strip()
            for line in path.read_text().splitlines()
            if line.strip() and not line.startswith("#")
        ]

    def test_kafka_topics(self):
        topics = self._load_topics("kafka_topics.txt")
        self.assertEqual(len(topics), 10)
        # Every topic name should be safe to use as a Delta table identifier
        # after the from_topics.py sanitization (replace . and -).
        for t in topics:
            self.assertTrue(all(c.isalnum() or c in "._-" for c in t),
                            f"topic {t!r} has unexpected characters")

    def test_eventhub_topics(self):
        topics = self._load_topics("eventhub_topics.txt")
        self.assertGreater(len(topics), 0)
        # We deliberately use dotted names here; ensure the recipe's
        # sanitization will produce alpha-only table names.
        self.assertTrue(any("." in t for t in topics),
                        "eventhub_topics.txt should exercise sanitization with dotted names")


class LauncherRegistryTests(unittest.TestCase):
    """Import the launcher and confirm its SCENARIOS registry resolves
    every referenced answers/CSV/topics file."""

    @classmethod
    def setUpClass(cls):
        # The launcher modifies sys.path to make the in-tree package
        # importable; importing it here exercises that branch too.
        launcher = REPO_ROOT / "demo" / "launch_dab_template_demo.py"
        spec = importlib.util.spec_from_file_location("launch_dab_template_demo", launcher)
        cls.module = importlib.util.module_from_spec(spec)
        sys.modules["launch_dab_template_demo"] = cls.module
        spec.loader.exec_module(cls.module)

    def test_scenarios_registered(self):
        self.assertEqual(
            set(self.module.SCENARIOS.keys()),
            {
                "cloudfiles",
                "cloudfiles_combined",
                "multi_pipeline_cloudfiles",
                "kafka",
                "eventhub",
                "delta",
            },
        )

    def test_cloudfiles_scenario_set_matches_registry(self):
        """Scenarios that share the cloudFiles demo data + from_volume recipe
        must be listed in `_CLOUDFILES_SCENARIO_NAMES`; otherwise the data
        upload (STAGE 2) and `_demo_landing` seeding (STAGE 4) won't run for
        them, and the recipe will crash with `FileNotFoundError`."""
        self.assertEqual(
            self.module._CLOUDFILES_SCENARIO_NAMES,
            {
                "cloudfiles",
                "cloudfiles_combined",
                "multi_pipeline_cloudfiles",
            },
        )

    def test_cloudfiles_combined_uses_combined_pipeline_mode(self):
        """`cloudfiles_combined` exists specifically to demonstrate the
        combined topology with the same demo data as `cloudfiles`."""
        scenario = self.module.SCENARIOS["cloudfiles_combined"]
        answers = json.loads(scenario.answers_file.read_text())
        self.assertEqual(answers["pipeline_mode"], "combined")
        self.assertEqual(answers["layer"], "bronze_silver")
        self.assertEqual(answers["source_format"], "cloudFiles")
        # Same flows + recipe + recipe args as the split variant -> users can
        # diff the two demo_runs/ directories side-by-side.
        split = self.module.SCENARIOS["cloudfiles"]
        self.assertEqual(scenario.extra_flows_csv, split.extra_flows_csv)
        self.assertEqual(scenario.recipe_name, split.recipe_name)
        self.assertEqual(
            scenario.recipe_args_template, split.recipe_args_template
        )

    def test_duplicate_ownership_stage_rejects_without_writing(self):
        import tempfile

        scenario = self.module.SCENARIOS["multi_pipeline_cloudfiles"]
        with tempfile.TemporaryDirectory() as tmp:
            bundle_dir = Path(tmp)
            (bundle_dir / "resources").mkdir()
            (bundle_dir / "conf").mkdir()
            tracked = bundle_dir / "resources" / "pipelines.yml"
            tracked.write_text("unchanged\n")

            def reject(command, *, output):
                print(
                    "ERROR: Duplicate bronze ownership for dataflow group "
                    "'dab_demo_cf_group'",
                    file=output,
                )
                self.assertEqual(
                    command.pipeline.dataflow_group,
                    "dab_demo_cf_group",
                )
                return 2

            with patch.object(
                self.module,
                "bundle_add_pipeline",
                side_effect=reject,
            ):
                self.module.stage_assert_duplicate_pipeline_rejected(
                    scenario,
                    bundle_dir,
                    uc_schema="ownership_it",
                )

            self.assertEqual(tracked.read_text(), "unchanged\n")

    def test_duplicate_ownership_stage_detects_rejected_write(self):
        import tempfile

        scenario = self.module.SCENARIOS["multi_pipeline_cloudfiles"]
        with tempfile.TemporaryDirectory() as tmp:
            bundle_dir = Path(tmp)
            (bundle_dir / "resources").mkdir()
            (bundle_dir / "conf").mkdir()
            tracked = bundle_dir / "README.md"
            tracked.write_text("before\n")

            def reject_after_write(_command, *, output):
                tracked.write_text("after\n")
                print(
                    "ERROR: Duplicate bronze ownership",
                    file=output,
                )
                return 2

            with (
                patch.object(
                    self.module,
                    "bundle_add_pipeline",
                    side_effect=reject_after_write,
                ),
                self.assertRaisesRegex(SystemExit, "modified bundle file"),
            ):
                self.module.stage_assert_duplicate_pipeline_rejected(
                    scenario,
                    bundle_dir,
                    uc_schema="ownership_it",
                )

    def test_set_onboarding_file_path_updates_named_parameter(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "onboarding.yml"
            path.write_text(yaml.safe_dump({
                "resources": {
                    "jobs": {
                        "onboarding": {
                            "tasks": [{
                                "task_key": "onboard_dataflowspecs",
                                "python_wheel_task": {
                                    "named_parameters": {
                                        "onboarding_file_path": "old",
                                    },
                                },
                            }],
                        },
                    },
                },
            }, sort_keys=False))

            self.module._set_onboarding_file_path(
                path,
                "/Volumes/catalog/schema/volume/conf/onboarding.yml",
            )

            doc = yaml.safe_load(path.read_text())
            params = doc["resources"]["jobs"]["onboarding"]["tasks"][0][
                "python_wheel_task"
            ]["named_parameters"]
            self.assertEqual(
                params["onboarding_file_path"],
                "/Volumes/catalog/schema/volume/conf/onboarding.yml",
            )

    def test_set_onboarding_file_path_reports_missing_task(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "onboarding.yml"
            path.write_text(yaml.safe_dump({
                "resources": {
                    "jobs": {
                        "onboarding": {
                            "tasks": [],
                        },
                    },
                },
            }))

            with self.assertRaisesRegex(
                ValueError,
                "missing task `onboard_dataflowspecs`",
            ):
                self.module._set_onboarding_file_path(path, "/new/path")

    def test_delta_scenario_marked_as_workspace_required(self):
        delta = self.module.SCENARIOS["delta"]
        self.assertTrue(
            delta.recipe_requires_workspace,
            "delta uses recipes/from_uc.py which lists UC tables; offline dry-run is not supported",
        )
        # Other scenarios should NOT be marked as workspace-required so that
        # `--scenario all` works without credentials.
        for name in ("cloudfiles", "cloudfiles_combined", "kafka", "eventhub"):
            self.assertFalse(
                self.module.SCENARIOS[name].recipe_requires_workspace,
                f"{name} should run offline in dry-run mode",
            )

    def test_every_scenario_file_exists(self):
        for name, scenario in self.module.SCENARIOS.items():
            self.assertTrue(scenario.answers_file.is_file(),
                            f"{name}: answers_file missing -> {scenario.answers_file}")
            if scenario.extra_flows_csv is not None:
                self.assertTrue(scenario.extra_flows_csv.is_file(),
                                f"{name}: extra_flows_csv missing -> {scenario.extra_flows_csv}")

    def test_topic_files_referenced_by_topics_recipes_exist(self):
        for name in ("kafka", "eventhub"):
            scenario = self.module.SCENARIOS[name]
            topics_args = [a for a in scenario.recipe_args_template if a.endswith(".txt")]
            self.assertEqual(len(topics_args), 1, f"{name}: expected one topics file arg")
            self.assertTrue(Path(topics_args[0]).is_file(),
                            f"{name}: topics file missing -> {topics_args[0]}")

    def test_scenario_recipe_names_are_real_recipes(self):
        valid = {"from_uc.py", "from_volume.py", "from_topics.py", "from_inventory.py"}
        for name, scenario in self.module.SCENARIOS.items():
            self.assertIn(scenario.recipe_name, valid,
                          f"{name}: unknown recipe {scenario.recipe_name}")


class CloudfilesDatasetReuseTests(unittest.TestCase):
    """The launcher should ship REAL files from demo/resources/data into the
    user's UC volume so cloudFiles flows point at data that actually exists."""

    @classmethod
    def setUpClass(cls):
        launcher = REPO_ROOT / "demo" / "launch_dab_template_demo.py"
        spec = importlib.util.spec_from_file_location("launch_dab_template_demo_reuse", launcher)
        cls.module = importlib.util.module_from_spec(spec)
        sys.modules["launch_dab_template_demo_reuse"] = cls.module
        spec.loader.exec_module(cls.module)

    def test_dataset_registry_points_at_existing_demo_data(self):
        """All four registry entries must exist under demo/resources/data/."""
        registry = self.module._CLOUDFILES_DEMO_DATASETS
        self.assertEqual(set(registry.keys()), {"customers", "transactions", "products", "stores"})
        for table, src_dir in registry.items():
            self.assertTrue(src_dir.is_dir(),
                            f"{table}: missing demo data dir at {src_dir}")
            files = [p for p in src_dir.iterdir() if p.is_file()]
            self.assertGreater(len(files), 0,
                               f"{table}: demo dir {src_dir} contains no files")

    def test_upload_uses_files_api_with_overwrite(self):
        """`_upload_cloudfiles_demo_data` writes every file under each table
        into ``/Volumes/<cat>/<sch>/<vol>/demo_data/<table>/`` with overwrite."""
        from unittest.mock import patch, MagicMock

        with patch("databricks.sdk.WorkspaceClient") as WS:
            ws = MagicMock()
            WS.return_value = ws
            base = self.module._upload_cloudfiles_demo_data(
                "cat", "sch", "vol", profile=None,
            )

        self.assertEqual(base, "/Volumes/cat/sch/vol/demo_data")
        # Should have called files.upload for every shipped file.
        registry = self.module._CLOUDFILES_DEMO_DATASETS
        expected_calls = sum(
            len([p for p in src.iterdir() if p.is_file()]) for src in registry.values()
        )
        self.assertEqual(ws.files.upload.call_count, expected_calls)
        for call in ws.files.upload.call_args_list:
            self.assertTrue(call.kwargs["overwrite"],
                            "uploads must be idempotent (overwrite=True)")
            self.assertTrue(call.kwargs["file_path"].startswith("/Volumes/cat/sch/vol/demo_data/"),
                            f"unexpected file_path: {call.kwargs['file_path']}")

    def test_materialize_csv_substitutes_demo_data_volume_path(self):
        """`_materialize_csv` swaps the placeholder when the path is provided."""
        import tempfile
        scenario = self.module.SCENARIOS["cloudfiles"]
        with tempfile.TemporaryDirectory() as tmp:
            out = self.module._materialize_csv(
                scenario, "cat", Path(tmp),
                demo_data_volume_path="/Volumes/cat/sch/vol/demo_data",
            )
            text = out.read_text()
            self.assertNotIn("{demo_data_volume_path}", text)
            self.assertIn("/Volumes/cat/sch/vol/demo_data/customers/", text)
            self.assertIn(",csv\n", text, "format=csv must round-trip into the rendered CSV")

    def test_materialize_csv_uses_safe_placeholder_when_path_missing(self):
        """Without --apply-prepare-wheel, the CSV still renders (offline-safe)."""
        import tempfile
        scenario = self.module.SCENARIOS["cloudfiles"]
        with tempfile.TemporaryDirectory() as tmp:
            out = self.module._materialize_csv(
                scenario, "cat", Path(tmp),
                demo_data_volume_path=None,
            )
            text = out.read_text()
            self.assertNotIn("{demo_data_volume_path}", text)
            # The placeholder is intentionally obviously fake so it never
            # silently lands in real onboarding files.
            self.assertIn("__placeholder__", text)


class RewriteAndPrunePathsTests(unittest.TestCase):
    """``_rewrite_and_prune_flow_paths`` must:

      * rewrite ``${workspace.file_path}/conf/...`` -> ``<volume_conf>/...``
        when the local file exists,
      * delete the key entirely when the local file is missing (so the
        downstream pipeline falls back to its default instead of crashing
        on a dangling reference, which is the original Spark
        ``PATH_NOT_FOUND`` failure mode this rewrite was added to fix),
      * leave empty strings and unrelated values untouched."""

    @classmethod
    def setUpClass(cls):
        launcher = REPO_ROOT / "demo" / "launch_dab_template_demo.py"
        spec = importlib.util.spec_from_file_location("launch_dab_template_demo_paths", launcher)
        cls.module = importlib.util.module_from_spec(spec)
        sys.modules["launch_dab_template_demo_paths"] = cls.module
        spec.loader.exec_module(cls.module)

    def _new_conf_with(self, files: list) -> Path:
        """Create a temporary conf/ tree that contains exactly ``files``."""
        import tempfile
        tmp = Path(tempfile.mkdtemp(prefix="rewrite_test_"))
        conf = tmp / "conf"
        conf.mkdir()
        for rel in files:
            p = conf / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text("placeholder")
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp))
        return conf

    def test_rewrites_existing_paths_to_uc_volume(self):
        conf = self._new_conf_with([
            "dqe/example_table/bronze_expectations.yml",
            "silver_transformations.yml",
        ])
        flow = {
            "data_flow_id": "100",
            "bronze_data_quality_expectations_json_dev":
                "${workspace.file_path}/conf/dqe/example_table/bronze_expectations.yml",
            "silver_transformation_json_dev":
                "${workspace.file_path}/conf/silver_transformations.yml",
        }
        self.module._rewrite_and_prune_flow_paths(
            flow, conf, "${workspace.file_path}/conf", "/Volumes/cat/sch/vol/conf",
        )
        self.assertEqual(
            flow["bronze_data_quality_expectations_json_dev"],
            "/Volumes/cat/sch/vol/conf/dqe/example_table/bronze_expectations.yml",
        )
        self.assertEqual(
            flow["silver_transformation_json_dev"],
            "/Volumes/cat/sch/vol/conf/silver_transformations.yml",
        )

    def test_drops_dangling_paths_so_pipeline_uses_defaults(self):
        conf = self._new_conf_with([])  # nothing exists
        flow = {
            "data_flow_id": "101",
            "bronze_data_quality_expectations_json_dev":
                "${workspace.file_path}/conf/dqe/customers/bronze_expectations.yml",
            "silver_transformation_json_dev":
                "${workspace.file_path}/conf/silver_transformations.yml",
            "source_details": {
                "source_table": "customers",
                "source_schema_path":
                    "${workspace.file_path}/conf/schemas/customers.ddl",
            },
        }
        self.module._rewrite_and_prune_flow_paths(
            flow, conf, "${workspace.file_path}/conf", "/Volumes/cat/sch/vol/conf",
        )
        self.assertNotIn("bronze_data_quality_expectations_json_dev", flow)
        self.assertNotIn("silver_transformation_json_dev", flow)
        self.assertNotIn("source_schema_path", flow["source_details"])
        # Unrelated keys remain.
        self.assertEqual(flow["data_flow_id"], "101")
        self.assertEqual(flow["source_details"]["source_table"], "customers")

    def test_seeds_quarantine_defaults_when_dqe_kept(self):
        """The engine accesses bronze_quarantine_table unconditionally when
        DQE is set; the helper must seed an empty default so the resulting
        Row schema has that column (avoiding PySparkValueError)."""
        conf = self._new_conf_with(["dqe/example_table/bronze_expectations.yml"])
        flow = {
            "data_flow_id": "100",
            "bronze_database_dev": "cat.bronze",
            "bronze_data_quality_expectations_json_dev":
                "${workspace.file_path}/conf/dqe/example_table/bronze_expectations.yml",
        }
        self.module._rewrite_and_prune_flow_paths(
            flow, conf, "${workspace.file_path}/conf", "/Volumes/cat/sch/vol/conf",
        )
        self.assertEqual(flow["bronze_quarantine_table"], "")
        self.assertEqual(flow["bronze_quarantine_table_path_dev"], "")
        self.assertEqual(flow["bronze_database_quarantine_dev"], "cat.bronze")
        self.assertEqual(flow["bronze_quarantine_table_properties"], {})
        self.assertEqual(flow["bronze_quarantine_table_cluster_by"], [])

    def test_does_not_seed_quarantine_when_dqe_pruned(self):
        """If we drop the DQE reference, no quarantine defaults are added
        (the engine never enters the DQE code path)."""
        conf = self._new_conf_with([])  # nothing -> DQE will be pruned
        flow = {
            "data_flow_id": "100",
            "bronze_data_quality_expectations_json_dev":
                "${workspace.file_path}/conf/dqe/missing/bronze_expectations.yml",
        }
        self.module._rewrite_and_prune_flow_paths(
            flow, conf, "${workspace.file_path}/conf", "/Volumes/cat/sch/vol/conf",
        )
        self.assertNotIn("bronze_data_quality_expectations_json_dev", flow)
        self.assertNotIn("bronze_quarantine_table", flow)

    def test_leaves_empty_string_and_unrelated_values_alone(self):
        conf = self._new_conf_with([])
        flow = {
            "bronze_table_path_dev": "",
            "silver_table_path_dev": "",
            "bronze_table": "customers",
        }
        before = dict(flow)
        self.module._rewrite_and_prune_flow_paths(
            flow, conf, "${workspace.file_path}/conf", "/Volumes/cat/sch/vol/conf",
        )
        self.assertEqual(flow, before)


class SeedDemoLandingTests(unittest.TestCase):
    """`_seed_demo_landing` must populate the recipe's landing tree with REAL
    CSV files (copied from demo/resources/data/) so the recipe-generated
    flows have actual data once the launcher uploads them to UC."""

    @classmethod
    def setUpClass(cls):
        launcher = REPO_ROOT / "demo" / "launch_dab_template_demo.py"
        spec = importlib.util.spec_from_file_location("launch_dab_template_demo_seed", launcher)
        cls.module = importlib.util.module_from_spec(spec)
        sys.modules["launch_dab_template_demo_seed"] = cls.module
        spec.loader.exec_module(cls.module)

    def test_seed_copies_real_files_into_each_landing_subdir(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            self.module._seed_demo_landing(Path(tmp))
            landing = Path(tmp) / "_demo_landing"
            for sub in ("customers_streaming", "orders_streaming", "events_streaming"):
                d = landing / sub
                self.assertTrue(d.is_dir(), f"missing landing subdir {sub}")
                files = [p for p in d.iterdir() if p.is_file()]
                self.assertGreater(
                    len(files), 0,
                    f"{sub}: expected at least one seeded CSV (so the recipe's "
                    f"flow has real data), got an empty dir",
                )

    def test_seed_is_idempotent(self):
        """Re-seeding must not duplicate or corrupt files (we just copy if absent)."""
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            self.module._seed_demo_landing(Path(tmp))
            self.module._seed_demo_landing(Path(tmp))
            landing = Path(tmp) / "_demo_landing"
            for sub in ("customers_streaming", "orders_streaming", "events_streaming"):
                files = sorted(p.name for p in (landing / sub).iterdir() if p.is_file())
                self.assertEqual(files, sorted(set(files)),
                                 f"{sub}: re-seeding produced duplicate file names")


class MaterializeLocalSourcePathTests(unittest.TestCase):
    """`_materialize_local_source_paths_to_volume` should upload local files
    to UC and rewrite source_path_dev to the resulting volume path so the
    pipeline reads from a path that actually exists on the workspace."""

    @classmethod
    def setUpClass(cls):
        launcher = REPO_ROOT / "demo" / "launch_dab_template_demo.py"
        spec = importlib.util.spec_from_file_location("launch_dab_template_demo_local", launcher)
        cls.module = importlib.util.module_from_spec(spec)
        sys.modules["launch_dab_template_demo_local"] = cls.module
        spec.loader.exec_module(cls.module)

    def _flow_with_local_path(self, local_dir: Path, table: str = "customers_streaming",
                              fmt_option: str = "json") -> dict:
        return {
            "data_flow_id": "200",
            "source_format": "cloudFiles",
            "source_details": {
                "source_table": table,
                "source_path_dev": str(local_dir),
            },
            "bronze_reader_options": {
                "cloudFiles.format": fmt_option,
                "cloudFiles.inferColumnTypes": "true",
            },
        }

    def test_uploads_files_and_rewrites_source_path(self):
        import tempfile
        from unittest.mock import MagicMock

        with tempfile.TemporaryDirectory() as tmp:
            bundle_dir = Path(tmp)
            local_dir = bundle_dir / "_demo_landing" / "customers_streaming"
            local_dir.mkdir(parents=True)
            (local_dir / "LOAD00000001.csv").write_text("id,name\n1,a\n")
            (local_dir / "LOAD00000002.csv").write_text("id,name\n2,b\n")

            ws = MagicMock()
            flow = self._flow_with_local_path(local_dir)
            result = self.module._materialize_local_source_paths_to_volume(
                flow, bundle_dir, ws, "/Volumes/cat/sch/vol/demo_data",
            )

            self.assertTrue(result, "rewrite should succeed when files exist")
            self.assertEqual(
                flow["source_details"]["source_path_dev"],
                "/Volumes/cat/sch/vol/demo_data/customers_streaming/",
            )
            # Both files uploaded to the same volume target with overwrite=True.
            self.assertEqual(ws.files.upload.call_count, 2)
            for call in ws.files.upload.call_args_list:
                self.assertTrue(call.kwargs["overwrite"])
                self.assertTrue(call.kwargs["file_path"].startswith(
                    "/Volumes/cat/sch/vol/demo_data/customers_streaming/"
                ))
            # cloudFiles.format flips to csv to match the uploaded files.
            self.assertEqual(flow["bronze_reader_options"]["cloudFiles.format"], "csv")

    def test_returns_false_for_empty_local_dir(self):
        import tempfile
        from unittest.mock import MagicMock

        with tempfile.TemporaryDirectory() as tmp:
            bundle_dir = Path(tmp)
            local_dir = bundle_dir / "_demo_landing" / "empty"
            local_dir.mkdir(parents=True)

            ws = MagicMock()
            flow = self._flow_with_local_path(local_dir, table="empty")
            result = self.module._materialize_local_source_paths_to_volume(
                flow, bundle_dir, ws, "/Volumes/cat/sch/vol/demo_data",
            )

            self.assertFalse(result, "should report False so caller drops the flow")
            self.assertEqual(ws.files.upload.call_count, 0)
            # Path is left untouched (caller will drop the flow).
            self.assertEqual(flow["source_details"]["source_path_dev"], str(local_dir))

    def test_returns_none_for_uc_volume_paths(self):
        from unittest.mock import MagicMock
        ws = MagicMock()
        flow = {
            "data_flow_id": "201",
            "source_details": {
                "source_table": "customers",
                "source_path_dev": "/Volumes/cat/sch/vol/demo_data/customers/",
            },
            "bronze_reader_options": {"cloudFiles.format": "csv"},
        }
        result = self.module._materialize_local_source_paths_to_volume(
            flow, Path("/tmp"), ws, "/Volumes/cat/sch/vol/demo_data",
        )
        self.assertIsNone(result, "UC volume paths should be left to the existence-check step")
        self.assertEqual(ws.files.upload.call_count, 0)


class EnsureTargetSchemasTests(unittest.TestCase):
    """`_ensure_target_schemas` must read variables.yml, then create the
    sdp_meta / bronze / silver schemas on the workspace if they don't exist
    (so onboarding + SDP don't fail with SCHEMA_NOT_FOUND)."""

    @classmethod
    def setUpClass(cls):
        launcher = REPO_ROOT / "demo" / "launch_dab_template_demo.py"
        spec = importlib.util.spec_from_file_location("launch_dab_template_demo_schemas", launcher)
        cls.module = importlib.util.module_from_spec(spec)
        sys.modules["launch_dab_template_demo_schemas"] = cls.module
        spec.loader.exec_module(cls.module)

    def _bundle_with_variables(self, *, sdp_meta="sdp_meta_dab_demo_cf",
                               bronze="sdp_meta_bronze_dab_demo_cf",
                               silver="sdp_meta_silver_dab_demo_cf") -> Path:
        import tempfile
        import yaml as _yaml
        tmp = Path(tempfile.mkdtemp(prefix="ensure_schemas_"))
        (tmp / "resources").mkdir()
        (tmp / "resources" / "variables.yml").write_text(_yaml.safe_dump({
            "variables": {
                "sdp_meta_schema": {"default": sdp_meta},
                "bronze_target_schema": {"default": bronze},
                "silver_target_schema": {"default": silver},
            }
        }, sort_keys=False))
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp))
        return tmp

    def test_resolve_returns_unique_ordered_schemas(self):
        bundle_dir = self._bundle_with_variables(
            sdp_meta="meta_x", bronze="bronze_x", silver="silver_x",
        )
        self.assertEqual(
            self.module._resolve_target_schemas(bundle_dir),
            ["meta_x", "bronze_x", "silver_x"],
        )

    def test_resolve_dedupes_when_combined_pipeline_uses_one_schema(self):
        # combined pipelines often re-use the same schema for bronze+silver
        bundle_dir = self._bundle_with_variables(
            sdp_meta="combined", bronze="combined", silver="combined",
        )
        self.assertEqual(
            self.module._resolve_target_schemas(bundle_dir),
            ["combined"],
        )

    def test_creates_missing_schemas_and_skips_existing(self):
        from unittest.mock import patch, MagicMock
        from databricks.sdk.errors import NotFound

        bundle_dir = self._bundle_with_variables()
        ws = MagicMock()

        # First schema exists, second/third are missing.
        existing = {"cat.sdp_meta_dab_demo_cf"}

        def fake_get(name: str):
            if name in existing:
                return MagicMock()
            raise NotFound(f"missing {name}")

        ws.schemas.get.side_effect = fake_get
        ws.catalogs.get.return_value = MagicMock()

        with patch("databricks.sdk.WorkspaceClient", return_value=ws):
            self.module._ensure_target_schemas(bundle_dir, "cat", profile=None)

        self.assertEqual(ws.catalogs.get.call_count, 1)
        self.assertEqual(ws.schemas.get.call_count, 3)
        # Only the two missing schemas got created.
        self.assertEqual(ws.schemas.create.call_count, 2)
        created_names = sorted(call.kwargs["name"] for call in ws.schemas.create.call_args_list)
        self.assertEqual(created_names,
                         ["sdp_meta_bronze_dab_demo_cf", "sdp_meta_silver_dab_demo_cf"])
        for call in ws.schemas.create.call_args_list:
            self.assertEqual(call.kwargs["catalog_name"], "cat")

    def test_missing_catalog_is_actionable_error(self):
        from unittest.mock import patch, MagicMock
        from databricks.sdk.errors import NotFound

        bundle_dir = self._bundle_with_variables()
        ws = MagicMock()
        ws.catalogs.get.side_effect = NotFound("catalog 'cat' not found")

        with patch("databricks.sdk.WorkspaceClient", return_value=ws):
            with self.assertRaises(SystemExit) as ctx:
                self.module._ensure_target_schemas(bundle_dir, "cat", profile=None)
        self.assertIn("Catalog 'cat' is not accessible", str(ctx.exception))
        # We never tried to create a schema in a missing catalog.
        self.assertEqual(ws.schemas.create.call_count, 0)

    def test_already_exists_is_treated_as_success(self):
        """If two demo runs race the create call, we tolerate ALREADY_EXISTS."""
        from unittest.mock import patch, MagicMock
        from databricks.sdk.errors import NotFound

        bundle_dir = self._bundle_with_variables(
            sdp_meta="m", bronze="b", silver="s",
        )
        ws = MagicMock()
        ws.catalogs.get.return_value = MagicMock()
        ws.schemas.get.side_effect = NotFound("missing")
        ws.schemas.create.side_effect = Exception("Schema 'cat.m' ALREADY_EXISTS")

        with patch("databricks.sdk.WorkspaceClient", return_value=ws):
            # Should not raise even though every create call says ALREADY_EXISTS.
            self.module._ensure_target_schemas(bundle_dir, "cat", profile=None)


class StageBundleInitCleanupTests(unittest.TestCase):
    """`stage_bundle_init` removes a stale scaffold (so re-runs are idempotent)."""

    @classmethod
    def setUpClass(cls):
        launcher = REPO_ROOT / "demo" / "launch_dab_template_demo.py"
        spec = importlib.util.spec_from_file_location("launch_dab_template_demo_cleanup", launcher)
        cls.module = importlib.util.module_from_spec(spec)
        sys.modules["launch_dab_template_demo_cleanup"] = cls.module
        spec.loader.exec_module(cls.module)

    def _invoke(self, tmp_root: Path, *, clean: bool, pre_create: bool,
                expect_raise: bool = False):
        """Run stage_bundle_init against a caller-owned tmp dir so the
        caller can assert on disk state before the dir is cleaned up."""
        import shutil
        from unittest.mock import patch

        scenario = self.module.SCENARIOS["cloudfiles"]
        bundle_dir = self.module._bundle_dir_from_scaffold(scenario, tmp_root, "main")
        if pre_create:
            bundle_dir.mkdir(parents=True)
            (bundle_dir / "marker.txt").write_text("stale")

        def fake_bundle_init(cmd):
            bundle_dir.mkdir(parents=True, exist_ok=True)
            (bundle_dir / "fresh.txt").write_text("scaffolded")
            return 0

        with patch.object(shutil, "which", return_value="/fake/databricks"), \
             patch.object(self.module, "bundle_init", side_effect=fake_bundle_init):
            if expect_raise:
                with self.assertRaises(SystemExit) as ctx:
                    self.module.stage_bundle_init(
                        scenario, tmp_root, "main", profile=None, clean=clean,
                    )
                return ctx.exception, bundle_dir
            result_dir = self.module.stage_bundle_init(
                scenario, tmp_root, "main", profile=None, clean=clean,
            )
            return result_dir, bundle_dir

    def test_clean_removes_stale_scaffold(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            result_dir, bundle_dir = self._invoke(Path(tmp), clean=True, pre_create=True)
            self.assertEqual(result_dir, bundle_dir)
            self.assertFalse((bundle_dir / "marker.txt").exists(),
                             "stale scaffold should be removed before bundle init")
            self.assertTrue((bundle_dir / "fresh.txt").exists(),
                            "fresh scaffold should land in place of the stale one")

    def test_no_clean_with_existing_scaffold_raises(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            exc, _ = self._invoke(Path(tmp), clean=False, pre_create=True, expect_raise=True)
            self.assertIn("already exists", str(exc))

    def test_no_clean_with_no_existing_scaffold_succeeds(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            result_dir, bundle_dir = self._invoke(Path(tmp), clean=False, pre_create=False)
            self.assertEqual(result_dir, bundle_dir)
            self.assertTrue((bundle_dir / "fresh.txt").exists())


if __name__ == "__main__":
    unittest.main()
