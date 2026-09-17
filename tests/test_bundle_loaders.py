"""Tests for the interactive ``_load_bundle_*_config`` loaders + cli.py dispatchers.

These functions translate ``WorkspaceInstaller`` prompts into the
typed dataclasses (``BundlePrepareWheelCommand``, ``BundleValidateCommand``,
``BundleAddFlowCommand``) that drive the actual ``bundle ...``
runners. Pre-existing tests cover the runners end-to-end via rendered
templates, but the loader prompt-translation layer was almost entirely
uncovered (lines 1133-1236 of ``bundle.py``, ~85 stmts) along with the
matching three-line cli.py dispatchers (``bundle_prepare_wheel``,
``bundle_validate``, ``bundle_add_flow``).

The loaders are pure prompt-translation: feed them a fake ``wsi`` that
returns canned answers and assert the resulting command dataclass.

Bonus targets:
- ``_build_source_details`` ``eventhub`` and ``snapshot`` branches
  (lines 850-874) — also pure functions, also missed by the existing
  ``_sdp_meta_sanity_checks`` end-to-end tests because those only
  drive the ``cloudFiles`` and ``delta`` paths.
- The ``_volume_path_exists`` helper's NotFound branch.
"""
from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from databricks.labs.sdp_meta.bundle import (
    BundleAddFlowCommand,
    BundlePrepareWheelCommand,
    BundleValidateCommand,
    FlowSpec,
    _build_source_details,
    _load_bundle_add_flow_config,
    _load_bundle_prepare_wheel_config,
    _load_bundle_validate_config,
)


def _make_wsi(*, questions: dict, choices: dict) -> MagicMock:
    """Build a fake ``WorkspaceInstaller`` returning canned answers.

    ``_question(text, default=...)`` -> looks up by EXACT prompt text.
    ``_choice(text, choices)`` -> looks up by EXACT prompt text.

    Using exact prompt text is intentional: if the loader's prompt
    wording changes, the test fails loud and we update both at once,
    rather than the test silently matching a now-obsolete prompt.
    """
    wsi = MagicMock()

    def _question_side_effect(text, default=None):
        if text not in questions:
            raise AssertionError(
                f"unexpected _question prompt: {text!r}; "
                f"known prompts: {list(questions)}"
            )
        return questions[text]

    def _choice_side_effect(text, options, **_):
        if text not in choices:
            raise AssertionError(
                f"unexpected _choice prompt: {text!r}; "
                f"known prompts: {list(choices)}"
            )
        return choices[text]

    wsi._question.side_effect = _question_side_effect
    wsi._choice.side_effect = _choice_side_effect
    return wsi


# A patch target that sidesteps ``_load_bundle_prepare_wheel_config``'s
# UC-identifier validation. The real ``prompt_uc_identifier`` retries
# on validation failure; we just want it to return what the user typed.
_IDENT_PATCH = "databricks.labs.sdp_meta.bundle._ident_prompt"


class TestLoadBundlePrepareWheelConfig(unittest.TestCase):
    """``bundle_prepare_wheel`` interactive parameter loader."""

    def test_minimal_inputs_no_pip_mirror(self):
        """Empty pip mirror prompts -> ``pip_index_url`` / extras are None."""
        wsi = _make_wsi(
            questions={
                "pip --index-url (blank to use default / $PIP_INDEX_URL)": "",
                "pip --extra-index-url (space-separated, blank for none)": "",
            },
            choices={
                "Auto-create the schema and volume if they don't exist?": "True",
            },
        )
        with patch(_IDENT_PATCH, side_effect=lambda _wsi, _text, **_kw: {
            "Unity Catalog catalog name": "main",
            "UC schema for the wheel volume": "sdp_meta_dataflowspecs",
            "UC volume name": "sdp_meta_wheels",
        }[_text]):
            with patch.dict("os.environ", {}, clear=False) as _:
                cfg = _load_bundle_prepare_wheel_config(wsi)

        self.assertIsInstance(cfg, BundlePrepareWheelCommand)
        self.assertEqual(cfg.uc_catalog, "main")
        self.assertEqual(cfg.uc_schema, "sdp_meta_dataflowspecs")
        self.assertEqual(cfg.uc_volume, "sdp_meta_wheels")
        self.assertIsNone(cfg.pip_index_url)
        self.assertIsNone(cfg.pip_extra_index_urls)
        self.assertTrue(cfg.create_if_missing)

    def test_pip_mirror_and_multiple_extras(self):
        wsi = _make_wsi(
            questions={
                "pip --index-url (blank to use default / $PIP_INDEX_URL)":
                    "https://pypi.internal.example.com/simple",
                "pip --extra-index-url (space-separated, blank for none)":
                    "https://m1.example.com/simple https://m2.example.com/simple",
            },
            choices={
                "Auto-create the schema and volume if they don't exist?": "False",
            },
        )
        with patch(_IDENT_PATCH, side_effect=lambda _wsi, _text, **_kw: {
            "Unity Catalog catalog name": "prod",
            "UC schema for the wheel volume": "wheels",
            "UC volume name": "vol",
        }[_text]):
            cfg = _load_bundle_prepare_wheel_config(wsi)

        self.assertEqual(cfg.pip_index_url, "https://pypi.internal.example.com/simple")
        self.assertEqual(
            cfg.pip_extra_index_urls,
            ["https://m1.example.com/simple", "https://m2.example.com/simple"],
        )
        self.assertFalse(cfg.create_if_missing)


class TestLoadBundleValidateConfig(unittest.TestCase):
    """``bundle_validate`` interactive parameter loader."""

    def test_default_bundle_dir_no_target(self):
        wsi = _make_wsi(
            questions={
                "Bundle directory": ".",
                "Bundle target (blank for default)": "",
            },
            choices={},
        )
        cfg = _load_bundle_validate_config(wsi)
        self.assertIsInstance(cfg, BundleValidateCommand)
        self.assertEqual(cfg.bundle_dir, ".")
        self.assertIsNone(cfg.target)

    def test_custom_dir_and_target(self):
        wsi = _make_wsi(
            questions={
                "Bundle directory": "/path/to/bundle",
                "Bundle target (blank for default)": "prod",
            },
            choices={},
        )
        cfg = _load_bundle_validate_config(wsi)
        self.assertEqual(cfg.bundle_dir, "/path/to/bundle")
        self.assertEqual(cfg.target, "prod")


class TestLoadBundleAddFlowConfig(unittest.TestCase):
    """``bundle_add_flow`` interactive parameter loader.

    Five source-format branches × {single, csv} mode = 6 distinct
    code paths. We cover one representative test per source-format
    branch plus the csv shortcut.
    """

    def test_csv_mode_skips_per_flow_prompts(self):
        wsi = _make_wsi(
            questions={
                "Bundle directory": ".",
                "Path to CSV file": "my_flows.csv",
            },
            choices={
                "Add a single flow or batch from CSV?": "csv",
                "Dry run (preview only, no file write)?": "True",
            },
        )
        cfg = _load_bundle_add_flow_config(wsi)
        self.assertIsInstance(cfg, BundleAddFlowCommand)
        self.assertEqual(cfg.from_csv, "my_flows.csv")
        self.assertEqual(cfg.flows, [])
        self.assertTrue(cfg.dry_run)

    def _single_flow_questions(self, source_format: str, **extras):
        """Common per-flow prompts plus source-format specific ones."""
        base = {
            "Bundle directory": ".",
            "Bronze table name (leave blank if silver-only)": "orders_bronze",
            "Silver table name (blank = same as bronze)": "",
            "data_flow_id (use `auto` to auto-increment)": "auto",
            "data_flow_group (blank = use bundle default)": "",
            "Flow layer (blank = use bundle default)": "",
            "Bronze target schema (blank = use bundle default)": "",
            "Silver target schema (blank = use bundle default)": "",
        }
        base.update(extras)
        return base, {
            "Add a single flow or batch from CSV?": "single",
            "Dry run (preview only, no file write)?": "False",
            "Source format": source_format,
        }

    def test_single_flow_cloudfiles_branch(self):
        questions, choices = self._single_flow_questions(
            "cloudFiles",
            **{
                "Source path (e.g. /Volumes/raw/landing/orders/)": "/Volumes/raw/orders/",
                "Source schema DDL path (blank to skip)": "",
            },
        )
        wsi = _make_wsi(questions=questions, choices=choices)
        cfg = _load_bundle_add_flow_config(wsi)
        self.assertEqual(len(cfg.flows), 1)
        flow = cfg.flows[0]
        self.assertEqual(flow.source_format, "cloudFiles")
        self.assertEqual(flow.source_path, "/Volumes/raw/orders/")
        self.assertIsNone(flow.source_schema_path)
        self.assertEqual(flow.bronze_table, "orders_bronze")

    def test_single_flow_delta_branch(self):
        questions, choices = self._single_flow_questions(
            "delta",
            **{
                "Source database": "raw",
                "Source table": "orders_raw",
            },
        )
        wsi = _make_wsi(questions=questions, choices=choices)
        cfg = _load_bundle_add_flow_config(wsi)
        flow = cfg.flows[0]
        self.assertEqual(flow.source_format, "delta")
        self.assertEqual(flow.source_database, "raw")
        self.assertEqual(flow.source_table, "orders_raw")

    def test_single_flow_kafka_branch(self):
        questions, choices = self._single_flow_questions(
            "kafka",
            **{
                "kafka.bootstrap.servers": "broker.example.com:9092",
                "subscribe (topic name)": "events",
            },
        )
        wsi = _make_wsi(questions=questions, choices=choices)
        cfg = _load_bundle_add_flow_config(wsi)
        flow = cfg.flows[0]
        self.assertEqual(flow.source_format, "kafka")
        self.assertEqual(flow.kafka_bootstrap_servers, "broker.example.com:9092")
        self.assertEqual(flow.kafka_topic, "events")

    def test_single_flow_eventhub_branch(self):
        questions, choices = self._single_flow_questions(
            "eventhub",
            **{
                "eventhub.namespace": "my-eh-ns",
                "eventhub.name": "events",
            },
        )
        wsi = _make_wsi(questions=questions, choices=choices)
        cfg = _load_bundle_add_flow_config(wsi)
        flow = cfg.flows[0]
        self.assertEqual(flow.source_format, "eventhub")
        # eventhub uses kafka_bootstrap_servers and kafka_topic as the carrier
        # for the namespace and name (it's the EH-via-Kafka-API pattern).
        self.assertEqual(flow.kafka_bootstrap_servers, "my-eh-ns")
        self.assertEqual(flow.kafka_topic, "events")

    def test_single_flow_snapshot_branch(self):
        questions, choices = self._single_flow_questions(
            "snapshot",
            **{
                "Snapshot source path (e.g. /Volumes/raw/snapshots/orders/)":
                    "/Volumes/raw/snapshots/orders/",
            },
        )
        wsi = _make_wsi(questions=questions, choices=choices)
        cfg = _load_bundle_add_flow_config(wsi)
        flow = cfg.flows[0]
        self.assertEqual(flow.source_format, "snapshot")
        self.assertEqual(flow.source_path, "/Volumes/raw/snapshots/orders/")


class TestBuildSourceDetails(unittest.TestCase):
    """``_build_source_details`` branches the ``_sdp_meta_sanity_checks``
    e2e tests don't reach: ``eventhub``, ``snapshot``, and the
    ``ValueError`` validation branches.
    """

    def test_kafka_requires_bootstrap_and_topic(self):
        spec = FlowSpec(source_format="kafka")
        with self.assertRaises(ValueError) as cm:
            _build_source_details(spec, "main")
        self.assertIn("kafka_bootstrap_servers", str(cm.exception))
        self.assertIn("kafka_topic", str(cm.exception))

    def test_kafka_with_schema_path_includes_it(self):
        spec = FlowSpec(
            source_format="kafka",
            kafka_bootstrap_servers="b:9092",
            kafka_topic="t",
            source_schema_path="/path/schema.ddl",
        )
        details = _build_source_details(spec, "main")
        self.assertEqual(details["kafka.bootstrap.servers"], "b:9092")
        self.assertEqual(details["subscribe"], "t")
        self.assertEqual(details["startingOffsets"], "earliest")
        self.assertEqual(details["source_schema_path"], "/path/schema.ddl")

    def test_eventhub_requires_topic(self):
        spec = FlowSpec(source_format="eventhub", kafka_bootstrap_servers="ns")
        with self.assertRaises(ValueError) as cm:
            _build_source_details(spec, "main")
        self.assertIn("eventhub.name", str(cm.exception))

    def test_eventhub_full_mapping(self):
        spec = FlowSpec(
            source_format="eventhub",
            kafka_bootstrap_servers="prod-ns",
            kafka_topic="events",
            source_schema_path="/p/s.ddl",
        )
        details = _build_source_details(spec, "main")
        self.assertEqual(details["eventhub.namespace"], "prod-ns")
        self.assertEqual(details["eventhub.name"], "events")
        self.assertEqual(details["eventhub.port"], "9093")
        # Placeholders propagate when the user hasn't filled them yet.
        self.assertEqual(details["eventhub.accessKeyName"], "<your-sas-policy-name>")
        self.assertEqual(details["eventhub.accessKeySecretName"], "<your-secret-name>")
        self.assertEqual(details["eventhub.secretsScopeName"], "<your-secret-scope>")
        self.assertEqual(details["kafka.sasl.mechanism"], "PLAIN")
        self.assertEqual(details["kafka.security.protocol"], "SASL_SSL")
        self.assertEqual(details["source_schema_path"], "/p/s.ddl")

    def test_eventhub_namespace_falls_back_to_placeholder(self):
        spec = FlowSpec(
            source_format="eventhub",
            kafka_topic="events",
            # kafka_bootstrap_servers (carrying namespace) intentionally unset
        )
        details = _build_source_details(spec, "main")
        self.assertEqual(details["eventhub.namespace"], "<your-eventhub-namespace>")

    def test_snapshot_with_explicit_source_path(self):
        spec = FlowSpec(
            source_format="snapshot",
            source_path="/Volumes/raw/snapshots/orders/",
            snapshot_format="parquet",
        )
        details = _build_source_details(spec, "main")
        self.assertEqual(details["source_path_dev"], "/Volumes/raw/snapshots/orders/")
        self.assertEqual(details["snapshot_format"], "parquet")

    def test_snapshot_falls_back_to_template_path(self):
        spec = FlowSpec(source_format="snapshot", bronze_table="orders")
        details = _build_source_details(spec, "main")
        self.assertEqual(
            details["source_path_dev"],
            "/Volumes/main/landing/snapshots/orders/",
        )
        # Default snapshot_format is ``delta``.
        self.assertEqual(details["snapshot_format"], "delta")


class TestCliBundleDispatchers(unittest.TestCase):
    """The cli.py thin dispatchers (``bundle_prepare_wheel``,
    ``bundle_validate``, ``bundle_add_flow``) just glue the loader to
    its runner. They were uncovered because the in-process tests
    avoided constructing an SDPMeta + wsi pair. Exercising them is
    cheap and adds 30+ lines of coverage.
    """

    def test_bundle_prepare_wheel_calls_loader_then_runner(self):
        from databricks.labs.sdp_meta.cli import bundle_prepare_wheel

        sdp_meta = MagicMock()
        loader_result = BundlePrepareWheelCommand(
            uc_catalog="main",
            uc_schema="sdp_meta_dataflowspecs",
            uc_volume="sdp_meta_wheels",
        )
        with (
            patch(
                "databricks.labs.sdp_meta.bundle._load_bundle_prepare_wheel_config",
                return_value=loader_result,
            ) as mock_loader,
            patch(
                "databricks.labs.sdp_meta.bundle.bundle_prepare_wheel"
            ) as mock_run,
        ):
            mock_run.return_value = 0
            bundle_prepare_wheel(sdp_meta, flags={})
            mock_loader.assert_called_once_with(sdp_meta._wsi)
            mock_run.assert_called_once_with(loader_result)

    def test_bundle_validate_calls_loader_then_runner_success(self):
        from databricks.labs.sdp_meta.cli import bundle_validate

        sdp_meta = MagicMock()
        loader_result = BundleValidateCommand(bundle_dir=".", target=None)
        with (
            patch(
                "databricks.labs.sdp_meta.bundle._load_bundle_validate_config",
                return_value=loader_result,
            ),
            patch(
                "databricks.labs.sdp_meta.bundle.bundle_validate",
                return_value=0,
            ),
        ):
            # Successful run (rc == 0) must NOT call sys.exit.
            bundle_validate(sdp_meta, flags={})

    def test_bundle_validate_exits_when_runner_returns_nonzero(self):
        from databricks.labs.sdp_meta.cli import bundle_validate

        sdp_meta = MagicMock()
        loader_result = BundleValidateCommand(bundle_dir=".", target=None)
        with (
            patch(
                "databricks.labs.sdp_meta.bundle._load_bundle_validate_config",
                return_value=loader_result,
            ),
            patch(
                "databricks.labs.sdp_meta.bundle.bundle_validate",
                return_value=2,
            ),
            self.assertRaises(SystemExit) as cm,
        ):
            bundle_validate(sdp_meta, flags={})
        self.assertEqual(cm.exception.code, 2)

    def test_bundle_add_flow_calls_loader_then_runner_success(self):
        from databricks.labs.sdp_meta.cli import bundle_add_flow

        sdp_meta = MagicMock()
        loader_result = BundleAddFlowCommand(bundle_dir=".", flows=[])
        with (
            patch(
                "databricks.labs.sdp_meta.bundle._load_bundle_add_flow_config",
                return_value=loader_result,
            ),
            patch(
                "databricks.labs.sdp_meta.bundle.bundle_add_flow",
                return_value=0,
            ),
        ):
            bundle_add_flow(sdp_meta, flags={})

    def test_bundle_add_flow_exits_when_runner_fails(self):
        from databricks.labs.sdp_meta.cli import bundle_add_flow

        sdp_meta = MagicMock()
        loader_result = BundleAddFlowCommand(bundle_dir=".", flows=[])
        with (
            patch(
                "databricks.labs.sdp_meta.bundle._load_bundle_add_flow_config",
                return_value=loader_result,
            ),
            patch(
                "databricks.labs.sdp_meta.bundle.bundle_add_flow",
                return_value=1,
            ),
            self.assertRaises(SystemExit) as cm,
        ):
            bundle_add_flow(sdp_meta, flags={})
        self.assertEqual(cm.exception.code, 1)


if __name__ == "__main__":
    unittest.main()
