"""Backward-compatibility tests: v0.1.0 must not break v0.0.10 customers.

When a customer upgrades the wheel from v0.0.10 to v0.1.0, three
independent surfaces have to keep working without any action on their
side:

1. **Persisted dataflowspec Delta tables.** The customer's existing
   ``bronze_dataflowspec`` / ``silver_dataflowspec`` Delta tables were
   written by v0.0.10 code and physically lack the v0.1.0 columns
   (``clusterByAuto``, ``cdcApplyChangesFlows``,
   ``cdcApplyChangesFlowsSchemas``, ``rowFilter``,
   ``quarantineRowFilter``). When v0.1.0's
   :func:`get_bronze_dataflow_spec` / :func:`get_silver_dataflow_spec`
   read those tables, :meth:`DataflowSpecUtils.populate_additional_df_cols`
   must backfill the missing columns with ``None`` so the dataclass
   instantiation succeeds.

2. **v0.0.10-format onboarding files.** After applying the documented
   migration for the intentionally corrected missing-quarantine-target case,
   re-onboarding must persist every unrelated new field with its safe default.

3. **DataflowPipeline runtime.** A pipeline constructed from a legacy
   v0.0.10 spec (new fields ``None``) must take the legacy code paths
   — no row-filter dispatch, no multi-source CDC dispatch, no
   ``clusterByAuto`` behaviour.

The fixtures here are intentionally pinned to the v0.0.10
``BronzeDataflowSpec`` / ``SilverDataflowSpec`` shapes (24 / 25
fields, sourced from ``git show v0.0.10:src/dataflow_spec.py``).

If a future PR adds a field to either dataclass, the contract is:

    1. Add the new field to ``additional_bronze_df_columns`` /
       ``additional_silver_df_columns`` in :class:`DataflowSpecUtils`
       so reads of legacy persisted Delta tables backfill ``None``.
    2. Extend ``NEW_BRONZE_FIELDS_AT_READ`` /
       ``NEW_SILVER_FIELDS_AT_READ`` below so this suite asserts the
       new field reads back as ``None``.
    3. Extend ``EXPECTED_BRONZE_DEFAULTS_AT_ONBOARDING`` /
       ``EXPECTED_SILVER_DEFAULTS_AT_ONBOARDING`` with the value the
       onboarding parser persists when the field is absent in the
       JSON. Use ``None`` unless the field has a deliberate
       non-``None`` default (e.g. boolean opt-in flags default to
       ``False``, see ``clusterByAuto``).

A new dataclass field that doesn't make it into the additional lists
will break customer pipelines on upgrade, and these tests will fail —
which is exactly what we want.
"""
import copy
import json
import sys
import tempfile
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

# Mock pyspark.pipelines BEFORE importing runtime modules — the test
# runner doesn't ship a Spark version with pyspark.pipelines yet, so the
# production imports inside dataflow_pipeline.py would otherwise fail.
# (Mirrors the pattern in tests/test_dataflow_pipeline.py.)
sys.modules["pyspark.pipelines"] = MagicMock()

from tests.utils import SDPFrameworkTestCase  # noqa: E402
from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline  # noqa: E402
from databricks.labs.sdp_meta.dataflow_spec import (  # noqa: E402
    BronzeDataflowSpec,
    DataflowSpecUtils,
    DQE_CONTRACT_VERSION,
    SilverDataflowSpec,
)
from databricks.labs.sdp_meta.onboard_dataflowspec import OnboardDataflowspec  # noqa: E402


# ---------------------------------------------------------------------------
# Frozen v0.0.10 fixtures
# ---------------------------------------------------------------------------

# v0.0.10 ``BronzeDataflowSpec`` field set, in dataclass declaration order.
# Sourced verbatim from ``git show v0.0.10:src/dataflow_spec.py``. DO NOT
# add v0.1.0 fields here — that's the entire point of the fixture.
V0_0_10_BRONZE_ROW = {
    "dataFlowId": "100",
    "dataFlowGroup": "A1",
    "sourceFormat": "cloudFiles",
    "sourceDetails": {"path": "tests/resources/data/customers"},
    "readerConfigOptions": {"cloudFiles.format": "json"},
    "targetFormat": "delta",
    "targetDetails": {
        "database": "bronze",
        "table": "customers",
        "path": "/tmp/sdp_meta_compat/bronze/customers",
    },
    "tableProperties": {},
    "schema": None,
    "partitionColumns": [],
    "cdcApplyChanges": None,
    "applyChangesFromSnapshot": None,
    "dataQualityExpectations": None,
    "quarantineTargetDetails": {},
    "quarantineTableProperties": {},
    "appendFlows": [],
    "appendFlowsSchemas": {},
    "version": "v1",
    "createDate": datetime(2025, 1, 1),
    "createdBy": "v0_0_10_user",
    "updateDate": datetime(2025, 1, 1),
    "updatedBy": "v0_0_10_user",
    "clusterBy": [],
    "sinks": [],
}

# v0.0.10 ``SilverDataflowSpec`` field set.
V0_0_10_SILVER_ROW = {
    "dataFlowId": "200",
    "dataFlowGroup": "A1",
    "sourceFormat": "delta",
    "sourceDetails": {
        "database": "bronze",
        "table": "customers",
        "path": "/tmp/sdp_meta_compat/bronze/customers",
    },
    "readerConfigOptions": {},
    "targetFormat": "delta",
    "targetDetails": {
        "database": "silver",
        "table": "customers",
        "path": "/tmp/sdp_meta_compat/silver/customers",
    },
    "tableProperties": {},
    "selectExp": ["id", "email"],
    "whereClause": [],
    "partitionColumns": [],
    "cdcApplyChanges": None,
    "applyChangesFromSnapshot": None,
    "dataQualityExpectations": None,
    "quarantineTargetDetails": {},
    "quarantineTableProperties": {},
    "appendFlows": [],
    "appendFlowsSchemas": {},
    "version": "v1",
    "createDate": datetime(2025, 1, 1),
    "createdBy": "v0_0_10_user",
    "updateDate": datetime(2025, 1, 1),
    "updatedBy": "v0_0_10_user",
    "clusterBy": [],
    "sinks": [],
}

# Fields added after v0.0.10. The expected default per field depends on the
# upgrade path:
#
#   - Read-time path (persisted Delta dataflowspec table missing these
#     columns): every field is backfilled to ``None`` by
#     :meth:`DataflowSpecUtils.populate_additional_df_cols`.
#
#   - Onboarding path (customer re-runs onboarding with their old
#     v0.0.10 JSON): every field defaults to ``None`` EXCEPT
#     ``clusterByAuto``, which deliberately defaults to ``False``
#     because it's a boolean opt-in flag and ``False`` is the natural
#     "feature disabled" state (see ``__get_cluster_by_auto`` in
#     ``onboard_dataflowspec.py``). Both ``None`` and ``False`` are
#     non-breaking: the runtime treats them identically (no
#     auto-clustering — same behaviour as v0.0.10 which had no such
#     field at all).
NEW_BRONZE_FIELDS_AT_READ = [
    "clusterByAuto",
    "cdcApplyChangesFlows",
    "cdcApplyChangesFlowsSchemas",
    "rowFilter",
    "quarantineRowFilter",
    "qualityConfig",
    "dqeContract",
]
NEW_SILVER_FIELDS_AT_READ = [
    "clusterByAuto",
    "cdcApplyChangesFlows",
    "rowFilter",
    "quarantineRowFilter",
    "qualityConfig",
    "dqeContract",
]
# ``cdcApplyChangesFlowsSchemas`` defaults to ``{}`` (empty map), not
# ``None``, because :meth:`OnboardDataflowspec.get_cdc_apply_changes_flows_json`
# returns ``(None, {})`` when the field is absent (line 1515 in
# ``onboard_dataflowspec.py``). The runtime treats ``None`` and ``{}``
# identically — both fail the ``if dataflow_spec.cdcApplyChangesFlowsSchemas``
# truthiness check — so this is non-breaking. Only bronze has
# ``cdcApplyChangesFlowsSchemas``; silver flows always read from Delta
# upstream and don't carry per-flow schemas.
EXPECTED_BRONZE_DEFAULTS_AT_ONBOARDING = {
    "clusterByAuto": False,
    "cdcApplyChangesFlows": None,
    "cdcApplyChangesFlowsSchemas": {},
    "rowFilter": None,
    "quarantineRowFilter": None,
    "qualityConfig": None,
    "dqeContract": DQE_CONTRACT_VERSION,
}
EXPECTED_SILVER_DEFAULTS_AT_ONBOARDING = {
    "clusterByAuto": False,
    "cdcApplyChangesFlows": None,
    "rowFilter": None,
    "quarantineRowFilter": None,
    "qualityConfig": None,
    "dqeContract": DQE_CONTRACT_VERSION,
}


# ---------------------------------------------------------------------------
# 1. Persisted dataflowspec Delta-table compatibility (no Spark needed)
# ---------------------------------------------------------------------------

class TestV010PersistedDataflowSpecCompatibility(unittest.TestCase):
    """Read-time backfill: ``populate_additional_df_cols`` + dataclass init.

    Models the path :func:`get_bronze_dataflow_spec` /
    :func:`get_silver_dataflow_spec` take when reading a customer's
    v0.0.10-written dataflowspec Delta table.
    """

    def test_populate_additional_bronze_columns_backfills_v0_1_0_new_fields(self):
        legacy_row = copy.deepcopy(V0_0_10_BRONZE_ROW)
        # Sanity: the fixture lacks every v0.1.0 field.
        for field in NEW_BRONZE_FIELDS_AT_READ:
            self.assertNotIn(
                field, legacy_row,
                f"Fixture is wrong: {field!r} must not be in a v0.0.10 row",
            )

        target_row = DataflowSpecUtils.populate_additional_df_cols(
            legacy_row, DataflowSpecUtils.additional_bronze_df_columns,
        )

        for field in NEW_BRONZE_FIELDS_AT_READ:
            self.assertIn(
                field, target_row,
                f"{field!r} not backfilled — additional_bronze_df_columns is incomplete",
            )
            self.assertIsNone(
                target_row[field],
                f"{field!r} backfilled with {target_row[field]!r}; expected None",
            )

    def test_populate_additional_silver_columns_backfills_v0_1_0_new_fields(self):
        legacy_row = copy.deepcopy(V0_0_10_SILVER_ROW)
        for field in NEW_SILVER_FIELDS_AT_READ:
            self.assertNotIn(
                field, legacy_row,
                f"Fixture is wrong: {field!r} must not be in a v0.0.10 row",
            )

        target_row = DataflowSpecUtils.populate_additional_df_cols(
            legacy_row, DataflowSpecUtils.additional_silver_df_columns,
        )

        for field in NEW_SILVER_FIELDS_AT_READ:
            self.assertIn(
                field, target_row,
                f"{field!r} not backfilled — additional_silver_df_columns is incomplete",
            )
            self.assertIsNone(
                target_row[field],
                f"{field!r} backfilled with {target_row[field]!r}; expected None",
            )

    def test_bronze_dataflow_spec_constructs_from_v0_0_10_legacy_row(self):
        """``BronzeDataflowSpec(**backfilled_legacy_row)`` must not raise.

        If this fails (typically ``TypeError: __init__() missing N required
        positional arguments``), customer pipelines crash on first read of
        their v0.0.10-written ``bronze_dataflowspec`` Delta table.
        """
        target_row = DataflowSpecUtils.populate_additional_df_cols(
            copy.deepcopy(V0_0_10_BRONZE_ROW),
            DataflowSpecUtils.additional_bronze_df_columns,
        )
        spec = BronzeDataflowSpec(**target_row)

        for field in NEW_BRONZE_FIELDS_AT_READ:
            self.assertIsNone(getattr(spec, field))
        # Sanity: existing v0.0.10 fields preserved.
        self.assertEqual(spec.dataFlowId, "100")
        self.assertEqual(spec.sourceFormat, "cloudFiles")

    def test_silver_dataflow_spec_constructs_from_v0_0_10_legacy_row(self):
        target_row = DataflowSpecUtils.populate_additional_df_cols(
            copy.deepcopy(V0_0_10_SILVER_ROW),
            DataflowSpecUtils.additional_silver_df_columns,
        )
        spec = SilverDataflowSpec(**target_row)

        for field in NEW_SILVER_FIELDS_AT_READ:
            self.assertIsNone(getattr(spec, field))
        self.assertEqual(spec.dataFlowId, "200")
        self.assertEqual(spec.sourceFormat, "delta")


# ---------------------------------------------------------------------------
# 2. v0.0.10 onboarding-file compatibility (Spark needed)
# ---------------------------------------------------------------------------

class TestV010OnboardingFileCompatibility(SDPFrameworkTestCase):
    """v0.0.10 onboarding JSON → v0.1.0 onboarding code → persisted spec.

    The existing ``test_onboard_bronze_silver_with_v10`` in
    ``tests/test_onboard_dataflowspec.py`` only asserts row count.
    Here we round-trip through the real Delta read path and verify every
    persisted spec has safe defaults for new fields. Re-onboarding stamps
    ``dqeContract`` with the current contract; unrelated fields remain
    disabled.
    """

    def _onboard_v0_0_10(self):
        params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        with open(self.onboarding_json_v10_file, encoding="utf-8") as handle:
            onboarding = json.load(handle)
        # Release A intentionally rejects historical rows that configured
        # expect_or_quarantine without a target. Supply the documented
        # migration so this test can isolate defaults for unrelated fields.
        for flow in onboarding:
            for layer in ("bronze", "silver"):
                rules_path = flow.get(
                    f"{layer}_data_quality_expectations_json_dev"
                )
                if not rules_path or flow.get(f"{layer}_quarantine_table"):
                    continue
                with open(rules_path, encoding="utf-8") as rules_file:
                    rules = json.load(rules_file)
                if not rules.get("expect_or_quarantine"):
                    continue
                table = flow[f"{layer}_table"]
                flow[f"{layer}_database_quarantine_dev"] = flow[
                    f"{layer}_database_dev"
                ]
                flow[f"{layer}_quarantine_table"] = f"{table}_quarantine"
                flow[f"{layer}_quarantine_table_path_dev"] = (
                    f"tests/resources/data/{layer}/{table}_quarantine"
                )
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", encoding="utf-8"
        ) as migrated:
            json.dump(onboarding, migrated)
            migrated.flush()
            params["onboarding_file_path"] = migrated.name
            OnboardDataflowspec(self.spark, params).onboard_dataflow_specs()
        return params

    def _read_specs(self, params, layer, spec_class, additional_cols):
        """Read all rows from the persisted dataflowspec Delta table and
        construct dataclass instances via the same backfill path the
        runtime uses (``populate_additional_df_cols`` + ``Spec(**row)``).

        We bypass :func:`get_bronze_dataflow_spec` /
        :func:`get_silver_dataflow_spec` because those require
        ``spark.conf`` keys (``layer``, ``<layer>.group`` /
        ``<layer>.dataflowIds``) for runtime filtering — irrelevant to
        the backward-compat surface under test, and they'd force us to
        either iterate per-group or fabricate a dataflow_ids list.
        """
        table_key = f"{layer}_dataflowspec_table"
        df = self.spark.read.table(f"{params['database']}.{params[table_key]}")
        specs = []
        for row in df.collect():
            target_row = DataflowSpecUtils.populate_additional_df_cols(
                row.asDict(), additional_cols,
            )
            specs.append(spec_class(**target_row))
        return specs

    def test_v0_0_10_onboarding_persists_safe_defaults_for_new_bronze_fields(self):
        params = self._onboard_v0_0_10()
        bronze_specs = self._read_specs(
            params, "bronze", BronzeDataflowSpec,
            DataflowSpecUtils.additional_bronze_df_columns,
        )
        self.assertGreater(
            len(bronze_specs), 0,
            "v0.0.10 onboarding produced no bronze specs",
        )
        for spec in bronze_specs:
            for field, expected in EXPECTED_BRONZE_DEFAULTS_AT_ONBOARDING.items():
                actual = getattr(spec, field)
                self.assertEqual(
                    actual, expected,
                    f"v0.0.10 onboarding produced bronze spec with {field}="
                    f"{actual!r} on dataFlowId={spec.dataFlowId}; "
                    f"expected {expected!r} for backward compatibility",
                )

    def test_v0_0_10_onboarding_persists_safe_defaults_for_new_silver_fields(self):
        params = self._onboard_v0_0_10()
        silver_specs = self._read_specs(
            params, "silver", SilverDataflowSpec,
            DataflowSpecUtils.additional_silver_df_columns,
        )
        self.assertGreater(
            len(silver_specs), 0,
            "v0.0.10 onboarding produced no silver specs",
        )
        for spec in silver_specs:
            for field, expected in EXPECTED_SILVER_DEFAULTS_AT_ONBOARDING.items():
                actual = getattr(spec, field)
                self.assertEqual(
                    actual, expected,
                    f"v0.0.10 onboarding produced silver spec with {field}="
                    f"{actual!r} on dataFlowId={spec.dataFlowId}; "
                    f"expected {expected!r} for backward compatibility",
                )


# ---------------------------------------------------------------------------
# 3. DataflowPipeline runtime compatibility (Spark needed, dp mocked)
# ---------------------------------------------------------------------------

class TestV010DataflowPipelineRuntimeCompatibility(SDPFrameworkTestCase):
    """A v0.0.10-shape spec (new fields ``None``) must reach legacy code paths.

    The ``write_layer_table`` dispatch order is, for bronze:

        1. snapshot source     → ``apply_changes_from_snapshot``
        2. dataQualityExpectations → ``write_layer_with_dqe``
        3. cdcApplyChangesFlows (v0.1.0)  → ``cdc_apply_changes_flows``
        4. cdcApplyChanges     → ``cdc_apply_changes``
        5. else                → ``_write_standard_table``

    A v0.0.10 spec hits branch 3 only if the customer's legacy
    onboarding file accidentally contained ``cdcApplyChangesFlows`` —
    impossible by definition, but guarded here too.
    """

    def _legacy_bronze_spec(self, **overrides):
        target_row = DataflowSpecUtils.populate_additional_df_cols(
            copy.deepcopy(V0_0_10_BRONZE_ROW),
            DataflowSpecUtils.additional_bronze_df_columns,
        )
        target_row.update(overrides)
        return BronzeDataflowSpec(**target_row)

    def test_dataflow_pipeline_init_with_legacy_bronze_spec_is_safe(self):
        """Init from a legacy spec must not raise; row-filter helpers return None."""
        # UC ON — even so, with rowFilter / quarantineRowFilter unset
        # (None on the legacy spec), the helpers must return None.
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        self.addCleanup(
            self.spark.conf.unset, "spark.databricks.unityCatalog.enabled",
        )

        spec = self._legacy_bronze_spec()
        view_name = f"{spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, spec, view_name, None)

        self.assertIsNone(pipeline._get_row_filter())
        self.assertIsNone(pipeline._get_quarantine_row_filter())
        self.assertFalse(pipeline._legacy_dqe_strict())
        # Multi-source AUTO CDC parsed-attribute is None on legacy specs.
        self.assertIsNone(pipeline.cdcApplyChangesFlows)
        # Single-source CDC default is also None.
        self.assertIsNone(pipeline.cdcApplyChanges)

    @patch("databricks.labs.sdp_meta.dataflow_pipeline.dp")
    def test_legacy_bronze_spec_dispatches_to_standard_write(self, mock_dp):
        """No DQE, no CDC, no row filter → ``_write_standard_table`` (i.e. ``dp.table``)."""
        mock_dp.table = MagicMock(return_value=lambda func: func)
        mock_dp.create_streaming_table = MagicMock()
        mock_dp.create_auto_cdc_flow = MagicMock()
        mock_dp.expect_all_or_drop = MagicMock(return_value=lambda func: func)

        spec = self._legacy_bronze_spec()
        view_name = f"{spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, spec, view_name, None)
        pipeline.read_bronze = MagicMock()

        pipeline.write_bronze()

        # Standard write fired.
        self.assertGreaterEqual(mock_dp.table.call_count, 1)
        # No CDC dispatch.
        mock_dp.create_streaming_table.assert_not_called()
        mock_dp.create_auto_cdc_flow.assert_not_called()
        # Standard write never carries a row_filter on a legacy spec.
        _, kwargs = mock_dp.table.call_args
        self.assertIsNone(kwargs.get("row_filter"))

    @patch("databricks.labs.sdp_meta.dataflow_pipeline.dp")
    def test_legacy_bronze_spec_with_single_source_cdc_dispatches_to_legacy_cdc(
        self, mock_dp,
    ):
        """``cdcApplyChanges`` (legacy single-source) → ``cdc_apply_changes`` path.

        The dispatch must NOT take the v0.1.0 multi-source
        ``cdc_apply_changes_flows`` branch (``cdcApplyChangesFlows`` is
        ``None`` on a legacy spec).
        """
        mock_dp.create_streaming_table = MagicMock()
        mock_dp.create_auto_cdc_flow = MagicMock()
        mock_dp.table = MagicMock(return_value=lambda func: func)

        cdc_payload = json.dumps({
            "keys": ["id"],
            "sequence_by": "operation_date",
            "scd_type": "1",
        })
        spec = self._legacy_bronze_spec(cdcApplyChanges=cdc_payload)
        # Legacy spec carries no multi-source CDC.
        self.assertIsNone(spec.cdcApplyChangesFlows)

        view_name = f"{spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, spec, view_name, None)
        pipeline.read_bronze = MagicMock()
        pipeline.write_bronze()

        # Legacy CDC path: exactly one streaming table + one auto-CDC flow.
        mock_dp.create_streaming_table.assert_called_once()
        mock_dp.create_auto_cdc_flow.assert_called_once()


if __name__ == "__main__":
    unittest.main()
