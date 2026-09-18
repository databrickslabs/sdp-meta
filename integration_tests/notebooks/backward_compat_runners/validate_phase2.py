# Databricks notebook source
# Phase 2 validator -- runs AFTER the wheel has been swapped to v0.1.0
# AND a fresh incremental cycle has consumed the Phase 2 seed batch.
#
# Three classes of assertions:
#
#   (1) Data preservation. Every row Phase 1 wrote must still be there.
#       We compare against the per-table counts the Phase 1 validator
#       persisted to a UC volume scratch file.
#   (2) Incremental growth. Phase 2 fed N new customer rows and M new
#       transaction rows into the watched source paths; bronze must
#       grow by AT LEAST those row deltas (DQE may quarantine some, so
#       we use >= rather than ==).
#   (3) Schema compatibility. The dataflowspec persisted by v0.0.10
#       must read cleanly through v0.1.0's ``BronzeDataflowSpec`` /
#       ``SilverDataflowSpec`` dataclasses, with new v0.1.0 fields
#       backfilled to their documented defaults
#       (``rowFilter``/``quarantineRowFilter``/``cdcApplyChangesFlows``
#       -> ``None``, ``cdcApplyChangesFlowsSchemas`` -> ``{}``,
#       ``clusterByAuto`` -> ``False``).
#
# Together (1)+(2)+(3) prove the customer-pipeline-doesn't-break
# contract end-to-end: same job, same notebook, same dataflowspec --
# only the wheel changed, and v0.0.10's persisted state is fully
# consumable by v0.1.0.
#
# This notebook is a regular jobs ``notebook_task`` -- not a DLT
# runner -- so it doesn't get the wheel for free from the pipeline's
# ``configuration.dlt_meta_whl``. We install the v0.1.0 main wheel
# explicitly at the top of cell 1 so cell 2's
# ``from src.dataflow_spec import …`` actually has the package on
# sys.path. Every other path in the notebook lives below the install
# in cell 2 so it sees the freshly-installed package.
target_main_whl = dbutils.widgets.get("target_main_whl")
%pip install $target_main_whl  # noqa: E999

# COMMAND ----------

import json
from importlib.metadata import PackageNotFoundError, version

import pandas as pd

run_id = dbutils.widgets.get("run_id")
uc_catalog_name = dbutils.widgets.get("uc_catalog_name")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
sdp_meta_schema = dbutils.widgets.get("sdp_meta_schema")
output_file_path = dbutils.widgets.get("output_file_path")
uc_volume_path = dbutils.widgets.get("uc_volume_path").rstrip("/")
phase2_customer_delta = int(dbutils.widgets.get("phase2_customer_delta"))
phase2_transaction_delta = int(dbutils.widgets.get("phase2_transaction_delta"))
target_install_surface = dbutils.widgets.get("target_install_surface")
target_package_version = dbutils.widgets.get("target_package_version")
source_profile = dbutils.widgets.get("source_profile")
source_ref = dbutils.widgets.get("source_ref")
target_ref = dbutils.widgets.get("target_ref")
phase2_append_onboarding = (
    dbutils.widgets.get("phase2_append_onboarding").lower() == "true"
)

log_list = []
log_list.append(
    f"Backward-compat Phase 2 ({source_ref} -> {target_ref}) validation starting."
)
# Validation contract: run_backward_compat_tests.py scans persisted report cells
# for the exact "Failed!" marker. Keep that wording on every failure written
# to log_list; checks that raise directly are enforced by the failed task.

# Whenever a target package version is pinned (compat_wheelhouse mode, or
# install_mode=pypi where Phase 2 installed dlt-meta==<version> from the
# live index), prove the legacy PyPI distribution resolved the primary
# distribution at the same release version.
if target_install_surface == "compat_wheelhouse" or (
    target_package_version and target_main_whl.startswith("dlt-meta==")
):
    try:
        compat_version = version("dlt-meta")
        primary_version = version("databricks-labs-sdp-meta")
    except PackageNotFoundError as exc:
        raise AssertionError(
            f"Compatibility redirect distribution missing: {exc}"
        ) from exc

    assert compat_version == target_package_version, (
        f"dlt-meta={compat_version!r}, expected={target_package_version!r}"
    )
    assert primary_version == target_package_version, (
        f"databricks-labs-sdp-meta={primary_version!r}, "
        f"expected={target_package_version!r}"
    )
    log_list.append(
        "Compatibility wheel resolution: "
        f"dlt-meta=={compat_version} resolved "
        f"databricks-labs-sdp-meta=={primary_version}. Passed!"
    )

# (1) Read Phase 1 counts back.
phase1_counts_path = f"{uc_volume_path}/tmp/backward_compat_phase1_counts_{run_id}.json"
phase1_raw = dbutils.fs.head(phase1_counts_path, 1024 * 1024)
phase1_counts = json.loads(phase1_raw)
log_list.append(f"Loaded Phase 1 counts: {phase1_counts}")

# (2) Delta budgets (incremental rows fed into the watched source paths
# at the start of Phase 2). Bronze must grow by AT LEAST the delta;
# silver follows bronze (CDC + dedup may shrink the silver delta below
# the bronze delta, so we use >= for bronze and >0 for silver).
budgets = {
    f"{uc_catalog_name}.{bronze_schema}.customers": phase2_customer_delta,
    f"{uc_catalog_name}.{bronze_schema}.transactions": phase2_transaction_delta,
}
for table, expected_delta in budgets.items():
    actual = spark.sql(f"SELECT count(*) AS cnt FROM {table}").collect()[0].cnt
    prior = phase1_counts.get(table, 0)
    growth = int(actual) - int(prior)
    if growth >= expected_delta:
        log_list.append(
            f"Phase2 bronze growth {table}: prior={prior} actual={actual} "
            f"delta={growth} expected>={expected_delta}. Passed!"
        )
    else:
        log_list.append(
            f"Phase2 bronze growth {table}: prior={prior} actual={actual} "
            f"delta={growth} expected>={expected_delta}. Failed!"
        )

# Silver tables must at least retain Phase 1 rows. CDC apply_changes can
# both grow and shrink silver counts under fresh batches (dedup), so
# the strict invariant is "row count must not regress to below Phase 1
# baseline" -- not "must equal Phase 1 + delta".
silver_tables = [
    f"{uc_catalog_name}.{silver_schema}.customers",
    f"{uc_catalog_name}.{silver_schema}.transactions",
]
for table in silver_tables:
    actual = int(spark.sql(f"SELECT count(*) AS cnt FROM {table}").collect()[0].cnt)
    prior = int(phase1_counts.get(table, 0))
    # Silver `customers` is an append flow target -> count must grow.
    # Silver `transactions` is CDC SCD-1 -> count may stay flat (dedup
    # rewrote the same keys). In both cases, count must NOT drop.
    if actual >= prior:
        log_list.append(
            f"Phase2 silver retention {table}: prior={prior} actual={actual}. Passed!"
        )
    else:
        log_list.append(
            f"Phase2 silver retention {table}: prior={prior} actual={actual}. "
            "Failed!"
        )

# (3) Schema-compatibility check: load SOURCE's persisted dataflowspec
# rows through TARGET's dataclasses. Legacy sources must receive the
# documented field backfills and import through the ``src.*`` shim;
# current sources retain their canonical namespace and row shape.
log_list.append(
    f"Verifying {source_ref} persisted dataflowspec rows are consumable "
    f"by {target_ref}..."
)

bronze_table = f"{uc_catalog_name}.{sdp_meta_schema}.bronze_dataflowspec"
silver_table = f"{uc_catalog_name}.{sdp_meta_schema}.silver_dataflowspec"

if phase2_append_onboarding:
    expected_new_fields = {
        bronze_table: {
            "clusterByAuto",
            "cdcApplyChangesFlows",
            "cdcApplyChangesFlowsSchemas",
            "rowFilter",
            "quarantineRowFilter",
        },
        silver_table: {
            "clusterByAuto",
            "cdcApplyChangesFlows",
            "rowFilter",
            "quarantineRowFilter",
        },
    }
    for table, expected_fields in expected_new_fields.items():
        physical_fields = set(spark.read.table(table).columns)
        missing = expected_fields - physical_fields
        status = "Passed" if not missing else "Failed"
        log_list.append(
            f"Phase2 additive schema evolution {table}: "
            f"missing={sorted(missing)}. {status}!"
        )

    phase1_audit_path = (
        f"{uc_volume_path}/tmp/"
        f"backward_compat_phase1_spec_audit_{run_id}.json"
    )
    phase1_spec_audit = json.loads(
        dbutils.fs.head(phase1_audit_path, 1024 * 1024)
    )
    for layer, table in (
        ("bronze", bronze_table),
        ("silver", silver_table),
    ):
        rows = {
            row.dataFlowId: row
            for row in spark.read.table(table).collect()
        }
        inserted = rows.get("schema-evolution-new")
        inserted_ok = (
            inserted is not None and inserted.clusterByAuto is True
        )
        inserted_status = "Passed" if inserted_ok else "Failed"
        log_list.append(
            f"Phase2 {layer} new schema-evolution row retained current "
            f"field values. {inserted_status}!"
        )

        existing = rows.get("100")
        expected_audit = phase1_spec_audit[layer]
        audit_ok = (
            existing is not None
            and existing.createDate.isoformat()
            == expected_audit["createDate"]
            and existing.createdBy == expected_audit["createdBy"]
        )
        audit_status = "Passed" if audit_ok else "Failed"
        log_list.append(
            f"Phase2 {layer} existing row retained create audit fields. "
            f"{audit_status}!"
        )

        expected_count = 4 if layer == "bronze" else 3
        count_status = (
            "Passed" if len(rows) == expected_count else "Failed"
        )
        log_list.append(
            f"Phase2 {layer} append onboarding is idempotent: "
            f"row_count={len(rows)} expected={expected_count}. "
            f"{count_status}!"
        )

try:
    if source_profile == "legacy":
        from src.dataflow_spec import (
            BronzeDataflowSpec,
            DataflowSpecUtils,
            SilverDataflowSpec,
        )
        import_surface = "src.* compatibility shim"
    else:
        from databricks.labs.sdp_meta.dataflow_spec import (
            BronzeDataflowSpec,
            DataflowSpecUtils,
            SilverDataflowSpec,
        )
        import_surface = "databricks.labs.sdp_meta"
    log_list.append(
        "Imported BronzeDataflowSpec/SilverDataflowSpec via "
        f"{import_surface}. Passed!"
    )
except Exception as exc:
    # Capture the full traceback so DLT-runtime-specific failures
    # (e.g. a top-level statement in cli.py iterating over a config
    # that's None on serverless) are debuggable from the persisted
    # log without re-running the job to read driver stdout.
    import traceback as _tb
    tb_text = _tb.format_exc()
    log_list.append(
        f"Failed to import target dataflow spec: "
        f"{type(exc).__name__}: {exc}. Failed!"
    )
    log_list.append(f"  traceback: {tb_text}")
    BronzeDataflowSpec = SilverDataflowSpec = DataflowSpecUtils = None

if BronzeDataflowSpec is not None:
    # Bronze: confirm every persisted row materializes into a
    # dataclass without TypeError, with the new v0.1.0 fields
    # backfilled to their documented defaults via
    # DataflowSpecUtils.populate_additional_df_cols (the same helper
    # the runtime path uses).
    #
    # populate_additional_df_cols expects a per-row dict (it calls
    # ``onboarding_row_dict.keys()`` internally) -- not a Spark
    # DataFrame. Spark Connect serverless rejects ``DataFrame.keys()``
    # with PySparkAttributeError, so we MUST collect rows first and
    # call the helper per-dict, mirroring how
    # ``DataflowSpecUtils.get_bronze_dataflow_spec`` consumes it
    # internally (see ``dataflow_spec.py:380-389``).
    bronze_rows = [r.asDict() for r in spark.read.table(bronze_table).collect()]
    bronze_rows = [
        DataflowSpecUtils.populate_additional_df_cols(
            r, DataflowSpecUtils.additional_bronze_df_columns
        )
        for r in bronze_rows
    ]
    rows = bronze_rows
    bronze_ok = 0
    for row in rows:
        try:
            spec = BronzeDataflowSpec(**row)
        except Exception as exc:
            log_list.append(
                f"BronzeDataflowSpec from row dataFlowId={row.get('dataFlowId')} "
                f"failed: {exc}. Failed!"
            )
            continue
        if source_profile != "legacy":
            bronze_ok += 1
            continue
        # New v0.1.0 bronze fields must be present on the dataclass
        # AND backfilled to whatever ``populate_additional_df_cols``
        # (the read-time helper used by ``get_bronze_dataflow_spec``)
        # writes when the column is absent from a v0.0.10-shape Delta
        # row. That helper just sets missing columns to ``None``
        # unconditionally (see ``dataflow_spec.py:391-396``), so the
        # backward-compat invariant for THIS code path is "every new
        # field is None on v0.0.10 rows".
        #
        # NOTE on divergence: the unit-test fixture
        # ``EXPECTED_BRONZE_DEFAULTS_AT_ONBOARDING`` (in
        # ``tests/test_backward_compat_v0_0_10.py``) expects
        # ``clusterByAuto=False`` and ``cdcApplyChangesFlowsSchemas={}``
        # because that test exercises a different code path:
        # re-running v0.1.0 onboarding against a v0.0.10 JSON file,
        # which goes through ``__get_cluster_by_auto`` (returns False)
        # and ``get_cdc_apply_changes_flows_json`` (returns {}). Don't
        # copy those expectations here -- they're for onboarding-side
        # defaults, not read-side backfills.
        new_v011_fields = (
            "rowFilter",
            "quarantineRowFilter",
            "cdcApplyChangesFlows",
            "cdcApplyChangesFlowsSchemas",
            "clusterByAuto",
        )
        defaults_ok = phase2_append_onboarding or all(
            getattr(spec, fname, "MISSING") is None
            for fname in new_v011_fields
        )
        if defaults_ok:
            bronze_ok += 1
        else:
            actuals = {
                fname: getattr(spec, fname, "MISSING") for fname in new_v011_fields
            }
            log_list.append(
                f"BronzeDataflowSpec dataFlowId={spec.dataFlowId} "
                f"defaults wrong: {actuals}. Failed!"
            )
    if bronze_ok == len(rows):
        contract = (
            "current fields persisted by append onboarding"
            if phase2_append_onboarding
            else (
                "legacy fields backfilled to v0.1.0 defaults"
                if source_profile == "legacy"
                else "current-shape fields preserved"
            )
        )
        log_list.append(
            f"BronzeDataflowSpec backward-compat: {bronze_ok}/{len(rows)} rows "
            f"materialized with {contract}. Passed!"
        )
    else:
        log_list.append(
            f"BronzeDataflowSpec backward-compat: only {bronze_ok}/{len(rows)} OK. Failed!"
        )

    # Same per-row pattern as bronze above -- pass dicts, not the
    # DataFrame, because Spark Connect serverless doesn't expose
    # ``DataFrame.keys()`` and ``populate_additional_df_cols`` calls
    # ``onboarding_row_dict.keys()`` internally.
    silver_rows = [r.asDict() for r in spark.read.table(silver_table).collect()]
    silver_rows = [
        DataflowSpecUtils.populate_additional_df_cols(
            r, DataflowSpecUtils.additional_silver_df_columns
        )
        for r in silver_rows
    ]
    rows = silver_rows
    silver_ok = 0
    for row in rows:
        try:
            spec = SilverDataflowSpec(**row)
        except Exception as exc:
            log_list.append(
                f"SilverDataflowSpec from row dataFlowId={row.get('dataFlowId')} "
                f"failed: {exc}. Failed!"
            )
            continue
        if source_profile != "legacy":
            silver_ok += 1
            continue
        # Silver has the same set of new v0.1.0 fields as bronze
        # MINUS ``cdcApplyChangesFlowsSchemas`` (silver doesn't carry
        # a per-flow schemas map -- see additional_silver_df_columns
        # in dataflow_spec.py:307-324). Same read-side backfill
        # invariant applies: every new field is None on v0.0.10 rows.
        new_v011_silver_fields = (
            "rowFilter",
            "quarantineRowFilter",
            "cdcApplyChangesFlows",
            "clusterByAuto",
        )
        defaults_ok = phase2_append_onboarding or all(
            getattr(spec, fname, "MISSING") is None
            for fname in new_v011_silver_fields
        )
        if defaults_ok:
            silver_ok += 1
        else:
            actuals = {
                fname: getattr(spec, fname, "MISSING")
                for fname in new_v011_silver_fields
            }
            log_list.append(
                f"SilverDataflowSpec dataFlowId={spec.dataFlowId} "
                f"defaults wrong: {actuals}. Failed!"
            )
    if silver_ok == len(rows):
        contract = (
            "current fields persisted by append onboarding"
            if phase2_append_onboarding
            else (
                "legacy fields backfilled to v0.1.0 defaults"
                if source_profile == "legacy"
                else "current-shape fields preserved"
            )
        )
        log_list.append(
            f"SilverDataflowSpec backward-compat: {silver_ok}/{len(rows)} rows "
            f"materialized with {contract}. Passed!"
        )
    else:
        log_list.append(
            f"SilverDataflowSpec backward-compat: only {silver_ok}/{len(rows)} OK. Failed!"
        )

pd.DataFrame(log_list).to_csv(output_file_path)
