# Databricks notebook source
# Phase 1 validator -- runs AFTER the v0.0.10 wheel has finished both A1
# (initial bronze + silver) and A2 (incremental bronze) workflows.
#
# Asserts the same row counts the existing cloudfiles integration test
# already validates (see
# ``integration_tests/notebooks/cloudfile_runners/validate.py``). We run
# the SAME assertions here so the two suites stay in lockstep -- a
# v0.0.10 customer landing on these counts is what "running in prod"
# looks like, and Phase 2 has to grow these numbers, not lose them.
import json

import pandas as pd

run_id = dbutils.widgets.get("run_id")
uc_catalog_name = dbutils.widgets.get("uc_catalog_name")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
output_file_path = dbutils.widgets.get("output_file_path")
source_ref = dbutils.widgets.get("source_ref")
log_list = []

log_list.append(
    f"Backward-compat Phase 1 ({source_ref} wheel) validation starting."
)

# Expected row counts after Phase 1 (A1 initial + A2 incremental). These
# match ``cloudfile_runners/validate.py`` exactly: same seed data, same
# DQE rules, same A1+A2 ordering.
EXPECTED_COUNTS = {
    f"{uc_catalog_name}.{bronze_schema}.customers": 51453,
    f"{uc_catalog_name}.{bronze_schema}.customers_quarantine": 256,
    f"{uc_catalog_name}.{bronze_schema}.transactions": 10002,
    f"{uc_catalog_name}.{bronze_schema}.transactions_quarantine": 6,
    f"{uc_catalog_name}.{silver_schema}.customers": 73212,
    f"{uc_catalog_name}.{silver_schema}.transactions": 8759,
}

phase1_counts = {}
for table, expected in EXPECTED_COUNTS.items():
    actual = spark.sql(f"SELECT count(*) AS cnt FROM {table}").collect()[0].cnt
    phase1_counts[table] = int(actual)
    # Validation contract: run_backward_compat_tests.py scans persisted report
    # cells for the exact "Failed!" marker. Keep this status wording unchanged.
    status = "Passed" if int(actual) == expected else "Failed"
    log_list.append(
        f"Phase1 count {table}: expected={expected} actual={actual}. {status}!"
    )

# Hand the per-table Phase 1 counts off to the Phase 2 validator. We
# avoid persisting them to the dataflowspec table or any user-facing
# catalog: a transient driver-local file under /Volumes/.../tmp/ keeps
# the contract narrow and the cleanup script's job simple.
uc_volume_path = dbutils.widgets.get("uc_volume_path").rstrip("/")
phase1_counts_dump = f"{uc_volume_path}/tmp/backward_compat_phase1_counts_{run_id}.json"
dbutils.fs.put(phase1_counts_dump, json.dumps(phase1_counts), overwrite=True)
log_list.append(f"Phase1 counts persisted -> {phase1_counts_dump}")

pd.DataFrame(log_list).to_csv(output_file_path)
