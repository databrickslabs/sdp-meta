# Databricks notebook source
# MAGIC %md
# MAGIC # SDP-META (formerly DLT-META) - Interactive Demo
# MAGIC
# MAGIC **SDP-META** is a metadata-driven framework for
# MAGIC [Lakeflow Spark Declarative Pipelines](https://docs.databricks.com/en/delta-live-tables/index.html).
# MAGIC It automates Bronze and Silver data pipelines by leveraging metadata
# MAGIC recorded in an onboarding JSON/YAML file.
# MAGIC A single generic pipeline reads the **DataflowSpec** metadata and
# MAGIC orchestrates all data processing workloads.
# MAGIC
# MAGIC ### Architecture
# MAGIC ```
# MAGIC ┌──────────────────┐    ┌───────────────────┐    ┌────────────────────────┐
# MAGIC │  Onboarding File │───>│  OnboardDataflow   │───>│  DataflowSpec Tables   │
# MAGIC │  (JSON / YAML)   │    │  spec API          │    │  (Bronze/Silver Delta) │
# MAGIC └──────────────────┘    └───────────────────┘    └──────────┬─────────────┘
# MAGIC                                                             │
# MAGIC                                                             ▼
# MAGIC                                                ┌────────────────────────┐
# MAGIC                                                │  Generic Lakeflow      │
# MAGIC                                                │  Declarative Pipeline  │
# MAGIC                                                │  (reads DataflowSpec,  │
# MAGIC                                                │  creates Bronze/Silver)│
# MAGIC                                                └────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### GitHub Resources
# MAGIC | Resource | Link |
# MAGIC |----------|------|
# MAGIC | **Source Code** | [sdp_meta](https://github.com/databrickslabs/sdp-meta/tree/main/src/databricks/labs/sdp_meta) |
# MAGIC | **Onboarding Template (JSON / YAML)** | [json/onboarding.template](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/json/onboarding.template) / [yml/onboarding.template.yml](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/yml/onboarding.template.yml) |
# MAGIC | **Append Flow Template (JSON / YAML)** | [json/cloudfiles-onboarding.template](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/json/cloudfiles-onboarding.template) / [yml/cloudfiles-onboarding.template.yml](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/yml/cloudfiles-onboarding.template.yml) |
# MAGIC | **Snapshot Template (JSON / YAML)** | [json/snapshot-onboarding.template](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/json/snapshot-onboarding.template) / [yml/snapshot-onboarding.template.yml](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/yml/snapshot-onboarding.template.yml) |
# MAGIC | **Sink Template (JSON / YAML)** | [json/kafka-sink-onboarding.template](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/json/kafka-sink-onboarding.template) / [yml/kafka-sink-onboarding.template.yml](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/yml/kafka-sink-onboarding.template.yml) |
# MAGIC | **Silver Transformations (JSON / YAML)** | [json/silver_transformations.json](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/json/silver_transformations.json) / [yml/silver_transformations.yml](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/yml/silver_transformations.yml) |
# MAGIC | **Data Quality (DQE)** | [json/dqe/](https://github.com/databrickslabs/sdp-meta/tree/main/demo/conf/json/dqe) / [yml/dqe/](https://github.com/databrickslabs/sdp-meta/tree/main/demo/conf/yml/dqe) |
# MAGIC | **Documentation** | [databrickslabs.github.io/sdp-meta](https://databrickslabs.github.io/sdp-meta/) |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Prerequisites: Fill in Your Details
# MAGIC
# MAGIC Set the parameters below then run **all cells in order**.
# MAGIC Everything else is created automatically.
# MAGIC
# MAGIC | Parameter | Description |
# MAGIC |-----------|-------------|
# MAGIC | **Git Branch** | Branch on `databrickslabs/sdp-meta` used to (a) install SDP-META when `Install Source = git_branch` and (b) fetch demo datasets / onboarding templates from `raw.githubusercontent.com` |
# MAGIC | **UC Catalog Name** | Unity Catalog catalog for the demo |
# MAGIC | **UC Schema Name** | Schema within the catalog |
# MAGIC | **Data Source** | `dbdatagen` (generate synthetic data) or `github` (download from repo) |
# MAGIC | **Install Source** | `git_branch` (default — installs SDP-META from the GitHub branch above), `pypi` (installs `databricks-labs-sdp-meta` from PyPI — preferred for published releases), or `whl_file` (installs from a pre-built wheel — preferred when validating a local build) |
# MAGIC | **Wheel File Path** | Required only when `Install Source = whl_file`. Path to the SDP-META wheel on a Volume / Workspace, e.g. `/Volumes/<catalog>/<schema>/<volume>/sdp_meta-<version>-py3-none-any.whl` |
# MAGIC | **PyPI Version** | Optional version pin when `Install Source = pypi` (e.g. `0.1.1`). Leave blank to install the latest published `databricks-labs-sdp-meta` |

# COMMAND ----------

dbutils.widgets.text(
    name="git_branch",
    defaultValue="main",
    label="Git Branch"
)
dbutils.widgets.text(
    name="uc_catalog_name",
    defaultValue="sdp_meta_demo",
    label="UC Catalog Name"
)
dbutils.widgets.text(
    name="uc_schema_name",
    defaultValue="retail_data",
    label="UC Schema Name"
)
dbutils.widgets.dropdown(
    name="data_source",
    defaultValue="dbdatagen",
    choices=["dbdatagen", "github"],
    label="Data Source"
)
dbutils.widgets.dropdown(
    name="onboarding_format",
    defaultValue="json",
    choices=["json", "yml"],
    label="Onboarding File Format"
)
# Lets the demo install SDP-META either from the GitHub branch
# (default — anyone can run the demo without building) or from a
# pre-built wheel on a Volume / Workspace path (preferred when
# validating a local build before merging). Note: ``git_branch`` is
# still used regardless of this choice — it controls where demo
# datasets and onboarding templates are fetched from on raw.github.
dbutils.widgets.dropdown(
    name="install_source",
    defaultValue="git_branch",
    choices=["git_branch", "pypi", "whl_file"],
    label="Install Source"
)
dbutils.widgets.text(
    name="whl_file_path",
    defaultValue="",
    label="Wheel File Path (when install_source=whl_file)"
)
dbutils.widgets.text(
    name="pypi_version",
    defaultValue="",
    label="PyPI Version (when install_source=pypi, optional)"
)
# Final-validation toggle. Off by default so SAs walking through the
# demo interactively don't get an unexpected hard fail at the end.
# When ``true``, the cell at the bottom of the notebook turns the demo
# into a smoke test: it asserts the deterministic tables hit their
# expected row counts and that every demo-produced table is non-empty.
# Use this in CI / pre-release smoke runs.
dbutils.widgets.dropdown(
    name="validate_counts",
    defaultValue="false",
    choices=["false", "true"],
    label="Validate Counts (smoke test mode)"
)
# Cleanup toggle. Off by default so SAs walking through the demo
# interactively can keep poking at bronze/silver tables after the
# notebook finishes. When ``true``, the cleanup cell at the bottom of
# the notebook drops every per-run resource the demo created
# (pipelines, runner notebooks, per-run schemas + the per-run
# config volume). The user-supplied UC catalog is intentionally
# preserved -- it's shared across runs and would clobber other
# work if dropped here. Use this in CI runs that need to leave the
# workspace clean.
dbutils.widgets.dropdown(
    name="cleanup",
    defaultValue="false",
    choices=["false", "true"],
    label="Cleanup (drop per-run resources at end)"
)

# COMMAND ----------

git_branch = dbutils.widgets.get("git_branch")
uc_catalog_name = dbutils.widgets.get("uc_catalog_name")
uc_schema_name = dbutils.widgets.get("uc_schema_name")
data_source = dbutils.widgets.get("data_source")
onboarding_format = dbutils.widgets.get("onboarding_format")
install_source = dbutils.widgets.get("install_source")
whl_file_path = dbutils.widgets.get("whl_file_path").strip()
pypi_version = dbutils.widgets.get("pypi_version").strip()
validate_counts = dbutils.widgets.get("validate_counts").lower() == "true"
cleanup = dbutils.widgets.get("cleanup").lower() == "true"

# Reject illegal UC identifiers up-front. The demo notebook splices these
# names directly into SQL throughout the rest of the cells, so any value
# that isn't a regular SQL identifier would only blow up much later with
# a confusing Spark error (issue #261).
#
# IMPORTANT: this cell runs *before* the %pip install of sdp-meta below,
# so we cannot import ``databricks.labs.sdp_meta.identifiers`` here -- it
# isn't on PYTHONPATH yet. We inline the same regular-SQL-identifier rule
# that ``validate_uc_identifier`` enforces (``[A-Za-z_][A-Za-z0-9_]*``,
# max 255 chars). The canonical, import-based check still happens after
# the Python restart in cell 1.1.
#
# KEEP IN SYNC WITH src/databricks/labs/sdp_meta/identifiers.py
# (``_REGULAR_IDENT_RE`` and ``_MAX_IDENT_LEN``). If the canonical regex
# or max-length tightens/loosens there, mirror the change here -- otherwise
# this pre-install gate will silently drift from the post-install one and
# the demo will reject names the rest of the codebase accepts (or vice
# versa).
import re as _re_preinstall
_REGULAR_IDENT_RE_PREINSTALL = _re_preinstall.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

def _validate_uc_identifier_preinstall(name, *, kind):
    if not isinstance(name, str) or not name:
        raise ValueError(
            f"{kind} must be a non-empty string, got {type(name).__name__}: {name!r}"
        )
    if len(name) > 255:
        raise ValueError(
            f"{kind} {name!r} is {len(name)} characters; maximum allowed is 255"
        )
    if not _REGULAR_IDENT_RE_PREINSTALL.match(name):
        raise ValueError(
            f"{kind} {name!r} is not a valid Databricks SQL regular identifier. "
            f"Names must match {_REGULAR_IDENT_RE_PREINSTALL.pattern} (letters, "
            f"digits and underscores only; must start with a letter or "
            f"underscore). Hyphens, periods, spaces and leading digits are not "
            f"supported."
        )

_validate_uc_identifier_preinstall(uc_catalog_name, kind="uc_catalog_name widget")
_validate_uc_identifier_preinstall(uc_schema_name, kind="uc_schema_name widget")

# Resolve the install target up-front so every downstream consumer
# (this notebook's %pip install, the runner-notebook %pip install, and
# the ``sdp_meta_whl`` pipeline config key passed into all 3 pipelines)
# uses the exact same value. Fail fast on a missing wheel path here —
# otherwise the error surfaces later inside the embedded runner
# notebook where it's much harder to diagnose.
if install_source == "whl_file":
    if not whl_file_path:
        raise ValueError(
            "install_source=whl_file requires the 'whl_file_path' widget "
            "to be set (e.g. /Volumes/<catalog>/<schema>/<volume>/"
            "sdp_meta-<version>-py3-none-any.whl). Either set the path "
            "or switch install_source back to 'git_branch'."
        )
    sdp_meta_install_target = whl_file_path
elif install_source == "pypi":
    # Install the published wheel from PyPI. Optional ``pypi_version``
    # widget pins a specific release (e.g. ``0.1.1``); leaving it blank
    # installs the latest. ``databricks-labs-sdp-meta`` is the canonical
    # PyPI package name; ``dlt-meta`` exists as a compatibility shim that
    # also resolves to it.
    if pypi_version:
        sdp_meta_install_target = (
            f"databricks-labs-sdp-meta=={pypi_version}"
        )
    else:
        sdp_meta_install_target = "databricks-labs-sdp-meta"
else:
    sdp_meta_install_target = (
        f"git+https://github.com/databrickslabs/"
        f"sdp-meta.git@{git_branch}"
    )

print(f"Git Branch         : {git_branch}")
print(f"UC Catalog         : {uc_catalog_name}")
print(f"UC Schema          : {uc_schema_name}")
print(f"Data Source        : {data_source}")
print(f"Onboarding Format  : {onboarding_format}")
print(f"Install Source     : {install_source}")
print(f"Install Target     : {sdp_meta_install_target}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Install SDP-META
# MAGIC
# MAGIC Installs from the chosen `Install Source`:
# MAGIC - `git_branch` — `pip install git+https://github.com/databrickslabs/sdp-meta.git@<branch>`
# MAGIC - `pypi` — `pip install databricks-labs-sdp-meta[==<version>]` (the published release on PyPI)
# MAGIC - `whl_file` — `pip install <whl_file_path>` (wheel on a Volume / Workspace path)

# COMMAND ----------

# dbldatagen is only needed for the "dbdatagen" data source option
extra_packages = " dbldatagen" if data_source == "dbdatagen" else ""
packages = sdp_meta_install_target + extra_packages
%pip install $packages  # noqa: E999
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo Summary
# MAGIC
# MAGIC **Use Case:** MySQL → AWS DMS Replication → S3 (CSV)
# MAGIC → Bronze (Lakeflow Spark Declarative Pipeline)
# MAGIC → Silver (CDC / SCD Type 2)
# MAGIC
# MAGIC | Stage | What You'll Do |
# MAGIC |-------|---------------|
# MAGIC | **1** | Setup — catalog, schema, volume, create resources |
# MAGIC | **2** | Onboard **Customers** & **Transactions** feeds |
# MAGIC | **3** | Create pipeline runner & Lakeflow Spark Declarative Pipeline |
# MAGIC | **4** | Validate initial load (Bronze + Silver tables) |
# MAGIC | **5** | Add **Products** & **Stores** feeds |
# MAGIC | **6** | Push incremental CDC data and re-run |
# MAGIC | **7** | Validate incremental results (SCD Type 2 history) |
# MAGIC | **8** | **Append Flow** — multi-source ingestion with file metadata |
# MAGIC | **9** | **Apply Changes From Snapshot** — SCD Type 1 & 2 |
# MAGIC | **10** | **DLT Sink** — write to external delta table |
# MAGIC | **11** | **Multi-Source AUTO CDC** — N regional CDC sources merged into 1 silver target |
# MAGIC
# MAGIC ### Features Demonstrated
# MAGIC - Metadata-driven onboarding (JSON or YAML → DataflowSpec tables, controlled by the `Onboarding File Format` widget)
# MAGIC - CloudFiles (Autoloader) ingestion
# MAGIC - CDC with `apply_changes` (SCD Type 2)
# MAGIC - Data quality (`expect_or_drop`, `expect_or_quarantine`)
# MAGIC - Quarantine tables for bad data
# MAGIC - Liquid clustering (`cluster_by`, `cluster_by_auto`)
# MAGIC - Silver transformations (column selection, expressions)
# MAGIC - Adding new feeds without modifying the pipeline
# MAGIC - Incremental processing
# MAGIC - **Append Flow** — `dp.append_flow` for multi-source → same target
# MAGIC - **File Metadata** — `_metadata.file_name`, `_metadata.file_path`
# MAGIC - **Apply Changes From Snapshot** — snapshot-based SCD Type 1 & 2
# MAGIC - **Pipeline Sink** — `dp.create_sink` to write to external delta
# MAGIC - **Multi-Source AUTO CDC** — N `dp.create_auto_cdc_flow` calls fan in to one silver target ([#294](https://github.com/databrickslabs/sdp-meta/issues/294))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Stage 1: Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Re-read widget values (after Python restart)

# COMMAND ----------

import csv
import json
import yaml
import os
import time

from pyspark.sql import Row

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import (
    NotebookLibrary,
    PipelineLibrary,
)
from databricks.sdk.service.workspace import (
    ExportFormat, Language,
)

git_branch = dbutils.widgets.get("git_branch")
uc_catalog_name = dbutils.widgets.get("uc_catalog_name")
uc_schema_name = dbutils.widgets.get("uc_schema_name")
data_source = dbutils.widgets.get("data_source")
onboarding_format = dbutils.widgets.get("onboarding_format")
install_source = dbutils.widgets.get("install_source")
whl_file_path = dbutils.widgets.get("whl_file_path").strip()
pypi_version = dbutils.widgets.get("pypi_version").strip()
validate_counts = dbutils.widgets.get("validate_counts").lower() == "true"
cleanup = dbutils.widgets.get("cleanup").lower() == "true"

# Re-resolve the install target after the Python restart so the
# pipeline runner notebooks (created later in this same cell-group) get
# the right value pumped into ``sdp_meta_whl``. Mirrors the resolution
# in the prerequisites cell — keep both copies in sync.
if install_source == "whl_file":
    if not whl_file_path:
        raise ValueError(
            "install_source=whl_file requires the 'whl_file_path' widget "
            "to be set."
        )
    sdp_meta_install_target = whl_file_path
elif install_source == "pypi":
    if pypi_version:
        sdp_meta_install_target = (
            f"databricks-labs-sdp-meta=={pypi_version}"
        )
    else:
        sdp_meta_install_target = "databricks-labs-sdp-meta"
else:
    sdp_meta_install_target = (
        f"git+https://github.com/databrickslabs/"
        f"sdp-meta.git@{git_branch}"
    )

# Re-validate after re-reading widgets in this cell to match the strict
# regular SQL identifier rule used everywhere else (issue #261). Cheap
# and keeps every entry point honest.
from databricks.labs.sdp_meta.identifiers import validate_uc_identifier  # noqa: E402
validate_uc_identifier(uc_catalog_name, kind="uc_catalog_name widget")
validate_uc_identifier(uc_schema_name, kind="uc_schema_name widget")

w = WorkspaceClient()


def run_pipeline_and_wait(w, pipeline_id, label=""):
    """Start a pipeline update and block until it completes."""
    resp = w.pipelines.start_update(pipeline_id=pipeline_id)
    update_id = resp.update_id
    host = w.config.host.rstrip("/")
    pipeline_url = f"{host}/pipelines/{pipeline_id}/updates/{update_id}"
    tag = f" ({label})" if label else ""
    print(f"Pipeline started{tag} — update_id: {update_id}")
    print(f"  URL: {pipeline_url}")
    while True:
        info = w.pipelines.get_update(
            pipeline_id=pipeline_id, update_id=update_id
        )
        state = info.update.state.value
        print(f"  state: {state}")
        if state in ("COMPLETED", "FAILED", "CANCELED"):
            break
        time.sleep(20)
    if state != "COMPLETED":
        raise RuntimeError(
            f"Pipeline ended with state: {state}. "
            f"Check the pipeline UI for details: {pipeline_url}"
        )
    print("Pipeline completed successfully.")


def create_pipeline(ws, **kwargs):
    """Create a Databricks pipeline, tolerating both the modern ``schema=``
    parameter and the legacy ``target=`` parameter.

    Thin wrapper around ``ws.pipelines.create(...)``. The SDK requires
    exactly one of ``schema`` or ``target``. ``schema`` is the
    direct-publishing-mode (DPM) API and is what Unity-Catalog workspaces
    with DPM enabled expect; ``target`` is the legacy fallback still
    needed on workspaces / pipelines where DPM is not enabled. This
    helper tries ``schema=`` first and retries with ``target=`` if the
    first attempt is rejected. If both fail, it raises a single
    ``RuntimeError`` that shows both underlying errors so the caller can
    see exactly why each attempt failed.

    Deprecation note: ``target=`` is documented as deprecated for
    pipeline creation in favor of ``schema=``. When it is fully removed
    from the SDK, the fallback branch below simply becomes dead code
    (``schema=`` succeeds on every DPM-enabled workspace, so we return
    before reaching the fallback) and this helper can be replaced with
    a plain ``ws.pipelines.create(**kwargs)`` call.
    """
    if "schema" not in kwargs:
        return ws.pipelines.create(**kwargs)
    schema_val = kwargs.pop("schema")
    try:
        return ws.pipelines.create(schema=schema_val, **kwargs)
    except Exception as e_schema:
        print(
            f"  [warn] pipelines.create(schema={schema_val!r}) failed: "
            f"{type(e_schema).__name__}: {e_schema}"
        )
        print(f"  [info] retrying with legacy target={schema_val!r} ...")
        try:
            return ws.pipelines.create(target=schema_val, **kwargs)
        except Exception as e_target:
            raise RuntimeError(
                f"pipelines.create() failed with both parameter forms.\n"
                f"  schema={schema_val!r}: "
                f"{type(e_schema).__name__}: {e_schema}\n"
                f"  target={schema_val!r}: "
                f"{type(e_target).__name__}: {e_target}"
            ) from e_target


def _write_onboarding(data, path):
    with open(path, "w") as fh:
        if path.endswith(".yml") or path.endswith(".yaml"):
            yaml.dump(data, fh, default_flow_style=False, allow_unicode=True)
        else:
            json.dump(data, fh, indent=2)


def _read_onboarding(path):
    with open(path, "r") as fh:
        if path.endswith(".yml") or path.endswith(".yaml"):
            return yaml.safe_load(fh)
        else:
            return json.load(fh)


# Extension used for every silver-transformation and DQE config file. Driven
# by the `onboarding_format` widget so the entire demo (onboarding spec +
# referenced silver/DQ files) stays in one consistent format.
conf_ext = "yml" if onboarding_format in ("yml", "yaml") else "json"


def _write_conf(data, path):
    """Serialize a config dict/list as YAML or JSON based on the path suffix."""
    with open(path, "w") as fh:
        if path.endswith((".yml", ".yaml")):
            yaml.dump(data, fh, default_flow_style=False, allow_unicode=True)
        else:
            json.dump(data, fh, indent=4)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Create Catalog, Schema, and Volume

# COMMAND ----------

bronze_schema = f"{uc_schema_name}_bronze"
silver_schema = f"{uc_schema_name}_silver"
# DLT direct publishing mode requires a pipeline-level target schema.
# This schema is a placeholder only — every table sets its own schema
# via DataflowSpec so nothing is ever written here.
pipeline_target_schema = f"{uc_schema_name}_pipeline_default"

spark.sql(f"USE CATALOG {uc_catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {uc_schema_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {bronze_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {silver_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {pipeline_target_schema}")
spark.sql(f"USE SCHEMA {uc_schema_name}")
spark.sql("CREATE VOLUME IF NOT EXISTS config")

# Row-filter UDF (UC Row-Level Security). `bronze_row_filter` /
# `silver_row_filter` in the onboarding spec is a *reference* to this
# function, so it must exist before the pipeline first creates the
# target table -- otherwise CREATE TABLE will fail.
#
# Predicate: admins see all rows; everyone else sees only customer_id
# <= 100. With NUM_CUSTOMERS=200 in this demo, that's half the rows
# for a non-admin reader -- a deterministic, visible signal that the
# filter is in effect (Stage 12 verifies it).
spark.sql(f"""
    CREATE OR REPLACE FUNCTION
        {uc_catalog_name}.{uc_schema_name}.customer_id_filter(cid INT)
    RETURNS BOOLEAN
    RETURN
        is_account_group_member('admins')
        OR cid IS NULL
        OR cid <= 100
""")

uc_volume_path = (
    f"/Volumes/{uc_catalog_name}/{uc_schema_name}/config"
)
pipeline_id_file = f"{uc_volume_path}/pipeline_id.txt"
pipeline_name = f"sdp_meta_demo_{uc_schema_name}"

print(f"Catalog          : {uc_catalog_name}")
print(f"Config Schema    : {uc_catalog_name}.{uc_schema_name}")
print(f"Bronze Schema    : {uc_catalog_name}.{bronze_schema}")
print(f"Silver Schema    : {uc_catalog_name}.{silver_schema}")
print(f"Pipeline Default : {uc_catalog_name}.{pipeline_target_schema}")
print(f"Volume           : {uc_volume_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Define Paths

# COMMAND ----------

demo_path = f"{uc_volume_path}/demo"
resources_path = f"{demo_path}/resources"
data_path = f"{resources_path}/data"
ddl_path = f"{resources_path}/ddl"
incremental_data_path = f"{resources_path}/incremental_data"
conf_path = f"{demo_path}/conf"
dqe_path = f"{conf_path}/dqe"
transformation_path = conf_path
onboarding_file_path = f"{uc_volume_path}/onboarding.{onboarding_format}"
af_data_path = f"{data_path}/append_flow"
snapshot_data_path = f"{data_path}/snapshots"
sink_path = f"{uc_volume_path}/data/sink"

for path in [
    demo_path, resources_path, data_path, ddl_path,
    incremental_data_path, conf_path, dqe_path,
    af_data_path, snapshot_data_path, sink_path,
]:
    os.makedirs(path, exist_ok=True)

print(f"Volume path       : {uc_volume_path}")
print(f"Data path         : {data_path}")
print(f"DDL path          : {ddl_path}")
print(f"DQE path          : {dqe_path}")
print(f"Append Flow path  : {af_data_path}")
print(f"Snapshot path     : {snapshot_data_path}")
print(f"Sink path         : {sink_path}")
print(f"Onboarding file   : {onboarding_file_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 Create Configuration Files
# MAGIC
# MAGIC DDL schemas, data quality expectations, and silver transformation
# MAGIC configs are created inline so the notebook is fully self-contained.
# MAGIC
# MAGIC > **DDL Schemas**: define column types for each source.
# MAGIC > See: [demo/resources/ddl/](https://github.com/databrickslabs/sdp-meta/tree/main/demo/resources/ddl)
# MAGIC
# MAGIC > **Data Quality**: `expect_or_drop` and `expect_or_quarantine`.
# MAGIC > See: [json/dqe/](https://github.com/databrickslabs/sdp-meta/tree/main/demo/conf/json/dqe) or [yml/dqe/](https://github.com/databrickslabs/sdp-meta/tree/main/demo/conf/yml/dqe)
# MAGIC
# MAGIC > **Silver Transformations**: column selection and expressions.
# MAGIC > See: [json/silver_transformations.json](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/json/silver_transformations.json) or [yml/silver_transformations.yml](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/yml/silver_transformations.yml)

# COMMAND ----------

ddl_files = {
    "customers.ddl": (
        "Op: string, dmsTimestamp: timestamp, customer_id: int, "
        "first_name: string, last_name: string, email: string, "
        "address: string, dob: date"
    ),
    "transactions.ddl": (
        "Op: string, dmsTimestamp: string, transaction_id: int, "
        "transaction_date: timestamp, customer_id: string, "
        "product_id: string, store_id: string"
    ),
    "products.ddl": (
        "Op: string, dmsTimestamp: string, product_id: int, "
        "name: string, price: double"
    ),
    "stores.ddl": (
        "Op: string, dmsTimestamp: string, "
        "store_id: string, address: string"
    ),
    "af_orders.ddl": (
        "order_id STRING, customer_id STRING, amount DOUBLE, "
        "item_count DOUBLE, order_date STRING, "
        "operation STRING, operation_date STRING"
    ),
    "iot_events.ddl": (
        "device_id STRING, device_name STRING, "
        "temp DOUBLE, humidity DOUBLE, "
        "battery_level DOUBLE, timestamp STRING"
    ),
}

for filename, content in ddl_files.items():
    with open(f"{ddl_path}/{filename}", "w") as fh:
        fh.write(content)
    print(f"  Created: {ddl_path}/{filename}")

# COMMAND ----------

# DQE configs keyed by base filename (no extension). The actual file
# extension (.json or .yml) is appended at write time based on conf_ext.
bronze_dqe = {
    "customers": {
        "expect_or_drop": {
            "no_rescued_data": "_rescued_data IS NULL",
            "valid_customer_id": "customer_id IS NOT NULL",
        },
        "expect_or_quarantine": {
            "quarantine_rule": (
                "_rescued_data IS NOT NULL OR customer_id IS NULL"
            ),
        },
    },
    "transactions": {
        "expect_or_drop": {
            "no_rescued_data": "_rescued_data IS NULL",
            "valid_transaction_id": "transaction_id IS NOT NULL",
            "valid_customer_id": "customer_id IS NOT NULL",
        },
        "expect_or_quarantine": {
            "quarantine_rule": (
                "_rescued_data IS NOT NULL "
                "OR transaction_id IS NULL "
                "OR customer_id IS NULL"
            ),
        },
    },
    "products": {
        "expect_or_drop": {
            "no_rescued_data": "_rescued_data IS NULL",
            "valid_product_id": "product_id IS NOT NULL",
        },
        "expect_or_quarantine": {
            "quarantine_rule": (
                "_rescued_data IS NOT NULL OR product_id IS NULL"
            ),
        },
    },
    "stores": {
        "expect_or_drop": {
            "no_rescued_data": "_rescued_data IS NULL",
            "valid_store_id": "store_id IS NOT NULL",
        },
        "expect_or_quarantine": {
            "quarantine_rule": (
                "_rescued_data IS NOT NULL OR store_id IS NULL"
            ),
        },
    },
    "af_orders_bronze_dqe": {
        "expect_or_drop": {
            "no_rescued_data": "_rescued_data IS NULL",
            "valid_order_id": "order_id IS NOT NULL",
        },
        "expect_or_quarantine": {
            "quarantine_rule": (
                "_rescued_data IS NOT NULL OR order_id IS NULL"
            ),
        },
    },
    "iot_events_bronze_dqe": {
        "expect_or_drop": {
            "valid_device_id": "device_id IS NOT NULL",
        },
        "expect_or_quarantine": {
            "quarantine_rule": "device_id IS NULL",
        },
    },
}

silver_dqe = {
    "customers_silver_dqe": {
        "expect_or_drop": {
            "valid_customer_id": "customer_id IS NOT NULL",
        },
    },
    "transactions_silver_dqe": {
        "expect_or_drop": {
            "valid_transaction_id": "transaction_id IS NOT NULL",
            "valid_customer_id": "customer_id IS NOT NULL",
        },
    },
    "products_silver_dqe": {
        "expect_or_drop": {
            "valid_product_id": "product_id IS NOT NULL",
        },
    },
    "stores_silver_dqe": {
        "expect_or_drop": {
            "valid_store_id": "store_id IS NOT NULL",
        },
    },
    "af_orders_silver_dqe": {
        "expect_or_drop": {
            "valid_order_id": "order_id IS NOT NULL",
        },
    },
}

for dqe_set in [bronze_dqe, silver_dqe]:
    for basename, content in dqe_set.items():
        out_path = f"{dqe_path}/{basename}.{conf_ext}"
        _write_conf(content, out_path)
        print(f"  Created: {out_path}")

# COMMAND ----------

silver_transformations = [
    {
        "target_table": "customers",
        "select_exp": [
            "customer_id",
            "concat(first_name,' ',last_name) as full_name",
            "email", "address", "dob",
            "dmsTimestamp", "Op", "_rescued_data",
        ],
    },
    {
        "target_table": "transactions",
        "select_exp": [
            "transaction_id", "transaction_date",
            "customer_id", "product_id", "store_id",
            "dmsTimestamp", "Op", "_rescued_data",
        ],
    },
    {
        "target_table": "products",
        "select_exp": [
            "product_id", "name", "price",
            "dmsTimestamp", "Op", "_rescued_data",
        ],
    },
    {
        "target_table": "stores",
        "select_exp": [
            "store_id", "address",
            "dmsTimestamp", "Op", "_rescued_data",
        ],
    },
]

st_path = f"{transformation_path}/silver_transformations.{conf_ext}"
_write_conf(silver_transformations, st_path)
print(f"  Created: {st_path}")

af_silver_transformations = [
    {
        "target_table": "orders",
        "select_exp": [
            "order_id", "customer_id", "amount",
            "item_count", "order_date",
            "operation", "operation_date",
            "_rescued_data",
        ],
    },
]

af_st_path = (
    f"{transformation_path}/af_silver_transformations.{conf_ext}"
)
_write_conf(af_silver_transformations, af_st_path)
print(f"  Created: {af_st_path}")

snapshot_silver_transformations = [
    {
        "target_table": "snap_products",
        "select_exp": [
            "product_id", "name", "price", "dmsTimestamp",
        ],
        "where_clause": ["__END_AT IS NULL"],
    },
    {
        "target_table": "snap_stores",
        "select_exp": [
            "store_id", "address", "dmsTimestamp",
        ],
    },
]

snap_st_path = (
    f"{transformation_path}/snapshot_silver_transformations.{conf_ext}"
)
_write_conf(snapshot_silver_transformations, snap_st_path)
print(f"  Created: {snap_st_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.5 Create Demo Data
# MAGIC
# MAGIC Based on your **Data Source** widget selection:
# MAGIC - **dbdatagen**: generates synthetic data using
# MAGIC   [dbldatagen](https://github.com/databrickslabs/dbldatagen)
# MAGIC - **github**: downloads sample data from the
# MAGIC   [sdp-meta repo](https://github.com/databrickslabs/sdp-meta/tree/main/demo/resources)

# COMMAND ----------

if data_source == "dbdatagen":
    import dbldatagen as dg
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType,
        TimestampType, DateType, DoubleType,
    )
    from datetime import datetime, timedelta

    base_ts = "2022-06-24 18:53:24"
    incr_ts = "2022-06-24 19:01:10"
    NUM_CUSTOMERS = 200
    NUM_TRANSACTIONS = 2000
    NUM_PRODUCTS = 20
    NUM_STORES = 4
    INCR_CUSTOMERS = 100
    INCR_PRODUCTS = 10
    INCR_STORES = 2
    INCR_TRANSACTIONS = 200

    # --- Initial load (no Op column — full load, not CDC) ---
    # Op will be NULL in Bronze for the initial dataset; this is intentional.
    # The DDL defines Op: string to match the DMS schema, but initial full-load
    # files don't carry an Op value. silver_cdc_apply_changes treats NULL Op
    # as an insert (apply_as_deletes: "Op = 'D'" won't match NULL).
    # --- Customers initial load ---
    customers_df = (
        dg.DataGenerator(spark, name="customers",
                         rowcount=NUM_CUSTOMERS, seedColumnName="_id")
        .withColumn("dmsTimestamp", StringType(),
                    values=[base_ts])
        .withColumn("customer_id", IntegerType(),
                    minValue=1, maxValue=NUM_CUSTOMERS,
                    uniqueValues=NUM_CUSTOMERS)
        .withColumn("first_name", StringType(),
                    values=["Alice", "Bob", "Carol", "David",
                            "Eve", "Frank", "Grace", "Henry",
                            "Ivy", "Jack", "Karen", "Leo",
                            "Mia", "Noah", "Olivia", "Paul"])
        .withColumn("last_name", StringType(),
                    values=["Smith", "Jones", "Brown", "Wilson",
                            "Taylor", "Davis", "Clark", "Hall",
                            "Allen", "Young", "King", "Wright"])
        .withColumn("email", "string",
                    template=r"\\w.\\w@example.com")
        .withColumn("address", StringType(),
                    template=r"\\d\\d\\d\\d Main St, City, ST \\d\\d\\d\\d\\d")
        .withColumn("dob", DateType(),
                    begin="1960-01-01", end="2005-12-31")
        .build()
        .drop("_id")
    )
    os.makedirs(f"{data_path}/customers", exist_ok=True)
    (customers_df.coalesce(1).write.mode("overwrite")
     .option("header", "true").csv(f"{data_path}/customers"))
    print(f"  Generated {NUM_CUSTOMERS} customers")

    # --- Transactions initial load ---
    transactions_df = (
        dg.DataGenerator(spark, name="transactions",
                         rowcount=NUM_TRANSACTIONS,
                         seedColumnName="_id")
        .withColumn("dmsTimestamp", StringType(),
                    values=[base_ts])
        .withColumn("transaction_id", IntegerType(),
                    minValue=1, maxValue=NUM_TRANSACTIONS,
                    uniqueValues=NUM_TRANSACTIONS)
        .withColumn("transaction_date", DateType(),
                    begin="2022-05-01", end="2022-06-24")
        .withColumn("customer_id", IntegerType(),
                    minValue=1, maxValue=NUM_CUSTOMERS)
        .withColumn("product_id", IntegerType(),
                    minValue=1, maxValue=NUM_PRODUCTS)
        .withColumn("store_id", IntegerType(),
                    minValue=1, maxValue=NUM_STORES)
        .build()
        .drop("_id")
    )
    os.makedirs(f"{data_path}/transactions", exist_ok=True)
    (transactions_df.coalesce(1).write.mode("overwrite")
     .option("header", "true").csv(f"{data_path}/transactions"))
    print(f"  Generated {NUM_TRANSACTIONS} transactions")

    # --- Products initial load ---
    products_df = (
        dg.DataGenerator(spark, name="products",
                         rowcount=NUM_PRODUCTS, seedColumnName="_id")
        .withColumn("dmsTimestamp", StringType(),
                    values=[base_ts])
        .withColumn("product_id", IntegerType(),
                    minValue=1, maxValue=NUM_PRODUCTS,
                    uniqueValues=NUM_PRODUCTS)
        .withColumn("name", StringType(),
                    values=["shorts", "hat", "accessories",
                            "sneakers", "coat", "sweater",
                            "boots", "sweatshirt", "jacket",
                            "scarf"])
        .withColumn("price", DoubleType(),
                    minValue=10.0, maxValue=999.0)
        .build()
        .drop("_id")
    )
    os.makedirs(f"{data_path}/products", exist_ok=True)
    (products_df.coalesce(1).write.mode("overwrite")
     .option("header", "true").csv(f"{data_path}/products"))
    print(f"  Generated {NUM_PRODUCTS} products")

    # --- Stores initial load ---
    stores_df = (
        dg.DataGenerator(spark, name="stores",
                         rowcount=NUM_STORES, seedColumnName="_id")
        .withColumn("dmsTimestamp", StringType(),
                    values=[base_ts])
        .withColumn("store_id", IntegerType(),
                    minValue=1, maxValue=NUM_STORES,
                    uniqueValues=NUM_STORES)
        .withColumn("address", StringType(),
                    template=r"\\d\\d\\d\\d Store Blvd, City, ST \\d\\d\\d\\d\\d")
        .build()
        .drop("_id")
    )
    os.makedirs(f"{data_path}/stores", exist_ok=True)
    (stores_df.coalesce(1).write.mode("overwrite")
     .option("header", "true").csv(f"{data_path}/stores"))
    print(f"  Generated {NUM_STORES} stores")

    # --- Incremental CDC data (Op=I for new inserts) ---
    incr_customers_df = (
        dg.DataGenerator(spark, name="incr_customers",
                         rowcount=INCR_CUSTOMERS,
                         seedColumnName="_id")
        .withColumn("Op", StringType(), values=["I"])
        .withColumn("dmsTimestamp", StringType(),
                    values=[incr_ts])
        .withColumn("customer_id", IntegerType(),
                    minValue=NUM_CUSTOMERS + 1,
                    maxValue=NUM_CUSTOMERS + INCR_CUSTOMERS,
                    uniqueValues=INCR_CUSTOMERS)
        .withColumn("first_name", StringType(),
                    values=["Alice", "Bob", "Carol", "David",
                            "Eve", "Frank", "Grace", "Henry"])
        .withColumn("last_name", StringType(),
                    values=["Smith", "Jones", "Brown", "Wilson",
                            "Taylor", "Davis", "Clark", "Hall"])
        .withColumn("email", "string",
                    template=r"\\w.\\w@example.com")
        .withColumn("address", StringType(),
                    template=r"\\d\\d\\d\\d Main St, City, ST \\d\\d\\d\\d\\d")
        .withColumn("dob", DateType(),
                    begin="1960-01-01", end="2005-12-31")
        .build()
        .drop("_id")
    )
    os.makedirs(f"{incremental_data_path}/customers", exist_ok=True)
    (incr_customers_df.coalesce(1).write.mode("overwrite")
     .option("header", "true")
     .csv(f"{incremental_data_path}/customers"))
    print(f"  Generated {INCR_CUSTOMERS} incremental customers")

    incr_products_df = (
        dg.DataGenerator(spark, name="incr_products",
                         rowcount=INCR_PRODUCTS,
                         seedColumnName="_id")
        .withColumn("Op", StringType(), values=["I"])
        .withColumn("dmsTimestamp", StringType(),
                    values=[incr_ts])
        .withColumn("product_id", IntegerType(),
                    minValue=NUM_PRODUCTS + 1,
                    maxValue=NUM_PRODUCTS + INCR_PRODUCTS,
                    uniqueValues=INCR_PRODUCTS)
        .withColumn("name", StringType(),
                    values=["sneakers", "sweater", "accessories",
                            "coat", "boots"])
        .withColumn("price", DoubleType(),
                    minValue=10.0, maxValue=999.0)
        .build()
        .drop("_id")
    )
    os.makedirs(f"{incremental_data_path}/products", exist_ok=True)
    (incr_products_df.coalesce(1).write.mode("overwrite")
     .option("header", "true")
     .csv(f"{incremental_data_path}/products"))
    print(f"  Generated {INCR_PRODUCTS} incremental products")

    incr_stores_df = (
        dg.DataGenerator(spark, name="incr_stores",
                         rowcount=INCR_STORES, seedColumnName="_id")
        .withColumn("Op", StringType(), values=["I"])
        .withColumn("dmsTimestamp", StringType(),
                    values=[incr_ts])
        .withColumn("store_id", IntegerType(),
                    minValue=NUM_STORES + 1,
                    maxValue=NUM_STORES + INCR_STORES,
                    uniqueValues=INCR_STORES)
        .withColumn("address", StringType(),
                    template=r"\\d\\d\\d\\d Store Blvd, City, ST \\d\\d\\d\\d\\d")
        .build()
        .drop("_id")
    )
    os.makedirs(f"{incremental_data_path}/stores", exist_ok=True)
    (incr_stores_df.coalesce(1).write.mode("overwrite")
     .option("header", "true")
     .csv(f"{incremental_data_path}/stores"))
    print(f"  Generated {INCR_STORES} incremental stores")

    incr_txn_df = (
        dg.DataGenerator(spark, name="incr_transactions",
                         rowcount=INCR_TRANSACTIONS,
                         seedColumnName="_id")
        .withColumn("Op", StringType(), values=["I"])
        .withColumn("dmsTimestamp", StringType(),
                    values=[incr_ts])
        .withColumn("transaction_id", IntegerType(),
                    minValue=NUM_TRANSACTIONS + 1,
                    maxValue=NUM_TRANSACTIONS + INCR_TRANSACTIONS,
                    uniqueValues=INCR_TRANSACTIONS)
        .withColumn("transaction_date", DateType(),
                    begin="2022-06-25", end="2022-07-10")
        .withColumn("customer_id", IntegerType(),
                    minValue=1,
                    maxValue=NUM_CUSTOMERS + INCR_CUSTOMERS)
        .withColumn("product_id", IntegerType(),
                    minValue=1,
                    maxValue=NUM_PRODUCTS + INCR_PRODUCTS)
        .withColumn("store_id", IntegerType(),
                    minValue=1,
                    maxValue=NUM_STORES + INCR_STORES)
        .build()
        .drop("_id")
    )
    os.makedirs(
        f"{incremental_data_path}/transactions", exist_ok=True
    )
    (incr_txn_df.coalesce(1).write.mode("overwrite")
     .option("header", "true")
     .csv(f"{incremental_data_path}/transactions"))
    print(f"  Generated {INCR_TRANSACTIONS} incremental transactions")

    print("\nAll data generated with dbdatagen.")

else:  # github
    import requests

    REPO_OWNER = "databrickslabs"
    REPO_NAME = "sdp-meta"

    def download_file(source, destination):
        raw_url = (
            f"https://raw.githubusercontent.com/"
            f"{REPO_OWNER}/{REPO_NAME}/{git_branch}/{source}"
        )
        os.makedirs(
            os.path.dirname(destination), exist_ok=True
        )
        resp = requests.get(raw_url)
        if resp.status_code == 200:
            with open(destination, "wb") as fh:
                fh.write(resp.content)
            print(f"  downloaded: {source}")
        else:
            print(
                f"  FAILED: {source} (HTTP {resp.status_code})"
            )

    api_url = (
        f"https://api.github.com/repos/"
        f"{REPO_OWNER}/{REPO_NAME}"
        f"/git/trees/{git_branch}?recursive=1"
    )
    headers = {"Accept": "application/vnd.github.v3+json"}
    response = requests.get(api_url, headers=headers)

    if response.status_code != 200:
        raise Exception(
            "Failed to fetch repo tree. "
            f"Status: {response.status_code}"
        )

    repo_data = response.json()
    print(
        f"Downloading data from GitHub "
        f"(branch: {git_branch})...\n"
    )
    excluded = ("afam", "snapshots", "iot", "cars", "eventhub")

    for item in repo_data.get("tree", []):
        if item["type"] != "blob":
            continue
        fp = item["path"]

        if (
            "demo/resources/data/" in fp
            and not any(x in fp for x in excluded)
        ):
            rel = fp.replace("demo/resources/data/", "")
            download_file(fp, f"{data_path}/{rel}")

        elif "demo/resources/incremental_data/" in fp:
            rel = fp.replace(
                "demo/resources/incremental_data/", ""
            )
            download_file(
                fp, f"{incremental_data_path}/{rel}"
            )

    print("\nAll data downloaded from GitHub.")

# --- Generate bad records for quarantine (both data sources) ---
bad_customers = (
    "dmsTimestamp,customer_id,first_name,last_name,"
    "email,address,dob\n"
    "2022-06-24 18:55:00,,,,bad_email,bad addr,\n"
    "2022-06-24 18:55:01,,Jane,Doe,"
    "jane@example.com,123 Main St,1990-01-01\n"
)
bad_transactions = (
    "dmsTimestamp,transaction_id,transaction_date,"
    "customer_id,product_id,store_id\n"
    "2022-06-24 18:55:00,,,,,\n"
    "2022-06-24 18:55:01,,2022-06-01,,,\n"
)
bad_products = (
    "dmsTimestamp,product_id,name,price\n"
    "2022-06-24 18:55:00,,,\n"
    "2022-06-24 18:55:01,,bad_product,not_a_price\n"
)
bad_stores = (
    "dmsTimestamp,store_id,address\n"
    "2022-06-24 18:55:00,,\n"
    "2022-06-24 18:55:01,,bad address\n"
)

bad_data_files = {
    "customers": bad_customers,
    "transactions": bad_transactions,
    "products": bad_products,
    "stores": bad_stores,
}

print("\nGenerating bad records for quarantine testing...")
for domain, content in bad_data_files.items():
    bad_path = f"{data_path}/{domain}/BAD_RECORDS.csv"
    os.makedirs(f"{data_path}/{domain}", exist_ok=True)
    with open(bad_path, "w") as fh:
        fh.write(content)
    print(f"  Created: {bad_path}")
print("Quarantine tables will capture these records.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.6 Create Append Flow Data
# MAGIC
# MAGIC Append Flow reads from **multiple source paths** and writes to the
# MAGIC **same target table** using `dp.append_flow`.
# MAGIC We create two separate source directories for orders.
# MAGIC See: [cloudfiles-onboarding.template](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/json/cloudfiles-onboarding.template)

# COMMAND ----------

af_orders_main = f"{af_data_path}/orders"
af_orders_secondary = f"{af_data_path}/orders_af"
os.makedirs(af_orders_main, exist_ok=True)
os.makedirs(af_orders_secondary, exist_ok=True)

orders_main_data = [
    {"order_id": "ORD001", "customer_id": "C001",
     "amount": 150.50, "item_count": 3,
     "order_date": "2022-06-24", "operation": "APPEND",
     "operation_date": "2022-06-24 18:55:00"},
    {"order_id": "ORD002", "customer_id": "C002",
     "amount": 89.99, "item_count": 1,
     "order_date": "2022-06-24", "operation": "APPEND",
     "operation_date": "2022-06-24 18:56:00"},
    {"order_id": "ORD003", "customer_id": "C003",
     "amount": 320.00, "item_count": 5,
     "order_date": "2022-06-24", "operation": "APPEND",
     "operation_date": "2022-06-24 18:57:00"},
    {"order_id": "ORD004", "customer_id": "C001",
     "amount": 45.25, "item_count": 2,
     "order_date": "2022-06-25", "operation": "APPEND",
     "operation_date": "2022-06-25 10:00:00"},
]

orders_af_data = [
    {"order_id": "ORD005", "customer_id": "C004",
     "amount": 200.00, "item_count": 4,
     "order_date": "2022-06-25", "operation": "APPEND",
     "operation_date": "2022-06-25 11:00:00"},
    {"order_id": "ORD006", "customer_id": "C005",
     "amount": 75.50, "item_count": 1,
     "order_date": "2022-06-25", "operation": "APPEND",
     "operation_date": "2022-06-25 12:00:00"},
    {"order_id": "ORD007", "customer_id": "C002",
     "amount": 500.00, "item_count": 10,
     "order_date": "2022-06-26", "operation": "APPEND",
     "operation_date": "2022-06-26 09:00:00"},
]

main_path = f"{af_orders_main}/orders_batch_1.json"
with open(main_path, "w") as fh:
    for record in orders_main_data:
        fh.write(json.dumps(record) + "\n")
print(f"  Created: {main_path} ({len(orders_main_data)} records)")

af_path = f"{af_orders_secondary}/orders_batch_af.json"
with open(af_path, "w") as fh:
    for record in orders_af_data:
        fh.write(json.dumps(record) + "\n")
print(f"  Created: {af_path} ({len(orders_af_data)} records)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.7 Create Snapshot Data
# MAGIC
# MAGIC Snapshot-based ingestion uses `apply_changes_from_snapshot` instead
# MAGIC of streaming. Each snapshot represents a full point-in-time view.
# MAGIC See: [snapshot-onboarding.template](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/json/snapshot-onboarding.template)

# COMMAND ----------

snap_stores_dir = f"{snapshot_data_path}/stores"
snap_incr_stores = f"{snapshot_data_path}/incremental_snapshots/stores"
os.makedirs(snap_stores_dir, exist_ok=True)
os.makedirs(snap_incr_stores, exist_ok=True)

snap_stores_load1 = (
    "dmsTimestamp,store_id,address\n"
    '2022-06-24 18:53:25,1,'
    '"6761 Brian Falls Navarrobury, VA 17977"\n'
    '2022-06-24 18:53:25,2,'
    '"4215 Bruce Shoals Apt. 920 Port Travis, SC 71335"\n'
    '2022-06-24 18:53:25,3,'
    '"96924 Gregory Mill Pricefurt, GA 68691"\n'
    '2022-06-24 18:53:25,4,'
    '"070 Cynthia Cliff Paulport, FL 21469"\n'
)
with open(f"{snap_stores_dir}/LOAD_1.csv", "w") as fh:
    fh.write(snap_stores_load1)
print(f"  Created: {snap_stores_dir}/LOAD_1.csv")

snap_stores_load2 = (
    "dmsTimestamp,store_id,address\n"
    '2022-06-25 10:00:00,1,'
    '"V2 6761 Brian Falls Navarrobury, VA 17977"\n'
    '2022-06-25 10:00:00,2,'
    '"V2 4215 Bruce Shoals Apt. 920 Port Travis, SC 71335"\n'
)
with open(f"{snap_incr_stores}/LOAD_2.csv", "w") as fh:
    fh.write(snap_stores_load2)
print(f"  Created: {snap_incr_stores}/LOAD_2.csv")

snap_products_dir = f"{snapshot_data_path}/products"
snap_incr_products = (
    f"{snapshot_data_path}/incremental_snapshots/products"
)
os.makedirs(snap_products_dir, exist_ok=True)
os.makedirs(snap_incr_products, exist_ok=True)

snap_products_load1 = (
    "dmsTimestamp,product_id,name,price\n"
    "2022-06-24 18:53:24,1,shorts,793.50\n"
    "2022-06-24 18:53:24,2,hat,598.91\n"
    "2022-06-24 18:53:24,3,coat,914.34\n"
    "2022-06-24 18:53:24,4,accessories,717.76\n"
    "2022-06-24 18:53:24,5,sneakers,975.06\n"
)
with open(f"{snap_products_dir}/LOAD_1.csv", "w") as fh:
    fh.write(snap_products_load1)
print(f"  Created: {snap_products_dir}/LOAD_1.csv")

snap_products_load2 = (
    "dmsTimestamp,product_id,name,price\n"
    "2022-06-25 10:00:00,1,shorts_v2,793.50\n"
    "2022-06-25 10:00:00,2,hat_v2,598.91\n"
    "2022-06-25 10:00:00,3,coat_v2,914.34\n"
    "2022-06-25 10:00:00,4,accessories,717.76\n"
    "2022-06-25 10:00:00,5,sneakers,975.06\n"
)
with open(f"{snap_incr_products}/LOAD_2.csv", "w") as fh:
    fh.write(snap_products_load2)
print(f"  Created: {snap_incr_products}/LOAD_2.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.8 Create IoT Events Data (for DLT Sink Demo)
# MAGIC
# MAGIC Simple IoT sensor events used to demonstrate writing
# MAGIC to external delta sinks.

# COMMAND ----------

iot_data_dir = f"{data_path}/iot_events"
os.makedirs(iot_data_dir, exist_ok=True)

iot_events = [
    {"device_id": "dev001", "device_name": "sensor-alpha",
     "temp": 22.5, "humidity": 45.0,
     "battery_level": 85.0, "timestamp": "2022-06-24 18:55:00"},
    {"device_id": "dev002", "device_name": "sensor-beta",
     "temp": 31.2, "humidity": 60.0,
     "battery_level": 72.0, "timestamp": "2022-06-24 18:55:01"},
    {"device_id": "dev003", "device_name": "sensor-gamma",
     "temp": 18.0, "humidity": 38.0,
     "battery_level": 95.0, "timestamp": "2022-06-24 18:55:02"},
    {"device_id": "dev001", "device_name": "sensor-alpha",
     "temp": 23.1, "humidity": 44.0,
     "battery_level": 84.0, "timestamp": "2022-06-24 19:00:00"},
    {"device_id": "dev002", "device_name": "sensor-beta",
     "temp": 32.0, "humidity": 62.0,
     "battery_level": 70.0, "timestamp": "2022-06-24 19:00:01"},
]

iot_path = f"{iot_data_dir}/iot_batch_1.csv"
with open(iot_path, "w", newline="") as fh:
    writer = csv.DictWriter(
        fh, fieldnames=iot_events[0].keys()
    )
    writer.writeheader()
    writer.writerows(iot_events)
print(f"  Created: {iot_path} ({len(iot_events)} records)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.9 Verify Created Resources

# COMMAND ----------

print("=== Sample Data (Initial Load) ===")
for domain in ["customers", "transactions", "products", "stores"]:
    dp = f"{data_path}/{domain}"
    if os.path.exists(dp):
        files = [
            f for f in os.listdir(dp)
            if not f.startswith("_") and not f.startswith(".")
        ]
        print(f"  {domain}/: {len(files)} file(s)")

print("\n=== Incremental Data (CDC) ===")
for domain in ["customers", "transactions", "products", "stores"]:
    dp = f"{incremental_data_path}/{domain}"
    if os.path.exists(dp):
        files = [
            f for f in os.listdir(dp)
            if not f.startswith("_") and not f.startswith(".")
        ]
        print(f"  {domain}/: {len(files)} file(s)")

print("\n=== Append Flow Data ===")
for d in ["orders", "orders_af"]:
    dp = f"{af_data_path}/{d}"
    if os.path.exists(dp):
        files = os.listdir(dp)
        print(f"  {d}/: {len(files)} file(s)")

print("\n=== Snapshot Data ===")
for d in ["products", "stores"]:
    dp = f"{snapshot_data_path}/{d}"
    if os.path.exists(dp):
        files = os.listdir(dp)
        print(f"  {d}/: {len(files)} snapshot(s)")

print(f"\n=== IoT Events ===")
print(f"  iot_events/: {len(os.listdir(iot_data_dir))} file(s)")

print(f"\n=== DDL Schemas ===\n  {os.listdir(ddl_path)}")
print(f"\n=== DQE Files ===\n  {os.listdir(dqe_path)}")
print(f"\n=== Config ===\n  {os.listdir(conf_path)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Stage 2: Onboarding — Customers & Transactions
# MAGIC
# MAGIC Onboarding converts a JSON config into **DataflowSpec** metadata
# MAGIC tables that drive the pipeline.
# MAGIC
# MAGIC ### Key Concepts
# MAGIC - **[Onboarding File](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/json/onboarding.template)**:
# MAGIC   JSON defining source/target for each data feed
# MAGIC - **[OnboardDataflowspec API](https://github.com/databrickslabs/sdp-meta/blob/main/src/databricks/labs/sdp_meta/onboard_dataflowspec.py)**:
# MAGIC   Reads the file and writes Bronze/Silver DataflowSpec tables
# MAGIC - **DataflowSpec Tables**: Delta tables storing pipeline metadata

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Create Onboarding File
# MAGIC
# MAGIC The spec for **Customers** and **Transactions** lives in the repo as a
# MAGIC committed sample, in **whichever format you picked in the widget**:
# MAGIC
# MAGIC - `json` → [`demo/conf/json/sample_onboarding.json`](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/json/sample_onboarding.json)
# MAGIC - `yml`  → [`demo/conf/yml/sample_onboarding.yml`](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/yml/sample_onboarding.yml)
# MAGIC
# MAGIC The cell below loads that file, substitutes runtime tokens
# MAGIC (`$data_path`, `$dqe_path`, `$uc_catalog_name`, …), prints the
# MAGIC rendered text so you actually see the chosen format, and writes
# MAGIC it to the UC volume as `onboarding.{json|yml}`. Each entry
# MAGIC defines:
# MAGIC - Source details (path, schema, format)
# MAGIC - Bronze target (table, data quality, quarantine)
# MAGIC - Silver target (CDC config, transformations)
# MAGIC
# MAGIC > **Note on field names:** keys like
# MAGIC > `bronze_data_quality_expectations_json_prod` and
# MAGIC > `silver_transformation_json_prod` are the runner's spec schema
# MAGIC > (see `OnboardDataflowspec`); the `_json_` suffix is part of
# MAGIC > the field name and does **not** mean the value must be JSON —
# MAGIC > the value may point to a `.json` or a `.yml` file.
# MAGIC >
# MAGIC > **CDC (SCD Type 2)**: Silver uses `apply_changes` for
# MAGIC > history tracking.
# MAGIC > See: [dataflow_pipeline.py](https://github.com/databrickslabs/sdp-meta/blob/main/src/databricks/labs/sdp_meta/dataflow_pipeline.py)

# COMMAND ----------

from string import Template

import requests


def _load_sample_onboarding_text(onboarding_format, conf_ext, git_branch):
    """Load the committed sample onboarding file for the chosen format.

    Resolution order (first match wins):

    1. **Workspace co-located** — when the notebook was imported as part of
       the sdp-meta repo (so its workspace path contains ``/demo/``), we
       read the sibling ``demo/conf/{json|yml}/sample_onboarding.{json|yml}``
       directly via ``open()``. Fast, offline-friendly.
    2. **GitHub raw fallback** — when the notebook was uploaded standalone
       (no ``/demo/`` in its workspace path, no co-located repo files), we
       fetch the same sample file from
       ``raw.githubusercontent.com/databrickslabs/sdp-meta/<git_branch>/...``.
       This mirrors the GitHub-download path already used in stage 1 for
       datasets, so the ``git_branch`` widget controls both.

    Returns:
        tuple[str, str]: ``(sample_text, source_label)`` where
        ``source_label`` is the path/URL the text was loaded from (for
        display in the cell output).
    """
    rel_path = f"demo/conf/{onboarding_format}/sample_onboarding.{conf_ext}"

    ctx = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
    )
    nb_path = ctx.notebookPath().get()

    if "/demo/" in nb_path:
        repo_root_ws = nb_path.rsplit("/demo/", 1)[0]
        if not repo_root_ws.startswith("/Workspace"):
            repo_root_ws = "/Workspace" + repo_root_ws
        ws_path = f"{repo_root_ws}/{rel_path}"
        try:
            with open(ws_path, "r") as fh:
                return fh.read(), ws_path
        except (FileNotFoundError, IsADirectoryError, PermissionError):
            pass

    raw_url = (
        f"https://raw.githubusercontent.com/databrickslabs/sdp-meta/"
        f"{git_branch}/{rel_path}"
    )
    resp = requests.get(raw_url, timeout=30)
    if resp.status_code == 200:
        return resp.text, raw_url
    raise RuntimeError(
        f"Could not load sample onboarding file. Notebook path "
        f"({nb_path}) is not under a sdp-meta repo checkout, and the "
        f"GitHub fallback at {raw_url} returned HTTP "
        f"{resp.status_code}. Either re-import the notebook under the "
        f"sdp-meta repo, or set the 'git_branch' widget to a branch that "
        f"contains {rel_path}."
    )


sample_text, sample_onboarding_source = _load_sample_onboarding_text(
    onboarding_format, conf_ext, git_branch
)

rendered_text = Template(sample_text).safe_substitute(
    data_path=data_path,
    ddl_path=ddl_path,
    dqe_path=dqe_path,
    transformation_path=transformation_path,
    uc_catalog_name=uc_catalog_name,
    uc_schema_name=uc_schema_name,
    bronze_schema=bronze_schema,
    silver_schema=silver_schema,
)

print(
    f"--- {sample_onboarding_source} (rendered as {onboarding_format}) ---\n"
)
print(rendered_text)

if conf_ext in ("yml", "yaml"):
    onboarding_json = yaml.safe_load(rendered_text)
else:
    onboarding_json = json.loads(rendered_text)

_write_onboarding(onboarding_json, onboarding_file_path)

print(f"Onboarding file: {onboarding_file_path}")
print(f"Data flows: {len(onboarding_json)} (customers, transactions)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Verify the Onboarding File on the UC Volume
# MAGIC
# MAGIC Cell 2.1 printed the *rendered template*. This cell prints the
# MAGIC bytes that actually landed on the UC volume after
# MAGIC `_write_onboarding(...)` serialized them — confirming round-trip
# MAGIC fidelity in the format you selected. This is the exact file the
# MAGIC `OnboardDataflowspec` runner will read in the next cell.

# COMMAND ----------

print(f"--- {onboarding_file_path} ({onboarding_format}) ---\n")
with open(onboarding_file_path, "r") as fh:
    print(fh.read())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Run Onboarding
# MAGIC
# MAGIC The
# MAGIC [OnboardDataflowspec](https://github.com/databrickslabs/sdp-meta/blob/main/src/databricks/labs/sdp_meta/onboard_dataflowspec.py)
# MAGIC API reads the onboarding file (JSON or YAML) and creates two Delta tables:
# MAGIC - `bronze_dataflowspec` — metadata for Bronze layer
# MAGIC - `silver_dataflowspec` — metadata for Silver layer

# COMMAND ----------

from databricks.labs.sdp_meta.onboard_dataflowspec import (
    OnboardDataflowspec,
)

onboarding_params = {
    "onboarding_file_path": onboarding_file_path,
    "database": f"{uc_catalog_name}.{uc_schema_name}",
    "env": "prod",
    "bronze_dataflowspec_table": "bronze_dataflowspec",
    "silver_dataflowspec_table": "silver_dataflowspec",
    "overwrite": "True",
    "version": "v1",
    "import_author": "demo_user",
}
print("Onboarding parameters:")
print(json.dumps(onboarding_params, indent=2))

OnboardDataflowspec(
    spark=spark, dict_obj=onboarding_params, uc_enabled=True
).onboard_dataflow_specs()
print("\nOnboarding complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Inspect DataflowSpec Tables

# COMMAND ----------

# DBTITLE 1,Bronze DataflowSpec
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{uc_schema_name}"
        ".bronze_dataflowspec"
    )
)

# COMMAND ----------

# DBTITLE 1,Silver DataflowSpec
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{uc_schema_name}"
        ".silver_dataflowspec"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Stage 3: Create Lakeflow Spark Declarative Pipeline
# MAGIC
# MAGIC The pipeline uses a single generic runner notebook that calls
# MAGIC `DataflowPipeline.invoke_dlt_pipeline(spark, layer)`.
# MAGIC
# MAGIC **Source**:
# MAGIC [DataflowPipeline](https://github.com/databrickslabs/sdp-meta/blob/main/src/databricks/labs/sdp_meta/dataflow_pipeline.py)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Create Pipeline Runner Notebook
# MAGIC
# MAGIC This cell writes the runner notebook into your workspace
# MAGIC automatically. No manual upload needed.

# COMMAND ----------

import base64

notebook_name = "sdp_meta_pipeline_runner"
notebook_dir = (
    f"/Workspace/Users/"
    f"{spark.sql('SELECT current_user()').first()[0]}"
    f"/sdp_meta_demo"
)

runner_content = (
    "# Databricks notebook source\n"
    'sdp_meta_whl = spark.conf.get("sdp_meta_whl")\n'
    "%pip install $sdp_meta_whl  # noqa: E999\n"
    "\n"
    "# COMMAND ----------\n"
    "\n"
    'layer = spark.conf.get("layer", None)\n'
    "\n"
    "from databricks.labs.sdp_meta.dataflow_pipeline "
    "import DataflowPipeline\n"
    "DataflowPipeline.invoke_dlt_pipeline(\n"
    "    spark, layer,\n"
    ")\n"
)

encoded = base64.b64encode(
    runner_content.encode("utf-8")
).decode("utf-8")

w.workspace.mkdirs(notebook_dir)
w.workspace.import_(
    content=encoded,
    path=f"{notebook_dir}/{notebook_name}",
    format=ExportFormat.SOURCE,
    language=Language.PYTHON,
    overwrite=True,
)

runner_notebook_path = f"{notebook_dir}/{notebook_name}"
print(f"Runner notebook created: {runner_notebook_path}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3.2 Create and Start the Pipeline
# MAGIC
# MAGIC The cell below creates the Lakeflow Spark Declarative Pipeline
# MAGIC programmatically using the Databricks SDK, then starts it and
# MAGIC waits for completion. The pipeline ID is persisted to the UC
# MAGIC Volume so later stages can trigger reruns without manual
# MAGIC interaction.

# COMMAND ----------

# All 3 pipelines (main, snapshot, sink) reuse the same install target
# resolved from the ``install_source`` / ``whl_file_path`` widgets. The
# runner notebooks read this value via ``spark.conf.get("sdp_meta_whl")``
# and feed it straight to ``%pip install`` — works for both
# ``git+https://...@branch`` and ``/Volumes/.../foo.whl`` shapes.
git_url_for_pip = sdp_meta_install_target

pipeline_config = {
    "layer": "bronze_silver",
    "bronze.group": "A1",
    "silver.group": "A1",
    "bronze.dataflowspecTable": (
        f"{uc_catalog_name}.{uc_schema_name}.bronze_dataflowspec"
    ),
    "silver.dataflowspecTable": (
        f"{uc_catalog_name}.{uc_schema_name}.silver_dataflowspec"
    ),
    "sdp_meta_whl": git_url_for_pip,
}

# Create pipeline (idempotent: skip if already exists)
existing = [
    p for p in w.pipelines.list_pipelines()
    if p.name == pipeline_name
]
if existing:
    pipeline_id = existing[0].pipeline_id
    print(f"Reusing existing pipeline: {pipeline_id}")
else:
    # Spark Declarative Pipelines direct publishing mode requires
    # a pipeline-level target schema. DataflowPipeline sets
    # catalog+schema on every @dp.table() call from DataflowSpec
    # (bronze_database_prod / silver_database_prod),
    # so this target is never used for actual routing.
    # A dedicated placeholder schema is used to keep it separate from
    # the DataflowSpec metadata schema and the Bronze/Silver data schemas.
    # Uses schema= on DPM-enabled workspaces and falls back to target=
    # on non-DPM workspaces (see create_pipeline helper).
    created = create_pipeline(
        w,
        name=pipeline_name,
        catalog=uc_catalog_name,
        schema=bronze_schema,
        libraries=[
            PipelineLibrary(
                notebook=NotebookLibrary(path=runner_notebook_path)
            )
        ],
        configuration=pipeline_config,
        development=True,
        serverless=True,
    )
    pipeline_id = created.pipeline_id
    print(f"Pipeline created: {pipeline_id}")

with open(pipeline_id_file, "w") as fh:
    fh.write(pipeline_id)
print(f"Pipeline ID saved to: {pipeline_id_file}")

run_pipeline_and_wait(w, pipeline_id, label="initial load")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Stage 4: Validate Initial Load
# MAGIC
# MAGIC After the pipeline completes in Stage 3, validate that Bronze
# MAGIC and Silver tables were created correctly.
# MAGIC
# MAGIC ### Data Flow
# MAGIC ```
# MAGIC Source Files (CSV)
# MAGIC   │
# MAGIC   ├─── CloudFiles (Autoloader) ──► Bronze Tables
# MAGIC   │                                   │
# MAGIC   │                     ┌──────────────┼──────────────┐
# MAGIC   │                     │              │              │
# MAGIC   │              expect_or_drop   Good Rows    expect_or_quarantine
# MAGIC   │              (dropped)             │         (quarantine table)
# MAGIC   │                                    │
# MAGIC   │                              Silver Tables
# MAGIC   │                          (CDC / SCD Type 2)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Data Flow Summary — Initial Load
# MAGIC
# MAGIC Shows row counts across all layers for each table:
# MAGIC **Source Files → Bronze → Quarantine → Silver**

# COMMAND ----------

# DBTITLE 1,Pipeline Data Flow Summary (Initial Load)
summary_rows = []
for table in ["customers", "transactions"]:
    src_dir = f"{data_path}/{table}"
    file_count = len([
        f for f in os.listdir(src_dir)
        if not f.startswith("_") and not f.startswith(".")
    ]) if os.path.exists(src_dir) else 0

    bronze_fqn = (
        f"{uc_catalog_name}.{bronze_schema}.{table}"
    )
    quarantine_fqn = (
        f"{uc_catalog_name}.{bronze_schema}"
        f".{table}_quarantine"
    )
    silver_fqn = (
        f"{uc_catalog_name}.{silver_schema}.{table}"
    )

    bronze_count = spark.sql(
        f"SELECT count(*) FROM {bronze_fqn}"
    ).first()[0]
    try:
        quarantine_count = spark.sql(
            f"SELECT count(*) FROM {quarantine_fqn}"
        ).first()[0]
    except Exception:
        quarantine_count = 0
    silver_count = spark.sql(
        f"SELECT count(*) FROM {silver_fqn}"
    ).first()[0]

    summary_rows.append(Row(
        Table=table,
        Source_Files=file_count,
        Bronze_Rows=bronze_count,
        Quarantine_Rows=quarantine_count,
        Silver_Rows=silver_count,
    ))

display(spark.createDataFrame(summary_rows))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Bronze Tables — Sample Data

# COMMAND ----------

# DBTITLE 1,Bronze Customers
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{bronze_schema}"
        ".customers LIMIT 10"
    )
)

# COMMAND ----------

# DBTITLE 1,Bronze Transactions
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{bronze_schema}"
        ".transactions LIMIT 10"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Quarantine Tables — Bad Records
# MAGIC
# MAGIC Records with NULL primary keys or malformed data are routed
# MAGIC here by `expect_or_quarantine` rules.
# MAGIC See: [DQE config](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/json/dqe/customers.json)

# COMMAND ----------

# DBTITLE 1,Customers Quarantine
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{bronze_schema}"
        ".customers_quarantine LIMIT 20"
    )
)

# COMMAND ----------

# DBTITLE 1,Transactions Quarantine
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{bronze_schema}"
        ".transactions_quarantine LIMIT 20"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 Silver Tables — CDC / SCD Type 2
# MAGIC
# MAGIC Silver tables use `apply_changes` for CDC processing.
# MAGIC Only clean records (passed DQE) flow to Silver.

# COMMAND ----------

# DBTITLE 1,Silver Customers (SCD Type 2)
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{silver_schema}"
        ".customers LIMIT 10"
    )
)

# COMMAND ----------

# DBTITLE 1,Silver Transactions (SCD Type 2)
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{silver_schema}"
        ".transactions LIMIT 10"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Stage 5: Add Products & Stores Feeds
# MAGIC
# MAGIC **Add new data feeds without modifying the pipeline.**
# MAGIC Update the onboarding file and re-run onboarding.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Update Onboarding File
# MAGIC
# MAGIC Add **Products** (data_flow_id: 103) and **Stores** (104).
# MAGIC
# MAGIC > **DDL**:
# MAGIC > [products.ddl](https://github.com/databrickslabs/sdp-meta/blob/main/demo/resources/ddl/products.ddl),
# MAGIC > [stores.ddl](https://github.com/databrickslabs/sdp-meta/blob/main/demo/resources/ddl/stores.ddl)

# COMMAND ----------

products_feed = {
    "data_flow_id": "103",
    "data_flow_group": "A1",
    "source_system": "mysql",
    "source_format": "cloudFiles",
    "source_details": {
        "source_path_prod": f"{data_path}/products",
        "source_schema_path": f"{ddl_path}/products.ddl",
    },
    "bronze_catalog_prod": uc_catalog_name,
    "bronze_database_prod": bronze_schema,
    "bronze_table": "products",
    "bronze_table_comment": "products bronze table",
    "bronze_reader_options": {
        "cloudFiles.format": "csv",
        "cloudFiles.rescuedDataColumn": "_rescued_data",
        "header": "true",
    },
    "bronze_cluster_by_auto": True,
    "bronze_data_quality_expectations_json_prod": (
        f"{dqe_path}/products.{conf_ext}"
    ),
    "bronze_catalog_quarantine_prod": uc_catalog_name,
    "bronze_database_quarantine_prod": bronze_schema,
    "bronze_quarantine_table": "products_quarantine",
    "bronze_quarantine_table_comment": (
        "products quarantine bronze table"
    ),
    "silver_catalog_prod": uc_catalog_name,
    "silver_database_prod": silver_schema,
    "silver_table": "products",
    "silver_table_comment": "products silver table",
    "silver_cdc_apply_changes": {
        "keys": ["product_id"],
        "sequence_by": "dmsTimestamp",
        "scd_type": "2",
        "apply_as_deletes": "Op = 'D'",
        "except_column_list": [
            "Op", "dmsTimestamp", "_rescued_data",
        ],
    },
    "silver_cluster_by_auto": True,
    "silver_transformation_json_prod": (
        f"{transformation_path}/silver_transformations.{conf_ext}"
    ),
    "silver_data_quality_expectations_json_prod": (
        f"{dqe_path}/products_silver_dqe.{conf_ext}"
    ),
}

stores_feed = {
    "data_flow_id": "104",
    "data_flow_group": "A1",
    "source_system": "mysql",
    "source_format": "cloudFiles",
    "source_details": {
        "source_path_prod": f"{data_path}/stores",
        "source_schema_path": f"{ddl_path}/stores.ddl",
    },
    "bronze_catalog_prod": uc_catalog_name,
    "bronze_database_prod": bronze_schema,
    "bronze_table": "stores",
    "bronze_table_comment": "stores bronze table",
    "bronze_reader_options": {
        "cloudFiles.format": "csv",
        "cloudFiles.rescuedDataColumn": "_rescued_data",
        "header": "true",
    },
    "bronze_data_quality_expectations_json_prod": (
        f"{dqe_path}/stores.{conf_ext}"
    ),
    "bronze_catalog_quarantine_prod": uc_catalog_name,
    "bronze_database_quarantine_prod": bronze_schema,
    "bronze_quarantine_table": "stores_quarantine",
    "bronze_quarantine_table_comment": (
        "stores quarantine bronze table"
    ),
    "silver_catalog_prod": uc_catalog_name,
    "silver_database_prod": silver_schema,
    "silver_table": "stores",
    "silver_table_comment": "stores silver table",
    "silver_cdc_apply_changes": {
        "keys": ["store_id"],
        "sequence_by": "dmsTimestamp",
        "scd_type": "2",
        "apply_as_deletes": "Op = 'D'",
        "except_column_list": [
            "Op", "dmsTimestamp", "_rescued_data",
        ],
    },
    "silver_transformation_json_prod": (
        f"{transformation_path}/silver_transformations.{conf_ext}"
    ),
    "silver_data_quality_expectations_json_prod": (
        f"{dqe_path}/stores_silver_dqe.{conf_ext}"
    ),
}

onboarding_json = _read_onboarding(onboarding_file_path)

onboarding_json.extend([products_feed, stores_feed])

_write_onboarding(onboarding_json, onboarding_file_path)

print(
    f"Onboarding file updated: "
    f"{len(onboarding_json)} data flows"
)
print(
    "  customers (100), transactions (101), "
    "products (103), stores (104)"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Re-run Onboarding

# COMMAND ----------

onboarding_params["overwrite"] = "True"

OnboardDataflowspec(
    spark=spark, dict_obj=onboarding_params, uc_enabled=True
).onboard_dataflow_specs()
print("Onboarding updated with products and stores!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Verify Updated DataflowSpec

# COMMAND ----------

# DBTITLE 1,Bronze DataflowSpec (4 feeds)
display(
    spark.sql(
        f"SELECT dataFlowId, sourceFormat, "
        f"targetDetails['table'] as target_table "
        f"FROM {uc_catalog_name}.{uc_schema_name}"
        ".bronze_dataflowspec"
    )
)

# COMMAND ----------

# DBTITLE 1,Silver DataflowSpec (4 feeds)
display(
    spark.sql(
        f"SELECT dataFlowId, "
        f"targetDetails['table'] as target_table "
        f"FROM {uc_catalog_name}.{uc_schema_name}"
        ".silver_dataflowspec"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.4 Re-run the Pipeline
# MAGIC
# MAGIC The same generic pipeline automatically picks up Products and
# MAGIC Stores from the updated DataflowSpec — no pipeline code changes needed.

# COMMAND ----------

with open(pipeline_id_file, "r") as fh:
    pipeline_id = fh.read().strip()
run_pipeline_and_wait(w, pipeline_id, label="add products & stores")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.5 Data Flow Summary — All 4 Feeds
# MAGIC
# MAGIC After re-running the pipeline with Products and Stores added,
# MAGIC verify row counts across all layers for every table.

# COMMAND ----------

# DBTITLE 1,Pipeline Data Flow Summary (4 Feeds)
all_tables = ["customers", "transactions", "products", "stores"]
summary_rows = []
for table in all_tables:
    src_dir = f"{data_path}/{table}"
    file_count = len([
        f for f in os.listdir(src_dir)
        if not f.startswith("_") and not f.startswith(".")
    ]) if os.path.exists(src_dir) else 0

    bronze_fqn = (
        f"{uc_catalog_name}.{bronze_schema}.{table}"
    )
    quarantine_fqn = (
        f"{uc_catalog_name}.{bronze_schema}"
        f".{table}_quarantine"
    )
    silver_fqn = (
        f"{uc_catalog_name}.{silver_schema}.{table}"
    )

    bronze_count = spark.sql(
        f"SELECT count(*) FROM {bronze_fqn}"
    ).first()[0]
    try:
        quarantine_count = spark.sql(
            f"SELECT count(*) FROM {quarantine_fqn}"
        ).first()[0]
    except Exception:
        quarantine_count = 0
    silver_count = spark.sql(
        f"SELECT count(*) FROM {silver_fqn}"
    ).first()[0]

    summary_rows.append(Row(
        Table=table,
        Source_Files=file_count,
        Bronze_Rows=bronze_count,
        Quarantine_Rows=quarantine_count,
        Silver_Rows=silver_count,
    ))

display(spark.createDataFrame(summary_rows))

# COMMAND ----------

# DBTITLE 1,Bronze Products — Sample Data
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{bronze_schema}"
        ".products LIMIT 10"
    )
)

# COMMAND ----------

# DBTITLE 1,Bronze Stores — Sample Data
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{bronze_schema}"
        ".stores LIMIT 10"
    )
)

# COMMAND ----------

# DBTITLE 1,Products Quarantine
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{bronze_schema}"
        ".products_quarantine LIMIT 20"
    )
)

# COMMAND ----------

# DBTITLE 1,Stores Quarantine
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{bronze_schema}"
        ".stores_quarantine LIMIT 20"
    )
)

# COMMAND ----------

# DBTITLE 1,Silver Products
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{silver_schema}"
        ".products LIMIT 10"
    )
)

# COMMAND ----------

# DBTITLE 1,Silver Stores
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{silver_schema}"
        ".stores LIMIT 10"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Stage 6: Incremental Data Load (CDC)
# MAGIC
# MAGIC Push incremental CDC data (Inserts, Updates, Deletes) and
# MAGIC re-run the pipeline. Incremental files use CDC format with an
# MAGIC `Op` column: `I` (Insert), `U` (Update), `D` (Delete).

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1 Preview Incremental Data

# COMMAND ----------

# DBTITLE 1,Incremental Files Available
for domain in ["customers", "transactions", "products", "stores"]:
    dp = f"{incremental_data_path}/{domain}"
    if os.path.exists(dp):
        files = [
            f for f in os.listdir(dp)
            if not f.startswith("_") and not f.startswith(".")
        ]
        print(f"{domain}: {len(files)} incremental file(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Copy Incremental Data to Source Directories
# MAGIC
# MAGIC Simulate the arrival of new CDC data by copying incremental
# MAGIC files to the source data directories.

# COMMAND ----------

for domain in ["customers", "transactions", "stores", "products"]:
    source = f"{incremental_data_path}/{domain}/"
    target = f"{data_path}/{domain}/"
    if os.path.exists(source.rstrip("/")):
        dbutils.fs.cp(source, target, recurse=True)
        print(f"Copied incremental data: {domain}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 Re-run the Pipeline
# MAGIC
# MAGIC CloudFiles (Autoloader) automatically detects the new incremental files.

# COMMAND ----------

with open(pipeline_id_file, "r") as fh:
    pipeline_id = fh.read().strip()
run_pipeline_and_wait(w, pipeline_id, label="incremental CDC")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Stage 7: Validate Incremental Results
# MAGIC
# MAGIC Verify that CDC changes (Insert / Update / Delete) were applied
# MAGIC correctly. Compare row counts before and after incremental load.
# MAGIC
# MAGIC ### Expected Behavior
# MAGIC ```
# MAGIC Incremental CDC Files (Op: I/U/D)
# MAGIC   │
# MAGIC   ├─── Autoloader picks up new files ──► Bronze (rows grow)
# MAGIC   │                                          │
# MAGIC   │                          DQE rules applied again
# MAGIC   │                                          │
# MAGIC   │                  Good rows ──► Silver (SCD Type 2)
# MAGIC   │                                  ├─ INSERTs: new rows
# MAGIC   │                                  ├─ UPDATEs: old row closed (__END_AT set),
# MAGIC   │                                  │           new row opened
# MAGIC   │                                  └─ DELETEs: row closed (__END_AT set)
# MAGIC   │
# MAGIC   └─── Bad records ──► Quarantine (rows grow)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.1 Data Flow Summary — After Incremental Load

# COMMAND ----------

# DBTITLE 1,Pipeline Data Flow Summary (After Incremental)
all_tables = ["customers", "transactions", "products", "stores"]
summary_rows = []
for table in all_tables:
    src_dir = f"{data_path}/{table}"
    file_count = len([
        f for f in os.listdir(src_dir)
        if not f.startswith("_") and not f.startswith(".")
    ]) if os.path.exists(src_dir) else 0

    inc_dir = f"{incremental_data_path}/{table}"
    inc_file_count = len([
        f for f in os.listdir(inc_dir)
        if not f.startswith("_") and not f.startswith(".")
    ]) if os.path.exists(inc_dir) else 0

    bronze_fqn = (
        f"{uc_catalog_name}.{bronze_schema}.{table}"
    )
    quarantine_fqn = (
        f"{uc_catalog_name}.{bronze_schema}"
        f".{table}_quarantine"
    )
    silver_fqn = (
        f"{uc_catalog_name}.{silver_schema}.{table}"
    )

    bronze_count = spark.sql(
        f"SELECT count(*) FROM {bronze_fqn}"
    ).first()[0]
    try:
        quarantine_count = spark.sql(
            f"SELECT count(*) FROM {quarantine_fqn}"
        ).first()[0]
    except Exception:
        quarantine_count = 0
    silver_count = spark.sql(
        f"SELECT count(*) FROM {silver_fqn}"
    ).first()[0]

    summary_rows.append(Row(
        Table=table,
        Initial_Files=file_count,
        Incremental_Files=inc_file_count,
        Bronze_Rows=bronze_count,
        Quarantine_Rows=quarantine_count,
        Silver_Rows=silver_count,
    ))

display(spark.createDataFrame(summary_rows))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 Silver Tables — SCD Type 2 History
# MAGIC
# MAGIC Look for `__START_AT` and `__END_AT` columns:
# MAGIC - **Current rows**: `__END_AT` is NULL
# MAGIC - **Historical rows**: `__END_AT` is set (closed by an Update or Delete)

# COMMAND ----------

# DBTITLE 1,Silver Customers — SCD Type 2 History
display(spark.sql(f"""
  SELECT customer_id, full_name, email, address,
         __START_AT, __END_AT
  FROM {uc_catalog_name}.{silver_schema}.customers
  ORDER BY customer_id, __START_AT
  LIMIT 30
"""))

# COMMAND ----------

# DBTITLE 1,Silver Transactions — SCD Type 2 History
display(spark.sql(f"""
  SELECT transaction_id, customer_id, product_id,
         store_id, __START_AT, __END_AT
  FROM {uc_catalog_name}.{silver_schema}.transactions
  ORDER BY transaction_id, __START_AT
  LIMIT 30
"""))

# COMMAND ----------

# DBTITLE 1,Silver Products — SCD Type 2 History
display(spark.sql(f"""
  SELECT product_id, name, price,
         __START_AT, __END_AT
  FROM {uc_catalog_name}.{silver_schema}.products
  ORDER BY product_id, __START_AT
  LIMIT 30
"""))

# COMMAND ----------

# DBTITLE 1,Silver Stores — SCD Type 2 History
display(spark.sql(f"""
  SELECT store_id, address, __START_AT, __END_AT
  FROM {uc_catalog_name}.{silver_schema}.stores
  ORDER BY store_id, __START_AT
  LIMIT 30
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Stage 8: Append Flow — Multi-Source Ingestion
# MAGIC
# MAGIC **Append Flow** reads from **multiple source paths** and writes to the
# MAGIC **same target table** using `dp.append_flow`. This enables:
# MAGIC - Consolidating data from multiple upstream systems
# MAGIC - Adding **file metadata columns** (file name, file path)
# MAGIC - Gradually adding new data feeds without schema changes
# MAGIC
# MAGIC ```
# MAGIC Source Path 1 (orders/)
# MAGIC   │
# MAGIC   ├─── Autoloader ──► bronze_orders ◄── Autoloader ──── Source Path 2 (orders_af/)
# MAGIC   │                        │                                  (append_flow)
# MAGIC   │                        │
# MAGIC   │              DQE + apply_changes
# MAGIC   │                        │
# MAGIC   │                   silver_orders
# MAGIC ```
# MAGIC
# MAGIC See: [cloudfiles-onboarding.template](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/json/cloudfiles-onboarding.template)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.1 Append Flow Onboarding
# MAGIC
# MAGIC The `bronze_append_flows` list defines additional sources that
# MAGIC feed into the same bronze target using `dp.append_flow`.
# MAGIC `source_metadata` adds file name/path to each record.

# COMMAND ----------

append_flow_feed = {
    "data_flow_id": "200",
    "data_flow_group": "A1",
    "source_system": "MYSQL",
    "source_format": "cloudFiles",
    "source_details": {
        "source_database": "APP",
        "source_table": "ORDERS",
        "source_path_prod": f"{af_data_path}/orders",
        "source_metadata": {
            "include_autoloader_metadata_column": "True",
            "autoloader_metadata_col_name": "source_metadata",
            "select_metadata_cols": {
                "input_file_name": "_metadata.file_name",
                "input_file_path": "_metadata.file_path",
            },
        },
        "source_schema_path": f"{ddl_path}/af_orders.ddl",
    },
    "bronze_catalog_prod": uc_catalog_name,
    "bronze_database_prod": bronze_schema,
    "bronze_table": "orders",
    "bronze_reader_options": {
        "cloudFiles.format": "json",
        "cloudFiles.inferColumnTypes": "true",
        "cloudFiles.rescuedDataColumn": "_rescued_data",
    },
    "bronze_cluster_by_auto": True,
    "bronze_data_quality_expectations_json_prod": (
        f"{dqe_path}/af_orders_bronze_dqe.{conf_ext}"
    ),
    "bronze_catalog_quarantine_prod": uc_catalog_name,
    "bronze_database_quarantine_prod": bronze_schema,
    "bronze_quarantine_table": "orders_quarantine",
    "bronze_append_flows": [
        {
            "name": "orders_bronze_append_flow",
            "create_streaming_table": False,
            "source_format": "cloudFiles",
            "source_details": {
                "source_path_prod": (
                    f"{af_data_path}/orders_af"
                ),
                "source_schema_path": (
                    f"{ddl_path}/af_orders.ddl"
                ),
            },
            "reader_options": {
                "cloudFiles.format": "json",
                "cloudFiles.inferColumnTypes": "true",
                "cloudFiles.rescuedDataColumn":
                    "_rescued_data",
            },
            "once": False,
        },
    ],
    "silver_catalog_prod": uc_catalog_name,
    "silver_database_prod": silver_schema,
    "silver_table": "orders",
    "silver_cdc_apply_changes": {
        "keys": ["order_id"],
        "sequence_by": "operation_date",
        "scd_type": "1",
        "apply_as_deletes": "operation = 'DELETE'",
        "except_column_list": [
            "operation", "operation_date", "_rescued_data",
        ],
    },
    "silver_transformation_json_prod": (
        f"{transformation_path}/af_silver_transformations.{conf_ext}"
    ),
    "silver_data_quality_expectations_json_prod": (
        f"{dqe_path}/af_orders_silver_dqe.{conf_ext}"
    ),
}

onboarding_json = _read_onboarding(onboarding_file_path)

onboarding_json.append(append_flow_feed)

_write_onboarding(onboarding_json, onboarding_file_path)

print(
    f"Onboarding updated: {len(onboarding_json)} data flows"
)
print("  Added: orders (200) with append_flow from orders_af")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.2 Re-run Onboarding

# COMMAND ----------

onboarding_params["overwrite"] = "True"

OnboardDataflowspec(
    spark=spark, dict_obj=onboarding_params, uc_enabled=True
).onboard_dataflow_specs()
print("Onboarding updated with append flow orders!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.3 Re-run the Pipeline
# MAGIC
# MAGIC The pipeline now also processes the **orders** table from two
# MAGIC sources via append flow.

# COMMAND ----------

with open(pipeline_id_file, "r") as fh:
    pipeline_id = fh.read().strip()
run_pipeline_and_wait(w, pipeline_id, label="append flow orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.4 Validate Append Flow Results
# MAGIC
# MAGIC Both source paths feed into the same `orders` table.
# MAGIC The `source_metadata` column shows which file each record came from.

# COMMAND ----------

# DBTITLE 1,Bronze Orders (from both sources via append_flow)
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{bronze_schema}"
        ".orders LIMIT 20"
    )
)

# COMMAND ----------

# DBTITLE 1,Bronze Orders — Row Count
display(
    spark.sql(
        f"SELECT count(*) as total_rows "
        f"FROM {uc_catalog_name}.{bronze_schema}.orders"
    )
)

# COMMAND ----------

# DBTITLE 1,Silver Orders
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{silver_schema}"
        ".orders LIMIT 20"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Stage 9: Apply Changes From Snapshot
# MAGIC
# MAGIC **Snapshot ingestion** uses `apply_changes_from_snapshot` to process
# MAGIC full point-in-time snapshots instead of streaming CDC events.
# MAGIC
# MAGIC - **SCD Type 2** (products): Updated records get expired, new
# MAGIC   versions added
# MAGIC - **SCD Type 1** (stores): Latest snapshot overwrites previous;
# MAGIC   missing records are deleted
# MAGIC
# MAGIC ```
# MAGIC Snapshot CSVs (LOAD_1, LOAD_2, ...)
# MAGIC   │
# MAGIC   ├── next_snapshot_and_version() callback
# MAGIC   │   reads versioned CSV files
# MAGIC   │
# MAGIC   ├── Bronze (apply_changes_from_snapshot)
# MAGIC   │       SCD Type 2: old versions expired
# MAGIC   │       SCD Type 1: overwrite
# MAGIC   │
# MAGIC   └── Silver (apply_changes_from_snapshot)
# MAGIC ```
# MAGIC
# MAGIC See: [snapshot-onboarding.template](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/json/snapshot-onboarding.template)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.1 Create Source Delta Table for Products Snapshots
# MAGIC
# MAGIC Products uses a **delta source table** for snapshot ingestion.
# MAGIC We load the initial CSV snapshot into a delta table.

# COMMAND ----------

snap_products_source_db = f"{uc_catalog_name}.{uc_schema_name}"
snap_products_source_table = "source_products_delta"

df = spark.read.format("csv").option("header", "true").load(
    f"{snapshot_data_path}/products/LOAD_1.csv"
)
df.write.format("delta").mode("overwrite").saveAsTable(
    f"{snap_products_source_db}.{snap_products_source_table}"
)
print(
    f"Source delta table created: "
    f"{snap_products_source_db}.{snap_products_source_table}"
)
display(spark.sql(
    f"SELECT * FROM {snap_products_source_db}"
    f".{snap_products_source_table}"
))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.2 Snapshot Onboarding
# MAGIC
# MAGIC - **Products** (data_flow_id 301): delta source with SCD Type 2
# MAGIC - **Stores** (data_flow_id 302): CSV snapshots with SCD Type 1

# COMMAND ----------

snap_products_feed = {
    "data_flow_id": "301",
    "data_flow_group": "SNAP",
    "source_system": "delta",
    "source_format": "snapshot",
    "source_details": {
        "snapshot_format": "delta",
        "source_catalog_prod": uc_catalog_name,
        "source_table": snap_products_source_table,
        "source_database": uc_schema_name,
    },
    # Snapshot feeds use a fully-qualified "catalog.schema" string in
    # bronze_database_prod (no separate bronze_catalog_prod field).
    # This differs from cloudFiles feeds which split catalog and schema.
    "bronze_database_prod": (
        f"{uc_catalog_name}.{bronze_schema}"
    ),
    "bronze_table": "snap_products",
    "bronze_apply_changes_from_snapshot": {
        "keys": ["product_id"],
        "scd_type": "2",
    },
    "silver_catalog_prod": uc_catalog_name,
    "silver_database_prod": silver_schema,
    "silver_table": "snap_products",
    "silver_table_comment": "products from snapshot SCD2",
    "silver_apply_changes_from_snapshot": {
        "keys": ["product_id"],
        "scd_type": "2",
    },
    "silver_transformation_json_prod": (
        f"{transformation_path}"
        f"/snapshot_silver_transformations.{conf_ext}"
    ),
}

snap_stores_feed = {
    "data_flow_id": "302",
    "data_flow_group": "SNAP",
    "source_system": "delta",
    "source_format": "snapshot",
    "source_details": {
        "source_path_prod": (
            f"{snapshot_data_path}/stores/LOAD_"
        ),
        "snapshot_format": "csv",
    },
    "bronze_reader_options": {"header": "true"},
    # Snapshot feeds use a fully-qualified "catalog.schema" string in
    # bronze_database_prod (no separate bronze_catalog_prod field).
    # This differs from cloudFiles feeds which split catalog and schema.
    "bronze_database_prod": (
        f"{uc_catalog_name}.{bronze_schema}"
    ),
    "bronze_table": "snap_stores",
    "bronze_apply_changes_from_snapshot": {
        "keys": ["store_id"],
        "scd_type": "1",
    },
    "silver_catalog_prod": uc_catalog_name,
    "silver_database_prod": silver_schema,
    "silver_table": "snap_stores",
    "silver_apply_changes_from_snapshot": {
        "keys": ["store_id"],
        "scd_type": "1",
    },
    "silver_transformation_json_prod": (
        f"{transformation_path}"
        f"/snapshot_silver_transformations.{conf_ext}"
    ),
}

onboarding_json = _read_onboarding(onboarding_file_path)

onboarding_json.extend([snap_products_feed, snap_stores_feed])

_write_onboarding(onboarding_json, onboarding_file_path)

print(
    f"Onboarding updated: {len(onboarding_json)} data flows"
)
print(
    "  Added: snap_products (301, SCD2), "
    "snap_stores (302, SCD1)"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.3 Re-run Onboarding

# COMMAND ----------

onboarding_params["overwrite"] = "True"

OnboardDataflowspec(
    spark=spark, dict_obj=onboarding_params, uc_enabled=True
).onboard_dataflow_specs()
print("Onboarding updated with snapshot feeds!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.4 Create Snapshot Runner Notebook & Pipeline
# MAGIC
# MAGIC Snapshot ingestion requires a `next_snapshot_and_version` callback
# MAGIC passed to `invoke_dlt_pipeline`. A separate runner notebook and
# MAGIC pipeline are created for the **SNAP** data-flow group.

# COMMAND ----------

snapshot_notebook_name = "sdp_meta_snapshot_runner"

snapshot_runner_content = (
    "# Databricks notebook source\n"
    'sdp_meta_whl = spark.conf.get("sdp_meta_whl")\n'
    "%pip install $sdp_meta_whl  # noqa: E999\n"
    "\n"
    "# COMMAND ----------\n"
    "\n"
    "# DBTITLE 1,Snapshot reader for apply_changes_from_snapshot\n"
    "from databricks.labs.sdp_meta.dataflow_spec "
    "import BronzeDataflowSpec\n"
    "\n"
    "\n"
    "def exist(path):\n"
    "    try:\n"
    "        return dbutils.fs.ls(path) is not None\n"
    "    except Exception:\n"
    "        return False\n"
    "\n"
    "\n"
    "def next_snapshot_and_version("
    "latest_snapshot_version, dataflow_spec):\n"
    "    latest_snapshot_version = "
    "latest_snapshot_version or 0\n"
    "    next_version = latest_snapshot_version + 1\n"
    "    bronze_dataflow_spec: BronzeDataflowSpec "
    "= dataflow_spec\n"
    "    options = bronze_dataflow_spec"
    ".readerConfigOptions\n"
    "    snapshot_format = bronze_dataflow_spec"
    '.sourceDetails["snapshot_format"]\n'
    "    snapshot_root_path = bronze_dataflow_spec"
    ".sourceDetails['path']\n"
    '    snapshot_path = f"{snapshot_root_path}'
    '{next_version}.csv"\n'
    "    if exist(snapshot_path):\n"
    "        snapshot = spark.read.format("
    "snapshot_format).options(**options)"
    ".load(snapshot_path)\n"
    "        return (snapshot, next_version)\n"
    "    else:\n"
    "        return None\n"
    "\n"
    "\n"
    "# COMMAND ----------\n"
    "\n"
    'layer = spark.conf.get("layer", None)\n'
    "\n"
    "from databricks.labs.sdp_meta.dataflow_pipeline "
    "import DataflowPipeline\n"
    "DataflowPipeline.invoke_dlt_pipeline(\n"
    "    spark, layer,\n"
    "    bronze_next_snapshot_and_version="
    "next_snapshot_and_version,\n"
    "    silver_next_snapshot_and_version=None,\n"
    ")\n"
)

snapshot_encoded = base64.b64encode(
    snapshot_runner_content.encode("utf-8")
).decode("utf-8")

w.workspace.import_(
    content=snapshot_encoded,
    path=f"{notebook_dir}/{snapshot_notebook_name}",
    format=ExportFormat.SOURCE,
    language=Language.PYTHON,
    overwrite=True,
)

snapshot_runner_path = f"{notebook_dir}/{snapshot_notebook_name}"
print(f"Snapshot runner notebook created: {snapshot_runner_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.5 Create & Start Snapshot Pipeline
# MAGIC
# MAGIC The pipeline processes the initial snapshot (LOAD_1).

# COMMAND ----------

snapshot_pipeline_name = f"sdp_meta_demo_snapshot_{uc_schema_name}"
snapshot_pipeline_id_file = (
    f"{uc_volume_path}/snapshot_pipeline_id.txt"
)

snapshot_pipeline_config = {
    "layer": "bronze_silver",
    "bronze.group": "SNAP",
    "silver.group": "SNAP",
    "bronze.dataflowspecTable": (
        f"{uc_catalog_name}.{uc_schema_name}.bronze_dataflowspec"
    ),
    "silver.dataflowspecTable": (
        f"{uc_catalog_name}.{uc_schema_name}.silver_dataflowspec"
    ),
    "sdp_meta_whl": git_url_for_pip,
}

existing_snap = [
    p for p in w.pipelines.list_pipelines()
    if p.name == snapshot_pipeline_name
]
if existing_snap:
    snapshot_pipeline_id = existing_snap[0].pipeline_id
    print(
        f"Reusing existing snapshot pipeline: "
        f"{snapshot_pipeline_id}"
    )
else:
    created_snap = create_pipeline(
        w,
        name=snapshot_pipeline_name,
        catalog=uc_catalog_name,
        schema=bronze_schema,
        libraries=[
            PipelineLibrary(
                notebook=NotebookLibrary(
                    path=snapshot_runner_path
                )
            )
        ],
        configuration=snapshot_pipeline_config,
        development=True,
        serverless=True,
    )
    snapshot_pipeline_id = created_snap.pipeline_id
    print(f"Snapshot pipeline created: {snapshot_pipeline_id}")

with open(snapshot_pipeline_id_file, "w") as fh:
    fh.write(snapshot_pipeline_id)
print(
    f"Snapshot pipeline ID saved to: "
    f"{snapshot_pipeline_id_file}"
)

run_pipeline_and_wait(
    w, snapshot_pipeline_id, label="snapshot initial load"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.6 Validate Initial Snapshots

# COMMAND ----------

# DBTITLE 1,Bronze snap_products (Snapshot SCD Type 2)
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{bronze_schema}"
        ".snap_products LIMIT 20"
    )
)

# COMMAND ----------

# DBTITLE 1,Bronze snap_stores (Snapshot SCD Type 1)
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{bronze_schema}"
        ".snap_stores LIMIT 20"
    )
)

# COMMAND ----------

# DBTITLE 1,Silver snap_products
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{silver_schema}"
        ".snap_products LIMIT 20"
    )
)

# COMMAND ----------

# DBTITLE 1,Silver snap_stores
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{silver_schema}"
        ".snap_stores LIMIT 20"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.7 Load Next Snapshot (Version 2)
# MAGIC
# MAGIC Simulate arrival of a new snapshot:
# MAGIC - Products: some items renamed (e.g., `shorts` → `shorts_v2`)
# MAGIC - Stores: only 2 of 4 stores remain (SCD1 will delete missing)

# COMMAND ----------

dbutils.fs.cp(
    f"{snapshot_data_path}/incremental_snapshots/"
    "stores/LOAD_2.csv",
    f"{snapshot_data_path}/stores/LOAD_2.csv",
    True,
)
print("Copied stores LOAD_2.csv")

df2 = spark.read.format("csv").option(
    "header", "true"
).load(
    f"{snapshot_data_path}/incremental_snapshots/"
    "products/LOAD_2.csv"
)
df2.write.format("delta").mode("overwrite").saveAsTable(
    f"{snap_products_source_db}.{snap_products_source_table}"
)
print("Updated source_products_delta with LOAD_2 data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.8 Re-run the Snapshot Pipeline (Snapshot V2)

# COMMAND ----------

with open(snapshot_pipeline_id_file, "r") as fh:
    snapshot_pipeline_id = fh.read().strip()
run_pipeline_and_wait(
    w, snapshot_pipeline_id, label="snapshot V2"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.9 Validate Snapshot V2 Changes

# COMMAND ----------

# DBTITLE 1,Bronze snap_products — SCD Type 2 History
display(spark.sql(f"""
  SELECT product_id, name, price,
         __START_AT, __END_AT
  FROM {uc_catalog_name}.{bronze_schema}.snap_products
  ORDER BY product_id, __START_AT
  LIMIT 30
"""))

# COMMAND ----------

# DBTITLE 1,Bronze snap_stores — SCD Type 1 (latest only)
display(spark.sql(f"""
  SELECT *
  FROM {uc_catalog_name}.{bronze_schema}.snap_stores
  LIMIT 20
"""))

# COMMAND ----------

# DBTITLE 1,Silver snap_products (current rows only via where_clause)
display(spark.sql(f"""
  SELECT *
  FROM {uc_catalog_name}.{silver_schema}.snap_products
  LIMIT 20
"""))

# COMMAND ----------

# DBTITLE 1,Silver snap_stores
display(spark.sql(f"""
  SELECT *
  FROM {uc_catalog_name}.{silver_schema}.snap_stores
  LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Stage 10: Pipeline Sink — Write to External Delta
# MAGIC
# MAGIC **Pipeline Sink** writes pipeline output to **external destinations**
# MAGIC (delta, kafka) using `dp.create_sink` + `dp.append_flow`.
# MAGIC This stage demonstrates writing IoT events to an external
# MAGIC delta table location.
# MAGIC
# MAGIC ```
# MAGIC Source (CSV via Autoloader)
# MAGIC   │
# MAGIC   ├─── Bronze Table (iot_events)
# MAGIC   │         │
# MAGIC   │         ├── DQE (expect_or_drop / quarantine)
# MAGIC   │         │
# MAGIC   │         └── Sink: dp.create_sink(format="delta")
# MAGIC   │                   writes to external Volume path
# MAGIC   │
# MAGIC   └─── Quarantine Table (iot_events_quarantine)
# MAGIC ```
# MAGIC
# MAGIC See: [kafka-sink-onboarding.template](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/json/kafka-sink-onboarding.template)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.1 DLT Sink Onboarding
# MAGIC
# MAGIC `bronze_sinks` defines external destinations. Each sink has a
# MAGIC format, options, select expression, and optional where clause.

# COMMAND ----------

iot_sink_path = (
    f"/Volumes/{uc_catalog_name}/{uc_schema_name}"
    "/config/data/sink/iot_events"
)

iot_sink_feed = {
    "data_flow_id": "400",
    "data_flow_group": "SINK",
    "source_system": "IoT",
    "source_format": "cloudFiles",
    "source_details": {
        "source_path_prod": f"{data_path}/iot_events",
        "source_schema_path": (
            f"{ddl_path}/iot_events.ddl"
        ),
    },
    "bronze_catalog_prod": uc_catalog_name,
    "bronze_database_prod": bronze_schema,
    "bronze_table": "iot_events",
    "bronze_reader_options": {
        "cloudFiles.format": "csv",
        "cloudFiles.rescuedDataColumn": "_rescued_data",
        "header": "true",
    },
    "bronze_data_quality_expectations_json_prod": (
        f"{dqe_path}/iot_events_bronze_dqe.{conf_ext}"
    ),
    "bronze_catalog_quarantine_prod": uc_catalog_name,
    "bronze_database_quarantine_prod": bronze_schema,
    "bronze_quarantine_table": "iot_events_quarantine",
    "bronze_sinks": [
        {
            "name": "iot_events_delta_sink",
            "format": "delta",
            "options": {
                "path": iot_sink_path,
            },
            "select_exp": [
                "device_id", "device_name",
                "temp", "humidity",
                "battery_level", "timestamp",
            ],
            "where_clause": "device_id IS NOT NULL",
        },
    ],
}

onboarding_json = _read_onboarding(onboarding_file_path)

onboarding_json.append(iot_sink_feed)

_write_onboarding(onboarding_json, onboarding_file_path)

print(
    f"Onboarding updated: {len(onboarding_json)} data flows"
)
print(
    f"  Added: iot_events (400) with delta sink at "
    f"{iot_sink_path}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.2 Re-run Onboarding

# COMMAND ----------

onboarding_params["overwrite"] = "True"

OnboardDataflowspec(
    spark=spark, dict_obj=onboarding_params, uc_enabled=True
).onboard_bronze_dataflow_spec()
print("Onboarding updated with IoT events + delta sink!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.3 Create & Start Sink Pipeline
# MAGIC
# MAGIC The sink pipeline runs under the **SINK** data-flow group with
# MAGIC its own runner notebook (no snapshot callback needed).

# COMMAND ----------

sink_pipeline_name = (
    f"sdp_meta_demo_sink_{uc_schema_name}"
)
sink_pipeline_id_file = (
    f"{uc_volume_path}/sink_pipeline_id.txt"
)

sink_pipeline_config = {
    "layer": "bronze",
    "bronze.group": "SINK",
    "bronze.dataflowspecTable": (
        f"{uc_catalog_name}.{uc_schema_name}"
        ".bronze_dataflowspec"
    ),
    "sdp_meta_whl": git_url_for_pip,
}

existing_sink = [
    p for p in w.pipelines.list_pipelines()
    if p.name == sink_pipeline_name
]
if existing_sink:
    sink_pipeline_id = existing_sink[0].pipeline_id
    print(
        f"Reusing existing sink pipeline: "
        f"{sink_pipeline_id}"
    )
else:
    created_sink = create_pipeline(
        w,
        name=sink_pipeline_name,
        catalog=uc_catalog_name,
        schema=bronze_schema,
        libraries=[
            PipelineLibrary(
                notebook=NotebookLibrary(
                    path=runner_notebook_path
                )
            )
        ],
        configuration=sink_pipeline_config,
        development=True,
        serverless=True,
    )
    sink_pipeline_id = created_sink.pipeline_id
    print(f"Sink pipeline created: {sink_pipeline_id}")

with open(sink_pipeline_id_file, "w") as fh:
    fh.write(sink_pipeline_id)
print(
    f"Sink pipeline ID saved to: {sink_pipeline_id_file}"
)

run_pipeline_and_wait(
    w, sink_pipeline_id, label="IoT sink"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.4 Validate DLT Sink Results

# COMMAND ----------

# DBTITLE 1,Bronze IoT Events
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{bronze_schema}"
        ".iot_events LIMIT 20"
    )
)

# COMMAND ----------

# DBTITLE 1,External Delta Sink — IoT Events
display(
    spark.read.format("delta").load(iot_sink_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Stage 11: Multi-Source AUTO CDC into a Single Silver Target
# MAGIC
# MAGIC **multi-source AUTO CDC**: merge **N** regional CDC sources
# MAGIC into **ONE** unified silver streaming table by calling
# MAGIC `dp.create_auto_cdc_flow` N times against the same target. Each
# MAGIC flow has its own `source_format`, `source_details`,
# MAGIC `reader_options`, `select_exp`, and `where_clause`, so per-source
# MAGIC schema normalization happens BEFORE the merge.
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────┐    ┌────────────────────────┐
# MAGIC │ customers_us  (raw)  │───>│ customers_us_cdc       │─┐
# MAGIC │ id, firstname, ...   │    │ (Bronze CDC streaming) │ │
# MAGIC └──────────────────────┘    └────────────────────────┘ │
# MAGIC                                                         │ create_
# MAGIC ┌──────────────────────┐    ┌────────────────────────┐ │ auto_
# MAGIC │ customers_eu  (raw)  │───>│ customers_eu_cdc       │─┤ cdc_flow
# MAGIC │ customer_id,         │    │ (Bronze CDC streaming) │ │   ×3       ┌────────────────────────┐
# MAGIC │ given_name,          │    └────────────────────────┘ ├─────────>  │ customers_regional     │
# MAGIC │ change_type...       │                                │            │ (Silver SCD-1 unified  │
# MAGIC └──────────────────────┘                                │            │  target)               │
# MAGIC                                                         │            └────────────────────────┘
# MAGIC ┌──────────────────────┐    ┌────────────────────────┐ │
# MAGIC │ customers_apac (raw) │───>│ customers_apac_cdc     │─┘
# MAGIC │ cust_id, fname, op   │    │ (Bronze CDC streaming) │
# MAGIC └──────────────────────┘    └────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC Each region uses a **different column shape on purpose** (US:
# MAGIC `id`/`firstname`/`operation`; EU:
# MAGIC `customer_id`/`given_name`/`change_type`; APAC:
# MAGIC `cust_id`/`fname`/`op`) so the per-flow `select_exp`
# MAGIC normalization is doing visible work the user can see in the
# MAGIC silver table.
# MAGIC
# MAGIC See:
# MAGIC [DESIGN_MULTI_SOURCE_AUTO_CDC.md](https://github.com/databrickslabs/sdp-meta/blob/main/DESIGN_MULTI_SOURCE_AUTO_CDC.md) ·
# MAGIC [multi-source-cdc-onboarding.template](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/json/multi-source-cdc-onboarding.template)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.1 Create Regional CDC Source Data
# MAGIC
# MAGIC Three per-region landing folders under `multi_source_cdc/` get
# MAGIC seeded with raw CDC events whose column shapes differ on purpose.
# MAGIC Each region seeds 3 customers + 1 update + 1 delete (5 events
# MAGIC each, 15 total raw events; after the silver SCD-1 merge with
# MAGIC `apply_as_deletes`, the surviving live row count = 6).

# COMMAND ----------

msc_data_path = f"{data_path}/multi_source_cdc"
msc_us_dir = f"{msc_data_path}/customers_us"
msc_eu_dir = f"{msc_data_path}/customers_eu"
msc_apac_dir = f"{msc_data_path}/customers_apac"
for path in (msc_us_dir, msc_eu_dir, msc_apac_dir):
    os.makedirs(path, exist_ok=True)

# US uses "id / firstname / lastname / operation / operation_date".
msc_us_data = [
    {"id": "us-001", "firstname": "Alice", "lastname": "Anderson",
     "email": "alice.us@example.com",
     "address": "123 Main St Springfield IL 62701",
     "operation": "APPEND",
     "operation_date": "2024-01-15 09:00:00"},
    {"id": "us-002", "firstname": "Bob", "lastname": "Brown",
     "email": "bob.us@example.com",
     "address": "456 Oak Ave Portland OR 97201",
     "operation": "APPEND",
     "operation_date": "2024-01-15 10:00:00"},
    {"id": "us-003", "firstname": "Carol", "lastname": "Clark",
     "email": "carol.us@example.com",
     "address": "789 Pine Rd Austin TX 78701",
     "operation": "APPEND",
     "operation_date": "2024-01-15 11:00:00"},
    {"id": "us-001", "firstname": "Alice", "lastname": "Anderson",
     "email": "alice.us@example.com",
     "address": "123 Main St Apt 4B Springfield IL 62701",
     "operation": "UPDATE",
     "operation_date": "2024-01-16 12:00:00"},
    {"id": "us-002", "firstname": "Bob", "lastname": "Brown",
     "email": "bob.us@example.com",
     "address": "456 Oak Ave Portland OR 97201",
     "operation": "DELETE",
     "operation_date": "2024-01-17 09:30:00"},
]

# EU uses "customer_id / given_name / family_name / change_type / change_ts"
# — totally different column names and a different op-code vocabulary
# from US.
msc_eu_data = [
    {"customer_id": "eu-001", "given_name": "Diana",
     "family_name": "Davies",
     "email_address": "diana.eu@example.com",
     "postal_address": "10 Downing Street London SW1A 2AA",
     "change_type": "INSERT",
     "change_ts": "2024-01-15 14:00:00"},
    {"customer_id": "eu-002", "given_name": "Emma",
     "family_name": "Evans",
     "email_address": "emma.eu@example.com",
     "postal_address": "4 Place de la Concorde Paris 75008",
     "change_type": "INSERT",
     "change_ts": "2024-01-15 15:00:00"},
    {"customer_id": "eu-003", "given_name": "Frank",
     "family_name": "Fischer",
     "email_address": "frank.eu@example.com",
     "postal_address": "Unter den Linden 1 Berlin 10117",
     "change_type": "INSERT",
     "change_ts": "2024-01-15 16:00:00"},
    {"customer_id": "eu-002", "given_name": "Emma",
     "family_name": "Evans",
     "email_address": "emma.eu.new@example.com",
     "postal_address": "4 Place de la Concorde Paris 75008",
     "change_type": "UPDATE",
     "change_ts": "2024-01-16 11:00:00"},
    {"customer_id": "eu-003", "given_name": "Frank",
     "family_name": "Fischer",
     "email_address": "frank.eu@example.com",
     "postal_address": "Unter den Linden 1 Berlin 10117",
     "change_type": "DELETE",
     "change_ts": "2024-01-17 10:00:00"},
]

# APAC uses "cust_id / fname / lname / op / op_time" — yet another
# column-shape, with single-char op codes (I/U/D).
msc_apac_data = [
    {"cust_id": "apac-001", "fname": "Grace", "lname": "Goh",
     "mail": "grace.apac@example.com",
     "addr": "1 Marina Bay Singapore 018989",
     "op": "I", "op_time": "2024-01-15 22:00:00"},
    {"cust_id": "apac-002", "fname": "Henry", "lname": "Hashimoto",
     "mail": "henry.apac@example.com",
     "addr": "2-1-1 Marunouchi Tokyo 100-0005",
     "op": "I", "op_time": "2024-01-15 23:00:00"},
    {"cust_id": "apac-003", "fname": "Isha", "lname": "Iyer",
     "mail": "isha.apac@example.com",
     "addr": "1 Hill Road Mumbai 400001",
     "op": "I", "op_time": "2024-01-16 00:00:00"},
    {"cust_id": "apac-002", "fname": "Henry", "lname": "Hashimoto",
     "mail": "henry.apac@example.com",
     "addr": "3-1-1 Marunouchi Tokyo 100-0005",
     "op": "U", "op_time": "2024-01-16 13:00:00"},
    {"cust_id": "apac-003", "fname": "Isha", "lname": "Iyer",
     "mail": "isha.apac@example.com",
     "addr": "1 Hill Road Mumbai 400001",
     "op": "D", "op_time": "2024-01-17 11:30:00"},
]

for region_dir, region_rows, filename in (
    (msc_us_dir, msc_us_data, "us_2024_01.json"),
    (msc_eu_dir, msc_eu_data, "eu_2024_01.json"),
    (msc_apac_dir, msc_apac_data, "apac_2024_01.json"),
):
    out_path = f"{region_dir}/{filename}"
    with open(out_path, "w") as fh:
        for record in region_rows:
            fh.write(json.dumps(record) + "\n")
    print(f"  Created: {out_path} ({len(region_rows)} records)")

# Per-region DDL files. Each one declares the *raw* source column
# shape — the per-flow ``select_exp`` in the silver onboarding row
# rewrites these into the canonical (customer_id, firstname,
# lastname, email, address, region) target shape.
msc_ddl_files = {
    "customers_us.ddl": (
        "address STRING, email STRING, firstname STRING, id STRING, "
        "lastname STRING, operation STRING, operation_date STRING"
    ),
    "customers_eu.ddl": (
        "postal_address STRING, email_address STRING, "
        "given_name STRING, customer_id STRING, family_name STRING, "
        "change_type STRING, change_ts STRING"
    ),
    "customers_apac.ddl": (
        "addr STRING, mail STRING, fname STRING, lname STRING, "
        "cust_id STRING, op STRING, op_time STRING"
    ),
}
for filename, content in msc_ddl_files.items():
    out_ddl = f"{ddl_path}/{filename}"
    with open(out_ddl, "w") as fh:
        fh.write(content)
    print(f"  Created: {out_ddl}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.2 Add Multi-Source CDC Rows to the Onboarding File
# MAGIC
# MAGIC Adds **4 new rows** to the existing onboarding file (all in a new
# MAGIC `data_flow_group: "MSC"` so they don't collide with the main
# MAGIC `A1` / snapshot `SNAP` / sink `SINK` groups):
# MAGIC
# MAGIC 1. **Three bronze rows** — one per region — that land the raw CDC
# MAGIC    events into `customers_us_cdc` / `customers_eu_cdc` /
# MAGIC    `customers_apac_cdc` via CloudFiles, each with its own source
# MAGIC    schema.
# MAGIC 2. **One silver row** with a `silver_cdc_apply_changes_flows`
# MAGIC    group block that defines the shared CDC config (keys,
# MAGIC    `sequence_by`, `scd_type`, `apply_as_deletes`,
# MAGIC    `except_column_list`) plus three per-flow entries — one per
# MAGIC    region — each with its own `source_format` (delta) and
# MAGIC    `select_exp` that maps that region's raw column shape into
# MAGIC    the canonical `(customer_id, firstname, lastname, email,
# MAGIC    address, region, operation, operation_date)` target shape
# MAGIC    before the merge.

# COMMAND ----------

msc_bronze_rows = [
    {
        "data_flow_id": "msc-100",
        "data_flow_group": "MSC",
        "source_system": "RegionalCDC-US",
        "source_format": "cloudFiles",
        "source_details": {
            "source_database": "APP",
            "source_table": "CUSTOMERS_US",
            "source_path_prod": msc_us_dir,
            "source_schema_path": f"{ddl_path}/customers_us.ddl",
        },
        "bronze_catalog_prod": uc_catalog_name,
        "bronze_database_prod": bronze_schema,
        "bronze_table": "customers_us_cdc",
        "bronze_reader_options": {
            "cloudFiles.format": "json",
            "cloudFiles.inferColumnTypes": "true",
            "cloudFiles.rescuedDataColumn": "_rescued_data",
        },
        "bronze_table_properties": {
            "pipelines.autoOptimize.managed": "true",
        },
    },
    {
        "data_flow_id": "msc-101",
        "data_flow_group": "MSC",
        "source_system": "RegionalCDC-EU",
        "source_format": "cloudFiles",
        "source_details": {
            "source_database": "APP",
            "source_table": "CUSTOMERS_EU",
            "source_path_prod": msc_eu_dir,
            "source_schema_path": f"{ddl_path}/customers_eu.ddl",
        },
        "bronze_catalog_prod": uc_catalog_name,
        "bronze_database_prod": bronze_schema,
        "bronze_table": "customers_eu_cdc",
        "bronze_reader_options": {
            "cloudFiles.format": "json",
            "cloudFiles.inferColumnTypes": "true",
            "cloudFiles.rescuedDataColumn": "_rescued_data",
        },
        "bronze_table_properties": {
            "pipelines.autoOptimize.managed": "true",
        },
    },
    {
        "data_flow_id": "msc-102",
        "data_flow_group": "MSC",
        "source_system": "RegionalCDC-APAC",
        "source_format": "cloudFiles",
        "source_details": {
            "source_database": "APP",
            "source_table": "CUSTOMERS_APAC",
            "source_path_prod": msc_apac_dir,
            "source_schema_path": f"{ddl_path}/customers_apac.ddl",
        },
        "bronze_catalog_prod": uc_catalog_name,
        "bronze_database_prod": bronze_schema,
        "bronze_table": "customers_apac_cdc",
        "bronze_reader_options": {
            "cloudFiles.format": "json",
            "cloudFiles.inferColumnTypes": "true",
            "cloudFiles.rescuedDataColumn": "_rescued_data",
        },
        "bronze_table_properties": {
            "pipelines.autoOptimize.managed": "true",
        },
    },
]

# The silver row is the heart of the demo: ONE target table
# (``customers_regional``) consumes ALL three bronze tables via
# ``silver_cdc_apply_changes_flows``. Each flow's ``select_exp``
# rewrites its region's raw column shape into the canonical target
# shape, including a constant ``region`` literal that proves the
# per-flow expression actually ran on silver.
msc_silver_row = {
    "data_flow_id": "msc-200",
    "data_flow_group": "MSC",
    "source_system": "RegionalCDC-Unified",
    "silver_catalog_prod": uc_catalog_name,
    "silver_database_prod": silver_schema,
    "silver_table": "customers_regional",
    "silver_table_properties": {
        "pipelines.reset.allowed": "false",
    },
    "silver_cdc_apply_changes_flows": {
        "keys": ["customer_id"],
        "sequence_by": "operation_date",
        "scd_type": "1",
        "apply_as_deletes": "operation = 'DELETE'",
        "except_column_list": [
            "operation", "operation_date", "_rescued_data",
        ],
        "flows": [
            {
                "name": "customers_us_silver",
                "source_format": "delta",
                "source_details": {
                    "source_catalog": uc_catalog_name,
                    "source_database": bronze_schema,
                    "source_table": "customers_us_cdc",
                },
                "select_exp": [
                    "id AS customer_id",
                    "firstname",
                    "lastname",
                    "email",
                    "address",
                    "'US' AS region",
                    "operation",
                    "operation_date",
                    "_rescued_data",
                ],
            },
            {
                "name": "customers_eu_silver",
                "source_format": "delta",
                "source_details": {
                    "source_catalog": uc_catalog_name,
                    "source_database": bronze_schema,
                    "source_table": "customers_eu_cdc",
                },
                "select_exp": [
                    "customer_id AS customer_id",
                    "given_name AS firstname",
                    "family_name AS lastname",
                    "email_address AS email",
                    "postal_address AS address",
                    "'EU' AS region",
                    "CASE WHEN change_type = 'INSERT' THEN 'APPEND' "
                    "WHEN change_type = 'UPDATE' THEN 'UPDATE' "
                    "WHEN change_type = 'DELETE' THEN 'DELETE' "
                    "END AS operation",
                    "change_ts AS operation_date",
                    "_rescued_data",
                ],
            },
            {
                "name": "customers_apac_silver",
                "source_format": "delta",
                "source_details": {
                    "source_catalog": uc_catalog_name,
                    "source_database": bronze_schema,
                    "source_table": "customers_apac_cdc",
                },
                "select_exp": [
                    "cust_id AS customer_id",
                    "fname AS firstname",
                    "lname AS lastname",
                    "mail AS email",
                    "addr AS address",
                    "'APAC' AS region",
                    "CASE WHEN op = 'I' THEN 'APPEND' "
                    "WHEN op = 'U' THEN 'UPDATE' "
                    "WHEN op = 'D' THEN 'DELETE' "
                    "END AS operation",
                    "op_time AS operation_date",
                    "_rescued_data",
                ],
            },
        ],
    },
}

onboarding_json = _read_onboarding(onboarding_file_path)
onboarding_json.extend(msc_bronze_rows)
onboarding_json.append(msc_silver_row)
_write_onboarding(onboarding_json, onboarding_file_path)

print(
    f"Onboarding updated: {len(onboarding_json)} data flows"
)
print(
    "  Added 3 bronze rows (msc-100/101/102) and 1 silver row "
    "(msc-200) in group MSC"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.3 Re-run Onboarding
# MAGIC
# MAGIC `onboard_dataflow_specs()` regenerates the bronze and silver
# MAGIC `dataflowspec` tables from the updated onboarding file. The new
# MAGIC `MSC` group rows are appended; the existing `A1` / `SNAP` /
# MAGIC `SINK` group rows are preserved unchanged.

# COMMAND ----------

onboarding_params["overwrite"] = "True"

OnboardDataflowspec(
    spark=spark, dict_obj=onboarding_params, uc_enabled=True
).onboard_dataflow_specs()
print(
    "Onboarding updated with multi-source CDC bronze + silver rows!"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.4 Create & Start Multi-Source CDC Pipeline
# MAGIC
# MAGIC The MSC pipeline runs both bronze and silver layers under the
# MAGIC `MSC` data-flow group. It reuses the **same generic runner
# MAGIC notebook** as Stage 3 (no snapshot callback needed) — that is
# MAGIC the whole point of metadata-driven: a new scenario gets
# MAGIC plugged in by writing onboarding rows, not by writing pipeline
# MAGIC code.

# COMMAND ----------

msc_pipeline_name = f"sdp_meta_demo_msc_{uc_schema_name}"
msc_pipeline_id_file = (
    f"{uc_volume_path}/msc_pipeline_id.txt"
)

msc_pipeline_config = {
    "layer": "bronze_silver",
    "bronze.group": "MSC",
    "silver.group": "MSC",
    "bronze.dataflowspecTable": (
        f"{uc_catalog_name}.{uc_schema_name}.bronze_dataflowspec"
    ),
    "silver.dataflowspecTable": (
        f"{uc_catalog_name}.{uc_schema_name}.silver_dataflowspec"
    ),
    "sdp_meta_whl": git_url_for_pip,
}

existing_msc = [
    p for p in w.pipelines.list_pipelines()
    if p.name == msc_pipeline_name
]
if existing_msc:
    msc_pipeline_id = existing_msc[0].pipeline_id
    print(
        f"Reusing existing MSC pipeline: {msc_pipeline_id}"
    )
else:
    created_msc = create_pipeline(
        w,
        name=msc_pipeline_name,
        catalog=uc_catalog_name,
        schema=bronze_schema,
        libraries=[
            PipelineLibrary(
                notebook=NotebookLibrary(
                    path=runner_notebook_path
                )
            )
        ],
        configuration=msc_pipeline_config,
        development=True,
        serverless=True,
    )
    msc_pipeline_id = created_msc.pipeline_id
    print(f"MSC pipeline created: {msc_pipeline_id}")

with open(msc_pipeline_id_file, "w") as fh:
    fh.write(msc_pipeline_id)
print(f"MSC pipeline ID saved to: {msc_pipeline_id_file}")

run_pipeline_and_wait(
    w, msc_pipeline_id, label="multi-source CDC"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.5 Validate Multi-Source CDC Results
# MAGIC
# MAGIC With the seed data:
# MAGIC
# MAGIC | Layer | Table | Expected rows | Why |
# MAGIC |-------|-------|---------------|-----|
# MAGIC | Bronze | `customers_us_cdc`   | 5  | 3 INSERTs + 1 UPDATE + 1 DELETE raw events |
# MAGIC | Bronze | `customers_eu_cdc`   | 5  | same shape |
# MAGIC | Bronze | `customers_apac_cdc` | 5  | same shape |
# MAGIC | Silver | `customers_regional` | 6  | SCD-1 + apply_as_deletes: 3 regions × (3 inserted − 1 deleted) |
# MAGIC
# MAGIC The per-region breakdown (2 US + 2 EU + 2 APAC) proves the per-
# MAGIC flow `select_exp` actually ran — each flow tags its rows with
# MAGIC a constant `region` literal that only that flow produces.

# COMMAND ----------

# DBTITLE 1,Bronze — customers_us_cdc (US raw shape)
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{bronze_schema}"
        ".customers_us_cdc"
    )
)

# COMMAND ----------

# DBTITLE 1,Bronze — customers_eu_cdc (EU raw shape, different column names)
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{bronze_schema}"
        ".customers_eu_cdc"
    )
)

# COMMAND ----------

# DBTITLE 1,Bronze — customers_apac_cdc (APAC raw shape, op codes I/U/D)
display(
    spark.sql(
        f"SELECT * FROM {uc_catalog_name}.{bronze_schema}"
        ".customers_apac_cdc"
    )
)

# COMMAND ----------

# DBTITLE 1,Silver — customers_regional (unified, post-CDC, normalized)
display(
    spark.sql(
        f"SELECT customer_id, firstname, lastname, email, "
        f"address, region "
        f"FROM {uc_catalog_name}.{silver_schema}.customers_regional "
        f"ORDER BY region, customer_id"
    )
)

# COMMAND ----------

# DBTITLE 1,Per-Region Live Counts (proves per-flow select_exp ran)
display(
    spark.sql(
        f"SELECT region, count(*) AS live_rows "
        f"FROM {uc_catalog_name}.{silver_schema}.customers_regional "
        f"GROUP BY region "
        f"ORDER BY region"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Final Data Flow Summary
# MAGIC
# MAGIC Complete view across all features demonstrated.

# COMMAND ----------

# DBTITLE 1,Complete Pipeline Data Flow Summary
all_tables_final = [
    ("customers", "cloudFiles + CDC", bronze_schema,
     silver_schema),
    ("transactions", "cloudFiles + CDC", bronze_schema,
     silver_schema),
    ("products", "cloudFiles + CDC", bronze_schema,
     silver_schema),
    ("stores", "cloudFiles + CDC", bronze_schema,
     silver_schema),
    ("orders", "Append Flow + CDC", bronze_schema,
     silver_schema),
    ("snap_products", "Snapshot SCD2", bronze_schema,
     silver_schema),
    ("snap_stores", "Snapshot SCD1", bronze_schema,
     silver_schema),
    ("iot_events", "CloudFiles + Sink", bronze_schema,
     None),
    ("customers_us_cdc", "Multi-source CDC (bronze, US)",
     bronze_schema, None),
    ("customers_eu_cdc", "Multi-source CDC (bronze, EU)",
     bronze_schema, None),
    ("customers_apac_cdc", "Multi-source CDC (bronze, APAC)",
     bronze_schema, None),
    ("customers_regional",
     "Multi-source AUTO CDC (silver, unified)",
     None, silver_schema),
]

summary_rows = []
for table, feature, b_schema, s_schema in all_tables_final:
    bronze_fqn = (
        f"{uc_catalog_name}.{b_schema}.{table}"
    )
    try:
        bronze_count = spark.sql(
            f"SELECT count(*) FROM {bronze_fqn}"
        ).first()[0]
    except Exception:
        bronze_count = 0

    quarantine_fqn = (
        f"{uc_catalog_name}.{b_schema}"
        f".{table}_quarantine"
    )
    try:
        quarantine_count = spark.sql(
            f"SELECT count(*) FROM {quarantine_fqn}"
        ).first()[0]
    except Exception:
        quarantine_count = 0

    silver_count = 0
    if s_schema:
        silver_fqn = (
            f"{uc_catalog_name}.{s_schema}.{table}"
        )
        try:
            silver_count = spark.sql(
                f"SELECT count(*) FROM {silver_fqn}"
            ).first()[0]
        except Exception:
            silver_count = 0

    summary_rows.append(Row(
        Table=table,
        Feature=feature,
        Bronze_Rows=bronze_count,
        Quarantine_Rows=quarantine_count,
        Silver_Rows=silver_count,
    ))

display(spark.createDataFrame(summary_rows))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 12: Row-Level Filtering (UC Row Filter)
# MAGIC
# MAGIC The `customers` flow in the onboarding spec has both
# MAGIC `bronze_row_filter` and `silver_row_filter` set to:
# MAGIC
# MAGIC ```
# MAGIC ROW FILTER ${uc_catalog_name}.${uc_schema_name}.customer_id_filter ON (customer_id)
# MAGIC ```
# MAGIC
# MAGIC The UDF — created in **Stage 1.2** before the pipeline ran — is:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE FUNCTION customer_id_filter(cid INT) RETURNS BOOLEAN
# MAGIC RETURN is_account_group_member('admins') OR cid IS NULL OR cid <= 100
# MAGIC ```
# MAGIC
# MAGIC So a non-admin reader sees only `customer_id <= 100` (≈ 100 of
# MAGIC the 200 generated rows); an admin sees all rows. Note: `bronze_row_filter`
# MAGIC / `silver_row_filter` are UC-only — `_get_row_filter()` in
# MAGIC `dataflow_pipeline.py` returns `None` when the pipeline is not
# MAGIC UC-enabled, so legacy hive metastore runs are unaffected.

# COMMAND ----------

# DBTITLE 1,Verify the row filter is enforced
print("Row filter UDF:")
display(spark.sql(f"""
    DESCRIBE FUNCTION EXTENDED
    {uc_catalog_name}.{uc_schema_name}.customer_id_filter
"""))

print("\nFiltered customers — bronze (rows visible to current user):")
bronze_filtered = spark.sql(f"""
    SELECT
        SUM(CASE WHEN customer_id <= 100 THEN 1 ELSE 0 END) AS within_filter,
        SUM(CASE WHEN customer_id  > 100 THEN 1 ELSE 0 END) AS outside_filter,
        COUNT(*)                                            AS total
    FROM {uc_catalog_name}.{bronze_schema}.customers
""").first()
print(
    f"  bronze.customers : within_filter={bronze_filtered.within_filter} "
    f"outside_filter={bronze_filtered.outside_filter} "
    f"total={bronze_filtered.total}"
)

print("\nFiltered customers — silver (rows visible to current user):")
silver_filtered = spark.sql(f"""
    SELECT
        SUM(CASE WHEN customer_id <= 100 THEN 1 ELSE 0 END) AS within_filter,
        SUM(CASE WHEN customer_id  > 100 THEN 1 ELSE 0 END) AS outside_filter,
        COUNT(*)                                            AS total
    FROM {uc_catalog_name}.{silver_schema}.customers
""").first()
print(
    f"  silver.customers : within_filter={silver_filtered.within_filter} "
    f"outside_filter={silver_filtered.outside_filter} "
    f"total={silver_filtered.total}"
)

# Sanity check (only meaningful for non-admins; admins legitimately see
# the unfiltered set, so we don't fail the demo for them).
running_as_admin = spark.sql(
    "SELECT is_account_group_member('admins') AS is_admin"
).first().is_admin
if not running_as_admin:
    assert bronze_filtered.outside_filter == 0, (
        f"row filter not enforced on bronze.customers — saw "
        f"{bronze_filtered.outside_filter} rows with customer_id > 100"
    )
    assert silver_filtered.outside_filter == 0, (
        f"row filter not enforced on silver.customers — saw "
        f"{silver_filtered.outside_filter} rows with customer_id > 100"
    )
    print("\nRow filter enforced on bronze + silver customers tables.")
else:
    print(
        "\nRunning as admin — UDF returned TRUE for every row; "
        "filter wiring confirmed but cannot assert restriction."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary
# MAGIC
# MAGIC | Feature | How It Was Used |
# MAGIC |---------|----------------|
# MAGIC | **Metadata-driven onboarding** | JSON config → DataflowSpec → generic pipeline |
# MAGIC | **CloudFiles (Autoloader)** | CSV files ingested with schema enforcement |
# MAGIC | **Data quality** | `expect_or_drop` and `expect_or_quarantine` |
# MAGIC | **Quarantine tables** | Bad records routed to separate tables |
# MAGIC | **CDC (SCD Type 2)** | `apply_changes` with keys, sequence_by |
# MAGIC | **Liquid clustering** | `cluster_by_auto` for automatic optimization |
# MAGIC | **Silver transformations** | Column selection and expressions via JSON |
# MAGIC | **Adding new feeds** | Products & Stores added — no pipeline changes |
# MAGIC | **Incremental processing** | CDC data (I/U/D) processed automatically |
# MAGIC | **Append Flow** | Multi-source → same target via `dp.append_flow` |
# MAGIC | **File metadata** | `_metadata.file_name`, `_metadata.file_path` |
# MAGIC | **Apply Changes From Snapshot** | Snapshot-based SCD Type 1 & 2 |
# MAGIC | **Pipeline Sink** | Write to external delta table via `dp.create_sink` |
# MAGIC | **Multi-Source AUTO CDC** | N `dp.create_auto_cdc_flow` calls → one unified silver streaming table ([#294](https://github.com/databrickslabs/sdp-meta/issues/294)) |
# MAGIC | **Row-level filtering** | `bronze_row_filter` / `silver_row_filter` → UC `ROW FILTER` ([#303](https://github.com/databrickslabs/sdp-meta/issues/303)) |
# MAGIC
# MAGIC ### Learn More
# MAGIC - [Full Documentation](https://databrickslabs.github.io/sdp-meta/)
# MAGIC - [Getting Started](https://databrickslabs.github.io/sdp-meta/getting_started/)
# MAGIC - [Onboarding Template](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/json/onboarding.template)
# MAGIC - [Source Code](https://github.com/databrickslabs/sdp-meta/tree/main/src/databricks/labs/sdp_meta)
# MAGIC - [Append Flows](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/json/cloudfiles-onboarding.template)
# MAGIC - [Apply Changes from Snapshot](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/json/snapshot-onboarding.template)
# MAGIC - [DLT Sink](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/json/kafka-sink-onboarding.template)
# MAGIC - [Multi-Source AUTO CDC](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/json/multi-source-cdc-onboarding.template) · [Design Doc](https://github.com/databrickslabs/sdp-meta/blob/main/DESIGN_MULTI_SOURCE_AUTO_CDC.md)
# MAGIC - [DABs](https://github.com/databrickslabs/sdp-meta/tree/main/demo/dabs)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Final Validation (Smoke Test Mode)
# MAGIC
# MAGIC When the **`Validate Counts`** widget is set to `true`, this cell
# MAGIC turns the demo into a smoke test: it asserts that every table
# MAGIC the demo is supposed to produce exists, is non-empty, and (for
# MAGIC tables fed by hardcoded data) hits an exact expected row count.
# MAGIC
# MAGIC The numeric expectations come from data that is hardcoded in
# MAGIC this notebook (`orders_main_data` + `orders_af_data`,
# MAGIC `iot_events`, the snapshot CSV literals, and the bad-records
# MAGIC strings) and so are deterministic regardless of the
# MAGIC `data_source` widget. The customers / transactions / products /
# MAGIC stores tables only get an existence + non-empty check because
# MAGIC their counts depend on whether `data_source = github` (CSVs from
# MAGIC the repo) or `dbdatagen` (random synthetic data).
# MAGIC
# MAGIC All failures are collected and reported in a single
# MAGIC `AssertionError` — easier to fix the demo / pipelines once than
# MAGIC to chase one failure at a time.

# COMMAND ----------

if not validate_counts:
    print(
        "Skipping final validation: 'validate_counts' widget is "
        "'false'. Set it to 'true' to enable the smoke-test mode."
    )
else:
    # ``failures`` is a list of human-readable strings; we collect
    # every failed expectation and raise once at the end so a CI
    # operator sees the full picture from one run instead of fixing
    # one regression, re-running, and finding the next.
    failures = []

    def _table_count(fqn):
        """Return row count for ``fqn``, or ``None`` if missing/unreadable.

        Returning ``None`` (rather than raising) lets the caller
        distinguish "table doesn't exist" from "table exists but is
        empty" — both are failures, but they're reported with
        different messages so the operator knows whether the bug is
        in pipeline wiring or in data routing.
        """
        try:
            return spark.sql(
                f"SELECT count(*) AS c FROM {fqn}"
            ).first().c
        except Exception:
            return None

    def _expect_exact(fqn, want):
        got = _table_count(fqn)
        if got is None:
            failures.append(f"{fqn}: missing (table not found)")
        elif got != want:
            failures.append(
                f"{fqn}: expected exactly {want} rows, got {got}"
            )

    def _expect_at_least(fqn, want):
        got = _table_count(fqn)
        if got is None:
            failures.append(f"{fqn}: missing (table not found)")
        elif got < want:
            failures.append(
                f"{fqn}: expected >= {want} rows, got {got}"
            )

    def _expect_nonempty(fqn):
        _expect_at_least(fqn, 1)

    # 1. Append flow — bronze ``orders`` is fed by two hardcoded JSON
    # literals (``orders_main_data`` = 4 rows, ``orders_af_data`` = 3
    # rows). Append flow is non-merging, so the count is exactly 7
    # regardless of widget choices. Drift here means
    # ``dp.append_flow`` wiring broke.
    _expect_exact(
        f"{uc_catalog_name}.{bronze_schema}.orders", 7
    )

    # 2. Sink — bronze ``iot_events`` is fed by a hardcoded 5-row CSV
    # literal. Drift here means the bronze table that backs the sink
    # is broken; the external sink target is validated separately by
    # the ``DLT Sink`` cells above.
    _expect_exact(
        f"{uc_catalog_name}.{bronze_schema}.iot_events", 5
    )

    # 3. Snapshot tables — fed by hardcoded CSV literals. The demo
    # runs the snapshot pipeline TWICE: once with LOAD_1, once after
    # LOAD_2 is copied over LOAD_1 (see "Snapshot V2" cells above).
    # Bounds below track the *final* state after the V2 run, not the
    # initial load:
    #   - ``snap_products`` is SCD Type 2 → history retained, so the
    #     final row count is ``>= LOAD_1 count`` (5). LOAD_2 keeps
    #     the same 5 keys but renames them; SCD2 inserts new rows
    #     for the changed values, so the actual count is closer to
    #     ~7-9 — we assert the lower bound (5).
    #   - ``snap_stores`` is SCD Type 1 → the demo deliberately drops
    #     stores 3 & 4 in LOAD_2 to show SCD1's delete-on-missing
    #     semantics, leaving exactly 2 rows. We assert ``>= 2`` (the
    #     LOAD_2 count) rather than exact 2 so future tweaks to the
    #     LOAD_2 fixture don't have to thread through here.
    _expect_at_least(
        f"{uc_catalog_name}.{bronze_schema}.snap_products", 5
    )
    _expect_at_least(
        f"{uc_catalog_name}.{bronze_schema}.snap_stores", 2
    )
    _expect_at_least(
        f"{uc_catalog_name}.{silver_schema}.snap_products", 5
    )
    _expect_at_least(
        f"{uc_catalog_name}.{silver_schema}.snap_stores", 2
    )

    # 4. Quarantine — for each domain we wrote 2 hardcoded bad rows
    # (lines ~963-983). Some may be DQE-dropped vs quarantined
    # depending on the DQE rules in ``demo/conf/json/dqe/``, so we
    # only assert ``>= 1``. An empty quarantine table after we
    # explicitly seeded bad data means quarantine routing is broken.
    for domain in ("customers", "transactions", "products", "stores"):
        _expect_nonempty(
            f"{uc_catalog_name}.{bronze_schema}"
            f".{domain}_quarantine"
        )

    # 5. Customers / transactions / products / stores — count varies
    # with ``data_source``: ``github`` uses fixed CSVs from the
    # repo, ``dbdatagen`` uses random synthetic data with no fixed
    # seed. Existence + non-empty is the strongest universal check;
    # tightening to exact counts would require either pinning the
    # dbdatagen seed across the codebase or splitting this branch by
    # ``data_source``, neither of which is worth the complexity.
    for domain in ("customers", "transactions", "products", "stores"):
        _expect_nonempty(
            f"{uc_catalog_name}.{bronze_schema}.{domain}"
        )
        _expect_nonempty(
            f"{uc_catalog_name}.{silver_schema}.{domain}"
        )

    # 6. Multi-source AUTO CDC (Stage 11) — every region seeds the
    # SAME shape: 3 INSERTs + 1 UPDATE + 1 DELETE = 5 raw bronze
    # rows. The silver target is SCD-1 with apply_as_deletes, so the
    # final live row count = (3 regions × 3 inserted) − (3 regions ×
    # 1 deleted) = 6. All counts come from hardcoded literals
    # (``msc_us_data`` / ``msc_eu_data`` / ``msc_apac_data``) and so
    # are deterministic regardless of the ``data_source`` widget.
    # Drift here means either the multi-source bronze fan-in or the
    # silver ``create_auto_cdc_flow`` fan-in is broken.
    for region in ("us", "eu", "apac"):
        _expect_exact(
            f"{uc_catalog_name}.{bronze_schema}"
            f".customers_{region}_cdc",
            5,
        )
    _expect_exact(
        f"{uc_catalog_name}.{silver_schema}.customers_regional",
        6,
    )

    if failures:
        raise AssertionError(
            "Demo final validation failed "
            f"({len(failures)} issue(s)):\n  - "
            + "\n  - ".join(failures)
        )
    print("Demo final validation passed: all expected tables present.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup (Optional)
# MAGIC
# MAGIC Drops every per-run resource the demo created -- pipelines,
# MAGIC runner notebooks, and per-run schemas (bronze, silver, pipeline
# MAGIC target, config). Controlled by the `cleanup` widget; safe to
# MAGIC re-run interactively (each step is wrapped in try/except).
# MAGIC
# MAGIC Intentionally **does NOT drop the UC catalog** -- the catalog
# MAGIC is user-supplied (via the `uc_catalog_name` widget), is shared
# MAGIC across runs, and dropping it would clobber other concurrent
# MAGIC demos. If you want to drop the catalog too, do it manually:
# MAGIC `DROP CATALOG <name> CASCADE`.

# COMMAND ----------

def _cleanup_demo_resources():
    """Drop every per-run resource the demo created.

    Resolution order matches the demo's creation order, reversed:

    1. Pipelines -- read pipeline IDs from the per-run pid files we
       wrote into ``uc_volume_path`` during pipeline creation. Falls
       back to a name-based lookup (``sdp_meta_demo_*_<schema>``) when
       the pid file is missing (e.g. cleanup re-run after the volume
       was already dropped).
    2. Runner notebooks -- ``runner_notebook_path`` and
       ``snapshot_runner_path``. The sink pipeline reuses
       ``runner_notebook_path``, so it's covered by the same delete.
    3. Per-run schemas -- ``bronze_schema``, ``silver_schema``,
       ``pipeline_target_schema``, ``uc_schema_name``. Each ``DROP
       SCHEMA ... CASCADE`` removes every table, view, and (for
       ``uc_schema_name``) the per-run config volume.

    Each step is independent and errors are swallowed with a print so
    a partial demo failure (e.g. snapshot pipeline never ran) doesn't
    block cleanup of everything else.
    """
    print("=" * 78)
    print(
        f"Cleanup: dropping per-run resources for "
        f"{uc_catalog_name}.{uc_schema_name}"
    )
    print("=" * 78)

    pipeline_specs = [
        ("main", pipeline_id_file, pipeline_name),
        (
            "snapshot",
            snapshot_pipeline_id_file,
            f"sdp_meta_demo_snapshot_{uc_schema_name}",
        ),
        ("sink", sink_pipeline_id_file, sink_pipeline_name),
        (
            "multi-source CDC",
            msc_pipeline_id_file,
            msc_pipeline_name,
        ),
    ]
    for label, pid_file, name in pipeline_specs:
        pid = None
        try:
            with open(pid_file, "r") as fh:
                pid = fh.read().strip()
        except Exception:
            for p in w.pipelines.list_pipelines():
                if p.name == name:
                    pid = p.pipeline_id
                    break
        if not pid:
            print(f"  Skipping {label} pipeline: not found ({name})")
            continue
        try:
            w.pipelines.delete(pipeline_id=pid)
            print(f"  Deleted {label} pipeline: {pid} ({name})")
        except Exception as exc:
            print(f"  Could not delete {label} pipeline {pid}: {exc}")

    for nb_path in [runner_notebook_path, snapshot_runner_path]:
        try:
            w.workspace.delete(nb_path)
            print(f"  Deleted runner notebook: {nb_path}")
        except Exception as exc:
            print(f"  Could not delete notebook {nb_path}: {exc}")

    for schema in [
        bronze_schema,
        silver_schema,
        pipeline_target_schema,
        uc_schema_name,
    ]:
        fqn = f"{uc_catalog_name}.{schema}"
        try:
            spark.sql(f"DROP SCHEMA IF EXISTS {fqn} CASCADE")
            print(f"  Dropped schema: {fqn}")
        except Exception as exc:
            print(f"  Could not drop schema {fqn}: {exc}")

    print(
        f"Cleanup complete for run schema={uc_schema_name} "
        f"(catalog {uc_catalog_name} preserved)."
    )


if cleanup:
    _cleanup_demo_resources()
else:
    print(
        "Cleanup skipped (cleanup widget = false). "
        "Re-run with cleanup=true to drop per-run resources, "
        "or call _cleanup_demo_resources() manually."
    )
