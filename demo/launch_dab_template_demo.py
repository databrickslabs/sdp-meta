"""End-to-end demo for the new sdp-meta DAB template features.

For each scenario (`cloudfiles`, `kafka`, `eventhub`, or `all`) this script
exercises every feature added in the DAB-template work:

  STAGE 1 - bundle-init        scaffold a bundle from the packaged template
  STAGE 2 - prepare-wheel      (--apply-prepare-wheel) build + upload sdp-meta
                               to a UC volume; otherwise pin a fake placeholder
                               to demonstrate the sentinel guard rails
  STAGE 3 - bundle-add-flow    bulk-append flows from
                               demo/dab_template_demo/flows/<scenario>_extra.csv
  STAGE 4 - recipe             run the rendered recipe that fits the source:
                                 cloudfiles -> recipes/from_volume.py (dry-run)
                                 kafka      -> recipes/from_topics.py
                                 eventhub   -> recipes/from_topics.py
                                 (the last two consume topic lists from
                                 demo/dab_template_demo/topics/)
  STAGE 5 - bundle-validate    run sdp-meta sanity checks + (when CLI present)
                               `databricks bundle validate`
  STAGE 6 - deploy + run       (--apply-deploy) actually deploy the bundle and
                               run onboarding + pipelines against the workspace

The first 5 stages need NO workspace access (great for dev loops). Only the
last stage talks to Databricks. Use ``--apply-prepare-wheel`` on its own to
upload a real wheel without deploying.

``--uc-schema`` is dual-purpose: it sets the schema for the wheel/demo-data
UC volume AND drives the rendered bundle's target schemas
(sdp_meta_schema = <uc-schema>, bronze = <uc-schema>_bronze,
silver = <uc-schema>_silver). When omitted, each scenario falls back to
its registered default (eg. ``sdp_meta_dab_demo_cf``) so the demo still
runs without any schema flag.

Usage:
    # Local exploration only - no workspace access needed.
    python demo/launch_dab_template_demo.py --scenario all \\
        --uc-catalog-name main --out-dir demo_runs

    # Build + upload wheel, validate (still no deploy).
    python demo/launch_dab_template_demo.py --scenario cloudfiles \\
        --uc-catalog-name main --apply-prepare-wheel \\
        --uc-schema sdp_meta_dab_demo_cf --uc-volume sdp_meta_wheels \\
        --profile DEFAULT

    # Full end-to-end: scaffold, append, recipe, prepare wheel, validate, deploy, run.
    python demo/launch_dab_template_demo.py --scenario kafka \\
        --uc-catalog-name main --apply-prepare-wheel --apply-deploy \\
        --uc-schema sdp_meta_dab_demo_kafka --uc-volume sdp_meta_wheels \\
        --profile DEFAULT

    # Force a fresh deployment each run (instead of overwriting the existing
    # dev deployment). --unique-bundle-name appends a UTC timestamp to
    # bundle.name; --bundle-name-suffix is a human-readable tag.
    python demo/launch_dab_template_demo.py --scenario cloudfiles_combined \\
        --uc-catalog-name main --apply-deploy --unique-bundle-name \\
        --profile DEFAULT
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
DEMO_DIR = REPO_ROOT / "demo" / "dab_template_demo"

# Allow importing the in-tree sdp-meta package directly (avoids requiring an
# editable install just to run the demo).
SRC = REPO_ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

# Importing from src after path patching, so flake8 E402 is expected/intended.
from databricks.labs.sdp_meta.bundle import (  # noqa: E402
    BundleAddFlowCommand,
    BundleAddPipelineCommand,
    BundleInitCommand,
    BundlePrepareWheelCommand,
    BundleValidateCommand,
    FlowSpec,
    PipelineSpec,
    _flows_from_csv,
    bundle_add_flow,
    bundle_add_pipeline,
    bundle_init,
    bundle_prepare_wheel,
    bundle_validate,
)
from databricks.labs.sdp_meta.identifiers import validate_uc_identifier  # noqa: E402


# ---------------------------------------------------------------------------
# Scenario registry
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Scenario:
    name: str
    answers_file: Path
    # Optional: if None, STAGE 3 (`bundle-add-flow --from-csv`) is skipped.
    # Used by the `delta` scenario where STAGE 4's `from_uc.py` recipe
    # already mirrors the same source tables; adding them again via CSV
    # would produce duplicate onboarding rows + a `Cannot redefine
    # dataset ..._inputview` error at pipeline runtime.
    extra_flows_csv: Optional[Path]
    recipe_name: str
    recipe_args_template: List[str]
    # ``recipe_args_template`` supports the following placeholders, expanded
    # by ``_run_recipe`` at invocation time:
    #   {bundle_dir}         -> absolute path of the scaffolded bundle
    #   {uc_catalog_name}    -> --uc-catalog-name
    #   {uc_source_catalog}  -> --uc-source-catalog (defaults to {uc_catalog_name})
    #   {uc_source_schema}   -> --uc-source-schema  (defaults to "staging")
    description: str
    # Per-scenario default UC schema names used when --uc-schema is NOT
    # passed on the CLI. When --uc-schema IS passed, all three are derived
    # from it via `_resolve_demo_schemas`. Keeping defaults out of the
    # answer JSONs lets the launcher render the same template either with
    # canned scenario-specific names (no flag) OR with user-provided ones
    # (flag), without forking the answer files.
    default_sdp_meta_schema: str = ""
    default_bronze_target_schema: str = ""
    default_silver_target_schema: str = ""
    # If True, the recipe is only attempted when --apply-recipe is set, because
    # it needs a real WorkspaceClient (no offline dry-run path). Today this
    # only applies to the `delta` scenario (recipes/from_uc.py lists UC tables).
    recipe_requires_workspace: bool = False


SCENARIOS = {
    "cloudfiles": Scenario(
        name="cloudfiles",
        answers_file=DEMO_DIR / "answers" / "cloudfiles_split.json",
        extra_flows_csv=DEMO_DIR / "flows" / "cloudfiles_extra.csv",
        recipe_name="from_volume.py",
        # Run from a fake local directory tree so the demo works without UC.
        recipe_args_template=[
            "--volume-path", "{bundle_dir}/_demo_landing",
            "--bundle-dir", "{bundle_dir}",
        ],
        description=(
            "Split bronze+silver, cloudFiles autoloader, YAML onboarding."
            " Demonstrates pipeline_mode=split and the from_volume recipe."
        ),
        default_sdp_meta_schema="sdp_meta_dab_demo_cf",
        default_bronze_target_schema="sdp_meta_bronze_dab_demo_cf",
        default_silver_target_schema="sdp_meta_silver_dab_demo_cf",
    ),
    "cloudfiles_combined": Scenario(
        name="cloudfiles_combined",
        # Same demo data + recipe as `cloudfiles`, but the bundle is rendered
        # with `pipeline_mode=combined` so bronze + silver run inside ONE
        # Lakeflow Spark Declarative Pipeline. Use this when you want to demo the
        # combined topology against real cloud-storage data (cloudfiles) and
        # not against placeholder Event Hubs / Kafka brokers.
        answers_file=DEMO_DIR / "answers" / "cloudfiles_combined.json",
        extra_flows_csv=DEMO_DIR / "flows" / "cloudfiles_extra.csv",
        recipe_name="from_volume.py",
        recipe_args_template=[
            "--volume-path", "{bundle_dir}/_demo_landing",
            "--bundle-dir", "{bundle_dir}",
        ],
        description=(
            "Combined bronze_silver in ONE Lakeflow Spark Declarative Pipeline,"
            " cloudFiles autoloader, YAML onboarding. Same demo data as"
            " `cloudfiles`; only `pipeline_mode=combined` differs."
        ),
        default_sdp_meta_schema="sdp_meta_dab_demo_cf",
        default_bronze_target_schema="sdp_meta_bronze_dab_demo_cf",
        default_silver_target_schema="sdp_meta_silver_dab_demo_cf",
    ),
    "multi_pipeline_cloudfiles": Scenario(
        name="multi_pipeline_cloudfiles",
        answers_file=DEMO_DIR / "answers" / "cloudfiles_multi_pipeline.json",
        extra_flows_csv=DEMO_DIR / "flows" / "cloudfiles_extra.csv",
        recipe_name="from_volume.py",
        recipe_args_template=[
            "--volume-path", "{bundle_dir}/_demo_landing",
            "--bundle-dir", "{bundle_dir}",
        ],
        description=(
            "FEVM integration scenario with an initial split bronze/silver "
            "CloudFiles topology and an independently configured combined "
            "bronze/silver topology with its own group and target schemas."
        ),
        default_sdp_meta_schema="sdp_meta_dab_demo_multi",
        default_bronze_target_schema="sdp_meta_bronze_dab_demo_multi",
        default_silver_target_schema="sdp_meta_silver_dab_demo_multi",
    ),
    "kafka": Scenario(
        name="kafka",
        answers_file=DEMO_DIR / "answers" / "kafka_bronze.json",
        extra_flows_csv=DEMO_DIR / "flows" / "kafka_extra.csv",
        recipe_name="from_topics.py",
        recipe_args_template=[
            "--source-format", "kafka",
            "--bootstrap-servers", "broker1.example.com:9092,broker2.example.com:9092",
            "--topics-file", str(DEMO_DIR / "topics" / "kafka_topics.txt"),
            "--bundle-dir", "{bundle_dir}",
        ],
        description=(
            "Bronze-only, Kafka, JSON onboarding. Demonstrates layer=bronze,"
            " bundle-add-flow CSV mode, and the from_topics recipe."
        ),
        default_sdp_meta_schema="sdp_meta_dab_demo_kafka",
        default_bronze_target_schema="sdp_meta_bronze_dab_demo_kafka",
        default_silver_target_schema="sdp_meta_silver_dab_demo_kafka",
    ),
    "eventhub": Scenario(
        name="eventhub",
        answers_file=DEMO_DIR / "answers" / "eventhub_combined.json",
        extra_flows_csv=DEMO_DIR / "flows" / "eventhub_extra.csv",
        recipe_name="from_topics.py",
        recipe_args_template=[
            "--source-format", "eventhub",
            "--bootstrap-servers", "my-eh-namespace.servicebus.windows.net:9093",
            "--topics-file", str(DEMO_DIR / "topics" / "eventhub_topics.txt"),
            "--bundle-dir", "{bundle_dir}",
        ],
        description=(
            "Combined bronze_silver in ONE Lakeflow Spark Declarative Pipeline,"
            " Event Hubs source. Demonstrates pipeline_mode=combined and the"
            " same from_topics recipe used for Event Hubs."
        ),
        default_sdp_meta_schema="sdp_meta_dab_demo_eh",
        default_bronze_target_schema="sdp_meta_bronze_dab_demo_eh",
        default_silver_target_schema="sdp_meta_silver_dab_demo_eh",
    ),
    "delta": Scenario(
        name="delta",
        answers_file=DEMO_DIR / "answers" / "delta_split.json",
        # No extra_flows_csv: STAGE 4's `from_uc.py` already mirrors every
        # table under the source schema. Registering them again via CSV
        # would create duplicate onboarding rows -> duplicate dataflow
        # specs -> `Cannot redefine dataset ..._inputview` at pipeline run.
        extra_flows_csv=None,
        recipe_name="from_uc.py",
        # from_uc.py mirrors every Delta table under <catalog>.<schema> into
        # the bundle's onboarding file. The source catalog/schema default to
        # {uc_catalog_name}/staging respectively; override with
        # --uc-source-catalog / --uc-source-schema if your upstream tables
        # live elsewhere. Only runs with --apply-recipe (needs a workspace).
        recipe_args_template=[
            "--source-catalog", "{uc_source_catalog}",
            "--source-schema", "{uc_source_schema}",
            "--bundle-dir", "{bundle_dir}",
        ],
        description=(
            "Split bronze+silver, source_format=delta (upstream Delta tables)."
            " Demonstrates the from_uc recipe that mirrors every table under"
            " a UC schema. STAGE 4 needs --apply-recipe + --profile (workspace)."
        ),
        recipe_requires_workspace=True,
        default_sdp_meta_schema="sdp_meta_dab_demo_delta",
        default_bronze_target_schema="sdp_meta_bronze_dab_demo_delta",
        default_silver_target_schema="sdp_meta_silver_dab_demo_delta",
    ),
}

# Scenarios that share the cloudFiles demo data set + `from_volume.py`
# recipe. The launcher's data-seeding (UC volume upload + local
# `_demo_landing` tree for the recipe) and CSV `{demo_data_volume_path}`
# substitution all gate on this set so adding a new cloudFiles topology
# (eg. `cloudfiles_combined`) only requires registering the scenario above.
_CLOUDFILES_SCENARIO_NAMES = {
    "cloudfiles",
    "cloudfiles_combined",
    "multi_pipeline_cloudfiles",
}

# Scenarios whose recipe (`from_uc.py`) requires upstream Delta tables in a
# UC schema. The launcher auto-seeds those tables before STAGE 4 when
# --apply-recipe is set (see `_seed_demo_delta_source`).
_DELTA_SCENARIO_NAMES = {"delta"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _banner(stage: str, message: str) -> None:
    bar = "=" * 78
    print(f"\n{bar}\n{stage}: {message}\n{bar}", flush=True)


def _resolve_demo_schemas(scenario: Scenario, uc_schema: Optional[str]) -> Dict[str, str]:
    """Resolve the three demo target schemas for a scenario.

    When ``--uc-schema`` is passed on the CLI, ALL three are derived from
    it (sdp_meta_schema = <uc-schema>, bronze = <uc-schema>_bronze, silver
    = <uc-schema>_silver). When it is NOT passed, the per-scenario
    defaults registered on the Scenario dataclass are used (preserving the
    previous hardcoded behaviour). This lets a single ``--uc-schema`` flag
    drive both the wheel-volume schema (used by `bundle_prepare_wheel`)
    AND the dataflowspec / bronze / silver target schemas (used by the
    rendered bundle).

    Note: when ``--scenario all`` is used together with ``--uc-schema``,
    every scenario will share the same target schemas and bundle deploys
    will collide on dataflowspec / bronze / silver tables. Either pick a
    single scenario, or omit ``--uc-schema`` to fall back to the
    per-scenario defaults.
    """
    if uc_schema:
        return {
            "sdp_meta_schema": uc_schema,
            "bronze_target_schema": f"{uc_schema}_bronze",
            "silver_target_schema": f"{uc_schema}_silver",
        }
    return {
        "sdp_meta_schema": scenario.default_sdp_meta_schema,
        "bronze_target_schema": scenario.default_bronze_target_schema,
        "silver_target_schema": scenario.default_silver_target_schema,
    }


def _render_answers_dict(scenario: Scenario, uc_catalog_name: str,
                         uc_schema: Optional[str]) -> Dict:
    """Read the scenario's answers JSON and substitute all CLI placeholders.

    Returns the parsed dict (with ``_comment`` left in place). This is the
    single source of truth for placeholder substitution; both
    ``_materialize_answers_file`` (which writes the rendered JSON for
    `databricks bundle init`) and ``_bundle_dir_from_scaffold`` /
    ``stage_bundle_init`` (which need to know what ``bundle_name`` will
    be in advance) call through here so the three callsites can never
    drift out of sync on the placeholder set.
    """
    if not scenario.answers_file.is_file():
        raise SystemExit(f"Answer file not found: {scenario.answers_file}")
    schemas = _resolve_demo_schemas(scenario, uc_schema)
    rendered_text = (
        scenario.answers_file.read_text()
        .replace("{uc_catalog_name}", uc_catalog_name)
        .replace("{sdp_meta_schema}", schemas["sdp_meta_schema"])
        .replace("{bronze_target_schema}", schemas["bronze_target_schema"])
        .replace("{silver_target_schema}", schemas["silver_target_schema"])
    )
    return json.loads(rendered_text)


_BUNDLE_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")


def _resolve_bundle_name(default_name: str, *, suffix: Optional[str] = None,
                         unique: bool = False) -> str:
    """Apply optional CLI overrides to the scenario's default bundle name.

    DAB dev mode is intentionally idempotent per ``bundle.name`` × user --
    re-deploying with the same name OVERWRITES the previous deployment in
    the workspace (so iterating doesn't litter the workspace with stale
    jobs/pipelines). To force a NEW deployment, change ``bundle.name``:

      - ``suffix``: appended as ``<default>_<suffix>`` (eg. ``v2`` -> ``..._v2``).
      - ``unique``: append a UTC timestamp ``_YYYYMMDD_HHMMSS`` to guarantee
        a fresh name on every run.

    Both can be combined: ``<default>_<suffix>_<timestamp>``.

    The resulting name is validated against DAB's bundle-name grammar
    (lowercase ASCII letter followed by letters/digits/underscores) so we
    fail fast with a helpful message instead of letting `databricks bundle
    init` reject it later.
    """
    name = default_name
    if suffix:
        name = f"{name}_{suffix}"
    if unique:
        ts = _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%d_%H%M%S")
        name = f"{name}_{ts}"
    if not _BUNDLE_NAME_PATTERN.match(name):
        raise SystemExit(
            f"Resolved bundle name {name!r} is not a valid DAB bundle name. "
            "It must match ^[a-z][a-z0-9_]*$ (lowercase letter, then "
            "letters/digits/underscores). Adjust --bundle-name-suffix to "
            "use only [a-z0-9_]."
        )
    return name


def _materialize_answers_file(scenario: Scenario, uc_catalog_name: str, dest_dir: Path,
                              *, uc_schema: Optional[str] = None,
                              bundle_name_override: Optional[str] = None) -> Path:
    """Render the answers JSON for `databricks bundle init` and write it.

    See ``_render_answers_dict`` for the placeholder set. The rendered
    ``bundle_name`` is replaced when ``bundle_name_override`` is set.
    Callers MUST resolve the final name once (eg. in ``stage_bundle_init``)
    and pass the literal here so the bundle dir computed by
    ``_bundle_dir_from_scaffold`` matches the value DAB actually receives
    — otherwise a `--unique-bundle-name` timestamp would drift between
    the two callsites.
    """
    rendered = _render_answers_dict(scenario, uc_catalog_name, uc_schema)
    rendered.pop("_comment", None)
    if bundle_name_override:
        rendered["bundle_name"] = bundle_name_override
    out = dest_dir / f"answers_{scenario.name}.json"
    out.write_text(json.dumps(rendered, indent=2))
    return out


def _materialize_csv(scenario: Scenario, uc_catalog_name: str, dest_dir: Path,
                     *, demo_data_volume_path: Optional[str] = None,
                     uc_source_catalog: Optional[str] = None,
                     uc_source_schema: Optional[str] = None) -> Path:
    """Substitute placeholders in the flow CSV.

    Recognized placeholders:
      - ``{uc_catalog_name}`` -> ``--uc-catalog-name``.
      - ``{uc_source_catalog}`` -> ``--uc-source-catalog`` (defaults to
        ``--uc-catalog-name``). Used by the delta scenario to point flows
        at the upstream Delta tables' catalog.
      - ``{uc_source_schema}`` -> ``--uc-source-schema`` (defaults to
        ``staging``). Used by the delta scenario for the upstream schema
        AND for the per-row ``source_system`` tag so the resulting flows
        are self-describing.
      - ``{demo_data_volume_path}`` -> the UC-volume base path where this
        launcher uploaded the seed datasets (cloudfiles scenario only). When
        not provided (offline / no --apply-prepare-wheel), we fall back to a
        clearly fake placeholder under the same volume name so users can see
        the intended shape without the run blowing up.
    """
    effective_source_catalog = uc_source_catalog or uc_catalog_name
    effective_source_schema = uc_source_schema or "staging"
    raw = scenario.extra_flows_csv.read_text()
    rendered = (
        raw
        .replace("{uc_catalog_name}", uc_catalog_name)
        .replace("{uc_source_catalog}", effective_source_catalog)
        .replace("{uc_source_schema}", effective_source_schema)
    )
    if "{demo_data_volume_path}" in rendered:
        if demo_data_volume_path:
            rendered = rendered.replace("{demo_data_volume_path}", demo_data_volume_path)
        else:
            # Offline placeholder. Keeps the CSV parseable; the SDP Pipeline
            # would obviously fail to read these paths if you tried to deploy
            # without --apply-prepare-wheel, but bundle-validate is happy.
            rendered = rendered.replace(
                "{demo_data_volume_path}",
                f"/Volumes/{uc_catalog_name}/__placeholder__/__placeholder__/demo_data",
            )
    out = dest_dir / scenario.extra_flows_csv.name
    out.write_text(rendered)
    return out


def _set_dependency_default(bundle_dir: Path, dependency: str) -> None:
    """Replace `__SET_ME__` placeholder in resources/variables.yml."""
    var_path = bundle_dir / "resources" / "variables.yml"
    doc = yaml.safe_load(var_path.read_text())
    doc["variables"]["sdp_meta_dependency"]["default"] = dependency
    var_path.write_text(yaml.safe_dump(doc, sort_keys=False))
    print(f"[STAGE 2] Pinned sdp_meta_dependency in {var_path}\n          -> {dependency}")


# Each entry maps a recipe-discovered subdir name to a shipped dataset that
# the launcher copies in to seed it. Picking real files (instead of empty
# dirs) means the recipe-generated flows have actual data once the launcher
# uploads the seeded landing tree to the UC volume in STAGE 6.
_DEMO_LANDING_SEEDS = {
    "customers_streaming": REPO_ROOT / "demo" / "resources" / "data" / "customers",
    "orders_streaming": REPO_ROOT / "demo" / "resources" / "data" / "transactions",
    "events_streaming": REPO_ROOT / "demo" / "resources" / "data" / "products",
}


# Tables seeded into the delta scenario's source schema by
# `_seed_demo_delta_source`. Each (key, value) pair becomes:
#   1. CSVs under <value>/*.csv get uploaded to
#      <demo_data_volume_path>/source_delta/<key>/
#   2. The `seed_delta_source` notebook (run as a one-time serverless
#      job) reads <volume>/source_delta/<key>/ and writes
#      <uc_source_catalog>.<uc_source_schema>.<key> as a Delta table.
# The keys MUST match the tables referenced in
# `demo/dab_template_demo/flows/delta_extra.csv` -- otherwise STAGE 3's
# onboarding entries would point at tables that don't exist when the
# deployed pipeline runs at STAGE 6.
_DELTA_SOURCE_SEEDS: Dict[str, Path] = {
    "customers": REPO_ROOT / "demo" / "resources" / "data" / "customers",
    "transactions": REPO_ROOT / "demo" / "resources" / "data" / "transactions",
    "stores": REPO_ROOT / "demo" / "resources" / "data" / "stores",
    "products": REPO_ROOT / "demo" / "resources" / "data" / "products",
}


# Path of the seed notebook source on disk (uploaded to the workspace by
# `_seed_demo_delta_source`). Kept as a module-level constant so the test
# suite can pin its location.
_DELTA_SEED_NOTEBOOK_SRC: Path = (
    DEMO_DIR / "seed" / "seed_delta_source.py"
)


def _upload_delta_source_csvs_to_volume(ws, demo_data_volume_path: str) -> str:
    """Upload the delta scenario's seed CSVs into the wheel UC volume.

    Returns the base path (``<demo_data_volume_path>/source_delta``) the
    seed notebook reads from. We nest under a `source_delta/` prefix so the
    upload doesn't collide with the cloudfiles scenarios' uploads when
    both scenarios share the same volume.
    """
    base = f"{demo_data_volume_path.rstrip('/')}/source_delta"
    print(f"[STAGE 4 prep] Uploading delta seed CSVs into {base}/ ...")
    for table, src_dir in _DELTA_SOURCE_SEEDS.items():
        if not src_dir.is_dir():
            print(f"          - SKIP {table}: source dir {src_dir} missing")
            continue
        files = sorted(p for p in src_dir.iterdir() if p.is_file() and p.suffix == ".csv")
        if not files:
            print(f"          - SKIP {table}: no CSVs under {src_dir}")
            continue
        for src_file in files:
            dst = f"{base}/{table}/{src_file.name}"
            with src_file.open("rb") as fh:
                ws.files.upload(file_path=dst, contents=fh, overwrite=True)
            print(f"          - {src_file.name} -> {dst}")
    return base


def _upload_delta_seed_notebook(ws) -> str:
    """Upload the seed notebook to the caller's workspace home.

    Returns the absolute workspace path of the uploaded notebook so
    ``jobs.submit`` can reference it. We upload as ``format=SOURCE`` /
    ``language=PYTHON``; the file already starts with the
    ``# Databricks notebook source`` magic header so the workspace renders
    it as a notebook rather than a plain .py file.
    """
    from databricks.sdk.service.workspace import ImportFormat, Language

    if not _DELTA_SEED_NOTEBOOK_SRC.is_file():
        raise SystemExit(
            f"Seed notebook source not found at {_DELTA_SEED_NOTEBOOK_SRC}. "
            "Did you delete demo/dab_template_demo/seed/seed_delta_source.py?"
        )
    me = ws.current_user.me().user_name
    target_dir = f"/Users/{me}/.dlt_meta_demo"
    ws.workspace.mkdirs(target_dir)
    target = f"{target_dir}/seed_delta_source"
    print(f"[STAGE 4 prep] Uploading seed notebook to {target}")
    ws.workspace.upload(
        path=target,
        content=_DELTA_SEED_NOTEBOOK_SRC.read_bytes(),
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        overwrite=True,
    )
    return target


def _run_delta_seed_notebook(ws, *, notebook_path: str, source_catalog: str,
                             source_schema: str, input_volume_path: str,
                             tables: List[str]) -> None:
    """Submit + block on a one-time job that runs the seed notebook.

    Uses serverless compute (``environment_key`` + ``JobEnvironment`` with
    ``client="2"``) so the demo doesn't require the user to plumb a
    cluster id in. Surfaces the notebook's ``dbutils.notebook.exit(...)``
    JSON payload (the per-table row counts) when the run succeeds; on
    failure, surfaces ``run_page_url`` and the error so the user can jump
    straight to the workspace UI.
    """
    from databricks.sdk.service import jobs
    from databricks.sdk.service.compute import Environment

    print("[STAGE 4 prep] Submitting one-time seed job (serverless) ...")
    waiter = ws.jobs.submit(
        run_name=f"dlt-meta-demo-seed-delta-source-{source_schema}",
        tasks=[
            jobs.SubmitTask(
                task_key="seed_delta_source",
                notebook_task=jobs.NotebookTask(
                    notebook_path=notebook_path,
                    base_parameters={
                        "source_catalog": source_catalog,
                        "source_schema": source_schema,
                        "input_volume_path": input_volume_path,
                        "tables": ",".join(tables),
                    },
                ),
                environment_key="default",
            )
        ],
        environments=[
            jobs.JobEnvironment(
                environment_key="default",
                spec=Environment(client="2"),
            )
        ],
    )
    run = waiter.result()
    state = run.state
    if state and state.result_state and state.result_state.value == "SUCCESS":
        out = ws.jobs.get_run_output(run_id=run.tasks[0].run_id) if run.tasks else None
        notebook_output = (out.notebook_output.result if out and out.notebook_output else None)
        print(f"[STAGE 4 prep] Seed job SUCCESS. Run page: {run.run_page_url}")
        if notebook_output:
            print(f"[STAGE 4 prep] Seed notebook output: {notebook_output}")
        return
    raise SystemExit(
        f"Seed notebook job FAILED ({state.result_state if state else '?'}: "
        f"{state.state_message if state else ''}). "
        f"Inspect logs at {run.run_page_url}"
    )


def _seed_demo_delta_source(*, profile: Optional[str], uc_catalog: str,
                            uc_schema: str, create_if_missing: bool,
                            demo_data_volume_path: Optional[str]) -> None:
    """Create + populate the delta scenario's source schema in UC.

    The ``delta`` scenario's recipe (`recipes/from_uc.py`) lists tables in
    ``<uc_catalog>.<uc_schema>`` via the Workspace API, and the flows added
    in STAGE 3 from ``delta_extra.csv`` reference the same schema. This
    helper makes the demo end-to-end runnable by:

    1. Ensuring ``<uc_catalog>.<uc_schema>`` exists (auto-create gated by
       ``--create-missing-uc``, mirroring the wheel-volume + target-schema
       behaviour elsewhere in the launcher).
    2. Uploading the three ``_DELTA_SOURCE_SEEDS`` CSV datasets shipped
       under ``demo/resources/data/`` into the wheel UC volume (under a
       ``source_delta/`` subdir so it doesn't collide with cloudfiles
       uploads).
    3. Uploading the seed notebook (``demo/dab_template_demo/seed/
       seed_delta_source.py``) to the caller's workspace home.
    4. Submitting a one-time serverless job that runs the seed notebook,
       which in turn writes one Delta table per CSV subdir into the source
       schema.

    Step 2/3/4 require ``demo_data_volume_path`` (the wheel volume) — i.e.
    ``--apply-prepare-wheel`` must have been set. We check for that early
    and surface a clear error otherwise.
    """
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.errors import AlreadyExists, ResourceAlreadyExists

    if not demo_data_volume_path:
        raise SystemExit(
            "The `delta` scenario seeds upstream tables by uploading CSVs to "
            "the wheel UC volume, which requires --apply-prepare-wheel "
            "(plus --uc-schema and --uc-volume) to have been set in STAGE 2. "
            "Re-run with those flags, or pre-create the source tables "
            f"under {uc_catalog}.{uc_schema} and pass an existing "
            "--uc-source-catalog/--uc-source-schema instead."
        )

    ws = WorkspaceClient(profile=profile) if profile else WorkspaceClient()
    schema_fqn = f"{uc_catalog}.{uc_schema}"

    print(f"[STAGE 4 prep] Ensuring source schema exists: {schema_fqn}")
    try:
        ws.catalogs.get(uc_catalog)
    except Exception as e:
        raise SystemExit(
            f"Catalog '{uc_catalog}' is not accessible: {e}. "
            "Catalogs are never auto-created; create it first or pass "
            "--uc-source-catalog pointing at a catalog you can use."
        )
    try:
        ws.schemas.get(schema_fqn)
        print(f"          - {schema_fqn} (exists)")
    except Exception:
        if not create_if_missing:
            raise SystemExit(
                f"Source schema '{schema_fqn}' does not exist and "
                "--no-create-missing-uc was passed. Either create it manually "
                "or drop the flag so the launcher can auto-create it."
            )
        try:
            ws.schemas.create(name=uc_schema, catalog_name=uc_catalog)
            print(f"          - {schema_fqn} (created)")
        except (AlreadyExists, ResourceAlreadyExists):  # pragma: no cover
            print(f"          - {schema_fqn} (raced; ok)")

    input_volume_base = _upload_delta_source_csvs_to_volume(ws, demo_data_volume_path)
    notebook_path = _upload_delta_seed_notebook(ws)
    _run_delta_seed_notebook(
        ws,
        notebook_path=notebook_path,
        source_catalog=uc_catalog,
        source_schema=uc_schema,
        input_volume_path=input_volume_base,
        tables=list(_DELTA_SOURCE_SEEDS.keys()),
    )


def _seed_demo_landing(bundle_dir: Path) -> None:
    """Create a landing tree under <bundle_dir>/_demo_landing for the
    `recipes/from_volume.py` recipe to discover.

    Each subdir is populated with a copy of one of the shipped demo CSVs so
    the recipe-generated flows have REAL data behind them once the launcher
    uploads the tree to the UC volume during STAGE 6 (and rewrites the
    rendered onboarding.yml's local source_path_dev to the volume path).
    """
    landing = bundle_dir / "_demo_landing"
    landing.mkdir(exist_ok=True)
    for name, src_dir in _DEMO_LANDING_SEEDS.items():
        dst = landing / name
        dst.mkdir(exist_ok=True)
        if not src_dir.is_dir():
            continue
        for src_file in src_dir.iterdir():
            if not src_file.is_file():
                continue
            target = dst / src_file.name
            if not target.is_file():
                shutil.copyfile(src_file, target)
    print(f"[STAGE 4 prep] Seeded landing tree at {landing} (copied real CSVs from demo/resources/data)")


def _bundle_dir_from_scaffold(scenario: Scenario, out_dir: Path, uc_catalog_name: str,
                              *, uc_schema: Optional[str] = None,
                              bundle_name_override: Optional[str] = None) -> Path:
    """Return the path the template renders into for a given scenario.

    Uses the same `_render_answers_dict` substitution as
    ``_materialize_answers_file`` so the directory we expect on disk
    matches the value DAB actually receives. Callers MUST share a single
    resolved ``bundle_name_override`` value with the matching
    ``_materialize_answers_file`` call.
    """
    answers = _render_answers_dict(scenario, uc_catalog_name, uc_schema)
    return out_dir / (bundle_name_override or answers["bundle_name"])


def _run_recipe(scenario: Scenario, bundle_dir: Path, *, apply: bool,
                uc_catalog_name: str, profile: Optional[str],
                uc_source_catalog: Optional[str] = None,
                uc_source_schema: Optional[str] = None) -> int:
    """Invoke the rendered Python recipe inside the bundle as a subprocess."""
    recipe = bundle_dir / "recipes" / scenario.recipe_name
    if not recipe.is_file():
        print(f"[STAGE 4] WARNING: recipe {recipe} not found; skipping.")
        return 0

    # ``uc_source_catalog``/``uc_source_schema`` only apply to the ``delta``
    # scenario today (recipes/from_uc.py). Default the catalog to
    # ``--uc-catalog-name`` so the common single-catalog case Just Works,
    # and the schema to ``staging`` to preserve historical behaviour.
    effective_source_catalog = uc_source_catalog or uc_catalog_name
    effective_source_schema = uc_source_schema or "staging"

    substitutions = {
        "{bundle_dir}": str(bundle_dir),
        "{uc_catalog_name}": uc_catalog_name,
        "{uc_source_catalog}": effective_source_catalog,
        "{uc_source_schema}": effective_source_schema,
    }
    args = []
    for a in scenario.recipe_args_template:
        for placeholder, value in substitutions.items():
            a = a.replace(placeholder, value)
        args.append(a)
    if apply:
        args.append("--apply")
    if profile and "--profile" not in args:
        args.extend(["--profile", profile])

    cmd = [sys.executable, str(recipe), *args]
    print("[STAGE 4] $ " + " ".join(cmd))
    env = os.environ.copy()
    env["PYTHONPATH"] = str(SRC) + os.pathsep + env.get("PYTHONPATH", "")
    result = subprocess.run(cmd, env=env)
    return result.returncode


def _print_onboarding_summary(bundle_dir: Path) -> None:
    """Pretty-print the onboarding file's per-flow IDs and source formats."""
    for ext in ("yml", "json"):
        onboarding = bundle_dir / "conf" / f"onboarding.{ext}"
        if not onboarding.is_file():
            continue
        text = onboarding.read_text()
        flows = yaml.safe_load(text) if ext == "yml" else json.loads(text)
        if not isinstance(flows, list):
            continue
        print(f"\n[STAGE 5 summary] conf/onboarding.{ext} -> {len(flows)} flow(s)")
        for f in flows:
            print(
                f"  - data_flow_id={f.get('data_flow_id'):>5}  "
                f"source_format={f.get('source_format'):<11}  "
                f"bronze_table={f.get('bronze_table', '-')}"
            )
        return


# ---------------------------------------------------------------------------
# Stage runners
# ---------------------------------------------------------------------------

def stage_bundle_init(scenario: Scenario, out_dir: Path, uc_catalog_name: str,
                      profile: Optional[str], *, clean: bool = True,
                      uc_schema: Optional[str] = None,
                      bundle_name_suffix: Optional[str] = None,
                      unique_bundle_name: bool = False) -> Path:
    _banner("STAGE 1", f"databricks bundle init  (scenario={scenario.name})")
    if not shutil.which("databricks"):
        raise SystemExit(
            "The `databricks` CLI is not on PATH. Install it before running "
            "this demo: https://docs.databricks.com/dev-tools/cli/install.html"
        )

    # Log which schema names the rendered bundle will use so the user can
    # see at a glance whether --uc-schema is driving them or the
    # per-scenario defaults are. Cheap and high-signal during multi-scenario runs.
    schemas = _resolve_demo_schemas(scenario, uc_schema)
    print(
        f"[STAGE 1] Target schemas (catalog={uc_catalog_name}): "
        f"sdp_meta={schemas['sdp_meta_schema']}, "
        f"bronze={schemas['bronze_target_schema']}, "
        f"silver={schemas['silver_target_schema']}"
        + ("  [from --uc-schema]" if uc_schema else "  [scenario defaults]")
    )

    # Resolve bundle_name ONCE so the dir we compute below and the
    # answers JSON we materialize next agree on the value (relevant when
    # --unique-bundle-name is set, which would otherwise generate a
    # different timestamp on each call).
    bundle_name_override: Optional[str] = None
    if bundle_name_suffix or unique_bundle_name:
        json_default = _render_answers_dict(
            scenario, uc_catalog_name, uc_schema,
        )["bundle_name"]
        bundle_name_override = _resolve_bundle_name(
            json_default, suffix=bundle_name_suffix, unique=unique_bundle_name,
        )
        print(
            f"[STAGE 1] Bundle name override: {json_default!r} -> {bundle_name_override!r}"
            f"  (suffix={bundle_name_suffix!r}, unique={unique_bundle_name})"
        )

    # `databricks bundle init` refuses to overwrite an existing scaffold, so
    # the demo is non-idempotent by default. Re-running after a previous
    # attempt would error with "one or more files already exist". Wipe the
    # target directory first unless --no-clean was passed.
    bundle_dir = _bundle_dir_from_scaffold(
        scenario, out_dir, uc_catalog_name,
        uc_schema=uc_schema, bundle_name_override=bundle_name_override,
    )
    if bundle_dir.exists():
        if clean:
            print(f"[STAGE 1] Removing existing scaffold at {bundle_dir} (use --no-clean to keep)")
            shutil.rmtree(bundle_dir)
        else:
            raise SystemExit(
                f"Bundle directory already exists at {bundle_dir}. "
                "Re-run without --no-clean (or delete it manually) to re-scaffold."
            )

    with tempfile.TemporaryDirectory(prefix="dab_demo_") as tmp:
        tmp = Path(tmp)
        answers = _materialize_answers_file(
            scenario, uc_catalog_name, tmp,
            uc_schema=uc_schema, bundle_name_override=bundle_name_override,
        )
        rc = bundle_init(BundleInitCommand(
            output_dir=str(out_dir),
            config_file=str(answers),
            profile=profile,
        ))
    if rc != 0:
        raise SystemExit(f"bundle init failed with exit code {rc}")
    if not bundle_dir.is_dir():
        raise SystemExit(f"Expected scaffolded bundle at {bundle_dir}, but it does not exist.")

    # DAB respects .gitignore from the nearest enclosing .git directory upward
    # when deciding what to upload. The launcher scaffolds bundles under
    # `demo_runs/`, which is .gitignored at the dlt-meta repo root -- so
    # without our own git boundary, DAB sees EVERY file in the bundle as
    # ignored, syncs zero files, and the pipeline fails at runtime with
    # `NOTEBOOK_NOT_FOUND_EXCEPTION` because notebooks/init_sdp_meta_pipeline
    # never reached the workspace. Initialize an empty git repo inside the
    # bundle so DAB stops there and only honors the bundle's own .gitignore.
    _ensure_bundle_git_boundary(bundle_dir)

    # The DAB template seeds a placeholder `data_flow_id: "100"` /
    # `example_table` onboarding entry pointing at `<catalog>.landing.example_table`.
    # For the `delta` scenario that table doesn't exist (and the launcher
    # never seeds it), so leaving it in produces an extra dataflow spec
    # that fails at pipeline runtime with `TABLE_OR_VIEW_NOT_FOUND`.
    # Strip it so STAGE 4's recipe (from_uc.py for delta, from_topics.py
    # for kafka/eventhub) is the only thing populating onboarding.yml.
    if (
        scenario.name in _DELTA_SCENARIO_NAMES
        or scenario.name in {"kafka", "eventhub", "multi_pipeline_cloudfiles"}
    ):
        _strip_example_onboarding_entry(bundle_dir)

    print(f"\n[STAGE 1] Bundle scaffolded at {bundle_dir}")
    return bundle_dir


def _strip_example_onboarding_entry(bundle_dir: Path) -> None:
    """Remove the scaffolded `data_flow_id: "100"` placeholder from conf/onboarding.{yml,json}.

    The DAB template ships a single example flow as a teaching aid; this
    helper drops it so scenarios whose subsequent stages populate the
    file from real sources end up with exactly the rows those stages
    added (no `example_table` ghost row pointing at non-existent
    upstream data).
    """
    yml = bundle_dir / "conf" / "onboarding.yml"
    js = bundle_dir / "conf" / "onboarding.json"
    if yml.is_file():
        rows = yaml.safe_load(yml.read_text()) or []
        kept = [r for r in rows if str(r.get("data_flow_id", "")) != "100"]
        yml.write_text(yaml.safe_dump(kept, sort_keys=False))
        print(f"[STAGE 1] Stripped placeholder data_flow_id=100 from {yml.name} "
              f"({len(rows)} -> {len(kept)} entries)")
    elif js.is_file():
        rows = json.loads(js.read_text())
        kept = [r for r in rows if str(r.get("data_flow_id", "")) != "100"]
        js.write_text(json.dumps(kept, indent=2))
        print(f"[STAGE 1] Stripped placeholder data_flow_id=100 from {js.name} "
              f"({len(rows)} -> {len(kept)} entries)")


def _ensure_bundle_git_boundary(bundle_dir: Path) -> None:
    """`git init` the bundle directory so DAB doesn't inherit a parent .gitignore.

    No-op if `git` isn't on PATH or the directory is already a git repo
    (idempotent + safe to re-run). Failure is logged and swallowed so a
    missing `git` binary never blocks the demo.
    """
    if (bundle_dir / ".git").exists():
        return
    git = shutil.which("git")
    if not git:
        print(
            "[STAGE 1] WARNING: `git` not on PATH; cannot create a git boundary "
            "inside the bundle. If the bundle directory is .gitignored by an "
            "ancestor repo, `databricks bundle deploy` will silently sync zero "
            "files and pipelines will fail with NOTEBOOK_NOT_FOUND_EXCEPTION."
        )
        return
    try:
        subprocess.run(
            [git, "init", "--quiet", str(bundle_dir)],
            check=True, capture_output=True, text=True,
        )
        print(
            f"[STAGE 1] Initialized empty git repo at {bundle_dir}/.git "
            "so `databricks bundle deploy` doesn't inherit the parent repo's "
            ".gitignore (which would exclude every bundle file)."
        )
    except Exception as exc:  # pragma: no cover - defensive
        print(
            f"[STAGE 1] WARNING: `git init` failed in {bundle_dir}: {exc}. "
            "If your bundle directory is .gitignored by an ancestor repo, "
            "`databricks bundle deploy` will silently sync zero files."
        )


# Datasets shipped under demo/resources/data/ that we re-use for the
# cloudfiles scenario so flows point at REAL files instead of placeholder
# UC paths. Keep the keys aligned with rows in flows/cloudfiles_extra.csv.
_CLOUDFILES_DEMO_DATASETS = {
    "customers": REPO_ROOT / "demo" / "resources" / "data" / "customers",
    "transactions": REPO_ROOT / "demo" / "resources" / "data" / "transactions",
    "products": REPO_ROOT / "demo" / "resources" / "data" / "products",
    "stores": REPO_ROOT / "demo" / "resources" / "data" / "stores",
}


def _upload_cloudfiles_demo_data(uc_catalog: str, uc_schema: str, uc_volume: str,
                                 profile: Optional[str]) -> str:
    """Upload the four shipped CSV datasets into the user's UC volume.

    Returns the base path (``/Volumes/<cat>/<sch>/<vol>/demo_data``) that
    `_materialize_csv` substitutes into ``{demo_data_volume_path}``.

    The upload is idempotent (overwrite=True) so re-runs are cheap. Reusing
    the same volume that holds the wheel keeps the demo to a single UC
    permission requirement.
    """
    from databricks.sdk import WorkspaceClient  # local import: keeps unit tests light

    base = f"/Volumes/{uc_catalog}/{uc_schema}/{uc_volume}/demo_data"
    ws = WorkspaceClient(profile=profile) if profile else WorkspaceClient()
    print(f"[STAGE 2] Uploading cloudFiles demo datasets into {base}/ ...")
    for table, src_dir in _CLOUDFILES_DEMO_DATASETS.items():
        if not src_dir.is_dir():
            print(f"          - SKIP {table}: source dir {src_dir} not found")
            continue
        for src_file in sorted(src_dir.iterdir()):
            if not src_file.is_file():
                continue
            dst = f"{base}/{table}/{src_file.name}"
            with src_file.open("rb") as fh:
                ws.files.upload(file_path=dst, contents=fh, overwrite=True)
            print(f"          - {src_file.name} -> {dst}")
    return base


def stage_prepare_wheel(scenario: Scenario, bundle_dir: Path, *,
                        apply: bool, uc_catalog_name: str,
                        uc_schema: Optional[str], uc_volume: Optional[str],
                        profile: Optional[str],
                        pip_index_url: Optional[str] = None,
                        pip_extra_index_urls: Optional[List[str]] = None,
                        create_if_missing: bool = True) -> Optional[str]:
    """Returns the demo_data UC-volume base path (cloudfiles only, when
    --apply-prepare-wheel is set), otherwise None."""
    _banner("STAGE 2", "bundle prepare-wheel  (build + upload sdp-meta wheel)")
    if not apply:
        # Pin a fake placeholder that LOOKS like a UC volume path. The
        # bundle-validate guard rails accept this shape; the runner notebook
        # will refuse it at runtime if it sees the literal `__SET_ME__` -- which
        # is the point of this stage in --no-apply mode.
        placeholder = (
            f"/Volumes/{uc_catalog_name}/sdp_meta_dab_demo/sdp_meta_wheels/"
            "databricks_labs_sdp_meta-0.0.0-py3-none-any.whl"
        )
        print("[STAGE 2] --apply-prepare-wheel was NOT set; pinning a placeholder path.")
        print("          (sanity checks accept the shape; runtime would fail-fast if used.)")
        _set_dependency_default(bundle_dir, placeholder)
        return None

    if not (uc_schema and uc_volume):
        raise SystemExit("--apply-prepare-wheel requires --uc-schema and --uc-volume")

    volume_path = bundle_prepare_wheel(BundlePrepareWheelCommand(
        uc_catalog=uc_catalog_name,
        uc_schema=uc_schema,
        uc_volume=uc_volume,
        profile=profile,
        pip_index_url=pip_index_url,
        pip_extra_index_urls=pip_extra_index_urls,
        create_if_missing=create_if_missing,
    ))
    _set_dependency_default(bundle_dir, volume_path)

    # For the cloudfiles scenarios (split + combined), additionally seed the
    # four demo datasets into the same UC volume so the CSV flows point at
    # real data. Both scenarios share the same demo data and recipe; only
    # the rendered SDP topology differs.
    if scenario.name in _CLOUDFILES_SCENARIO_NAMES:
        return _upload_cloudfiles_demo_data(uc_catalog_name, uc_schema, uc_volume, profile)
    # For the delta scenario, return the wheel-volume base so STAGE 4 can
    # upload its seed CSVs under <base>/source_delta/<table>/ via
    # `_seed_demo_delta_source`. We don't pre-upload here (unlike
    # cloudfiles) because the delta seeder also has to launch a notebook
    # job that consumes the uploaded files; keeping the upload + the job
    # submission together in one helper is easier to reason about.
    if scenario.name in _DELTA_SCENARIO_NAMES:
        return f"/Volumes/{uc_catalog_name}/{uc_schema}/{uc_volume}"
    return None


def stage_add_flow(scenario: Scenario, bundle_dir: Path, uc_catalog_name: str,
                   demo_data_volume_path: Optional[str] = None,
                   uc_source_catalog: Optional[str] = None,
                   uc_source_schema: Optional[str] = None) -> None:
    if scenario.extra_flows_csv is None:
        _banner("STAGE 3", "bundle-add-flow --from-csv  (skipped: scenario has no extra CSV)")
        print(
            "[STAGE 3] Skipping bundle-add-flow: this scenario relies entirely on "
            f"STAGE 4's recipe ({scenario.recipe_name}) for flow generation."
        )
        return
    _banner("STAGE 3", f"bundle-add-flow --from-csv  ({scenario.extra_flows_csv.name})")
    with tempfile.TemporaryDirectory(prefix="dab_demo_csv_") as tmp:
        csv_path = _materialize_csv(
            scenario, uc_catalog_name, Path(tmp),
            demo_data_volume_path=demo_data_volume_path,
            uc_source_catalog=uc_source_catalog,
            uc_source_schema=uc_source_schema,
        )
        flows = _flows_from_csv(csv_path)
    rc = bundle_add_flow(BundleAddFlowCommand(
        bundle_dir=str(bundle_dir),
        flows=flows,
        dry_run=False,
    ))
    if rc != 0:
        raise SystemExit(f"bundle-add-flow failed with exit code {rc}")


def stage_add_independent_pipeline(
    scenario: Scenario,
    bundle_dir: Path,
    *,
    uc_schema: Optional[str],
    demo_data_volume_path: Optional[str],
) -> None:
    if scenario.name != "multi_pipeline_cloudfiles":
        return

    _banner(
        "STAGE 3B",
        "bundle-add-pipeline + inherited bundle-add-flow  (secondary topology)",
    )
    schemas = _resolve_demo_schemas(scenario, uc_schema)
    secondary_bronze = f"{schemas['bronze_target_schema']}_secondary"
    secondary_silver = f"{schemas['silver_target_schema']}_secondary"
    rc = bundle_add_pipeline(BundleAddPipelineCommand(
        bundle_dir=str(bundle_dir),
        pipeline=PipelineSpec(
            name="secondary",
            layer="bronze_silver",
            pipeline_mode="combined",
            dataflow_group="dab_demo_cf_secondary_group",
            bronze_target_schema=secondary_bronze,
            silver_target_schema=secondary_silver,
        ),
    ))
    if rc != 0:
        raise SystemExit(f"bundle-add-pipeline failed with exit code {rc}")

    volume_base = demo_data_volume_path or (
        "/Volumes/__placeholder__/__placeholder__/__placeholder__/demo_data"
    )
    rc = bundle_add_flow(BundleAddFlowCommand(
        bundle_dir=str(bundle_dir),
        flows=[FlowSpec(
            source_format="cloudFiles",
            source_path=f"{volume_base}/customers/",
            bronze_table="secondary_customers",
            silver_table="secondary_customers",
            data_flow_group="dab_demo_cf_secondary_group",
            source_system="demo_assets_secondary",
            cloudfiles_format="csv",
        )],
    ))
    if rc != 0:
        raise SystemExit(
            f"bundle-add-flow for secondary topology failed with exit code {rc}"
        )


def stage_recipe(scenario: Scenario, bundle_dir: Path, *, apply_recipe: bool,
                 uc_catalog_name: str, profile: Optional[str],
                 uc_source_catalog: Optional[str] = None,
                 uc_source_schema: Optional[str] = None,
                 create_missing_uc: bool = True,
                 demo_data_volume_path: Optional[str] = None) -> None:
    _banner("STAGE 4", f"recipes/{scenario.recipe_name}  ({'apply' if apply_recipe else 'dry-run'})")
    if scenario.recipe_requires_workspace and not apply_recipe:
        print(
            f"[STAGE 4] recipe {scenario.recipe_name} requires a live workspace "
            "(no offline dry-run path). Skipping. Re-run with --apply-recipe "
            "and --profile to exercise it."
        )
        return
    if scenario.name in _CLOUDFILES_SCENARIO_NAMES:
        _seed_demo_landing(bundle_dir)
    if scenario.name in _DELTA_SCENARIO_NAMES:
        # Resolve effective source coords the same way `_run_recipe` does so
        # the seeded schema and the recipe's `--source-schema` always agree.
        effective_source_catalog = uc_source_catalog or uc_catalog_name
        effective_source_schema = uc_source_schema or "staging"
        _seed_demo_delta_source(
            profile=profile,
            uc_catalog=effective_source_catalog,
            uc_schema=effective_source_schema,
            create_if_missing=create_missing_uc,
            demo_data_volume_path=demo_data_volume_path,
        )
    rc = _run_recipe(scenario, bundle_dir, apply=apply_recipe,
                     uc_catalog_name=uc_catalog_name, profile=profile,
                     uc_source_catalog=uc_source_catalog,
                     uc_source_schema=uc_source_schema)
    if rc != 0:
        # Recipe dry-run prints the proposed plan with rc=0; non-zero means a
        # real failure (eg. duplicate id, validation error).
        raise SystemExit(f"recipe {scenario.recipe_name} failed with exit code {rc}")


def stage_validate(bundle_dir: Path, profile: Optional[str]) -> None:
    _banner("STAGE 5", "bundle-validate  (sdp-meta sanity checks + databricks validate)")
    rc = bundle_validate(BundleValidateCommand(
        bundle_dir=str(bundle_dir),
        profile=profile,
    ))
    if rc != 0:
        raise SystemExit(f"bundle-validate exited with code {rc}")
    _print_onboarding_summary(bundle_dir)


# Per-flow keys that store a path to another conf file. The launcher rewrites
# ``${workspace.file_path}/conf/...`` -> ``<uc_volume_conf_base>/...`` for
# each one, AND drops the key entirely when the referenced file is missing
# on disk (so the pipeline doesn't crash trying to read a non-existent DDL).
# These keys live both at the top of each flow and (for source_schema_path)
# nested under ``source_details``.
_PATH_KEYS_TOP_LEVEL = (
    "bronze_data_quality_expectations_json_dev",
    "silver_data_quality_expectations_json_dev",
    "silver_transformation_json_dev",
    "bronze_transformation_json_dev",
    "bronze_table_path_dev",
    "silver_table_path_dev",
)
_PATH_KEYS_SOURCE_DETAILS = (
    "source_schema_path",
)


def _rewrite_and_prune_flow_paths(flow: dict, conf_root: Path,
                                  workspace_conf_token: str,
                                  volume_conf_base: str) -> None:
    """Substitute workspace.file_path tokens with the UC-volume base, and drop
    path fields whose local file is missing.

    Mutates ``flow`` in place. Empty-string values (template defaults like
    ``bronze_table_path_dev: ''``) are left as-is — only fields that actually
    point at ``${workspace.file_path}/conf/...`` are touched.
    """
    def _maybe_rewrite(container: dict, key: str) -> None:
        val = container.get(key)
        if not isinstance(val, str) or not val.startswith(workspace_conf_token):
            return
        # Resolve the local path the value WOULD point at if the file existed.
        rel = val[len(workspace_conf_token):].lstrip("/")
        local = conf_root / rel
        if not local.is_file():
            # Reference is dangling locally -> the pipeline would fail to
            # read it. Drop the key so the engine falls back to defaults
            # (e.g. cloudFiles.inferColumnTypes for a missing source_schema_path).
            del container[key]
            return
        container[key] = f"{volume_conf_base}/{rel}"

    for k in _PATH_KEYS_TOP_LEVEL:
        _maybe_rewrite(flow, k)
    src_details = flow.get("source_details")
    if isinstance(src_details, dict):
        for k in _PATH_KEYS_SOURCE_DETAILS:
            _maybe_rewrite(src_details, k)

    # When DQE is kept, the engine accesses `bronze_quarantine_table` (and
    # similar) from the Spark Row unconditionally, raising
    # `PySparkValueError: bronze_quarantine_table` if the column is missing
    # from the inferred schema. Seed empty defaults so the column shows up
    # in the schema and the runtime check (`if row["..."]:`) is falsy.
    if flow.get("bronze_data_quality_expectations_json_dev"):
        flow.setdefault("bronze_database_quarantine_dev",
                        flow.get("bronze_database_dev", ""))
        flow.setdefault("bronze_quarantine_table", "")
        flow.setdefault("bronze_quarantine_table_path_dev", "")
        flow.setdefault("bronze_quarantine_table_partitions", "")
        flow.setdefault("bronze_quarantine_table_properties", {})
        flow.setdefault("bronze_quarantine_table_cluster_by", [])


def _materialize_local_source_paths_to_volume(flow: dict, bundle_dir: Path,
                                              ws, volume_data_base: str) -> Optional[bool]:
    """Detect a flow whose ``source_path_dev`` is a LOCAL filesystem path,
    upload its files to the UC volume, and rewrite the path. Also flips
    ``cloudFiles.format`` to match the file extension we just uploaded.

    Returns:
        True  -> rewrite succeeded; flow now points at a real UC volume path.
        False -> flow's source_path_dev is local but the dir is empty/missing.
                 Caller should drop the flow (no real data behind it).
        None  -> flow does not have a local source_path_dev (already a
                 ``/Volumes/...`` path or no source_path_dev at all). Caller
                 should leave the flow alone.

    Mirrors the pattern used by ``integration_tests/run_integration_tests.py``
    which uploads ``demo/resources/data/...`` into a UC volume so DLT
    pipelines can stream from a path that actually exists.
    """
    src_details = flow.get("source_details")
    if not isinstance(src_details, dict):
        return None
    raw = src_details.get("source_path_dev")
    if not isinstance(raw, str) or not raw:
        return None
    # Already a UC volume path: leave the rewrite to the existence-check below.
    if raw.startswith("/Volumes/"):
        return None

    local = Path(raw)
    if not local.is_absolute():
        local = bundle_dir / raw
    if not local.is_dir():
        return False
    files = sorted([p for p in local.iterdir() if p.is_file()])
    if not files:
        return False

    table_name = src_details.get("source_table") or local.name
    dst_base = f"{volume_data_base}/{table_name}"
    for src_file in files:
        with src_file.open("rb") as fh:
            ws.files.upload(file_path=f"{dst_base}/{src_file.name}",
                            contents=fh, overwrite=True)
    src_details["source_path_dev"] = f"{dst_base}/"

    # Make cloudFiles.format match the data we just uploaded so the pipeline
    # actually parses the rows. Defaults to json from the engine; flip to csv
    # / parquet / json based on the dominant suffix.
    suffix_counts: Dict[str, int] = {}
    for src_file in files:
        suffix_counts[src_file.suffix.lower()] = suffix_counts.get(src_file.suffix.lower(), 0) + 1
    dominant_suffix = max(suffix_counts, key=suffix_counts.get) if suffix_counts else ""
    fmt_by_suffix = {".csv": "csv", ".json": "json", ".parquet": "parquet", ".avro": "avro"}
    new_format = fmt_by_suffix.get(dominant_suffix)
    if new_format and isinstance(flow.get("bronze_reader_options"), dict):
        flow["bronze_reader_options"]["cloudFiles.format"] = new_format

    return True


def _uc_volume_path_exists(ws, path: str) -> bool:
    """Best-effort existence check for a UC volume directory. We only use
    this to drop flows pointing at the seeded TEMPLATE placeholder
    (``/Volumes/<cat>/landing/files/example_table/``) before runtime so the
    pipeline doesn't fail on a flow the user never wired up."""
    try:
        ws.files.get_directory_metadata(path)
        return True
    except Exception:
        try:
            # Some SDK versions expose listing instead of metadata.
            list(ws.files.list_directory_contents(path))
            return True
        except Exception:
            return False


def _stage_conf_to_uc_volume(bundle_dir: Path, uc_catalog: str, uc_schema: str,
                             uc_volume: str, profile: Optional[str]) -> str:
    """Upload bundle's conf/* tree to a UC volume and rewrite workspace refs.

    Why: the seeded onboarding job's `python_wheel_task` passes
    ``onboarding_file_path: ${workspace.file_path}/conf/onboarding.yml`` which
    DAB expands to ``/Workspace/Users/.../files/conf/onboarding.yml``. On
    serverless compute, Spark's text/json reader treats that as
    ``dbfs:/Workspace/...`` and fails with PATH_NOT_FOUND. Mirroring the
    pattern used by ``integration_tests/run_integration_tests.py``: stage the
    conf files into a UC volume and pass the volume path instead.

    For onboarding files (yml/json) we ALSO:
      * Rewrite ``${workspace.file_path}/conf/...`` -> ``<uc_volume_conf>/...``
        for every per-flow path key (DQE, silver_transformation, source
        schema DDL, ...), so the pipeline can resolve them at runtime.
      * Drop path keys whose local file does not exist, so the engine falls
        back to its defaults instead of erroring on a dangling reference
        (typical for the placeholder ``conf/schemas/example_table.ddl`` and
        per-table DQE files that recipes / bundle-add-flow do not emit).

    Returns the UC-volume base for conf, e.g.
    ``/Volumes/<cat>/<sch>/<vol>/conf``.
    """
    import io

    from databricks.sdk import WorkspaceClient

    conf_root = bundle_dir / "conf"
    if not conf_root.is_dir():
        raise SystemExit(f"Expected {conf_root} to exist after bundle init.")

    volume_conf_base = f"/Volumes/{uc_catalog}/{uc_schema}/{uc_volume}/conf"
    volume_data_base = f"/Volumes/{uc_catalog}/{uc_schema}/{uc_volume}/demo_data"
    workspace_conf_token = "${workspace.file_path}/conf"

    ws = WorkspaceClient(profile=profile) if profile else WorkspaceClient()
    print(f"[STAGE 6] Staging conf/ -> {volume_conf_base}/  (Spark-readable on serverless)")
    uploaded = 0
    pruned = 0
    repointed = 0
    dropped = 0
    for src_path in sorted(conf_root.rglob("*")):
        if not src_path.is_file():
            continue
        rel = src_path.relative_to(conf_root).as_posix()
        dst = f"{volume_conf_base}/{rel}"

        # Special-case the onboarding file: parse it, rewrite per-flow path
        # references, prune dangling ones, push local source data to UC, and
        # drop flows whose UC volume source path doesn't exist.
        is_onboarding = src_path.name.startswith("onboarding.") and src_path.parent == conf_root
        if is_onboarding and src_path.suffix.lower() in (".yml", ".yaml", ".json"):
            if src_path.suffix.lower() == ".json":
                doc = json.loads(src_path.read_text())
            else:
                doc = yaml.safe_load(src_path.read_text())
            if isinstance(doc, list):
                before = sum(_count_path_keys(f) for f in doc)
                kept: list = []
                for flow in doc:
                    _rewrite_and_prune_flow_paths(
                        flow, conf_root, workspace_conf_token, volume_conf_base,
                    )
                    # 1) Local source_path_dev -> upload + rewrite to UC.
                    local_status = _materialize_local_source_paths_to_volume(
                        flow, bundle_dir, ws, volume_data_base,
                    )
                    if local_status is True:
                        repointed += 1
                    elif local_status is False:
                        dropped += 1
                        print(
                            f"[STAGE 6] DROP flow data_flow_id="
                            f"{flow.get('data_flow_id')!r}: local source_path_dev "
                            f"{flow.get('source_details', {}).get('source_path_dev')!r} "
                            "is empty/missing"
                        )
                        continue
                    # 2) Drop flows pointing at non-existent UC volume paths
                    # (typically the seeded /Volumes/<cat>/landing/files/...
                    # template placeholder the user never created).
                    src_path_now = flow.get("source_details", {}).get("source_path_dev")
                    if (isinstance(src_path_now, str) and src_path_now.startswith("/Volumes/")
                            and not _uc_volume_path_exists(ws, src_path_now.rstrip("/"))):
                        dropped += 1
                        print(
                            f"[STAGE 6] DROP flow data_flow_id="
                            f"{flow.get('data_flow_id')!r}: UC source_path_dev "
                            f"{src_path_now!r} does not exist on the workspace"
                        )
                        continue
                    kept.append(flow)
                doc = kept
                after = sum(_count_path_keys(f) for f in doc)
                pruned += before - after
            if src_path.suffix.lower() == ".json":
                payload = json.dumps(doc, indent=2).encode("utf-8")
            else:
                payload = yaml.safe_dump(doc, sort_keys=False).encode("utf-8")
            ws.files.upload(file_path=dst, contents=io.BytesIO(payload), overwrite=True)
        elif src_path.suffix.lower() in (".yml", ".yaml", ".json"):
            # Sub-conf files (DQE, transformations, ...) get a plain text
            # rewrite — they may contain `${workspace.file_path}/conf` too
            # (e.g. nested transformation refs).
            text = src_path.read_text()
            patched = text.replace(workspace_conf_token, volume_conf_base)
            ws.files.upload(file_path=dst, contents=io.BytesIO(patched.encode("utf-8")),
                            overwrite=True)
        else:
            with src_path.open("rb") as fh:
                ws.files.upload(file_path=dst, contents=fh, overwrite=True)
        uploaded += 1
    msg = f"[STAGE 6] Uploaded {uploaded} conf file(s) under {volume_conf_base}/"
    extras = []
    if pruned:
        extras.append(f"pruned {pruned} dangling path ref(s)")
    if repointed:
        extras.append(f"uploaded local data + repointed {repointed} flow(s) to {volume_data_base}/")
    if dropped:
        extras.append(f"dropped {dropped} flow(s) with no real source data")
    if extras:
        msg += "  (" + "; ".join(extras) + ")"
    print(msg)
    return volume_conf_base


def _count_path_keys(flow: dict) -> int:
    """How many of the rewritable path keys are populated on this flow."""
    if not isinstance(flow, dict):
        return 0
    n = sum(1 for k in _PATH_KEYS_TOP_LEVEL
            if isinstance(flow.get(k), str) and flow.get(k))
    src_details = flow.get("source_details")
    if isinstance(src_details, dict):
        n += sum(1 for k in _PATH_KEYS_SOURCE_DETAILS
                 if isinstance(src_details.get(k), str) and src_details.get(k))
    return n


def _resolve_target_schemas(bundle_dir: Path) -> List[str]:
    """Return the list of UC schemas the rendered bundle expects to write to.

    Reading these from ``resources/variables.yml`` (instead of hard-coding
    them) means the helper still works when a user re-scaffolds the bundle
    with different answers.
    """
    var_path = bundle_dir / "resources" / "variables.yml"
    doc = yaml.safe_load(var_path.read_text())
    schemas = []
    for key in ("sdp_meta_schema", "bronze_target_schema", "silver_target_schema"):
        val = doc.get("variables", {}).get(key, {}).get("default")
        if val and val not in schemas:
            schemas.append(val)
    pipelines_path = bundle_dir / "resources" / "sdp_meta_pipelines.yml"
    if pipelines_path.is_file():
        pipelines_doc = yaml.safe_load(pipelines_path.read_text()) or {}
        pipelines = (
            pipelines_doc.get("resources", {}).get("pipelines", {}) or {}
        )
        for pipeline in pipelines.values():
            if not isinstance(pipeline, dict):
                continue
            config = pipeline.get("configuration") or {}
            for key in (
                "sdp_meta.bronzeTargetSchema",
                "sdp_meta.silverTargetSchema",
            ):
                val = config.get(key)
                if val and "${" not in str(val) and val not in schemas:
                    schemas.append(val)
    return schemas


def _ensure_target_schemas(bundle_dir: Path, uc_catalog: str,
                           profile: Optional[str]) -> None:
    """Create the bronze/silver/dataflowspec schemas if they don't exist.

    The dataflowspec table that the onboarding job writes to, and the
    bronze/silver target schemas that SDP writes its tables to, must
    already exist in Unity Catalog before either job runs (SDP refuses to
    auto-create schemas, and the onboarding job fails with SCHEMA_NOT_FOUND
    when it tries to MERGE INTO a missing schema).

    Mirrors the auto-create-on-demand behaviour of
    ``bundle_prepare_wheel`` for the wheel volume's schema.
    """
    from databricks.sdk import WorkspaceClient  # local import: keeps unit tests light
    from databricks.sdk.errors import AlreadyExists, ResourceAlreadyExists

    schemas = _resolve_target_schemas(bundle_dir)
    if not schemas:
        return
    print(f"[STAGE 6] Ensuring target schemas exist in {uc_catalog}: {schemas}")
    ws = WorkspaceClient(profile=profile) if profile else WorkspaceClient()
    try:
        ws.catalogs.get(uc_catalog)
    except Exception as e:
        raise SystemExit(
            f"Catalog '{uc_catalog}' is not accessible: {e}. "
            "Catalogs are never auto-created; create it first or use a different "
            "--uc-catalog-name."
        )
    for schema in schemas:
        try:
            ws.schemas.get(f"{uc_catalog}.{schema}")
            print(f"          - {uc_catalog}.{schema} (exists)")
            continue
        except Exception:
            pass
        try:
            ws.schemas.create(name=schema, catalog_name=uc_catalog)
            print(f"          - {uc_catalog}.{schema} (created)")
        except (AlreadyExists, ResourceAlreadyExists):  # pragma: no cover
            print(f"          - {uc_catalog}.{schema} (raced; ok)")
        except Exception as e:
            # Some SDK paths surface a concurrent-create race as a plain
            # Exception whose message carries the canonical Databricks
            # `ALREADY_EXISTS` error code rather than the typed AlreadyExists
            # subclass. We *only* tolerate this exact case, and we surface
            # the original exception text so an unexpected swallow is still
            # debuggable from the demo log.
            if "ALREADY_EXISTS" not in str(e):
                raise SystemExit(
                    f"Failed to create schema '{uc_catalog}.{schema}': {e}. "
                    "Make sure your principal has `USE CATALOG` + `CREATE SCHEMA` "
                    "on the catalog."
                )
            print(
                f"          - {uc_catalog}.{schema} (raced; treating as ok). "
                f"Underlying error: {e}"
            )


def _resolve_onboarding_file_name(bundle_dir: Path) -> str:
    """Read variables.yml to learn the onboarding file basename.

    The template defaults to onboarding.yml, but a user could rename it via
    --config-file or by editing resources/variables.yml. We don't want the
    demo to silently mis-target the wrong file.
    """
    var_path = bundle_dir / "resources" / "variables.yml"
    doc = yaml.safe_load(var_path.read_text())
    name = doc.get("variables", {}).get("onboarding_file_name", {}).get("default")
    if not name:
        raise SystemExit(
            f"Could not read onboarding_file_name default from {var_path}. "
            "Did the template change?"
        )
    return name


def stage_deploy_and_run(bundle_dir: Path, profile: Optional[str], *,
                         uc_catalog: Optional[str] = None,
                         uc_schema: Optional[str] = None,
                         uc_volume: Optional[str] = None) -> None:
    """Run `databricks bundle deploy` + `bundle run` for onboarding & pipelines.

    When uc_catalog/schema/volume are provided, ALSO stages the bundle's
    conf/ tree into ``/Volumes/<cat>/<sch>/<vol>/conf/`` and overrides the
    onboarding job's ``onboarding_file_path`` parameter so Spark on
    serverless can actually read the file. This is the workaround for
    PATH_NOT_FOUND when reading workspace files via Spark.
    """
    _banner("STAGE 6", "databricks bundle deploy + run  (this hits the workspace)")
    cli = shutil.which("databricks")
    if not cli:
        raise SystemExit("databricks CLI not on PATH; cannot deploy.")
    base = [cli]
    if profile:
        base.extend(["--profile", profile])

    # 0) ensure target schemas exist (SDP + onboarding fail if they don't).
    if uc_catalog:
        _ensure_target_schemas(bundle_dir, uc_catalog, profile)

    # Stage the rewritten/pruned conf tree before deployment and point only
    # the onboarding task at it. Passing --python-named-params to `bundle run`
    # forwards the parameter to every Python wheel task, including stage_conf,
    # whose CLI correctly rejects onboarding_file_path.
    if uc_catalog and uc_schema and uc_volume:
        volume_conf_base = _stage_conf_to_uc_volume(
            bundle_dir, uc_catalog, uc_schema, uc_volume, profile,
        )
        onboarding_file_name = _resolve_onboarding_file_name(bundle_dir)
        onboarding_path = f"{volume_conf_base}/{onboarding_file_name}"
        onboarding_job_path = (
            bundle_dir / "resources" / "sdp_meta_onboarding_job.yml"
        )
        onboarding_job = yaml.safe_load(onboarding_job_path.read_text())
        tasks = onboarding_job["resources"]["jobs"]["onboarding"]["tasks"]
        onboarding_task = next(
            task for task in tasks
            if task.get("task_key") == "onboard_dataflowspecs"
        )
        onboarding_task["python_wheel_task"]["named_parameters"][
            "onboarding_file_path"
        ] = onboarding_path
        onboarding_job_path.write_text(
            yaml.safe_dump(onboarding_job, sort_keys=False)
        )
        print(
            f"[STAGE 6] Onboarding task file path -> {onboarding_path}"
        )

    # 1) deploy. Capture stderr so we can detect the silent
    # "no files to sync" failure mode (caused by the bundle dir being
    # ignored by an ancestor .gitignore) before pipelines fail with
    # NOTEBOOK_NOT_FOUND_EXCEPTION at runtime.
    deploy_cmd = base + ["bundle", "deploy", "--target", "dev"]
    print("[STAGE 6] $ " + " ".join(deploy_cmd))
    deploy_proc = subprocess.run(
        deploy_cmd, cwd=bundle_dir,
        stderr=subprocess.PIPE, text=True,
    )
    if deploy_proc.stderr:
        sys.stderr.write(deploy_proc.stderr)
    if deploy_proc.returncode != 0:
        raise SystemExit(
            f"command bundle deploy --target dev failed with code {deploy_proc.returncode}"
        )
    if "no files to sync" in (deploy_proc.stderr or "").lower():
        raise SystemExit(
            "[STAGE 6] FATAL: `databricks bundle deploy` reported "
            "'no files to sync' -- the bundle directory is being filtered out "
            "by an ancestor repo's .gitignore (DAB walks up to the nearest "
            ".git directory and applies its .gitignore). The launcher normally "
            "fixes this by `git init`-ing the bundle directory in STAGE 1; if "
            "you see this error, that step likely failed (see earlier "
            "[STAGE 1] WARNING). Re-run after either (a) installing `git`, or "
            "(b) running `git init` manually inside "
            f"{bundle_dir}, or (c) moving --out-dir outside any "
            ".gitignored ancestor."
        )

    if not (uc_catalog and uc_schema and uc_volume):
        print(
            "[STAGE 6] WARNING: --apply-prepare-wheel was not set with UC "
            "catalog/schema/volume; the onboarding job will read from "
            "${workspace.file_path}/conf/... which is known to fail on "
            "serverless Spark with PATH_NOT_FOUND."
        )

    for sub in (
        ["bundle", "run", "onboarding", "--target", "dev"],
        ["bundle", "run", "pipelines", "--target", "dev"],
    ):
        cmd = base + sub
        print("[STAGE 6] $ " + " ".join(cmd))
        result = subprocess.run(cmd, cwd=bundle_dir)
        if result.returncode != 0:
            # `databricks bundle run pipelines` only reports the job-level
            # INTERNAL_ERROR; the actual Python/Spark traceback lives in the
            # Spark Declarative Pipelines (SDP) event log. Fetch it so users
            # don't have to chase the run URL by hand.
            if sub[:3] == ["bundle", "run", "pipelines"]:
                _dump_pipeline_failure_diagnostics(bundle_dir, profile)
            raise SystemExit(f"command {' '.join(sub)} failed with code {result.returncode}")


def _dump_pipeline_failure_diagnostics(bundle_dir: Path,
                                       profile: Optional[str]) -> None:
    """Print ERROR-level events from each Spark Declarative Pipelines (SDP) pipeline in the bundle.

    Called only when ``bundle run pipelines`` fails. Best-effort: any failure
    inside this helper is logged and swallowed so we never mask the original
    pipeline failure with a diagnostics-tool failure.
    """
    print("")
    print("=" * 78)
    print("[STAGE 6 diag] bundle run pipelines failed — fetching SDP event log")
    print("=" * 78)
    cli = shutil.which("databricks")
    if not cli:
        print("[STAGE 6 diag] databricks CLI not on PATH; skipping event-log fetch.")
        return
    base = [cli]
    if profile:
        base.extend(["--profile", profile])
    summary_cmd = base + ["bundle", "summary", "--target", "dev", "--output", "json"]
    try:
        proc = subprocess.run(
            summary_cmd, cwd=bundle_dir,
            capture_output=True, text=True, check=False,
        )
    except Exception as exc:  # pragma: no cover - defensive
        print(f"[STAGE 6 diag] could not run `bundle summary`: {exc}")
        return
    if proc.returncode != 0:
        print(
            f"[STAGE 6 diag] `bundle summary` exited {proc.returncode}; "
            f"skipping event-log fetch.\nstderr: {proc.stderr.strip()[:500]}"
        )
        return
    try:
        summary = json.loads(proc.stdout)
    except Exception as exc:
        print(f"[STAGE 6 diag] could not parse `bundle summary` JSON: {exc}")
        return
    pipelines = ((summary.get("resources") or {}).get("pipelines") or {})
    if not pipelines:
        print("[STAGE 6 diag] no pipelines found in bundle summary.")
        return
    try:
        from databricks.sdk import WorkspaceClient  # local import: optional dep
    except Exception as exc:
        print(f"[STAGE 6 diag] databricks-sdk not importable: {exc}")
        return
    try:
        ws = WorkspaceClient(profile=profile) if profile else WorkspaceClient()
    except Exception as exc:
        print(f"[STAGE 6 diag] could not build WorkspaceClient: {exc}")
        return
    for pkey, pdef in sorted(pipelines.items()):
        pid = pdef.get("id")
        pname = pdef.get("name", pkey)
        if not pid:
            continue
        print(f"\n[STAGE 6 diag] --- pipeline {pkey!r} (id={pid}, name={pname!r}) ---")
        try:
            events = list(ws.pipelines.list_pipeline_events(
                pipeline_id=pid, max_results=100,
            ))
        except Exception as exc:
            print(f"  [STAGE 6 diag] list_pipeline_events failed: {exc}")
            continue
        errors = [e for e in events if str(getattr(e, "level", "")).upper().endswith("ERROR")]
        if not errors:
            print("  [STAGE 6 diag] no ERROR-level events found in last 100 events.")
            continue
        # Newest first; show up to the 5 most recent error events.
        for ev in errors[:5]:
            ts = getattr(ev, "timestamp", "")
            etype = getattr(ev, "event_type", "")
            msg = getattr(ev, "message", "")
            err_obj = getattr(ev, "error", None)
            print(f"  [{ts}] {etype}: {msg}")
            if err_obj is not None:
                exceptions = getattr(err_obj, "exceptions", None) or []
                for ex in exceptions[:3]:
                    cls = getattr(ex, "class_name", "")
                    text = getattr(ex, "message", "")
                    print(f"    -> {cls}: {text}")
        if len(errors) > 5:
            print(f"  [STAGE 6 diag] ...and {len(errors) - 5} more ERROR event(s).")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _selected_scenarios(name: str) -> List[Scenario]:
    if name == "all":
        return list(SCENARIOS.values())
    if name not in SCENARIOS:
        raise SystemExit(f"Unknown scenario {name!r}. Choose one of: all, {', '.join(SCENARIOS)}")
    return [SCENARIOS[name]]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", maxsplit=1)[0])
    parser.add_argument("--scenario", default="all",
                        choices=["cloudfiles", "cloudfiles_combined",
                                 "multi_pipeline_cloudfiles", "kafka",
                                 "eventhub", "delta", "all"],
                        help="Which source scenario to run (default: all). "
                             "`cloudfiles` renders pipeline_mode=split; "
                             "`cloudfiles_combined` renders pipeline_mode=combined "
                             "(bronze+silver in ONE SDP Pipeline, same demo data).")
    parser.add_argument("--uc-catalog-name", required=True,
                        help="Unity Catalog catalog the demo writes into. "
                             "Substituted into the answers files and CSVs.")
    parser.add_argument("--out-dir", default="demo_runs",
                        help="Where the scaffolded bundles get written (default: demo_runs/)")
    parser.add_argument("--profile", default=None,
                        help="Databricks CLI profile (used for deploy + prepare-wheel)")
    parser.add_argument("--uc-schema", default=None,
                        help="UC schema name. Drives BOTH (a) the wheel/demo-data UC volume "
                             "schema used by --apply-prepare-wheel, AND (b) the rendered "
                             "bundle's target schemas: sdp_meta_schema=<uc-schema>, "
                             "bronze_target_schema=<uc-schema>_bronze, "
                             "silver_target_schema=<uc-schema>_silver. "
                             "When omitted, per-scenario defaults are used "
                             "(e.g. sdp_meta_dab_demo_cf for the cloudfiles scenarios). "
                             "WARNING: with --scenario all + --uc-schema, every scenario "
                             "shares the same target schemas and bundle deploys will "
                             "collide on dataflowspec / bronze / silver tables.")
    parser.add_argument("--uc-volume", default=None,
                        help="UC volume for prepare-wheel (only with --apply-prepare-wheel)")
    parser.add_argument("--apply-prepare-wheel", action="store_true",
                        help="Actually build + upload the sdp-meta wheel. "
                             "Without this flag, the demo pins a placeholder.")
    parser.add_argument("--pip-index-url", default=os.environ.get("PIP_INDEX_URL"),
                        help="Forwarded to `pip wheel` as --index-url. Use this "
                             "when pypi.org is not reachable from your network "
                             "(e.g. https://pypi.internal.example.com/simple). "
                             "Defaults to $PIP_INDEX_URL.")
    parser.add_argument("--pip-extra-index-url", action="append", default=None,
                        help="Forwarded to `pip wheel` as --extra-index-url. "
                             "Can be passed multiple times. Defaults to "
                             "$PIP_EXTRA_INDEX_URL (space-separated).")
    parser.add_argument("--apply-recipe", action="store_true",
                        help="Pass --apply to the recipe so it actually appends to onboarding. "
                             "Without this flag, the recipe runs in dry-run mode.")
    parser.add_argument("--apply-deploy", action="store_true",
                        help="Run STAGE 6 (deploy + run onboarding + pipelines). "
                             "Without this flag, the demo stops after STAGE 5.")
    parser.add_argument("--no-clean", dest="clean", action="store_false",
                        help="Do NOT remove an existing bundle directory before "
                             "STAGE 1 re-scaffolds. Default: clean (so re-runs are "
                             "idempotent).")
    parser.set_defaults(clean=True)
    parser.add_argument("--bundle-name-suffix", default=None,
                        help="Append this suffix to the rendered bundle name "
                             "(eg. --bundle-name-suffix v2 -> "
                             "dab_demo_cloudfiles_combined_v2). Use this to deploy "
                             "side-by-side with an existing dev deployment instead "
                             "of overwriting it. Must contain only [a-z0-9_].")
    parser.add_argument("--unique-bundle-name", action="store_true",
                        help="Append a UTC timestamp _YYYYMMDD_HHMMSS to the "
                             "bundle name so EVERY run produces a fresh deployment "
                             "(combine with --bundle-name-suffix if you also want a "
                             "human-readable tag). Without this flag, DAB dev mode "
                             "intentionally OVERWRITES the previous deployment with "
                             "the same bundle.name × user (idempotent iteration).")
    parser.add_argument("--no-create-missing-uc", dest="create_missing_uc",
                        action="store_false",
                        help="Do NOT auto-create the UC schema / volume during "
                             "bundle-prepare-wheel. Default: create them if they "
                             "don't exist (catalogs are never auto-created).")
    parser.set_defaults(create_missing_uc=True)
    parser.add_argument("--uc-source-catalog", default=None,
                        help="UC catalog containing the upstream Delta tables that "
                             "the `delta` scenario's recipes/from_uc.py mirrors "
                             "into the bundle. Defaults to --uc-catalog-name when "
                             "omitted (i.e. source and target live in the same "
                             "catalog). Only used by scenarios whose recipe "
                             "references {uc_source_catalog}.")
    parser.add_argument("--uc-source-schema", default=None,
                        help="UC schema (inside --uc-source-catalog) containing "
                             "the upstream Delta tables for the `delta` scenario. "
                             "Defaults to 'staging'. Only used by scenarios whose "
                             "recipe references {uc_source_schema}.")
    args = parser.parse_args()

    # Validate UC identifiers up-front so the demo fails fast with a clear
    # error message rather than producing a bundle that breaks at deploy
    # time on a hyphenated catalog (issue #261). Strict regular SQL
    # identifier rule applied to every UC name the demo accepts.
    validate_uc_identifier(args.uc_catalog_name, kind="--uc-catalog-name")
    if args.uc_schema:
        validate_uc_identifier(args.uc_schema, kind="--uc-schema")
    if args.uc_volume:
        validate_uc_identifier(args.uc_volume, kind="--uc-volume")
    if args.uc_source_catalog:
        validate_uc_identifier(args.uc_source_catalog, kind="--uc-source-catalog")
    if args.uc_source_schema:
        validate_uc_identifier(args.uc_source_schema, kind="--uc-source-schema")

    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"Demo output dir: {out_dir}")

    failures: List[str] = []
    for scenario in _selected_scenarios(args.scenario):
        print(f"\n\n{'#' * 78}\n# SCENARIO: {scenario.name}\n# {scenario.description}\n{'#' * 78}")
        try:
            bundle_dir = stage_bundle_init(
                scenario, out_dir, args.uc_catalog_name, args.profile,
                clean=args.clean, uc_schema=args.uc_schema,
                bundle_name_suffix=args.bundle_name_suffix,
                unique_bundle_name=args.unique_bundle_name,
            )
            extras = args.pip_extra_index_url
            if not extras and os.environ.get("PIP_EXTRA_INDEX_URL"):
                extras = [u for u in os.environ["PIP_EXTRA_INDEX_URL"].split() if u]
            demo_data_volume_path = stage_prepare_wheel(
                scenario, bundle_dir,
                apply=args.apply_prepare_wheel,
                uc_catalog_name=args.uc_catalog_name,
                uc_schema=args.uc_schema, uc_volume=args.uc_volume,
                profile=args.profile,
                pip_index_url=args.pip_index_url,
                pip_extra_index_urls=extras,
                create_if_missing=args.create_missing_uc,
            )
            stage_add_flow(
                scenario, bundle_dir, args.uc_catalog_name,
                demo_data_volume_path=demo_data_volume_path,
                uc_source_catalog=args.uc_source_catalog,
                uc_source_schema=args.uc_source_schema,
            )
            stage_add_independent_pipeline(
                scenario,
                bundle_dir,
                uc_schema=args.uc_schema,
                demo_data_volume_path=demo_data_volume_path,
            )
            stage_recipe(
                scenario, bundle_dir,
                apply_recipe=args.apply_recipe,
                uc_catalog_name=args.uc_catalog_name,
                profile=args.profile,
                uc_source_catalog=args.uc_source_catalog,
                uc_source_schema=args.uc_source_schema,
                create_missing_uc=args.create_missing_uc,
                demo_data_volume_path=demo_data_volume_path,
            )
            stage_validate(bundle_dir, args.profile)
            if args.apply_deploy:
                stage_deploy_and_run(
                    bundle_dir, args.profile,
                    uc_catalog=args.uc_catalog_name,
                    uc_schema=args.uc_schema,
                    uc_volume=args.uc_volume,
                )
        except SystemExit as exc:
            print(f"\n[SCENARIO {scenario.name}] FAILED: {exc}")
            failures.append(scenario.name)

    print("\n" + "=" * 78)
    if failures:
        print(f"Done with errors. Failed scenarios: {', '.join(failures)}")
        return 1
    print("Done. All requested scenarios completed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
