# sdp-meta DAB (Declarative Automation Bundle) interface

A summary of the new bundle-based deployment path for sdp-meta. This is the recommended way to deploy sdp-meta in any environment beyond ad-hoc exploration: state lives in git, you get `dev`/`prod` promotion for free, and there is no interactive CLI step.

## TL;DR

```bash
databricks labs install sdp-meta

# Fast path: zero prompts, dev-friendly defaults (cloudFiles + bronze_silver +
# split + pypi). Edit resources/variables.yml afterwards to point at your real
# catalog/schema/sdp_meta_dependency.
databricks labs sdp-meta bundle-init --quickstart   # one shot, no prompts
# OR walk through the prompts:
databricks labs sdp-meta bundle-init                # 13 prompts
cd <bundle_name>

# (optional, until sdp-meta is on PyPI)
databricks labs sdp-meta bundle-prepare-wheel

# add real flows — interactively or in bulk from CSV
databricks labs sdp-meta bundle-add-flow      # single flow, interactive prompts
# OR
databricks labs sdp-meta bundle-add-flow      # then choose csv mode + path
# starter CSV is shipped at conf/samples/flows.csv

databricks labs sdp-meta bundle-validate
databricks bundle deploy --target dev
databricks bundle run onboarding --target dev
databricks bundle run pipelines  --target dev
```

## What you get

| Artifact | Purpose |
| --- | --- |
| `databricks.yml` | Bundle definition with `dev` (development mode) and `prod` (production mode) targets. |
| `resources/variables.yml` | Every knob exposed as a bundle variable so you override per-target without editing other files. |
| `resources/sdp_meta_onboarding_job.yml` | `python_wheel_task` that calls `databricks_labs_sdp_meta:run` to write/update dataflow specs. |
| `resources/sdp_meta_pipelines.yml` | Lakeflow Spark Declarative Pipeline(s) plus a job that runs them end-to-end. |
| `notebooks/init_sdp_meta_pipeline.py` | Runner notebook installed by each pipeline; pip-installs sdp-meta from `${var.sdp_meta_dependency}` and calls `DataflowPipeline.invoke_dlt_pipeline(spark, layer)`. |
| `conf/onboarding.{yml,json}` | Seeded flow definition. Branches at render time on the chosen `source_format`. |
| `conf/silver_transformations.{yml,json}` | Per-target SELECT projections (silver layers only). |
| `conf/dqe/example_table/bronze_expectations.{yml,json}` | SDP expectations applied to the bronze table. |
| `README.md` | Generated, layer-aware getting-started doc inside the bundle. |
| `.gitignore` | Ignores `.databricks/`, `.venv/`, `__pycache__/`. |

## CLI commands

| Command | What it does |
| --- | --- |
| `databricks labs sdp-meta bundle-init` | Scaffolds a new bundle from the packaged template via `databricks bundle init`. Prompts for 13 knobs (see below). Pass `--quickstart` to skip every prompt and use developer defaults; pass `--output-dir <path>` to scaffold somewhere other than the current directory. |
| `databricks labs sdp-meta bundle-prepare-wheel` | Builds the local sdp-meta wheel and uploads it to a UC volume. Prints the resulting `/Volumes/...` path so you can paste it into `resources/variables.yml` as the `sdp_meta_dependency` default. |
| `databricks labs sdp-meta bundle-validate` | Runs `databricks bundle validate` plus sdp-meta-specific static checks. |
| `databricks labs sdp-meta bundle-add-flow` | Appends one or more flow entries to the bundle's onboarding file. Two modes: single (interactive prompts per source format) and CSV (batch). Auto-increments `data_flow_id`, refuses to write on collisions, preserves YAML/JSON. |
| `databricks labs sdp-meta bundle-add-pipeline` | Adds an independently configured pipeline topology and wires its task(s) into the bundle's `pipelines` job. |

`bundle-validate` catches common authoring mistakes that the upstream `databricks bundle validate` doesn't:

- onboarding file referenced by `variables.yml` is missing
- onboarding file is not a YAML/JSON list of flow dicts
- a pipeline's `bronze.group` / `silver.group` is not used by any onboarding flow
- a pipeline is missing layer-specific dataflowspec table configuration
- a pipeline is missing or duplicated in the `pipelines` job
- a split silver task does not depend on a bronze task for the same group
- `<your-...>` placeholders left in `conf/onboarding.{yml,json}` (eventhub keys, kafka brokers, etc.)
- `<your-...>` placeholders left in `databricks.yml` itself — most commonly an uncommented `run_as.service_principal_name` block that still says `<your-prod-service-principal-application-id>`. (Comments are stripped at YAML parse time, so the shipped commented block is silent until you uncomment it.)

### bundle-init --quickstart

`--quickstart` is a non-interactive shortcut. It writes a `databricks bundle init --config-file` JSON pre-answering every template prompt with developer-friendly defaults — `cloudFiles + bronze_silver + split + pypi + onboarding.yml + dataflow_group=my_group + uc_catalog_name=main` — and then runs the bundle scaffolder. The only thing it deliberately leaves as the `__SET_ME__` sentinel is `sdp_meta_dependency`, so `bundle-validate` will still catch it before deploy. Typical first-time flow on a fresh checkout:

```bash
databricks labs sdp-meta bundle-init --quickstart
cd my_sdp_meta_pipeline
# point sdp_meta_dependency at a real PyPI coord OR a UC volume wheel:
sed -i.bak 's/__SET_ME__/databricks-labs-sdp-meta==0.1.0/' resources/variables.yml
databricks labs sdp-meta bundle-validate
databricks bundle deploy --target dev
```

## Template prompts

Listed in the order the template asks for them.

| # | Prompt | Default | Notes |
|---|---|---|---|
| 1 | `bundle_name` | `my_sdp_meta_pipeline` | Folder name and prefix for every job/pipeline name. |
| 2 | `uc_catalog_name` | `main` | UC catalog holding the sdp-meta schema and target schemas. |
| 3 | `sdp_meta_schema` | `sdp_meta_dataflowspecs` | Holds the `bronze_dataflowspec` / `silver_dataflowspec` tables. |
| 4 | `bronze_target_schema` | `sdp_meta_bronze` | Where the bronze SDP Pipeline writes its tables. |
| 5 | `silver_target_schema` | `sdp_meta_silver` | Where the silver SDP Pipeline writes its tables. |
| 6 | `layer` | `bronze_silver` | One of `bronze`, `silver`, `bronze_silver`. |
| 7 | `pipeline_mode` | `split` | Only used when `layer=bronze_silver`. `split` deploys two SDP Pipelines (silver depends on bronze); `combined` deploys ONE SDP Pipeline that materializes both layers in the same DAG. |
| 8 | `source_format` | `cloudFiles` | Seed for the example flow: `cloudFiles` / `delta` / `kafka` / `eventhub` / `snapshot`. |
| 9 | `onboarding_file_format` | `yaml` | `yaml` or `json` — chosen format flows into the seeded onboarding, silver, and DQE files. |
| 10 | `dataflow_group` | `my_group` | Ties flows in the onboarding file to the pipeline's `*.group` configuration. |
| 11 | `wheel_source` | `pypi` | `pypi` or `volume_path`. Picks the install style; `bundle-validate` enforces that `sdp_meta_dependency` matches. |
| 12 | `sdp_meta_dependency` | `__SET_ME__` | Concrete install spec. PyPI coordinate (e.g. `databricks-labs-sdp-meta==0.1.0`) or `/Volumes/...` wheel path. The `__SET_ME__` sentinel is rejected by both `bundle-validate` and the runner notebook so you can't accidentally deploy with a placeholder. |
| 13 | `author` | `sdp-meta-user` | Written to the `import_author` column on dataflowspec rows. |

## Choosing pipeline_mode (when `layer=bronze_silver`)

- **`split` (default)** — two SDP Pipelines (`bronze`, `silver`), one job that runs silver after bronze. Each pipeline has its own update history; you can recompute one without disturbing the other. Recommended when the layers have different SLAs or lifecycles.
- **`combined`** — one SDP Pipeline (`bronze_silver`) whose `configuration.layer = bronze_silver` makes sdp-meta build both layers in the same DAG. Lower fixed overhead (one pipeline to schedule and monitor), one update cycle, single set of expectations metrics. Recommended when bronze→silver is a tight unit.

You can switch later: change `pipeline_mode` in `resources/variables.yml`, redeploy, re-run.

## Choosing wheel_source (PyPI vs local wheel)

The `wheel_source` prompt decides which install style this bundle uses; `sdp_meta_dependency` carries the concrete spec. Both feed the same code path: the onboarding job's `environments.dependencies` and the pipeline runner notebook's `%pip install`.

### When to pick `pypi`

Once `databricks-labs-sdp-meta` is published, this is the steady-state choice. At the prompts:

```
Where sdp-meta is installed from. ... [pypi]:        pypi
Concrete value the bundle pins for installs. ... [__SET_ME__]:    databricks-labs-sdp-meta==0.1.0
```

If you keep `__SET_ME__`, bundle-validate will fail (good), so you can't accidentally deploy a placeholder. You can also paste the sentinel now and edit `resources/variables.yml` later.

### When to pick `volume_path`

This is the right choice today, while sdp-meta isn't on PyPI. Two-step flow:

```bash
# 1. At the bundle-init prompts:
#       wheel_source:        volume_path
#       sdp_meta_dependency: __SET_ME__         (just press Enter)

# 2. Build + upload the wheel; reuse the same UC catalog/schema you picked above:
cd <bundle_name>
databricks labs sdp-meta bundle-prepare-wheel
#   → prints: /Volumes/<cat>/<sch>/<vol>/databricks_labs_sdp_meta-0.1.0-py3-none-any.whl

# 3. Paste that into resources/variables.yml:
#       sdp_meta_dependency:
#         default: /Volumes/<cat>/<sch>/<vol>/databricks_labs_sdp_meta-0.1.0-py3-none-any.whl

# 4. Validate
databricks labs sdp-meta bundle-validate
```

### Guard rails

`bundle-validate` enforces three rules:

- `sdp_meta_dependency == "__SET_ME__"` → fail (the placeholder was never replaced).
- `wheel_source = volume_path` but `sdp_meta_dependency` is not a `/Volumes/...` path → fail.
- `wheel_source = pypi` but `sdp_meta_dependency` IS a `/Volumes/...` path → fail.

The runner notebook also fails fast (with the same actionable message) before the `%pip install` step if it sees `__SET_ME__` at runtime — useful if validate was skipped.

## Adding flows with `bundle-add-flow`

The seeded `conf/onboarding.*` ships with a single example flow. For real-world bundles with N tables, the `bundle-add-flow` command appends properly-shaped flow entries — pulling defaults (catalog, target schemas, layer, dataflow group, file format) from the bundle's own `variables.yml` so every new flow stays consistent with the surrounding bundle.

### Single flow (interactive)

```bash
databricks labs sdp-meta bundle-add-flow
# Bundle directory [.]:           .
# Add a single flow or batch from CSV [single|csv]:    single
# Dry run [False|True]:           False
# Source format [cloudFiles|delta|kafka|eventhub|snapshot]:    cloudFiles
# Bronze table name:              orders
# Silver table name (blank = same as bronze):
# data_flow_id (use `auto`):      auto
# data_flow_group (blank = bundle default):
# Source path:                    /Volumes/raw/landing/orders/
# Source schema DDL path (blank to skip):
# → Appended 1 flow(s) to conf/onboarding.yml.
```

### Batch from CSV

A starter file is shipped at `conf/samples/flows.csv`. Edit (or replace) it, then:

```bash
databricks labs sdp-meta bundle-add-flow
# pick mode = csv, point at conf/samples/flows.csv
```

CSV columns recognized (extras silently ignored): `source_format`, `source_path`, `source_database`, `source_table`, `source_schema_path`, `kafka_bootstrap_servers`, `kafka_topic`, `starting_offsets`, `snapshot_format`, `bronze_table`, `silver_table`, `data_flow_id`, `data_flow_group`, `source_system`.

### Behavior

- **`data_flow_id`** — `auto` increments from `max(existing numeric ids) + 1` (starts at `100` if file is empty). Explicit ids that collide with existing ones are rejected with a non-zero exit code; nothing is written.
- **YAML/JSON respected** — the existing onboarding file's format is preserved on write.
- **Layer-aware** — bronze-only bundles only get bronze keys, silver-only get silver keys, etc.; the same defaults the template uses (target schemas, DQE/transformation paths) are applied.
- **`--dry-run`** — preview the flow ids, source formats, and table names that would be appended without modifying the file.
- **`source_format` validation** — `kafka` requires `kafka_bootstrap_servers` + `kafka_topic`; `eventhub` requires `kafka_topic`. Failing fast keeps malformed flows out of the file.

### Pair with `bundle-validate`

After appending, run:

```bash
databricks labs sdp-meta bundle-validate
```

The sanity checks confirm that each pipeline's configured group exists in the
onboarding file, every layer has the required dataflowspec-table settings, and
the `pipelines` job references each pipeline exactly once with valid split
dependencies.

## Generating flows programmatically (100s of tables)

`bundle-add-flow` is a thin CLI in front of a reusable engine — you can call it directly from Python whenever you have more than a handful of tables to onboard. The primitive is the `FlowSpec` dataclass; anything you can express as `List[FlowSpec]` is a valid input to `bundle_add_flow`.

```python
from databricks.labs.sdp_meta.bundle import (
    BundleAddFlowCommand, FlowSpec, bundle_add_flow,
)

flows = [
    FlowSpec(
        source_format="cloudFiles",
        source_path=f"/Volumes/raw/landing/{name}/",
        bronze_table=name,
        silver_table=name,
    )
    for name in my_table_names  # 1, 10, 1000 — same code path
]

bundle_add_flow(BundleAddFlowCommand(
    bundle_dir="./my_sdp_meta_pipeline",
    flows=flows,
    dry_run=True,   # preview first; flip to False to write
))
```

The engine handles, on every call, the things you'd otherwise have to re-implement:

| Concern | What the engine does |
| --- | --- |
| `data_flow_id` collisions | Auto-increments unset ids from `max(existing numeric id) + 1` (starts at `100` for an empty file). Explicit ids that already exist abort the write. |
| File format | Reads the existing `conf/onboarding.{yml,json}` and preserves its format. |
| Bundle defaults | Pulls catalog, target schemas, layer, dataflow_group, and DQE paths from the bundle's `resources/variables.yml`. |
| Layer-awareness | Bronze-only bundles emit only bronze keys; silver-only emit only silver; combined emit both. |
| Atomicity | The list is validated end-to-end; either every flow is appended or nothing is (no partial writes). |
| Source-format validation | Per-format required fields are enforced (e.g. `kafka` requires `kafka_bootstrap_servers` + `kafka_topic`). |

### Four shipped recipes (rendered into every bundle under `recipes/`)

`bundle-init` writes four runnable starter scripts — one per common source shape — so a 100-table onboarding is one command, not a 2,000-line YAML edit.

| Recipe | Source shape it covers | Discovery / input |
| --- | --- | --- |
| `recipes/from_uc.py` | N upstream **Delta** tables already in a UC schema. | `WorkspaceClient.tables.list(catalog, schema)` (+ optional `--table-filter`) |
| `recipes/from_volume.py` | Files landing as **one cloudFiles subdirectory per table** under a UC volume. | `WorkspaceClient.files.list_directory_contents(volume_path)` |
| `recipes/from_topics.py` | N **Kafka or Event Hubs** topics on the same broker / namespace. | `--topics a,b,c` or `--topics-file conf/samples/topics.txt` |
| `recipes/from_inventory.py` | Hand-curated list (Excel export, JIRA ticket, governance tool); apply naming conventions. | An `INVENTORY` dict (or any `List[FlowSpec]` you build) |

All four default to dry-run; pass `--apply` to actually write. Examples:

```bash
cd my_sdp_meta_pipeline

# A) 10 Delta tables upstream in raw.landing
python recipes/from_uc.py --source-catalog raw --source-schema landing
python recipes/from_uc.py --source-catalog raw --source-schema landing --apply

# B) cloudFiles paths under a UC volume (one subdir per table)
python recipes/from_volume.py --volume-path /Volumes/raw/landing/files
python recipes/from_volume.py --volume-path /Volumes/raw/landing/files --apply

# C) 10 Kafka topics off the same broker
python recipes/from_topics.py --source-format kafka \
    --bootstrap-servers "broker1:9092,broker2:9092" \
    --topics-file conf/samples/topics.txt --apply

# D) 10 Event Hubs in the same namespace
python recipes/from_topics.py --source-format eventhub \
    --bootstrap-servers "my-eh-namespace.servicebus.windows.net:9093" \
    --topics orders,customers,events,payments,shipments --apply

# E) Edit INVENTORY in from_inventory.py, then:
python recipes/from_inventory.py --apply
```

Each recipe is ~60-100 lines, uses argparse, and is templated with the bundle's catalog so the printed defaults match the surrounding bundle. Use them as starting points — copy `recipes/from_inventory.py`, swap `INVENTORY` for `pandas.read_excel(...)` or a SFDC query, and you have a fully bespoke generator that still produces collision-free, layer-aware flow entries.

#### Pattern cheatsheet — when none of the recipes fits exactly

Every shape below is a 4-line edit; the engine handles the rest.

```python
# N cloudFiles paths that don't share a parent directory:
PATHS = ["/Volumes/raw/zone_a/orders/", "s3://other-bucket/landing/events/"]
flows = [FlowSpec(source_format="cloudFiles", source_path=p,
                  bronze_table=p.rstrip('/').split('/')[-1],
                  silver_table=p.rstrip('/').split('/')[-1]) for p in PATHS]

# N fully-qualified upstream Delta tables in different schemas:
TABLES = ["raw.landing.orders", "ops.audit.events"]
flows = [FlowSpec(source_format="delta",
                  source_database=fqn.rsplit('.', 1)[0],
                  source_table=fqn.rsplit('.', 1)[1],
                  bronze_table=fqn.rsplit('.', 1)[1],
                  silver_table=fqn.rsplit('.', 1)[1]) for fqn in TABLES]
```

Mixed source formats in one bundle? Build separate lists, concatenate them, pass them all in one `bundle_add_flow` call — writes are atomic, so either every flow lands or nothing does.

### Operational pattern at scale

For an N-hundred-table onboarding the recommended loop is:

1. Run a recipe with `--dry-run` (the default). Inspect the printed flow ids and table names.
2. `git diff conf/onboarding.*` after `--apply` to review exactly what got appended.
3. `databricks labs sdp-meta bundle-validate` — the sanity checks re-parse the file, re-check `data_flow_group`/topology consistency, and confirm `wheel_source` ↔ `sdp_meta_dependency` still match.
4. `databricks bundle deploy --target dev && databricks bundle run onboarding --target dev` — the onboarding job upserts the new dataflow specs.
5. `databricks bundle run pipelines --target dev` — Lakeflow does incremental processing; only newly-added flows incur first-run backfill.

## Generated topology by `(layer, pipeline_mode)`

| `layer` | `pipeline_mode` | Pipelines emitted | `pipelines` job tasks |
| --- | --- | --- | --- |
| `bronze` | n/a | `bronze` | `bronze` |
| `silver` | n/a | `silver` | `silver` |
| `bronze_silver` | `split` (default) | `bronze`, `silver` | `bronze`, `silver` (silver depends_on bronze) |
| `bronze_silver` | `combined` | `bronze_silver` | `bronze_silver` (single task) |

## Lifecycle

```bash
# 1. Scaffold once
databricks labs sdp-meta bundle-init

cd <bundle_name>

# 2. (one-time, if not on PyPI) build + upload wheel
databricks labs sdp-meta bundle-prepare-wheel
# → paste printed /Volumes/... path into resources/variables.yml

# 3. Edit conf/onboarding.* with real sources/targets
$EDITOR conf/onboarding.yml

# 4. Validate
databricks labs sdp-meta bundle-validate

# 5. Deploy + run in dev
databricks bundle deploy --target dev
databricks bundle run onboarding --target dev   # writes dataflow specs
databricks bundle run pipelines  --target dev   # runs SDP

# 6. Promote to prod
databricks bundle deploy --target prod
databricks bundle run onboarding --target prod
databricks bundle run pipelines  --target prod
```

Per-target overrides (different catalogs / schemas / `sdp_meta_dependency`) live under `targets.<name>.variables` in `databricks.yml`.

## Production deployments and `run_as`

By default, both `dev` and `prod` targets run as the user invoking `databricks bundle deploy / run`. That's fine for ad-hoc / manual prod deploys, but for CI/CD you almost always want prod jobs and pipelines to run under a service principal so the bundle is decoupled from any one person's account (rotations, offboarding, vacation, etc.).

The shipped `databricks.yml` includes a commented `run_as` block in the prod target as a copy-pasteable starting point:

```yaml
targets:
  prod:
    mode: production
    permissions:
      - group_name: users
        level: CAN_VIEW
    # Recommended for CI/CD: run prod jobs/pipelines under a service principal
    # ...
    # run_as:
    #   service_principal_name: <your-prod-service-principal-application-id>
    variables:
      development_enabled: false
```

To opt in:

1. Uncomment the `run_as:` lines in `databricks.yml`.
2. Replace `<your-prod-service-principal-application-id>` with the **application_id** (a UUID) of a workspace-admin-granted service principal — not the display name. (`databricks service-principals list` shows both.)
3. Run `databricks labs sdp-meta bundle-validate` — it will fail loudly if you uncommented but forgot to substitute, and pass once you have a real value.
4. `databricks bundle deploy --target prod`.

The placeholder is detected by walking the parsed `databricks.yml` (PyYAML drops comments at parse time, so the commented block is silent until you uncomment), so the same convention extends to anything else you add via `<your-...>` markers — e.g. an admin user_name on `permissions`, a workspace host pin, etc.

## Extending the seeded bundle

| You want to | Edit | Then |
| --- | --- | --- |
| Add a flow | `databricks labs sdp-meta bundle-add-flow` (or hand-edit `conf/onboarding.*`) | `databricks bundle run onboarding` |
| Add many flows in bulk | edit `conf/samples/flows.csv` (or your own CSV) → `databricks labs sdp-meta bundle-add-flow` (csv mode) | `databricks bundle run onboarding` |
| Add 100s of flows from UC / a volume / an inventory | run a recipe under `recipes/` (see [Generating flows programmatically](#generating-flows-programmatically-100s-of-tables)) | `databricks bundle run onboarding` |
| Add DQE rules to a table | create `conf/dqe/<table>/bronze_expectations.*` and reference it from the flow's `bronze_data_quality_expectations_json_*` field | `databricks bundle run onboarding` then `bundle run pipelines` |
| Customize silver projections | edit `conf/silver_transformations.*` | `databricks bundle run pipelines` |
| Multiple groups in one bundle | duplicate the pipeline in `resources/sdp_meta_pipelines.yml` and point each at a different `*.group` value | `databricks bundle deploy` |
| Different prod catalog | add a per-target override under `targets.prod.variables.uc_catalog_name` in `databricks.yml` | `databricks bundle deploy --target prod` |

## Extending the runner notebook

The seeded `notebooks/init_sdp_meta_pipeline.py` is intentionally minimal — cell 1 installs the wheel, cell 2 invokes the framework:

```python
layer = spark.conf.get("layer", None)
from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline

DataflowPipeline.invoke_dlt_pipeline(spark, layer)
```

`invoke_dlt_pipeline` accepts several optional kwargs that hook user code into the pipeline. Three patterns ship as worked examples in [`examples/`](./examples/) — each is a one- or two-cell change to the runner notebook. Pick the variant that matches your source and copy the relevant block in **above** the `invoke_dlt_pipeline(...)` call, then update that call to pass the new function(s).

> Important: keep cell 1 of the notebook untouched. Serverless SDP rejects the runner with `SyntaxError: incomplete input` if there is any multi-line Python before `%pip install` in cell 1. Add your callbacks in **cell 2** (or in a new third cell, with a `# COMMAND ----------` break before it).

### Pattern 1 — snapshot ingestion (`source_format: snapshot`)

When your bronze flow uses `source_format: snapshot`, sdp-meta needs a callback that returns the next snapshot DataFrame and version on each pipeline tick. See [`examples/sdp_meta_pipeline_snapshot.ipynb`](./examples/sdp_meta_pipeline_snapshot.ipynb) for the canonical implementation.

Add this above the `invoke_dlt_pipeline(...)` call in cell 2:

```python
from databricks.labs.sdp_meta.dataflow_spec import BronzeDataflowSpec

def _exists(path: str) -> bool:
    try:
        return dbutils.fs.ls(path) is not None
    except Exception:
        return False

def bronze_next_snapshot_and_version(latest_snapshot_version, dataflow_spec):
    """Return (snapshot_df, version) or None when no new snapshot is available.
    Customize the path layout to match how your team drops snapshot files."""
    bronze: BronzeDataflowSpec = dataflow_spec
    next_version = (latest_snapshot_version or 0) + 1
    snapshot_root = bronze.sourceDetails["path"]
    snapshot_path = f"{snapshot_root}{next_version}.csv"
    if not _exists(snapshot_path):
        return None
    snapshot_df = (
        spark.read.format(bronze.sourceDetails["snapshot_format"])
        .options(**bronze.readerConfigOptions)
        .load(snapshot_path)
    )
    return snapshot_df, next_version
```

Then change the invoke line to:

```python
DataflowPipeline.invoke_dlt_pipeline(
    spark, layer,
    bronze_next_snapshot_and_version=bronze_next_snapshot_and_version,
)
```

### Pattern 2 — apply changes from snapshot (CDC into silver)

Same idea as Pattern 1 but the callback feeds the silver-layer `apply_changes_from_snapshot` codepath that materializes type-2 history into your silver tables. The current kwarg is `silver_next_snapshot_and_version`. See [`examples/sdp_meta_pipeline_apply_changes_from_snapshot.ipynb`](./examples/sdp_meta_pipeline_apply_changes_from_snapshot.ipynb) for the worked example (note: the example uses an older kwarg name `next_snapshot_and_version`; in the current `databricks.labs.sdp_meta` namespace it's `silver_next_snapshot_and_version`).

Define `silver_next_snapshot_and_version(...)` with the same shape as Pattern 1's bronze callback, then:

```python
DataflowPipeline.invoke_dlt_pipeline(
    spark, layer,
    silver_next_snapshot_and_version=silver_next_snapshot_and_version,
)
```

### Pattern 3 — custom bronze / silver transformations

For any non-trivial transformation that doesn't fit a `silver_transformations.*` `select_exp`, register a Python function. sdp-meta calls it with the inbound DataFrame and the matching `DataflowSpec`. See [`examples/sdp_meta_pipeline_custom_transform.ipynb`](./examples/sdp_meta_pipeline_custom_transform.ipynb).

```python
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

def bronze_custom_transform_func(input_df: DataFrame, dataflow_spec) -> DataFrame:
    return input_df.withColumn("ingested_via", lit("sdp_meta"))

def silver_custom_transform_func(input_df: DataFrame, dataflow_spec) -> DataFrame:
    return input_df.withColumn("silver_marker", lit("v1"))
```

```python
DataflowPipeline.invoke_dlt_pipeline(
    spark, layer,
    bronze_custom_transform_func=bronze_custom_transform_func,
    silver_custom_transform_func=silver_custom_transform_func,
)
```

The kwargs are independent — pass only the layer(s) you actually customize. They also compose with Pattern 1/2 (you can pass snapshot callbacks **and** transform funcs in the same call).

### Why we don't ship these as a template prompt

These hooks are user code: the snapshot path layout, the CDC version scheme, the transform logic — none of it is something the template can guess. Shipping stub callbacks would just create code every user has to delete. The runner notebook is the right place to add them, and the `examples/` notebooks above are the canonical reference implementations.

## Where everything lives

```
src/databricks/labs/sdp_meta/
├── bundle.py                                 # bundle-init / prepare-wheel / validate handlers
└── templates/
    └── dab/
        ├── README.md                         # template maintainer notes
        ├── databricks_template_schema.json   # 12 prompt definitions
        └── template/
            └── {{.bundle_name}}/             # rendered into <bundle_name>/
                ├── databricks.yml.tmpl
                ├── README.md.tmpl
                ├── .gitignore
                ├── resources/
                │   ├── variables.yml.tmpl
                │   ├── sdp_meta_onboarding_job.yml.tmpl
                │   └── sdp_meta_pipelines.yml.tmpl
                ├── notebooks/
                │   └── init_sdp_meta_pipeline.py.tmpl
                ├── recipes/                          # programmatic flow generators
                │   ├── README.md.tmpl
                │   ├── from_uc.py.tmpl
                │   ├── from_volume.py.tmpl
                │   ├── from_topics.py.tmpl
                │   └── from_inventory.py.tmpl
                └── conf/
                    ├── onboarding.{{...}}.tmpl
                    ├── samples/flows.csv.tmpl       # starter CSV for bundle-add-flow
                    ├── samples/topics.txt.tmpl     # starter topics list for from_topics.py
                    ├── silver_transformations.{{...}}.tmpl
                    └── dqe/example_table/bronze_expectations.{{...}}.tmpl

docs/content/getting_started/deploy_with_dabs.md   # user-facing docs page
labs.yml                                            # registers bundle-init / prepare-wheel / validate commands
MANIFEST.in                                         # ships templates/ inside the wheel
setup.py                                            # package_data covers templates/**/*
.github/workflows/onpush.yml                        # installs Databricks CLI for end-to-end render tests
tests/test_bundle.py                                # 25 tests (template shape, sanity rules, CLI handlers, e2e render)
```

## Test coverage

Run the bundle tests:

```bash
python -m pytest tests/test_bundle.py -v
```

61 tests organized into:

- **TemplateLayoutTests** (3) — schema is valid JSON, all required prompts present (incl. `wheel_source`, `pipeline_mode`), onboarding job uses the correct wheel entry point.
- **SanityChecksTests** (13) — every branch of `_sdp_meta_sanity_checks`: happy paths for split/combined, layer/pipeline mismatches in both directions, missing onboarding file, missing `variables.yml`, dataflow_group mismatch, combined-vs-split conflicts, sentinel-dependency rejection, wheel_source/value mismatch in both directions.
- **BundleInitUnitTests** (3) — argv assembly, `--config-file` / `--profile` forwarding, exit-code propagation.
- **BundlePrepareWheelUnitTests** (10) — required-arg validation, mocked WorkspaceClient upload to `/Volumes/<cat>/<sch>/<vol>/<wheel>`, `--pip-index-url` forwarding, `PIP_INDEX_URL` env-var fallback, UC schema/volume auto-creation (create/skip/race/error), `VolumeType.MANAGED` enum usage.
- **BundleValidateUnitTests** (4) — clean bundle returns 0, databricks failure propagates, sanity-check failure returns 1, non-bundle dir returns 2.
- **EndToEndRenderTests** (8) — actually invoke `databricks bundle init` against the packaged template and assert structural correctness: bronze/silver/combined topologies, runner-notebook cell-1 layout, no `whl:`/`pypi:` library entries, `volume_path` sentinel render with runtime-guard verification, Kafka topics recipe. Auto-skipped if the `databricks` CLI isn't on PATH.
- **BundleAddFlowTests** (14) — engine-level coverage for `bundle-add-flow`: cloudFiles/delta/JSON/YAML round-trips, auto-id increment that ignores non-numeric ids, explicit duplicate-id rejection, dry-run is read-only, kafka required-args validation, silver fallback to bronze name, layer-aware key emission, CSV batch import, `cloudfiles_format` per-row override, format-alias normalisation, non-bundle-dir rejection, and post-add `_sdp_meta_sanity_checks` clean.
- **SilverTransformationsAutoSeedTests** (6) — auto-seeder: single flow seeds a default `select_exp: ["*"]` row, no-duplicate guard, CSV bulk path seeds all silver tables, bronze-only layer is a no-op, JSON-format round-trip, missing-file quiet no-op.

CI installs the Databricks CLI before running the suite so the end-to-end render tests run on every PR.

## How this fits with the existing demo

The pre-existing `demo/dabs/` directory in the repo is left untouched — it remains the bespoke demo for the `people` flow, including the manual local-wheel deployment. The new generic template lives under `src/databricks/labs/sdp_meta/templates/dab/` and is shipped inside the wheel so any user with `databricks labs install sdp-meta` can scaffold their own bundle without copying files out of the repo.

## See also

- [`docs/content/getting_started/deploy_with_dabs.md`](docs/content/getting_started/deploy_with_dabs.md) — public-facing getting-started doc.
- [`README.md`](README.md) → "Deploy with Declarative Automation Bundles" — top-level pointer.
- [`src/databricks/labs/sdp_meta/templates/dab/README.md`](src/databricks/labs/sdp_meta/templates/dab/README.md) — template maintainer notes (how to render locally, conventions).
- Upstream Databricks docs:
  - [Declarative Automation Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/)
  - [`databricks bundle init`](https://docs.databricks.com/dev-tools/cli/bundle-commands.html#bundle-init)
  - [Lakeflow Spark Declarative Pipelines](https://docs.databricks.com/aws/en/dlt/)
