#### Run Integration Tests
1. Install [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)
    - Once you install Databricks CLI, authenticate your current machine to a Databricks Workspace:

    ```commandline
    databricks auth login --host WORKSPACE_HOST
    ```

2. Clone sdp-meta:
    ```commandline
    git clone https://github.com/databrickslabs/sdp-meta.git
    ```

3. Navigate to project directory:
    ```commandline
    cd sdp-meta
    ```

4. Create Python virtual environment:
    ```commandline
    python -m venv .venv
    ```

5. Activate virtual environment:
    ```commandline
    source .venv/bin/activate
    ```

6. Install required packages:
    ```commandline
    # Core requirements
    pip install "PyYAML>=6.0" setuptools databricks-sdk
    
    # Development requirements
    pip install delta-spark==3.0.0 pyspark==3.5.5 pytest>=7.0.0 coverage>=7.0.0
    
    # Integration test requirements
    pip install "typer[all]==0.6.1"
    ```

7. Set environment variables:
    ```commandline
    sdp_meta_home=$(pwd)
    export PYTHONPATH=$sdp_meta_home
    ```

9. Run integration test against cloudfile or eventhub or kafka using below options. To use the Databricks profile configured using CLI then pass ```--profile <profile-name>``` to below command otherwise provide workspace url and token in command line. You will also need to provide a Unity Catalog catalog for which the schemas, tables, and files will be created in.

    By default the runner uses the **JSON** onboarding spec. To run the same test against the **YAML** onboarding spec instead, add ```--onboarding_file_format=yaml``` to any of the commands below. Either format produces equivalent test runs — the runner reads the YAML template, substitutes runtime placeholders, writes the rendered `onboarding.yml`, and feeds it through `OnboardDataflowspec` (which converts to a driver-local temp JSON before handing to Spark). See the **Onboarding file format (JSON or YAML)** section at the bottom of this README for full details.

    - 9a. Run the command for  **cloudfiles**
        ```commandline
        python integration_tests/run_integration_tests.py  --source=cloudfiles --uc_catalog_name=<<uc catalog name>> --profile=<<DEFAULT>>
        ```

      Same test, **YAML onboarding**:
        ```commandline
        python integration_tests/run_integration_tests.py --source=cloudfiles --uc_catalog_name=<<uc catalog name>> --onboarding_file_format=yaml --profile=<<DEFAULT>>
        ```

    - 9b. Run the command for **eventhub**
        ```commandline
        python integration_tests/run_integration_tests.py --uc_catalog_name=<<uc catalog name>> --source=eventhub --dltmeta_sink1=iot --eventhub_secrets_scope_name=eventhubs_creds --eventhub_namespace=int_test-standard --eventhub_port=9093 --eventhub_producer_accesskey_name=producer --eventhub_consumer_accesskey_name=consumer  --eventhub_name_append_flow=test_append_flow --eventhub_accesskey_secret_name=test_secret_name --profile=<<DEFAULT>>
        ```
    Prerequisites for eventhub integration tests:
    1. Running eventhub instance
    2. Create databricks secrets scope for eventhub keys:
       ```commandline
       databricks secrets create-scope eventhubs_creds
       ```
    3. Create databricks secrets to store producer and consumer keys using the scope created in step 2

    Required arguments for EventHubs integration test:
    1. `--eventhub_name` : Your eventhub topic
    2. `--eventhub_namespace` : Eventhub namespace
    3. `--eventhub_port` : Eventhub port
    4. `--eventhub_secrets_scope_name` : Databricks secret scope name
    5. `--eventhub_producer_accesskey_name` : Eventhub producer access key name
    6. `--eventhub_consumer_accesskey_name` : Eventhub access key name


    - 9c. Run the command for **kafka**
        ```commandline
        python integration_tests/run_integration_tests.py --uc_catalog_name=<<uc catalog name>>  --source=kafka --kafka_source_topic=sdp-meta-integration-test --kafka_sink_topic=sdp-meta_inttest_topic --kafka_source_broker=host:9092 --profile=<<DEFAULT>>
        ```
    Optional secret configuration:
    ```commandline
    --kafka_source_servers_secrets_scope_name=<<scope_name>> --kafka_source_servers_secrets_scope_key=<<scope_key>>
    --kafka_sink_servers_secret_scope_name=<<scope_name>> --kafka_sink_servers_secret_scope_key=<<scope_key>>
    ```

    Prerequisites for kafka integration tests:
    1. Running kafka instance

    Required arguments for kafka integration test:
    1. `--kafka_topic` : Your kafka topic name
    2. `--kafka_broker` : Kafka broker address
    
    - 9d. Run the command for **snapshot**
        ```commandline
        python integration_tests/run_integration_tests.py --source=snapshot --uc_catalog_name=<<uc catalog name>> --profile=<<DEFAULT>>
        ```

    - 9e. Run the command for **multi_source_cdc** (issue [#294](https://github.com/databrickslabs/sdp-meta/issues/294))
        ```commandline
        python integration_tests/run_integration_tests.py --source=multi_source_cdc --uc_catalog_name=<<uc catalog name>> --profile=<<DEFAULT>>
        ```

        Same test, **YAML onboarding**:
        ```commandline
        python integration_tests/run_integration_tests.py --source=multi_source_cdc --uc_catalog_name=<<uc catalog name>> --onboarding_file_format=yaml --profile=<<DEFAULT>>
        ```

        End-to-end checks for the multi-source AUTO CDC code path. Seeds three regional bronze CDC tables (`customers_us_cdc`, `customers_eu_cdc`, `customers_apac_cdc`) — each with a different source column shape on purpose (US: `id`/`firstname`/`operation`; EU: `customer_id`/`given_name`/`change_type`; APAC: `cust_id`/`fname`/`op`) — then runs a single silver pipeline that merges all three into one unified `customers` SCD-1 table via `silver_cdc_apply_changes_flows` with per-flow `select_exp` normalization. The validator asserts per-region bronze counts, the silver live-row total, the per-region silver breakdown (proves each per-flow `select_exp` actually ran), and the exact surviving `customer_id` set.

        The workflow is a minimal 3-task fan-in: `setup_sdp_meta_pipeline_spec → sdp-meta-pipeline → validate_results` (no A2 incremental step, no publish-events step). The `sdp-meta-pipeline` task runs **one** combined Lakeflow Spark Declarative Pipeline configured with `layer=bronze_silver` (groups `bronze.group=A1` and `silver.group=A1`), so all three regional bronze CDC tables AND the unified silver multi-source AUTO CDC merge execute inside a single observable DLT flow graph — matching Stage 11 of the interactive demo notebook and the standalone `demo/launch_multi_source_cdc_demo.py`. Seed data lives under [`integration_tests/resources/data/multi_source_cdc/`](resources/data/multi_source_cdc/) and the onboarding template is [`integration_tests/conf/json/multi-source-cdc-onboarding.template`](conf/json/multi-source-cdc-onboarding.template) (YAML sibling: [`conf/yml/multi-source-cdc-onboarding.template.yml`](conf/yml/multi-source-cdc-onboarding.template.yml)).

    > **Tip:** any of the five sources (`cloudfiles`, `eventhub`, `kafka`, `snapshot`, `multi_source_cdc`) accepts ```--onboarding_file_format=yaml``` to run the same test against the YAML onboarding spec.


10. Once finished integration output file will be copied locally to
```integration-test-output_<run_id>.txt```

11. Output of a successful run should have the following in the file
```
,0
0,Completed Bronze Lakeflow Spark Declarative Pipeline.
1,Completed Silver Lakeflow Spark Declarative Pipeline.
2,Validating Lakeflow Spark Declarative Pipeline Bronze and Silver Table Counts...
3,Validating Counts for Table bronze_7d1d3ccc9e144a85b07c23110ea50133.transactions.
4,Expected: 10002 Actual: 10002. Passed!
5,Validating Counts for Table bronze_7d1d3ccc9e144a85b07c23110ea50133.transactions_quarantine.
6,Expected: 7 Actual: 7. Passed!
7,Validating Counts for Table bronze_7d1d3ccc9e144a85b07c23110ea50133.customers.
8,Expected: 98928 Actual: 98928. Passed!
9,Validating Counts for Table bronze_7d1d3ccc9e144a85b07c23110ea50133.customers_quarantine.
10,Expected: 1077 Actual: 1077. Passed!
11,Validating Counts for Table silver_7d1d3ccc9e144a85b07c23110ea50133.transactions.
12,Expected: 8759 Actual: 8759. Passed!
13,Validating Counts for Table silver_7d1d3ccc9e144a85b07c23110ea50133.customers.
14,Expected: 87256 Actual: 87256. Passed!
```

---

## Backward-compatibility upgrade test (any source → any target)

`integration_tests/run_backward_compat_tests.py` is a separate orchestrator that proves a customer's existing pipeline keeps working when the wheel is swapped from `--source_version` to `--target_version`. No notebook edits, no onboarding redo, no DLT checkpoint resets.

The test runs in **two phases against the same DLT pipelines** (same pipeline IDs across both phases — only the wheel path attached to each pipeline's `configuration` changes between Phase 1 and Phase 2):

| Phase | Wheel install spec | What runs |
|---|---|---|
| 1 | SOURCE main wheel only | onboard A1 → bronze A1 → silver → onboard A2 → bronze A2 → silver → validate Phase 1 row counts and persist them |
| 2 | TARGET main wheel only (one config key, one `%pip install`, byte-for-byte the customer's source-version notebook) | drop a small new incremental seed batch → bronze → silver → validate row counts (data preserved + grew), and dataclass compatibility (SOURCE-persisted dataflowspec rows materialize through TARGET's dataclasses with new fields backfilled to defaults) |

### Version profiles

Two version-line profiles ship in [`integration_tests/version_profiles.py`](version_profiles.py):

| Profile | Refs it owns | Distribution | Pipeline-config key | Runner notebook | Cross-namespace compatibility |
|---|---|---|---|---|---|
| `legacy` | `v0.0.1` … `v0.0.10`, `main` | `dlt_meta` | `dlt_meta_whl` (single) | imports `from src.*` | — |
| `current` | `v0.1`+, `feature/sdp-meta` | `databricks_labs_sdp_meta` | `sdp_meta_whl` (single) | imports `from databricks.labs.sdp_meta.*` | When the TARGET wheel comes from this profile and the SOURCE was `legacy`, the primary wheel ships real `dlt_meta` and `src` compatibility packages from `compat/`. The real `src` package is load-bearing for serverless `%pip install`, where imports continue in the same interpreter and a newly installed `.pth` is not processed. The standalone `dlt-meta` wrapper ships a best-effort `.pth` for normal installs followed by a fresh interpreter, but that `.pth` is not load-bearing on serverless. Same-namespace upgrades (`current → current`, e.g. `v0.1.0 → v0.1.1`) install one wheel and don't need any of this. |

**Why one wheel + one `%pip install`, not two?** The first cut of this test installed two wheels (main + a separate compat shim) under two pipeline-config keys, which forced either a `%pip install $a $b` magic shape or two separate `%pip install` lines. Both failed on serverless DLT: `%pip install` magic substitution is fragile when composing multiple wheels in one line (variables quote as single args), and two install lines in one cell don't reliably compose (only the last seems to survive). Bundling the shim into the main wheel sidesteps both — one wheel install satisfies both the canonical namespace and the legacy-namespace import surface in one shot.

Profiles are resolved from each git ref by prefix match; pass `--source_profile=<name>` / `--target_profile=<name>` to force resolution for custom branches the registry doesn't recognize.

### Install modes

`--install_mode` picks how each wheel reaches the cluster:

- **`local` (default)** — build wheels via [`integration_tests/wheel_builder.py`](wheel_builder.py) (which uses `git worktree` + `python setup.py bdist_wheel` against the source/target refs), upload them to the per-run UC volume, and reference UC volume paths in both `JobEnvironment.dependencies` and the runner notebook's `%pip install`. This matches what real customers do (install a pre-built artifact); does NOT require workspace egress to GitHub.
- **`git`** — skip the local build entirely. `JobEnvironment.dependencies` and `%pip install` resolve `git+https://github.com/databrickslabs/sdp-meta.git@<ref>` directly. Faster local iteration. The cluster MUST have egress to `--git_repo_url`.
- **`pypi`** — post-release verification. No build, no upload: Phase 1 installs `dlt-meta==<source_package_version>` and Phase 2 installs `dlt-meta==<target_package_version>` from the **live PyPI index** (through the workspace's PyPI proxy), proving the *published* redirect + primary packages upgrade a running deployment in place. Versions derive from the `--source_version`/`--target_version` tags with the `v` stripped (pip versions never carry a `v` prefix), or pin them with `--source_package_version`/`--target_package_version`. The uploaded runner/validate notebooks get `%pip install --force-reinstall`: serverless environment reuse can expose base-runtime (or prior-update) distribution metadata to pip without making the module importable from the isolated pipeline environment, so a plain install may be skipped as "already satisfied" and `databricks.labs.sdp_meta` then fails to import. Requires cluster egress to the PyPI proxy, and the proxy must have synced the freshly published packages (verify with `pip download dlt-meta==<ver> -d /tmp/x --index-url <proxy>/simple` before blaming the harness).

### Phase 2 install surface — `--target_install_surface`

- **`primary_wheel` (default)** — Phase 2 installs the target install spec directly (wheel path, git URL, or PyPI coordinate per `--install_mode`).
- **`compat_wheelhouse`** (requires `--install_mode=local`, legacy→current upgrades only) — builds the target primary wheel *and* the `compat/` dlt-meta redirect wheel, downloads all runtime dependencies, uploads everything to a UC-volume wheelhouse, and Phase 2 installs `dlt-meta==<target_package_version>` through offline pip resolution (`--no-index --find-links`). This simulates the customer's `pip install dlt-meta==<ver>` **before** the release is published.

### Choosing a scenario

| Scenario | Flags | Phase 1 installs | Phase 2 installs |
|---|---|---|---|
| Pre-release, iterate on uncommitted code | `--install_mode=local --build_target_from_worktree` | wheel built from `--source_version` tag | wheel built from working tree |
| Pre-release, simulate `pip install dlt-meta==<ver>` | add `--target_install_surface=compat_wheelhouse` | wheel built from tag | `dlt-meta==<ver>` via offline UC-volume wheelhouse |
| Post-release smoke test | `--install_mode=pypi` | `dlt-meta==<src ver>` from live PyPI | `dlt-meta==<tgt ver>` from live PyPI |
| Branch iteration, no builds | `--install_mode=git` | `git+repo@<source ref>` | `git+repo@<target ref>` |

Recommended release workflow: run `compat_wheelhouse` before tagging (hermetic, no live-index dependency), then `pypi` immediately after publishing (verifies exactly what customers download).

### Iterating on uncommitted target-side changes — `--build_target_from_worktree`

While developing the bundled compat shim or post-rename CLI aliases on a feature branch, you'll want to test changes that aren't pushed to `--target_version` yet. Pass `--build_target_from_worktree` (requires `--install_mode=local`) to build the TARGET main wheel from your local working tree instead of from the git ref:

```commandline
python integration_tests/run_backward_compat_tests.py \
    --uc_catalog_name=<<uc catalog name>> \
    --build_target_from_worktree \
    --profile=<<DEFAULT>>
```

The SOURCE wheel still comes from `--source_version` (e.g. `v0.0.10`) — that's the customer's already-released artifact and has nothing to do with local edits. Only the TARGET side honours the working tree, because that's where the unreleased bundled compat shim and CLI work live.

Use ONLY for development. Production runs should pin a real git ref so the test artifact is reproducible from version control.

### Common upgrade scenarios

```commandline
# Default (legacy → current): v0.0.10 → v0.1.0. Local wheel build.
python integration_tests/run_backward_compat_tests.py \
    --uc_catalog_name=<<uc catalog name>> \
    --profile=<<DEFAULT>>

# Legacy publishing mode on pipeline-managed standard compute. This creates
# pipelines with `target=` and `serverless=False`, then upgrades their wheel
# in place while retaining the legacy publishing mode.
python integration_tests/run_backward_compat_tests.py \
    --uc_catalog_name=<<uc catalog name>> \
    --pipeline_mode=standard_legacy \
    --pipeline_num_workers=2 \
    --profile=<<DEFAULT>>

# Same scenario, but install wheels via git+https instead of local build:
python integration_tests/run_backward_compat_tests.py \
    --uc_catalog_name=<<uc catalog name>> \
    --install_mode=git \
    --profile=<<DEFAULT>>

# Post-release smoke test: both phases from the LIVE PyPI index
# (Phase 1: dlt-meta==0.0.10, Phase 2: dlt-meta==0.1.0 via the redirect).
python integration_tests/run_backward_compat_tests.py \
    --uc_catalog_name=<<uc catalog name>> \
    --install_mode=pypi \
    --source_version=v0.0.10 \
    --target_version=v0.1.0 \
    --profile=<<DEFAULT>>

# Future release pair (current → current, e.g. v0.1.0 → v0.1.1).
# Same one-wheel/one-key contract — the namespace does not change,
# so no compat shim is needed in the wheel either way.
python integration_tests/run_backward_compat_tests.py \
    --uc_catalog_name=<<uc catalog name>> \
    --source_version=v0.1.0 \
    --target_version=v0.1.1 \
    --profile=<<DEFAULT>>

# Custom branches with explicit profile pins (used when the branch
# name doesn't match a registered prefix). Source ≠ target so the test
# actually exercises the legacy → current shim path:
python integration_tests/run_backward_compat_tests.py \
    --uc_catalog_name=<<uc catalog name>> \
    --source_version=v0.0.10            --source_profile=legacy \
    --target_version=feature/sdp-meta   --target_profile=current \
    --profile=<<DEFAULT>>

# Skip cleanup on success/failure so the run state is debuggable;
# add --cleanup to wipe pipelines/jobs/schemas/volumes when done.
python integration_tests/run_backward_compat_tests.py \
    --uc_catalog_name=<<uc catalog name>> --cleanup --profile=<<DEFAULT>>
```

### Pipeline execution modes

`--pipeline_mode` controls both pipeline compute and the publishing-mode
contract under test:

- `serverless_dpm` (default) creates serverless pipelines with `schema=`.
  This is the existing default-publishing-mode upgrade test.
- `standard_legacy` creates pipeline-managed standard compute with
  `serverless=False`, `target=`, and a default `PipelineCluster`. It requires
  `--pipeline_num_workers` (at least `1`) and verifies the legacy publishing
  mode after pipeline creation and after the Phase 2 wheel swap.

`standard_legacy` is intentionally not an existing all-purpose cluster ID:
Lakeflow Declarative Pipelines creates and manages its standard compute from
the supplied worker count. Use a UC-enabled workspace whose policy permits
standard pipeline compute and whose identity can create schemas, volumes, and
pipelines in `--uc_catalog_name`. Start with two workers; increase the count
only when the input size or your workspace policy requires it.

The test retains the source runner notebook, source `dlt_meta_whl`
configuration key, pipeline IDs, and checkpoints. It fails immediately if the
service returns a serverless or DPM (`schema=`) pipeline for this mode. After
a successful run, inspect the two emitted `backward_compat_phase*.csv` files:
every Phase 2 validation line must end in `Passed!`.

### What changes between phases (and what doesn't)

**Stays identical across both phases:**
- The three DLT pipeline IDs (created in Phase 1, reused in Phase 2 via `pipelines.update()`).
- Per-pipeline DLT checkpoints — Auto Loader picks up Phase 2's incremental seed by checkpoint, not by re-listing.
- The runner notebook contents — uploaded once from the SOURCE profile's runner. For legacy → current upgrades, the SOURCE's `from src.*` imports are kept verbatim and resolve in Phase 2 via the legacy-namespace compat shim BUNDLED into the target wheel (proving zero-code-change upgrade end-to-end).
- The pipeline-config KEY the runner notebook reads (`dlt_meta_whl` for legacy source, `sdp_meta_whl` for current source).
- The dataflowspec table — Phase 2 reads what Phase 1 persisted; new fields are backfilled to documented defaults by `populate_additional_df_cols`.

**Changes between Phase 1 and Phase 2 (and only this):**
- The value behind the SOURCE profile's pipeline-config key in each pipeline's `configuration` flips from the SOURCE main wheel path to the TARGET main wheel path. That's it — one key, one swap, per pipeline.
- The runner notebook itself is not touched — the same single `%pip install $key` line that ran in Phase 1 runs again in Phase 2, with `$key` resolving to the new wheel path.

### Output

Two CSVs land locally on completion:

```
backward_compat_phase1_<run_id>.csv   # row-count assertions per table after Phase 1
backward_compat_phase2_<run_id>.csv   # data preservation + growth + dataclass compat after Phase 2
```

Every line in either CSV ending in `Passed!` is a green assertion; any line ending in `Failed!` is a red one. The Phase 2 validator is the authoritative source for the "upgrade does not break the customer's pipeline" claim.

---

## Onboarding file format (JSON or YAML)

The integration test runner can drive every supported source (`cloudfiles`, `eventhub`, `kafka`, `snapshot`, `multi_source_cdc`) with either a JSON or a YAML onboarding spec. The format is selected per-run with a single CLI flag:

```commandline
--onboarding_file_format=json   # default
--onboarding_file_format=yaml   # also accepts 'yml'
```

### Where the conf files live

Templates and reference configs are organized into format-specific subdirectories so each format has its own self-contained tree:

```
integration_tests/conf/
├── json/
│   ├── cloudfiles-onboarding.template
│   ├── cloudfiles-onboarding_A2.template
│   ├── eventhub-onboarding.template
│   ├── kafka-onboarding.template
│   ├── snapshot-onboarding.template
│   ├── multi-source-cdc-onboarding.template
│   ├── silver_transformations.json
│   ├── silver_transformations_snapshot.json
│   └── dqe/
│       ├── customers/{bronze,silver}_data_quality_expectations.json
│       ├── iot/{bronze,silver}_data_quality_expectations.json
│       └── transactions/{bronze,silver}_data_quality_expectations.json
└── yml/
    ├── cloudfiles-onboarding.template.yml
    ├── cloudfiles-onboarding_A2.template.yml
    ├── eventhub-onboarding.template.yml
    ├── kafka-onboarding.template.yml
    ├── snapshot-onboarding.template.yml
    ├── multi-source-cdc-onboarding.template.yml
    ├── silver_transformations.yml
    ├── silver_transformations_snapshot.yml
    └── dqe/
        ├── customers/{bronze,silver}_data_quality_expectations.yml
        ├── iot/{bronze,silver}_data_quality_expectations.yml
        └── transactions/{bronze,silver}_data_quality_expectations.yml
```

The two trees are kept structurally equivalent: each YAML template references its `/yml/` siblings for `silver_transformation_*` and `*_data_quality_expectations_*` paths, and each JSON template references its `/json/` siblings. Pick a format and stay in it.

### How the path translation works

The dataclass defaults all point at the `/json/` tree. When the runner is started with `--onboarding_file_format=yaml`, `SDPMetaRunnerConf.__post_init__` rewrites every relevant path through a single helper, `_to_yaml_variant`:

| Default (JSON mode) | Becomes (YAML mode) |
|---|---|
| `integration_tests/conf/json/cloudfiles-onboarding.template` | `integration_tests/conf/yml/cloudfiles-onboarding.template.yml` |
| `integration_tests/conf/json/eventhub-onboarding.template`   | `integration_tests/conf/yml/eventhub-onboarding.template.yml`   |
| `integration_tests/conf/json/kafka-onboarding.template`      | `integration_tests/conf/yml/kafka-onboarding.template.yml`      |
| `integration_tests/conf/json/snapshot-onboarding.template`   | `integration_tests/conf/yml/snapshot-onboarding.template.yml`   |
| `integration_tests/conf/json/multi-source-cdc-onboarding.template` | `integration_tests/conf/yml/multi-source-cdc-onboarding.template.yml` |
| `integration_tests/conf/json/onboarding.json` *(generated)*  | `integration_tests/conf/yml/onboarding.yml` *(generated)*       |
| `integration_tests/conf/json/onboarding_A2.json` *(generated)* | `integration_tests/conf/yml/onboarding_A2.yml` *(generated)*  |

The transformation rules are:
1. Swap the path segment `/json/` → `/yml/`.
2. `.template` → `.template.yml`, `.json` → `.yml`. Already-YAML paths (`.yml`/`.yaml`) are returned unchanged.

### Inputs vs outputs

There are two distinct kinds of paths:

- **Templates (committed):** `cloudfiles_template`, `eventhub_template`, `kafka_template`, `snapshot_template`, `cloudfiles_A2_template`, `multi_source_cdc_template`. Both `/json/*.template` and `/yml/*.template.yml` siblings exist on disk.
- **Onboarding outputs (generated):** `onboarding_file_path`, `onboarding_A2_file_path`, `onboarding_fanout_file_path`. The runner reads the template, substitutes runtime placeholders (`{uc_volume_path}`, `{uc_catalog_name}`, `{bronze_schema}`, etc.) via `generate_onboarding_file()`, and writes the rendered result to the format-specific output path. These files are gitignored — they only exist after a test run.

### What happens after the file is written

Once the rendered onboarding file is written, it is handed to `OnboardDataflowspec.__get_onboarding_file_dataframe`:

- `.json` path → read directly with `spark.read.option("multiline","true").json(...)`.
- `.yml`/`.yaml` path → `convert_yml_to_json(...)` parses the YAML via Spark IO (so cloud paths like `/Volumes/...`, `dbfs:/...`, `s3://...`, `abfss://...` all work), serializes the parsed structure to a **driver-local** temp JSON file (`tempfile.mkdtemp("sdp_meta_onboarding_")`), and the local file is then handed to Spark's native JSON reader. The original YAML on the (possibly remote) source filesystem is never modified, and no JSON sibling is written next to it.

DQE files and silver-transformation files referenced from inside the onboarding spec follow the same `_load_structured_file` path: both `.json` and `.yml` extensions parse transparently.

### Worked example

Run cloudfiles end-to-end against the YAML spec:

```commandline
python integration_tests/run_integration_tests.py \
    --source=cloudfiles \
    --uc_catalog_name=<<uc catalog name>> \
    --onboarding_file_format=yaml \
    --profile=<<DEFAULT>>
```

What the runner does, in order:

1. `SDPMetaRunnerConf.__post_init__` sees `onboarding_file_format=yaml` and rewrites:
    - `cloudfiles_template` → `integration_tests/conf/yml/cloudfiles-onboarding.template.yml`
    - `cloudfiles_A2_template` → `integration_tests/conf/yml/cloudfiles-onboarding_A2.template.yml`
    - `onboarding_file_path` → `integration_tests/conf/yml/onboarding.yml`
    - `onboarding_A2_file_path` → `integration_tests/conf/yml/onboarding_A2.yml`
2. `generate_onboarding_file` reads the YAML template as text, substitutes placeholders, and writes the rendered YAML to `integration_tests/conf/yml/onboarding.yml`.
3. The rendered onboarding YAML is uploaded to the UC volume alongside its referenced YAML DQE and silver-transformation files.
4. The pipeline runs and validates row counts. The output file `integration-test-output_<run_id>.txt` is identical in shape to the JSON-mode output.

### Troubleshooting

- **`FileNotFoundError: integration_tests/conf/yml/<something>.template.yml`** — the YAML sibling for that source is missing. All five committed templates listed above must be present; the `_to_yaml_variant` helper does not synthesize missing siblings.
- **`Onboarding file format not supported!`** — `onboarding_file_path` does not end in `.json`, `.yml`, or `.yaml`. Check that you didn't override `--onboarding_file_path` with a non-conforming extension.
- **`YAML onboarding file '...' is empty or could not be parsed`** — the YAML template rendered to an empty document or the YAML is malformed after substitution. Inspect the generated `integration_tests/conf/yml/onboarding.yml` from the failed run.