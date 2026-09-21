# Getting started — zero to a running pipeline against your own data

A copy-paste path a newcomer (or an agent) can follow verbatim to go from raw
input files to a running bronze→silver pipeline built from **their** config. Uses
the bundle (DAB) path — the most turnkey option.

Prereqs: Databricks CLI authenticated (`databricks auth login --profile <p>`), a
Unity Catalog you can create schemas/volumes in, and serverless available.

---

## Step 0 — install the framework CLI

```bash
databricks labs install sdp-meta
```

## Step 1 — scaffold a bundle

```bash
databricks labs sdp-meta bundle-init --quickstart=true --output-dir=.
cd my_sdp_meta_pipeline
```

This creates a ready-to-edit bundle (`cloudFiles` + `bronze_silver` + `split` +
`pypi`):

```
my_sdp_meta_pipeline/
├── databricks.yml
├── resources/
│   ├── variables.yml               # <- you edit this (catalog/schema/deps)
│   ├── sdp_meta_onboarding_job.yml  # job that runs `onboard`
│   └── sdp_meta_pipelines.yml       # the bronze/silver SDP pipelines
├── conf/
│   ├── onboarding.yml               # <- your dataflowspec(s)
│   ├── silver_transformations.yml   # <- your silver projections/filters
│   ├── dqe/example_table/bronze_expectations.yml  # <- your DQ rules
│   └── samples/flows.csv            # template for batch-adding flows
└── notebooks/init_sdp_meta_pipeline.py
```

## Step 2 — point the bundle at your environment (`resources/variables.yml`)

Set these to your real values (defaults shown are the template's):

| Variable | Set to |
|----------|--------|
| `uc_catalog_name` | Your UC catalog, e.g. `acme`. |
| `sdp_meta_schema` | Schema holding the `bronze_dataflowspec` / `silver_dataflowspec` tables. |
| `bronze_target_schema` / `silver_target_schema` | Where bronze/silver tables are written. |
| `layer` | `bronze`, `silver`, or `bronze_silver`. |
| `pipeline_mode` | `split` (two pipelines, silver depends on bronze) or `combined`. |
| `dataflow_group` | Logical group name; every flow's `data_flow_group` must match it. |
| `wheel_source` / `sdp_meta_dependency` | `pypi` + `databricks-labs-sdp-meta` (default), or a `/Volumes/...whl` path for no-PyPI clusters. |

## Step 3 — describe YOUR data in `conf/onboarding.yml`

The scaffold seeds one `example_table` flow. Edit it to point at your input and
targets. Example: JSON order files landing in a Volume →
bronze `orders` → silver `orders`.

```yaml
- data_flow_id: "1"
  data_flow_group: ${var.dataflow_group}
  source_system: acme_orders
  source_format: cloudFiles
  source_details:
    source_database: raw
    source_table: orders
    source_path_dev: "/Volumes/acme/landing/files/orders/"   # <- your input path
  bronze_database_dev: "${var.uc_catalog_name}.${var.bronze_target_schema}"
  bronze_table: orders
  bronze_reader_options:
    cloudFiles.format: json
    cloudFiles.inferColumnTypes: "true"
    cloudFiles.rescuedDataColumn: _rescued_data
  bronze_data_quality_expectations_json_dev: "${workspace.file_path}/conf/dqe/orders/bronze_expectations.json"
  silver_database_dev: "${var.uc_catalog_name}.${var.silver_target_schema}"
  silver_table: orders
  silver_transformation_json_dev: "${workspace.file_path}/conf/silver_transformations.json"
```

> Don't have real column names yet? Leave `cloudFiles.inferColumnTypes: "true"`
> and let Auto Loader infer the schema; pin a DDL later via `source_schema_path`.

## Step 4 — express "clean" as data-quality rules (`conf/dqe/orders/bronze_expectations.yml`)

Your rule "drop rows with no order id":

```yaml
expect_or_drop:
  valid_order_id: "order_id IS NOT NULL"
```

- `expect_or_drop` → quarantine bad rows · `expect_or_fail` → halt · `expect` → warn.

## Step 5 — shape silver (`conf/silver_transformations.yml`)

Pick columns and filters for the silver `orders` table:

```yaml
- target_table: orders
  select_exp:
    - order_id
    - customer_id
    - amount
    - order_ts
  where_clause:
    - "amount IS NOT NULL"
```

(For dedup / CDC / SCD Type 2, add `silver_cdc_apply_changes` on the flow — see
[onboarding-spec.md](onboarding-spec.md#cdc--scd-type-2).)

## Step 6 — validate before touching the workspace

```bash
databricks labs sdp-meta bundle-validate
```

Catches layer/topology mismatches, `wheel_source` vs `sdp_meta_dependency`
conflicts, unresolved placeholders, and bad `dataflow_group` references — so the
deploy doesn't fail halfway.

## Step 7 — deploy and run

```bash
databricks bundle deploy --profile <p>   # uploads conf, creates the onboarding job + pipelines
databricks bundle run sdp_meta_onboarding_job --profile <p>   # writes the dataflowspec tables
databricks bundle run --profile <p>       # runs the bronze/silver SDP pipeline(s)
```

`onboard` (the job) must run before the pipeline — it writes the
`bronze_dataflowspec` / `silver_dataflowspec` tables the pipeline reads.

## Step 8 — verify

Query the outputs:

```sql
SELECT count(*) FROM acme.<bronze_target_schema>.orders;
SELECT count(*) FROM acme.<silver_target_schema>.orders;
-- dropped rows, if any, landed in the bronze quarantine table
```

## Step 9 — scale to more tables (the whole point)

You don't write more pipeline code — you add more metadata:

```bash
# interactive:
databricks labs sdp-meta bundle-add-flow
# batch from a spreadsheet (template at conf/samples/flows.csv):
databricks labs sdp-meta bundle-add-flow --csv conf/samples/flows.csv
```

It auto-increments `data_flow_id` and seeds `silver_transformations` for new
silver flows. Re-run Steps 6–7 and the new tables flow through the same
pipelines.

---

## No-bundle quick path (single onboarding file)

If you don't want a bundle, author `conf/onboarding.json` directly and run:

```bash
databricks labs sdp-meta onboard --profile <p>   # prompts for catalog/schema/layer/table names
databricks labs sdp-meta deploy  --profile <p>
```

Same three-step model (author → onboard → deploy); the bundle just makes it
reproducible and version-controlled.

## If the pipeline cluster has no PyPI access

Deliver the wheel from a UC Volume instead:

```bash
databricks labs sdp-meta deploy --build-and-upload-whl=true --profile <p>
# or point at a pre-uploaded wheel:
databricks labs sdp-meta deploy --whl-file-path=/Volumes/acme/libs/vol/databricks_labs_sdp_meta-0.1.1-py3-none-any.whl --profile <p>
```

On serverless the wheel is baked into the runner notebook's `%pip install` (not a
whl pipeline library, which serverless rejects).
