---
id: gold
title: Native SQL Gold Layer
sidebar_position: 9
---

# Native SQL Gold Layer

SDP-META uses DataflowSpec for repeatable Bronze and Silver processing. Gold
business models use native Lakeflow Spark Declarative Pipelines SQL in a
separate pipeline. Generated DAB workflows connect the two by running Gold
after Silver succeeds.

There is intentionally no `gold_dataflowspec` table and no Python Gold runtime.

## Architecture

![How native SQL Gold connects to metadata-driven Bronze and Silver](/img/gold-layer-architecture.svg)

The integration contract is the published Silver table, not a reference from a
Gold DataflowSpec row. The Gold pipeline receives the Silver location through
configuration:

```yaml
configuration:
  silver_catalog: ${var.uc_catalog_name}
  silver_schema: ${var.silver_target_schema}
```

Gold models reference Silver inputs using those values:

```sql
CREATE OR REFRESH MATERIALIZED VIEW customer_360 AS
SELECT
  customers.customer_id,
  customers.email,
  COUNT(transactions.transaction_id) AS transaction_count
FROM ${silver_catalog}.${silver_schema}.customers AS customers
LEFT JOIN ${silver_catalog}.${silver_schema}.transactions AS transactions
  ON transactions.customer_id = customers.customer_id
GROUP BY customers.customer_id, customers.email;
```

Within Gold, use unqualified dataset names so SDP builds the dependency graph:

```sql
CREATE OR REFRESH MATERIALIZED VIEW high_value_customers AS
SELECT *
FROM customer_360
WHERE transaction_count >= 5;
```

## Enable Gold in a new bundle

Run `bundle-init` and set:

- `gold_enabled`: `true`
- `gold_target_schema`: the Unity Catalog schema for Gold outputs
- `gold_models_path`: the bundle-relative SQL model directory

`gold_enabled` controls scaffold generation and is recorded as an immutable
marker in the rendered bundle. To add Gold later, use `bundle-add-gold`; do not
change or target-override that marker manually.

The generated workflow uses this order:

```text
onboarding -> bronze -> silver -> gold
```

For a combined Bronze/Silver pipeline:

```text
onboarding -> bronze_silver -> gold
```

The template does not seed example SQL: SDP-META cannot know your Silver
columns or business models. The generated bundle README provides authoring
guidance, and the configured `gold_models_path` (default: `gold/models/`) is
reserved for customer-authored pipeline source. `bundle-validate` fails until
you add your first `.sql` model. This is deliberate; a Gold pipeline with no
libraries would otherwise fail later at deploy time with a less helpful error.

## Add Gold to an existing bundle

The bundle must publish Silver tables:

```bash
databricks labs sdp-meta bundle-add-gold --bundle-dir <bundle-path>
cd <bundle-path>
databricks labs sdp-meta bundle-validate
```

Author at least one Silver-compatible `.sql` model under the configured
`gold_models_path` using the Gold section of the generated bundle README, then
deploy and run:

```bash
databricks bundle deploy --target dev
databricks bundle run pipelines --target dev
```

## Best practices

- Treat Silver schemas and columns as a stable contract. Coordinate breaking
  Silver changes with downstream Gold model owners.
- Keep Gold in a separate schema and pipeline to isolate business-model
  lifecycle, permissions, and failures from ingestion.
- Use configuration-driven fully qualified names for Silver inputs.
- Use unqualified names for Gold-to-Gold references; do not encode file order.
- Keep one published materialized view or streaming table declaration per SQL
  file. Temporary views may support that declaration.
- Run Gold through the generated Silver dependency instead of independent,
  potentially overlapping schedules.
- Use serverless Gold pipelines. Incremental materialized-view refresh requires
  compatible Silver Delta change tracking; SDP may otherwise recompute.
- Grant the pipeline identity `USE CATALOG`, `USE SCHEMA`, and `SELECT` on
  Silver, plus creation privileges on the Gold schema.
- Run `bundle-validate` in CI before deployment.

## Validation boundaries

`bundle-validate` performs shallow static checks:

- Gold is enabled only on a Silver-bearing topology.
- Model paths cannot escape the bundle.
- Each SQL file declares one supported materialized view or streaming table.
- Dataset names are unique across model files.
- Deprecated `LIVE TABLE` declarations and external template markers are
  rejected.

Databricks validates SQL semantics, permissions, and source columns when the
pipeline is deployed and run.

## Demos

- **DAB lifecycle:** `python demo/launch_dab_template_demo.py --scenario gold`
- **Non-DAB interoperability:** run
  `demo/SDP_META_INTERACTIVE_DEMO.py` with `gold_enabled=true`, or use
  `python demo/launch_interactive_demo.py --gold-enabled true ...`

Both demos use the runnable models under `demo/gold/models/`.

