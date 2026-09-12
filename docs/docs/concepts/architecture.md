---
id: architecture
title: SDP-META Architecture
sidebar_position: 1
---

# SDP-META Architecture

SDP-META is a metadata-driven framework for building Bronze and Silver data pipelines on Lakeflow Spark Declarative Pipelines. You define pipelines in an onboarding file (YAML or JSON); SDP-META reads that file and builds the full pipeline graph automatically.

![SDP-META Architecture](/img/sdp-meta-architecture.svg)

## How it works

SDP-META operates in two phases:

**Phase 1 — Onboarding.** The SDP-META Onboard Job reads your onboarding file and writes structured metadata into two Delta tables: `bronze_dataflowspec` and `silver_dataflowspec`. Run this job once on setup, and again whenever you change the onboarding file.

**Phase 2 — Pipeline runtime.** The Generic Declarative Pipeline reads the DataflowSpec tables at startup and dynamically constructs the pipeline graph — selecting the right reader for each source format, applying transformations, enforcing data quality expectations, setting up CDC flows, and wiring sinks. No pipeline code changes when your onboarding file changes.

## Bronze layer

The Bronze layer ingests raw data from source systems into Delta tables with minimal transformation.

| Source format | Description |
|---|---|
| `cloudFiles` | Autoloader — incremental file ingestion from cloud storage |
| `delta` | Existing Delta table or Delta Sharing source |
| `eventhub` | Azure Event Hubs streaming source |
| `kafka` | Apache Kafka streaming source |
| `snapshot` | Snapshot-based CDC using `create_auto_cdc_from_snapshot_flow` |

`expect_or_drop` failures are discarded and are not written to quarantine.
Quarantine routing requires `expect_or_quarantine` plus an explicitly
configured quarantine catalog/schema/table (and path for non-UC pipelines);
SDP-META never derives the quarantine table name. The opt-in Lakeflow quality
engine retains structured `_errors` diagnostics in quarantine, while the
legacy path adds no diagnostic column. Liquid clustering is supported on both
main and quarantine tables.

## Silver layer

The Silver layer reads from Bronze Delta tables and applies transformations, joins, and enrichment.

- SQL or Python transformations defined in `silver_transformations.yml` / `.json`
- CDC via `create_auto_cdc_flow` (SCD Type 1 and Type 2)
- Fan-out: one Bronze table can produce multiple Silver tables
- Liquid clustering on output tables

## Layer configuration

The `layer` parameter controls which layers the Onboard Job processes:

| Value | What gets created |
|---|---|
| `bronze` | Bronze DataflowSpec rows; Bronze pipeline only |
| `silver` | Silver DataflowSpec rows; Silver pipeline only |
| `bronze_silver` | Both Bronze and Silver rows; both pipelines |

:::note
`layer=silver` requires Bronze tables to already exist. Use `layer=bronze_silver` when setting up both layers from scratch.
:::

In DAB deployments with `pipeline_mode=split`, a workflow job sequences Bronze before Silver automatically. With `pipeline_mode=combined`, both layers run in a single pipeline DAG. See [Pipeline Chaining](./pipeline-chaining.md).
