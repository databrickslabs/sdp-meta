---
id: intro
title: Introduction
sidebar_position: 1
---

# SDP-META

SDP-META is a metadata-driven framework for building automated Bronze and Silver data pipelines on [Databricks Lakeflow Spark Declarative Pipelines](https://www.databricks.com/product/data-engineering/spark-declarative-pipelines). Define your pipelines in a JSON or YAML onboarding file; a single generic Declarative Pipeline builds the full processing graph automatically.

<aside className="doc-highlight-card doc-highlight-card--migration">
<span className="doc-highlight-card__badge">Migration</span>
<h3>Upgrading from DLT-META?</h3>

SDP-META was formerly named DLT-META. Version 0.2.0 requires the canonical
`databricks-labs-sdp-meta` package, `databricks labs sdp-meta` commands, and
`databricks.labs.sdp_meta` imports. Existing onboarding JSON/YAML can be
preserved; follow the migration guide before upgrading an old installation.

[Open the step-by-step migration guide →](./operations/migration.md)
</aside>

## What is SDP-META?

SDP-META helps data platform and data engineering teams standardize repeatable Bronze and Silver pipelines across many datasets.

Teams describe sources, targets, data quality rules, CDC behavior, transformations, and sinks in YAML or JSON. SDP-META persists that contract as DataflowSpec metadata and uses a single generic pipeline that reads the spec at runtime and builds the full processing graph.

Its primary value is reducing per-table pipeline code while giving teams consistent patterns, controls, and deployment options.

<aside className="doc-highlight-card doc-highlight-card--example">
<span className="doc-highlight-card__badge">Runnable example</span>
<h3>See the metadata model at scale</h3>

The [At-Scale Auto Loader Demo](https://github.com/databrickslabs/sdp-meta/tree/main/demo#at-scale-auto-loader-demo) creates 100 Bronze and 100 Silver tables from one generated onboarding model. It validates schema inference, schema hints, explicit DDLs, additive evolution, rescued data, quarantine, table-specific Silver logic, and the handoff to Gold.

This is the clearest runnable example of SDP-META's main advantage: adding standardized data flows through metadata instead of maintaining one pipeline implementation per table.

[Open the At-Scale Auto Loader Demo →](https://github.com/databrickslabs/sdp-meta/tree/main/demo#at-scale-auto-loader-demo)
</aside>

## Who is it for?

Use SDP-META when:

- Many datasets follow common ingestion and transformation patterns.
- A platform team wants reusable Bronze/Silver standards.
- New feeds should be onboarded through metadata instead of new pipeline code.
- Data quality, quarantine, CDC, clustering, and sinks need consistent implementation.
- Teams need the same model through Bundles, CLI, UI, or AI tools.

## When is it not the best fit?

Consider another approach when:

- You only have one or two simple pipelines.
- Gold-layer business modeling is the primary requirement.
- Most tables require unique application logic.
- A managed connector and downstream logic already satisfy the complete Bronze/Silver requirement.
- You require a product with a formal support SLA; SDP-META is a Labs project.

## Why SDP-META?

- **Persistent metadata contract:** DataflowSpec records remain queryable and governable in Delta tables.
- **Runtime-driven pipelines:** Metadata changes do not require maintaining generated per-table pipeline definitions.
- **Bronze/Silver specialization:** Built-in ingestion, quality, quarantine, CDC, fan-out, row-filtering, clustering, and sink patterns.
- **Multiple interfaces, one model:** Bundles, CLI, Databricks App, MCP, and agent workflows all use the same metadata contract.
- **Designed for repeatability:** Best suited to onboarding and operating many similarly structured data flows.

## Architecture

![SDP-META Architecture](/img/sdp-meta-architecture.svg)

SDP-META operates in two phases:

1. **Onboard Job** — reads your onboarding YAML or JSON file and writes structured metadata into `bronze_dataflowspec` and `silver_dataflowspec` Delta tables. Re-run this job whenever you change the onboarding file.
2. **Generic Declarative Pipeline** — at runtime, reads the DataflowSpec Delta tables and dynamically constructs the pipeline graph: sources, transformations, expectations, CDC flows, and sinks.

## Feature matrix

| Feature | Support |
|---|---|
| Input sources | Autoloader (cloudFiles), Delta, Eventhub, Kafka, Snapshot |
| Layers | Bronze, Silver |
| Custom transformations | Bronze and Silver layers |
| Data Quality Expectations | Bronze, Silver |
| Quarantine table | Bronze, Silver |
| CDC (create_auto_cdc_flow) | Bronze, Silver |
| CDC from Snapshot | Bronze layer |
| append_flow | Bronze layer |
| Liquid clustering | Bronze, Bronze Quarantine, Silver |
| create_sink (Delta, Kafka) | Bronze, Silver |
| Declarative Automation Bundles | Yes |
| SDP-META CLI | Yes |
| Databricks App (UI) | Yes |
| MCP Server | Yes |

For setup options, see [Getting Started](./getting-started/index.md).
