---
id: intro
title: Introduction
sidebar_position: 1
---

# SDP-META

SDP-META is a metadata-driven framework for building automated Bronze and Silver data pipelines on [Databricks Lakeflow Spark Declarative Pipelines](https://www.databricks.com/product/data-engineering/spark-declarative-pipelines). Define your pipelines in a JSON or YAML onboarding file; a single generic Declarative Pipeline builds the processing graph automatically. Generated DABs can optionally chain a separate native SDP SQL Gold pipeline over the published Silver tables.

:::important Upgrading from DLT-META?
The v0.1.0 release renames DLT-META to SDP-META. Existing onboarding JSON/YAML files continue to work, but new installs should use `databricks-labs-sdp-meta`, `databricks labs sdp-meta`, and `databricks.labs.sdp_meta` imports.

Start with the [DLT-META → SDP-META migration guide](./operations/migration.md) for the step-by-step upgrade plan and deprecation timeline.
:::

## What is SDP-META?

SDP-META helps data platform and data engineering teams standardize repeatable Bronze and Silver pipelines across many datasets.

Teams describe sources, targets, data quality rules, CDC behavior, transformations, and sinks in YAML or JSON. SDP-META persists that contract as DataflowSpec metadata and uses a single generic pipeline that reads the spec at runtime and builds the full processing graph.

Its primary value is reducing per-table pipeline code while giving teams consistent patterns, controls, and deployment options.

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
- You require metadata-driven Gold onboarding. Gold models are native SDP SQL,
  not Gold DataflowSpec records.
- Most tables require unique application logic.
- A managed connector and downstream logic already satisfy the complete Bronze/Silver requirement.
- You require a product with a formal support SLA; SDP-META is a Labs project.

## Why SDP-META?

- **Persistent metadata contract:** DataflowSpec records remain queryable and governable in Delta tables.
- **Runtime-driven pipelines:** Metadata changes do not require maintaining generated per-table pipeline definitions.
- **Bronze/Silver specialization:** Built-in ingestion, quality, quarantine, CDC, fan-out, row-filtering, clustering, and sink patterns.
- **Optional native SQL Gold:** Generated DABs can run business models in a
  separate Gold pipeline after Silver without introducing a second metadata
  runtime.
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
| Layers | Metadata-driven Bronze and Silver; optional native SDP SQL Gold in DABs |
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
