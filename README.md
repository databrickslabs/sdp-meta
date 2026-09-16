<p align="center">
  <img src="docs/static/img/sdp-meta-readme-banner.png" alt="SDP-META — Metadata-driven Lakeflow Spark Declarative Pipelines" width="900"/>
</p>

<!-- Top bar will be removed from PyPi packaged versions -->
<!-- Dont remove: exclude package -->

[Documentation](https://databrickslabs.github.io/sdp-meta/) |
[Release Notes](CHANGELOG.md) |
[Migration Guide](https://databrickslabs.github.io/sdp-meta/docs/operations/migration) |
[Examples](https://github.com/databrickslabs/sdp-meta/tree/main/demo/conf)

<!-- Dont remove: end exclude package -->

---

[![Documentation](https://img.shields.io/badge/docs-passing-green)](https://databrickslabs.github.io/sdp-meta/) [![PyPI](https://img.shields.io/pypi/v/databricks-labs-sdp-meta?label=pypi)](https://pypi.org/project/databricks-labs-sdp-meta/) [![Build](https://img.shields.io/github/actions/workflow/status/databrickslabs/sdp-meta/onpush.yml?branch=main)](https://github.com/databrickslabs/sdp-meta/actions/workflows/onpush.yml) [![Coverage](https://img.shields.io/codecov/c/github/databrickslabs/sdp-meta)](https://codecov.io/gh/databrickslabs/sdp-meta) [![Style](https://img.shields.io/badge/code%20style-flake8-blue)](https://github.com/PyCQA/flake8) [![PyPI Downloads](https://static.pepy.tech/badge/dlt-meta/month)](https://pepy.tech/projects/dlt-meta)

---

## Project Overview

`SDP-META` is a metadata-driven framework for [Lakeflow Spark Declarative Pipelines](https://www.databricks.com/product/data-engineering/spark-declarative-pipelines). Define your Bronze and Silver pipelines in a JSON or YAML onboarding file — a single generic Declarative Pipeline reads the resulting DataflowSpec at runtime and builds the full processing graph automatically. Generated DABs can optionally chain a separate native SDP SQL Gold pipeline for business models.

> 📺 **New to SDP-META?** Watch the [SDP-Meta Deep-Dive: Building Data Pipelines at Scale on Databricks](https://youtu.be/Z0CYboIzp9M) — a video walkthrough of the framework with a Demo.

**Who it's for:** platform and data engineering teams standardizing repeatable Bronze/Silver pipelines across many datasets — onboarding new feeds through metadata instead of new pipeline code, with consistent data quality, quarantine, CDC, clustering, and sink patterns available through Bundles, CLI, UI, MCP, and agent workflows. DAB users can extend those published Silver tables with native SDP SQL Gold models.

**When it's not the best fit:** one or two simple pipelines, teams seeking metadata-driven Gold onboarding (Gold uses native SDP SQL rather than DataflowSpec), tables that each need unique application logic, a managed connector and downstream logic that already satisfy the complete Bronze/Silver requirement, or a need for a formal support SLA (SDP-META is a Databricks Labs project). See the [Introduction](https://databrickslabs.github.io/sdp-meta/docs/intro) for the full positioning.

### Components

#### Metadata Interface

- **Onboarding file** (JSON or YAML) — sources, targets, CDC config, DQE rules. Examples: [`demo/conf/json/onboarding.template`](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/json/onboarding.template) · [`demo/conf/yml/onboarding.template.yml`](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/yml/onboarding.template.yml)
- **Data quality engines** — legacy expectations, the built-in Lakeflow engine,
  and optional Databricks Labs DQX support. Install DQX with
  `pip install "databricks-labs-sdp-meta[dqx]"`. See the
  [data-quality guide](https://databrickslabs.github.io/sdp-meta/docs/concepts/data-quality)
  and the new-engine [`examples/json/quality/`](examples/json/quality/) /
  [`examples/yml/quality/`](examples/yml/quality/) rules. Files under
  [`demo/conf/json/dqe/`](demo/conf/json/dqe/) use legacy predicate semantics.
- **Silver transformation file** — SQL `select_exp` and `where_clause` definitions. Examples: [`demo/conf/json/silver_transformations.json`](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/json/silver_transformations.json) · [`demo/conf/yml/silver_transformations.yml`](https://github.com/databrickslabs/sdp-meta/blob/main/demo/conf/yml/silver_transformations.yml)

#### Generic Lakeflow Spark Declarative Pipeline

- Reads DataflowSpec at runtime and dynamically wires sources, transformations, expectations, CDC flows, and sinks
- Supports Autoloader, Delta, Kafka, Eventhub, and Snapshot sources
- Applies [`create_auto_cdc_flow`](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-apply-changes), [`append_flow`](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-append-flow), and [`create_sink`](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-sink) based on metadata

## High-Level Process Flow

![SDP-META Architecture](https://raw.githubusercontent.com/databrickslabs/sdp-meta/main/docs/static/img/sdp-meta-architecture.svg)

## Feature Matrix

### Pipeline Capabilities

| Feature | Layers |
|---|---|
| Input sources — Autoloader, Delta, Kafka, Eventhub, Snapshot | Bronze, Silver |
| Medallion architecture | Bronze → Silver |
| Bronze ↔ Silver pipeline chaining (`layer=bronze_silver`) | Both |
| Custom transformation functions | Bronze, Silver |
| Metadata-driven quality — legacy expectations, Lakeflow, optional DQX | Bronze, Silver |
| Quarantine table | Bronze, Silver |
| Liquid clustering | Bronze, Bronze Quarantine, Silver |
| [`create_auto_cdc_flow`](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-apply-changes) — CDC via `bronze_cdc_apply_changes` | Bronze, Silver |
| Multi-source CDC (`bronze_cdc_apply_changes_flows` / `silver_cdc_apply_changes_flows`) | Bronze, Silver |
| [`create_auto_cdc_from_snapshot_flow`](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-apply-changes-from-snapshot) — Snapshot CDC | Bronze |
| [`append_flow`](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-append-flow) — via `bronze_append_flows` | Bronze |
| [`create_sink`](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-sink) — Delta and Kafka sinks | Bronze, Silver |
| Row filters | Bronze, Silver |
| Optional native SDP SQL business models in generated DABs | Gold |

### Deployment & Tooling

| Tool | Description |
|---|---|
| [Declarative Automation Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/) | Git-tracked pipelines, `dev`/`prod` targets, CI/CD-ready. Commands: `bundle-init`, `bundle-prepare-wheel`, `bundle-add-flow`, `bundle-add-gold`, `bundle-validate`. See [`DAB_README.md`](DAB_README.md). |
| [SDP-META CLI](https://databrickslabs.github.io/sdp-meta/docs/getting-started/cli) | `databricks labs sdp-meta onboard` · `deploy` · `bundle-*` |
| [SDP-META App](https://databrickslabs.github.io/sdp-meta/docs/getting-started/app) | Browser-based UI for onboarding, deployment, and pipeline monitoring |
| [MCP Server](https://databrickslabs.github.io/sdp-meta/docs/getting-started/mcp) | AI-assisted pipeline scaffolding via MCP-capable AI tools (Claude Code, Cursor, Claude Desktop, and others) |

## Getting Started

SDP-META has three ways to use it — pick the one that matches your role and how far you intend to take it:

1. **[Declarative Automation Bundle](GETTING_STARTED.md#path-a--declarative-automation-bundle-recommended)** *(recommended)* — `dev`/`prod` targets, git-tracked state, CI/CD-ready. `bundle-init --quickstart` skips every prompt and gets you a working bundle in one command.
2. **[Interactive `onboard` + `deploy` CLI](GETTING_STARTED.md#path-b--interactive-onboard--deploy-cli-exploration-only)** — kick the tires against a single workspace.
3. **[SDP-META Databricks App](GETTING_STARTED.md#path-c--sdp-meta-databricks-app-browser-ui)** — browser-based UI for non-developers, demos, and click-driven workflows.

See **[GETTING_STARTED.md](GETTING_STARTED.md)** for prereqs, install commands, full per-path walkthroughs, local development setup, and troubleshooting. Long form: [docs site](https://databrickslabs.github.io/sdp-meta/docs/getting-started).

## Upgrading from DLT-META

The v0.1.0 release renames **DLT-META** to **SDP-META**. Existing onboarding JSON/YAML files and pipeline behavior are unchanged, but new code should move to the new package, CLI, and import path:

| Area | Old | New |
|---|---|---|
| PyPI package | `dlt-meta` | `databricks-labs-sdp-meta` |
| Labs CLI | `databricks labs dlt-meta` | `databricks labs sdp-meta` |
| Python imports | `from dlt_meta import ...` | `from databricks.labs.sdp_meta import ...` |

The `dlt-meta` compatibility package remains available during v0.1.x, but it emits deprecation warnings and receives no new features. Legacy `src.*` imports are planned for removal in v0.2.0.

See the **[DLT-META → SDP-META migration guide](https://databrickslabs.github.io/sdp-meta/docs/operations/migration)** for the step-by-step plan, compatibility details, and deprecation timeline.

## Resources

- [Documentation](https://databrickslabs.github.io/sdp-meta/)
- [Video: SDP-Meta Deep-Dive](https://youtu.be/Z0CYboIzp9M)
- [FAQ](https://databrickslabs.github.io/sdp-meta/docs/faq)
- [Migration Guide](https://databrickslabs.github.io/sdp-meta/docs/operations/migration)
- [Release Notes](CHANGELOG.md)
- [GitHub Issues](https://github.com/databrickslabs/sdp-meta/issues)

## Project Support

Please note that all projects released under [`Databricks Labs`](https://www.databricks.com/learn/labs)
are provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements
(SLAs). They are provided AS-IS and we do not make any guarantees of any kind. Please do not submit a support ticket
relating to any issues arising from the use of these projects.

Any issues discovered through the use of this project should be filed as issues on the Github Repo.  
They will be reviewed as time permits, but there are no formal SLAs for support.
