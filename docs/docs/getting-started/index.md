---
id: index
title: Getting Started
sidebar_position: 1
---

# Getting Started

SDP-META supports several deployment paths for metadata-driven Bronze and
Silver pipelines. The DAB path can additionally chain a separate native SDP
SQL Gold pipeline over the published Silver tables.

## Prefer video?

Watch the **[SDP-Meta Deep-Dive: Building Data Pipelines at Scale on Databricks](https://youtu.be/Z0CYboIzp9M)** — a walkthrough of the framework with a Demo.

<div style={{position:'relative',paddingBottom:'56.25%',height:0,maxWidth:'640px'}}>
  <iframe
    style={{position:'absolute',top:0,left:0,width:'100%',height:'100%'}}
    src="https://www.youtube-nocookie.com/embed/Z0CYboIzp9M"
    title="SDP-Meta Deep-Dive: Building Data Pipelines at Scale on Databricks"
    loading="lazy"
    allow="accelerometer; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
    allowFullScreen></iframe>
</div>

:::tip Migrating from DLT-META?
If you are upgrading an existing `dlt-meta` installation after the v0.1.0 rename, follow the [migration guide](../operations/migration.md) first. Your onboarding files do not need to change, but package names, CLI commands, Python imports, and some compatibility shims have a deprecation timeline.
:::

## Prerequisites (all paths)

- **Python 3.10–3.12** — Python 3.10, 3.11, or 3.12 recommended. Python 3.13+ has known PySpark compatibility issues.
- **Databricks CLI v0.213 or later** — [Install guide](https://docs.databricks.com/en/dev-tools/cli/tutorial.html).

```bash
databricks labs install sdp-meta
databricks auth login --host YOUR_WORKSPACE_URL
```

:::note
To use the MCP server, install its optional dependencies with
`pip install 'databricks-labs-sdp-meta[mcp]'`. For local development without
MCP, install `databricks-labs-sdp-meta`.
:::

## Deployment paths

### Declarative Automation Bundles (recommended)

Git-tracked configuration, explicit `dev`/`prod` environments, CI/CD-friendly. Right for production workloads and team settings.

See [Declarative Automation Bundles](./dabs.md).

### Interactive CLI

Onboard and deploy with interactive prompts, no bundle YAML required. Good for exploring SDP-META or running a one-off pipeline.

See [Interactive CLI](./cli.md).

### Databricks App

Browser-based GUI for onboarding and managing pipelines. Ideal for non-engineers who need to manage pipelines without a terminal.

See [Databricks App](./app.md).

### MCP Server

AI-assisted scaffolding via Claude Code, Claude Desktop, or Cursor. Drive SDP-META configuration through natural language.

See [MCP Server](./mcp.md).

### Agent Skill

A portable agent skill that teaches any skill-aware AI agent the SDP-META workflow, so it can guide you from raw input data to a running pipeline. Complements the MCP Server.

See [Agent Skill](./agent-skill.md).
