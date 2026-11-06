---
id: mcp
title: MCP Server
sidebar_position: 6
---

# MCP Server

The SDP-META MCP (Model Context Protocol) server exposes local bundle
scaffolding, validation, and template inspection to MCP-capable clients. It
does not onboard or deploy pipelines. The server runs locally over stdio via
`databricks labs sdp-meta mcp` and uses MCP Python SDK 2.x.

:::tip
The [Agent Skill](./agent-skill.md) pairs with this MCP server for any skill-aware AI agent: the skill teaches the agent *how* and *when* to use these tools (workflow, ordering, guardrails), while the tools below do the actual work.
:::

## Prerequisites

- Python 3.10+
- [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/tutorial.html) authenticated against your workspace
- An MCP-capable client (Claude Code, Claude Desktop, Cursor, or similar)

## Authentication

Commands launched by the MCP server inherit the
[Databricks unified authentication](https://docs.databricks.com/en/dev-tools/auth/unified-auth.html)
configuration available to the server process. This can come from
`~/.databrickscfg`, environment variables, or another supported authentication
provider.

To select a profile from `~/.databrickscfg`, set the environment variable before
launching your MCP client:

```bash
export DATABRICKS_CONFIG_PROFILE=my-profile
```

Or pass it inline when wiring the server (Claude Code example):

```bash
SDP_META_MCP_ROOT="$PWD" DATABRICKS_CONFIG_PROFILE=my-profile \
  claude mcp add sdp-meta -- databricks labs sdp-meta mcp
```

The bundle initialization and validation tools also accept an optional
`profile` argument.

## Install

```bash
python -m pip install 'databricks-labs-sdp-meta[mcp]'
databricks labs install sdp-meta
```

Install the extra in the same Python environment used by the Databricks Labs
CLI. Desktop MCP clients do not always inherit an activated shell or its
`PATH`. If the `databricks` command cannot import the MCP SDK when launched by
your client, use the absolute Python executable configuration shown below.

## Test a local checkout

Contributors can test the current source instead of the published package:

```bash
cd /path/to/sdp-meta
python3.10 -m venv .venv-mcp
source .venv-mcp/bin/activate
python -m pip install -e '.[dev,mcp]'
python -m pytest tests/test_mcp_server.py -q
```

Run only the real stdio protocol round trip with:

```bash
python -m pytest \
  tests/test_mcp_server.py::ProtocolTests::test_stdio_transport_lists_calls_and_reads \
  -q
```

## Filesystem boundary

Before starting the server, set an explicit project directory. Every path read
or written by an MCP bundle tool must remain below this directory:

```bash
export SDP_META_MCP_ROOT=/absolute/path/to/your/project
```

The server refuses filesystem tool calls when this variable is missing or does
not identify an existing directory. `SDP_META_EXAMPLES_DIR` is an optional
development override for packaged examples; normal wheel installations do not
need it.

## Wire into your MCP client

### Claude Desktop

Add to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "sdp-meta": {
      "command": "databricks",
      "args": ["labs", "sdp-meta", "mcp"],
      "env": {
        "SDP_META_MCP_ROOT": "/absolute/path/to/your/project"
      }
    }
  }
}
```

### Claude Code

```bash
SDP_META_MCP_ROOT="$PWD" \
  claude mcp add sdp-meta -- databricks labs sdp-meta mcp
```

Or add to `.mcp.json` in your project root using the same `mcpServers` shape.

### Cursor

Use the same `mcpServers` JSON shape as Claude Desktop in your Cursor MCP
settings, including `SDP_META_MCP_ROOT`.

For a virtual environment or local checkout, use its absolute Python path. This
is also the most reliable configuration for desktop clients that do not inherit
your shell environment:

```json
{
  "mcpServers": {
    "sdp-meta": {
      "command": "/absolute/path/to/.venv-mcp/bin/python",
      "args": [
        "-c",
        "from databricks.labs.sdp_meta.mcp_server import run_stdio; run_stdio()"
      ],
      "env": {
        "SDP_META_MCP_ROOT": "/absolute/path/to/your/project"
      }
    }
  }
}
```

## Available tools

| Tool | Description |
|---|---|
| `sdp_meta_bundle_init` | Scaffold a new SDP-META DAB. Pass `quickstart=true` for developer defaults. |
| `sdp_meta_bundle_validate` | Run `databricks bundle validate` plus SDP-META checks against a scaffolded bundle. |
| `sdp_meta_bundle_add_flow` | Append one or more flow entries to a bundle's onboarding file. |
| `sdp_meta_bundle_add_pipeline` | Add and wire an independently configured pipeline into a bundle. |
| `sdp_meta_list_templates` | List the names of every packaged onboarding, DQE, and silver-transformation template. |
| `sdp_meta_get_onboarding_template` | Return the raw text of a packaged template by name. |

### Example tool arguments

List templates:

```json
{}
```

Read a template:

```json
{"name": "json/cloudfiles-onboarding.template"}
```

Initialize a quickstart bundle:

```json
{"output_dir": "demo", "quickstart": true, "profile": "DEFAULT"}
```

Validate a bundle:

```json
{"bundle_dir": "demo/my_sdp_meta_pipeline", "target": "dev"}
```

Preview adding a flow without modifying the bundle:

```json
{
  "bundle_dir": "demo/my_sdp_meta_pipeline",
  "dry_run": true,
  "flows": [
    {
      "source_format": "cloudFiles",
      "source_path": "/Volumes/main/landing/customers",
      "bronze_table": "customers_bronze"
    }
  ]
}
```

Preview adding an independently configured pipeline:

```json
{
  "bundle_dir": "demo/my_sdp_meta_pipeline",
  "dry_run": true,
  "pipeline": {
    "name": "customers_bronze",
    "layer": "bronze",
    "dataflow_group": "customers"
  }
}
```

All paths must resolve beneath `SDP_META_MCP_ROOT`.

## MCP resources

The server exposes packaged templates as MCP resources:

```
sdp-meta://templates/<format>/<filename>
```

Examples:
- `sdp-meta://templates/json/cloudfiles-onboarding.template`
- `sdp-meta://templates/yml/eventhub-onboarding.template.yml`

## Troubleshooting

**`ImportError: The mcp extra is not installed`** — install with `pip install 'databricks-labs-sdp-meta[mcp]'`.

**`SDP_META_MCP_ROOT must be set`** — set it to the existing project directory
the MCP tools may read and modify, then restart the server.

**The server appears idle when launched manually** — expected. The stdio
transport waits for protocol messages from an MCP client; launch it through
your MCP host.

**Bundle validation returns `returncode != 0`** — the tool ran successfully but
found an invalid bundle. This is a normal result; inspect its captured `output`
for the validation failures.

**A tool result has `is_error=true`** — the request could not be completed.
Common causes include an invalid argument, a path outside
`SDP_META_MCP_ROOT`, an unavailable or unknown template, or a failed
scaffolding/update command. For expected command failures, the error text
contains a JSON payload with `returncode` and captured `output`.

**No template resources are listed** — the server could not load its packaged
examples. Bundle initialization, validation, and flow tools remain available,
but template list/get calls return an actionable error. Reinstall
`databricks-labs-sdp-meta[mcp]`; development builds can instead set
`SDP_META_EXAMPLES_DIR` to a directory containing `json/` and `yml/`.
