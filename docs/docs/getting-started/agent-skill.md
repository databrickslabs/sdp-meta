---
id: agent-skill
title: Agent Skill
sidebar_position: 7
---

# Agent Skill

SDP-META ships an **Agent Skill** — a portable, agent-facing operating manual that teaches an AI coding agent how to use SDP-META correctly: the onboarding metadata model, the `onboard` → `deploy` workflow, DAB and CI/CD automation, and the common footguns. It is written in the [Agent Skills](https://docs.claude.com/en/docs/agents-and-tools/agent-skills) format (a `SKILL.md` file plus supporting references), which is portable across agents — not tied to any single tool.

When the skill is available to an agent, the agent uses it to scaffold, validate, and deploy pipelines against your own data, following the same procedure every time.

The skill source lives in the repo at [`skills/sdp-meta/`](https://github.com/databrickslabs/sdp-meta/tree/main/skills/sdp-meta):

```
skills/sdp-meta/
├── SKILL.md                                 # entry point (selected by its description)
└── references/
    ├── getting-started-walkthrough.md       # zero-to-running pipeline against your data
    ├── onboarding-spec.md                   # every dataflowspec field, DQ, CDC, formats
    ├── cli-and-bundles.md                   # onboard/deploy + DAB workflow + wheel flags
    ├── ci-cd-automation.md                  # OAuth, GitOps, promotion, and rollback
    └── mcp-tools.md                         # the MCP tools + how to register the server
```

## Works with any agent

The skill is just structured Markdown, so it is not locked to one tool:

- **Skill-aware hosts** — agents that natively support Agent Skills (for example [Claude Code](https://docs.claude.com/en/docs/claude-code), Claude Desktop, and the Claude Agent SDK) discover the skill automatically and load it when your request matches its description.
- **Any other agent** — point the agent at the `skills/sdp-meta/` folder as reference context (attach it, add it to the project's rules/instructions, or paste `SKILL.md`). The guidance still applies; only the auto-discovery differs.

## Skill vs. MCP Server

They are complementary — use both.

| | [MCP Server](./mcp.md) | Agent Skill |
|---|---|---|
| **What it is** | Tools the agent *calls* (scaffold, validate, add flow) | Knowledge that *steers* the agent (procedure, ordering, guardrails) |
| **Effect** | Executes SDP-META actions | Makes the agent drive those actions correctly and in order |
| **Activation** | Agent invokes a tool | Loaded when your request matches the skill's triggers (or attached as context) |
| **Best for** | Doing the work | Knowing *what* to do, *when*, and avoiding mistakes |

In practice: the skill tells the agent to run `onboard` before `deploy`, to copy
field names from real templates instead of inventing them, and that serverless
pipelines deliver the wheel via `%pip install` (not a whl library) — then the MCP
tools (or the CLI) do the actual scaffolding and deployment.

## Prerequisites

- An AI coding agent (any skill-aware host, or any agent you can give reference context to).
- The SDP-META CLI installed (`databricks labs install sdp-meta`), so the agent can run the commands the skill describes.
- Optionally the [MCP Server](./mcp.md), so the agent can scaffold without a live workspace.

## Make the skill available to your agent

### Skill-aware hosts (auto-discovery)

Hosts that support Agent Skills load them from a skills directory. For Claude Code that is `.claude/skills/` (this project) or `~/.claude/skills/` (all your projects):

```bash
# Project-scoped
mkdir -p .claude/skills
cp -r skills/sdp-meta .claude/skills/sdp-meta

# Or personal (available in every project)
mkdir -p ~/.claude/skills
cp -r skills/sdp-meta ~/.claude/skills/sdp-meta
```

Consult your agent's documentation for its skills directory if it differs.

### Any other agent (as context)

Give the agent the folder as reference material — attach `skills/sdp-meta/`, add it to the agent's project rules/instructions, or include `SKILL.md` in the prompt. The agent then follows the same workflow.

:::note
If you cloned this repository, `skills/sdp-meta/` is already present. If you only
installed the pip package, copy the `skills/sdp-meta/` folder from the
[GitHub repo](https://github.com/databrickslabs/sdp-meta/tree/main/skills/sdp-meta).
:::

## Use it

Describe what you want in natural language — no special command needed. The
skill's description triggers on phrasing like:

- "help me build an sdp-meta pipeline for these files"
- "onboard a dataflowspec for my orders table"
- "scaffold an sdp-meta bundle and add a silver flow"
- "deploy my sdp-meta bundle from GitHub Actions"

The agent loads the skill, then follows the
[getting-started walkthrough](https://github.com/databrickslabs/sdp-meta/blob/main/skills/sdp-meta/references/getting-started-walkthrough.md)
to take you from your raw input files to a running Bronze/Silver pipeline —
authoring `conf/onboarding`, data-quality rules, and silver transformations from
*your* data, then validating and deploying.

## Troubleshooting

**The agent doesn't seem to use the skill** — for a skill-aware host, confirm the
folder is at its skills directory (e.g. `.claude/skills/sdp-meta/SKILL.md`, not a
nested extra directory). Skills are matched by the `description` in `SKILL.md`
frontmatter; phrasing your request around SDP-META / onboarding / dataflowspec
helps it fire. For other agents, make sure the folder is actually attached as
context.

**Skill content looks out of date** — the skill points at canonical repo files
(`labs.yml`, `tests/resources/`, `examples/`, the MCP server) rather than
duplicating them, so those remain the source of truth. Re-copy the folder after
upgrading SDP-META.
