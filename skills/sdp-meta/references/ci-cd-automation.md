# CI/CD automation

Use this reference when a user wants to deploy an SDP-META bundle from GitHub
Actions, Azure DevOps, or another unattended CI/CD runner.

## Required workflow

Treat the bundle as the deployment contract. Keep `databricks.yml`,
`resources/`, onboarding metadata, data-quality rules, and transformations in
Git. Deploy reviewed commits only.

Run these commands from the generated bundle directory:

```bash
databricks bundle validate --target dev
databricks bundle deploy --target dev
databricks bundle run onboarding --target dev
databricks bundle run pipelines --target dev
```

- Validate every pull request.
- Deploy after merge.
- Run onboarding when committed metadata changes.
- Run pipelines as a release verification step only when workload semantics
  make an immediate run safe. Scheduled pipelines may only need deployment.
- Promote the same commit and wheel artifact to production with `--target prod`
  after an approval gate.

## Authentication

Use OAuth machine-to-machine authentication with a Databricks service
principal:

```bash
DATABRICKS_HOST=https://<workspace-host>
DATABRICKS_CLIENT_ID=<service-principal-application-id>
DATABRICKS_CLIENT_SECRET=<service-principal-oauth-secret>
```

Do not add `--profile` in CI. Profiles are for local development. A legacy
personal access token can use `DATABRICKS_HOST` and `DATABRICKS_TOKEN`, but
OAuth is preferred.

Store credentials in the CI platform's secret manager. Use separate service
principals and secrets for development and production. Never write secrets to
bundle variables, onboarding files, command arguments, logs, or artifacts.

## Current validation limitation

`databricks labs sdp-meta bundle-validate` currently prompts for the bundle
directory and target. Do not put it in an unattended workflow.

Use this non-interactive command in CI:

```bash
databricks bundle validate --target <target>
```

This validates the standard Databricks bundle but does not run SDP-META checks
for flow groups, pipeline ownership, dependency wiring, or placeholders. Run
the SDP-META validator locally before merging topology changes until it gains
non-interactive `--bundle-dir` and `--target` flags.

## Permissions

Grant the deployment service principal only what the bundle needs:

- workspace access and permission to manage the declared jobs and pipelines;
- permission to use the configured `run_as` identity, when present;
- `USE CATALOG`, `USE SCHEMA`, and `CREATE TABLE` on the configured catalogs
  and schemas;
- `CREATE SCHEMA` or `CREATE VOLUME` only when the bundle creates them;
- `READ VOLUME` and `WRITE VOLUME` for configuration or wheel Volumes;
- access to the sources, external locations, secrets, and message systems
  referenced by onboarding metadata.

Do not use workspace-admin or metastore-admin credentials as a shortcut.

## Targets and variables

Keep non-secret differences in `dev` and `prod` bundle targets. Override
declared variables through `BUNDLE_VAR_<variable_name>` when CI must supply an
environment-specific value:

```bash
export BUNDLE_VAR_uc_catalog_name=my_environment_catalog
databricks bundle validate --target prod
```

Protect production with manual approval, separate credentials, and serialized
deployments to the same target.

## Wheel delivery

Choose one strategy and pin the SDP-META version:

- **PyPI:** use
  `sdp_meta_dependency: databricks-labs-sdp-meta==<version>` when the runtime
  can reach the package index.
- **Unity Catalog Volume:** for a release, download the pinned wheel with
  `python -m pip download --only-binary=:all: --no-deps --dest dist
  databricks-labs-sdp-meta==<version>`, upload it to
  `/Volumes/<catalog>/<schema>/<volume>/`, and configure that path when
  internet access is unavailable or an internal artifact must be promoted.
  For an unreleased version, check out the SDP-META source and run
  `python -m pip wheel --no-deps --wheel-dir dist .` from its repository root.

Promote the same wheel artifact to production; do not rebuild it after
approval.

## Failure and rollback rules

- Stop on a non-zero validation, deployment, onboarding, or pipeline result.
- Retry only bounded transient platform failures. Do not retry invalid
  metadata or permission failures without correction.
- Capture command output as an artifact after checking it for sensitive data.
- Roll back configuration by reverting to a known-good commit, validating it,
  and redeploying it.
- Configuration rollback does not undo pipeline data writes. Use Delta history,
  backups, or a workload-specific recovery plan for data rollback.
- Use `databricks bundle destroy --target dev --auto-approve` only for isolated
  ephemeral environments, never as automated production cleanup.

The customer-facing guide and complete GitHub Actions/Azure DevOps examples
live at
`docs/docs/operations/ci-cd.md`.
