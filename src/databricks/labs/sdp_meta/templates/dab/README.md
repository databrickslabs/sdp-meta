# sdp-meta Declarative Automation Bundle template

Maintainer notes. Users normally don't read this — they invoke the template
via `databricks labs sdp-meta bundle-init` (recommended) or directly via
`databricks bundle init <path-to-this-dir>`.

## Layout

- `databricks_template_schema.json` — prompt definitions (bundle_name, catalog, layer, …).
- `template/{{.bundle_name}}/` — the bundle tree. Files ending in `.tmpl` are rendered through Go templates; others are copied verbatim. File *names* containing `{{...}}` are also rendered (see "Conditional filenames" below).
- `library/` — reserved for shared fragments (none today).

## Conditional filenames

Three template files have a Go template conditional embedded in their *name*:

```
conf/onboarding.{{if eq .onboarding_file_format "yaml"}}yml{{else}}json{{end}}.tmpl
conf/silver_transformations.{{if eq .onboarding_file_format "yaml"}}yml{{else}}json{{end}}.tmpl
conf/dqe/example_table/bronze_expectations.{{if eq .onboarding_file_format "yaml"}}yml{{else}}json{{end}}.tmpl
```

This is the [documented `databricks bundle init` pattern](https://docs.databricks.com/dev-tools/bundles/templates.html) for emitting a different file extension based on a user prompt — the engine resolves `{{...}}` in filenames at scaffold time, so the user's bundle ends up with a clean `conf/onboarding.yml` (or `conf/onboarding.json`) depending on what they answered for `onboarding_file_format`.

**End users never see the templated filenames** — they only see the rendered output. The ugliness is purely a maintainer-side cost.

**Why we didn't split into six files** (`onboarding.yml.tmpl` + `onboarding.json.tmpl` etc., gated by `{{skip}}` directives): the same logical config would have to be maintained in two formats, and any new field added to one branch but not the other would silently drift. The conditional-filename pattern keeps one source of truth per logical config; tests in `tests/test_bundle.py` exercise both YAML and JSON branches against the same single template.

If you're grepping the template tree for one of these files and the shell glob is awkward, use the file's stem:

```bash
# Find the onboarding template regardless of which extension branch you want.
find src/databricks/labs/sdp_meta/templates/dab -name 'onboarding.*tmpl'
```

## Testing a rendered output locally

```bash
cat > /tmp/sdp_meta_init.json <<JSON
{
  "bundle_name": "demo_bundle",
  "uc_catalog_name": "main",
  "sdp_meta_schema": "sdp_meta_dataflowspecs",
  "bronze_target_schema": "sdp_meta_bronze",
  "silver_target_schema": "sdp_meta_silver",
  "layer": "bronze_silver",
  "source_format": "cloudFiles",
  "onboarding_file_format": "yaml",
  "dataflow_group": "demo_group",
  "sdp_meta_dependency": "databricks-labs-sdp-meta",
  "author": "maintainer"
}
JSON

databricks bundle init \
  src/databricks/labs/sdp_meta/templates/dab \
  --config-file /tmp/sdp_meta_init.json \
  --output-dir /tmp/sdp_meta_render
```

Then:

```bash
cd /tmp/sdp_meta_render/demo_bundle
databricks bundle validate --target dev
```

## Conventions

- All user-tunable values live in `resources/variables.yml`; never hard-code `${bundle.name}`-adjacent values in the other resource files.
- Resource keys are generic (`onboarding`, `pipelines`, `bronze`, `silver`); names are built from `${bundle.name}` so the same template produces distinct resources per customer.
- The onboarding job uses `python_wheel_task` + `environments.dependencies`, matching the `sdp-meta` wheel's existing `databricks_labs_sdp_meta:run` entry point.
- The SDP Pipelines install sdp-meta inside the runner notebook (`notebooks/init_sdp_meta_pipeline.py`) via `%pip install --force-reinstall $sdp_meta_dependency`. We cannot use pipeline `libraries: - whl:` / `- pypi:` entries because **serverless SDP rejects both** ("Whl libraries are not supported"). The runner notebook must keep cell 1 as exactly `<var assignment> + %pip install --force-reinstall $var` — any multi-line Python before the `%pip` line trips the notebook parser with `SyntaxError: incomplete input`.
