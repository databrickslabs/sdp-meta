---
id: column-policies
title: Column Comments & Masks
sidebar_position: 9
---

# Column Comments & Masks

SDP-META can attach two kinds of Unity Catalog column-level governance to the
Bronze and Silver target tables directly from the onboarding file:

- **Column comments** — free-text documentation attached to a column.
- **Column masks** — Unity Catalog dynamic data masking: a registered UC
  masking function is applied to a column so unprivileged readers see a
  redacted value.

Both are declared as JSON objects keyed by column name.

## Configuration

Add `*_column_comments` and/or `*_column_masks` to an onboarding entry
(`bronze_` and `silver_` variants are both supported):

```json
{
  "data_flow_id": "1",
  "data_flow_group": "customers_group",
  "source_format": "delta",
  "silver_catalog_dev": "my_catalog",
  "silver_database_dev": "retail_silver",
  "silver_table": "customers_silver",
  "silver_transformation_json_dev": "/Volumes/.../silver_transformations.json",
  "silver_column_comments": {
    "customer_id": "Stable surrogate key",
    "ssn": "Social security number (masked for non-privileged readers)"
  },
  "silver_column_masks": {
    "ssn": "my_catalog.security.mask_ssn USING COLUMNS (region)"
  }
}
```

### Mask clause syntax

A mask value is spliced after `MASK` in the generated table DDL, so it uses
the Unity Catalog column-mask clause form:

```
<catalog>.<schema>.<function> [USING COLUMNS (<col>, ...)]
```

The masking function must already exist in Unity Catalog. The optional
`USING COLUMNS (...)` list passes additional columns to the function (for
conditional masking). Examples:

- `my_catalog.security.mask_ssn`
- `my_catalog.security.mask_ssn USING COLUMNS (region, tier)`

## How it is applied

When comments or masks are set, SDP-META derives the target table's schema
and renders it as a **DDL-string schema** — `` `col` type [NOT NULL] [COMMENT
'...'] [MASK <clause>] `` — which it passes to the generated Lakeflow
Declarative Pipeline table. (Column masks cannot be expressed through a
`StructType`; Unity Catalog only reads them from a DDL schema.) Comments and
masks are applied to the main target table across the standard, data-quality,
CDC apply-changes, and append-flow write paths.

### Combined `bronze_silver` topology

Silver column comments/masks are supported in **both** pipeline topologies:

- the **split** topology (a bronze pipeline, then a separate silver pipeline), and
- the **combined** `bronze_silver` topology (one pipeline that builds bronze and
  silver in the same run — e.g. the multi-source AUTO CDC demo).

A silver table derives its schema from its transform applied to the upstream
bronze table. In a combined run the bronze table is *produced in the same run*
and does not yet exist in Unity Catalog when the graph is built, so SDP-META
does **not** read it to resolve the silver policy schema. Instead it derives the
silver schema from the bronze dataflowspec's declared schema in-process, then
applies the silver transform to it:

- **Effective (target) schema, not the raw input schema.** The mapped bronze
  schema is the declared `source_schema_path` schema **augmented with the
  columns the bronze reader injects into the materialised target** — the
  `cloudFiles` rescued-data column (default `_rescued_data`, or your
  `cloudFiles.rescuedDataColumn`) and any autoloader metadata columns
  (`include_autoloader_metadata_column` / `select_metadata_cols`). So a silver
  `selectExp` that references `_rescued_data` (or a metadata column) resolves in
  a combined run exactly as it does against the physical table in the split
  topology.
- **Single-source and multi-source AUTO CDC.** For a standard silver spec the
  `selectExp` / `whereClause` is applied to the bronze target schema. For a
  **multi-source AUTO CDC** silver spec (`silver_cdc_apply_changes_flows`), each
  flow's source is resolved against its bronze schema, that flow's `select_exp`
  / `where_clause` is applied, and the per-flow schemas are merged (they must be
  compatible — same columns and types — since all flows land in one streaming
  table).

**Requirements & fail-fast.** In a combined run a bronze source feeding a
silver table with masks/comments **must have a declared schema**
(`source_schema_path`). If it does not — or if the silver transform references a
column that is not in the declared+augmented bronze schema (for example a column
added by a bronze `custom_transform_func`, which is not statically knowable) —
SDP-META **fails fast at graph-construction time** with an actionable error that
names the split-pipeline workaround, rather than surfacing an opaque
`TABLE_OR_VIEW_NOT_FOUND`. In those cases, declare the missing column in the
bronze source schema, or run bronze and silver as **separate pipelines** (the
split topology reads the already-materialised bronze table and has no such
restriction). Note that a `selectExp` of `*` cannot be statically validated
against a `custom_transform_func`'s output; prefer explicit column lists on a
silver table that carries policies in a combined run.

## Rules & limitations

- **Masks are Unity Catalog only.** On a non-UC pipeline, masks are silently
  dropped (mirroring row filters). Comments work on any Delta table.
- **Masks fail closed.** If a mask names a column that is not in the derived
  schema, onboarding/deploy raises an error rather than silently skipping it —
  a dropped mask would leave a column unexpectedly unprotected. A comment on an
  unknown column is skipped with a warning instead.
- **A declared schema is required for masks.** Silver derives its schema from
  the transform, so masks work out of the box. A Bronze table with an inferred
  schema (no `bronze_schema`) cannot carry masks — supply a schema, or apply
  masks on the Silver table.
- **Quarantine tables** do not receive column masks in this version (masking
  rejected rows would hide the very values operators need to triage them, and
  the mask map is keyed to the main table's columns).
- **SCD type 2 snapshot targets are not supported.** Column comments/masks on
  an `apply_changes_from_snapshot` target with `scd_type: 2` raise an error at
  runtime. An SCD2 table carries DLT-managed `__START_AT` / `__END_AT` system
  columns typed to the snapshot *version*; unlike the regular CDC path, the
  snapshot config has no `sequence_by` to derive that type from (it is decided
  at runtime by the snapshot source), so a complete explicit schema cannot be
  built. Rather than emit a schema missing those columns (which would break
  table creation) or silently drop a mask, the pipeline fails closed. Use
  **SCD type 1** for column policies on a snapshot target, or apply the
  `COMMENT` / `MASK` with a separate `ALTER TABLE` after the table is created.
  SCD1 snapshot targets are unaffected.

## Related

- [DataflowSpec Schema](../concepts/dataflowspec.md)
- [Row Filters](./row-filters)
