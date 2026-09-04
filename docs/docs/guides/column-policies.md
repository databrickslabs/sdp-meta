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

## Related

- [DataflowSpec Schema](../concepts/dataflowspec.md)
- [Row Filters](./row-filters)
