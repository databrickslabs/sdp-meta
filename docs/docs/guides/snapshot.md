---
id: snapshot
title: Snapshot Ingestion
sidebar_position: 4
---

# Snapshot Ingestion

Snapshot ingestion is for data sources that deliver full replacement files rather than incremental changes or CDC events. SDP-META maps snapshot ingestion to the Declarative Pipeline `apply_changes_from_snapshot` API, which compares each snapshot to the previous state to produce correct insert, update, and delete semantics.

## Configuration

Set `source_format` to `snapshot` and configure `bronze_apply_changes_from_snapshot`:

```json
[
  {
    "data_flow_id": "1",
    "data_flow_group": "snapshot_group",
    "source_format": "snapshot",
    "source_details": {
      "snapshot_format": "parquet",
      "source_path_dev": "s3://my-bucket/snapshots/customers/",
      "source_path_prod": "s3://my-prod-bucket/snapshots/customers/"
    },
    "bronze_catalog_dev": "my_catalog",
    "bronze_database_dev": "retail_bronze",
    "bronze_table": "customers_snapshot",
    "bronze_apply_changes_from_snapshot": {
      "keys": ["customer_id"],
      "scd_type": "1"
    }
  }
]
```

## `source_details` fields for snapshot

| Field | Description |
|---|---|
| `snapshot_format` | File format: `parquet`, `json`, `csv`, `avro`, etc. |
| `source_path_{env}` | Cloud storage path where snapshot files are delivered |

## `bronze_apply_changes_from_snapshot` fields

| Field | Type | Required | Description |
|---|---|---|---|
| `keys` | array of strings | Yes | Primary key columns |
| `scd_type` | string | Yes | `1` for overwrite, `2` to retain history |
| `track_history_column_list` | array of strings | No | (Type 2 only) Columns whose changes trigger a new history row |
| `track_history_except_column_list` | array of strings | No | (Type 2 only) Columns to exclude from history tracking |

## Silver layer with snapshot

Configure `silver_apply_changes_from_snapshot` for snapshot-apply semantics on the silver table:

```json
{
  "silver_apply_changes_from_snapshot": {
    "keys": ["customer_id"],
    "scd_type": "2",
    "track_history_except_column_list": ["last_updated_ts"]
  }
}
```

## Demo output

![Apply changes from snapshot demo](/img/acfs.png)

## Running the snapshot demo

```bash
python -m demo.launch_acfs_demo \
  --uc_catalog_name=<your_catalog> \
  --profile=<profile-name>
```

## Related

- [Declarative Pipeline `apply_changes_from_snapshot` API](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-apply-changes)
- [CDC with apply_changes](./cdc) — for streaming CDC sources
- [Onboarding File Fields](../reference/onboarding-fields)
