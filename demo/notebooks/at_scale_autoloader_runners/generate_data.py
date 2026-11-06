# Databricks notebook source
import json
from datetime import datetime, timezone

dbutils.widgets.text("uc_catalog_name", "")
dbutils.widgets.text("sdp_meta_schema", "")
dbutils.widgets.text("uc_volume_path", "")
dbutils.widgets.text("table_count", "100")
dbutils.widgets.dropdown("phase", "1", ["1", "2"])

catalog = dbutils.widgets.get("uc_catalog_name")
meta_schema = dbutils.widgets.get("sdp_meta_schema")
volume = dbutils.widgets.get("uc_volume_path").rstrip("/")
table_count = int(dbutils.widgets.get("table_count"))
phase = dbutils.widgets.get("phase")

if table_count < 3:
    raise ValueError("table_count must be at least 3")

explicit_count = max(1, table_count // 10)
hinted_count = max(1, table_count // 10)
inferred_count = table_count - explicit_count - hinted_count
first_hinted_index = inferred_count + 1
explicit_ddl = (
    "event_id BIGINT, customer_id BIGINT, event_ts TIMESTAMP, "
    "amount DECIMAL(12,2), region STRING"
)


def table_identity(index):
    if index == 1:
        return "orders", "inferred"
    if index <= inferred_count:
        return f"inferred_{index:03d}", "inferred"
    if index <= inferred_count + hinted_count:
        return f"hinted_{index:03d}", "hinted"
    return f"explicit_{index:03d}", "explicit"


def put_json_lines(path, rows):
    payload = "\n".join(json.dumps(row) for row in rows) + "\n"
    dbutils.fs.put(path, payload, overwrite=True)


root = f"{volume}/data/at_scale_autoloader"
landing_root = f"{root}/landing"
ddl_root = f"{root}/ddl"

if phase == "1":
    dbutils.fs.rm(root, recurse=True)
    dbutils.fs.mkdirs(landing_root)
    dbutils.fs.mkdirs(ddl_root)

    for index in range(1, table_count + 1):
        table_name, strategy = table_identity(index)
        rows = []
        for row_index in range(1, 5):
            rows.append(
                {
                    "event_id": (
                        None
                        if table_name == "orders" and row_index == 4
                        else index * 1000 + row_index
                    ),
                    "customer_id": (row_index % 3) + 1,
                    "event_ts": (
                        f"2026-01-{row_index:02d}T12:00:00Z"
                    ),
                    "amount": float(index * 10 + row_index) + 0.25,
                    "region": ["amer", "emea", "apj"][row_index % 3],
                }
            )
        target_dir = f"{landing_root}/{table_name}"
        dbutils.fs.mkdirs(target_dir)
        put_json_lines(f"{target_dir}/phase1.json", rows)

        if strategy == "explicit":
            dbutils.fs.put(
                f"{ddl_root}/{table_name}.ddl",
                explicit_ddl,
                overwrite=True,
            )

    dqe = {
        "expect": {
            "valid_event_id": "event_id IS NOT NULL",
        },
        "expect_or_quarantine": {
            "invalid_event_id": "event_id IS NULL",
        }
    }
    dbutils.fs.mkdirs(f"{volume}/conf/at_scale_autoloader")
    dbutils.fs.put(
        f"{volume}/conf/at_scale_autoloader/dqe.json",
        json.dumps(dqe, indent=2),
        overwrite=True,
    )
    silver_transformations = [
        {
            "target_table": table_identity(index)[0],
            "select_exp": ["*"],
        }
        for index in range(1, table_count + 1)
    ]
    dbutils.fs.put(
        (
            f"{volume}/conf/at_scale_autoloader/"
            "silver_transformations.json"
        ),
        json.dumps(silver_transformations, indent=2),
        overwrite=True,
    )

    dimension = [
        (1, "Ada", "silver", datetime(2025, 1, 1, tzinfo=timezone.utc)),
        (1, "Ada", "gold", datetime(2026, 1, 1, tzinfo=timezone.utc)),
        (2, "Grace", "platinum", datetime(2026, 1, 1, tzinfo=timezone.utc)),
        (3, "Linus", "bronze", datetime(2026, 1, 1, tzinfo=timezone.utc)),
    ]
    spark.createDataFrame(
        dimension,
        "customer_id BIGINT, customer_name STRING, customer_tier STRING, "
        "effective_ts TIMESTAMP",
    ).write.mode("overwrite").saveAsTable(
        f"{catalog}.{meta_schema}.customer_dimension"
    )
    print(
        f"Generated phase 1 for {table_count} tables: "
        f"{inferred_count} inferred, {hinted_count} hinted, "
        f"{explicit_count} explicit."
    )
else:
    table_name, strategy = table_identity(first_hinted_index)
    if strategy != "hinted":
        raise AssertionError(
            f"Expected {table_name} to be in the hinted cohort"
        )
    target_dir = f"{landing_root}/{table_name}"
    rows = [
        {
            "event_id": 900001,
            "customer_id": 1,
            "event_ts": "2026-02-01T12:00:00Z",
            "amount": 42.50,
            "region": "amer",
            "device_type": "mobile",
        },
        {
            "event_id": 900002,
            "customer_id": 2,
            "event_ts": "2026-02-02T12:00:00Z",
            "amount": "not-a-decimal",
            "region": "emea",
            "device_type": "web",
        },
    ]
    put_json_lines(f"{target_dir}/phase2.json", rows)
    print(
        f"Staged phase 2 for {table_name}: device_type evolution and "
        "one incompatible amount."
    )
