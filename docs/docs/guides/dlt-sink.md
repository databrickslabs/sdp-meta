---
id: dlt-sink
title: Declarative Pipeline Sink
sidebar_position: 6
---

# Declarative Pipeline Sink

The sink feature writes pipeline output to external Delta tables or Kafka topics outside the pipeline's managed storage. SDP-META maps the `bronze_sinks` and `silver_sinks` arrays to the Declarative Pipeline Sink API.

![Delta sink](/img/dlt_delta_sink.png)

![Kafka sink](/img/dlt_kafka_sink.png)

![Sink demo overview](/img/dlt_demo_sink.png)

## `bronze_sinks` configuration

### Delta sink

```json
{
  "bronze_sinks": [
    {
      "name": "bronze_sink",
      "format": "delta",
      "options": {
        "tablename": "my_catalog.my_schema.my_external_table"
      }
    }
  ]
}
```

### Kafka sink

```json
{
  "bronze_sinks": [
    {
      "name": "bronze_sink",
      "format": "kafka",
      "options": {
        "kafka.bootstrap.servers": "broker-host:9092",
        "topic": "my-output-topic"
      }
    }
  ]
}
```

## `silver_sinks` configuration

Same structure as `bronze_sinks`:

```json
{
  "silver_sinks": [
    {
      "name": "silver_sink",
      "format": "delta",
      "options": {
        "tablename": "my_catalog.my_schema.my_silver_external_table"
      }
    }
  ]
}
```

## Sink object fields

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | Yes | Logical name for the sink |
| `format` | string | Yes | `delta` or `kafka` |
| `options` | object | Yes | Format-specific options |

**Delta sink options:**

| Option | Description |
|---|---|
| `tablename` | Fully qualified table name: `catalog.schema.table` |

**Kafka sink options:**

| Option | Description |
|---|---|
| `kafka.bootstrap.servers` | Comma-separated broker addresses |
| `topic` | Target Kafka topic name |

## Full example

```json
[
  {
    "data_flow_id": "1",
    "data_flow_group": "orders_group",
    "source_format": "cloudFiles",
    "source_details": {
      "source_schema_path": "/Volumes/my_catalog/my_schema/my_volume/schema/orders.ddl",
      "source_path_dev": "s3://my-bucket/landing/orders/"
    },
    "bronze_catalog_dev": "my_catalog",
    "bronze_database_dev": "retail_bronze",
    "bronze_table": "orders_bronze",
    "bronze_sinks": [
      {
        "name": "bronze_sink",
        "format": "delta",
        "options": {
          "tablename": "external_catalog.archive_schema.orders_archive"
        }
      }
    ],
    "silver_catalog_dev": "my_catalog",
    "silver_database_dev": "retail_silver",
    "silver_table": "orders_silver",
    "silver_transformation_json_dev": "/Volumes/my_catalog/my_schema/my_volume/conf/silver_transformations.json",
    "silver_sinks": [
      {
        "name": "silver_sink",
        "format": "kafka",
        "options": {
          "kafka.bootstrap.servers": "broker-host:9092",
          "topic": "orders-processed"
        }
      }
    ]
  }
]
```

## Related

- [Declarative Pipeline Sink API](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-sink)
- [Onboarding File Fields — `bronze_sinks` and `silver_sinks`](../reference/onboarding-fields#bronze-layer-fields)
