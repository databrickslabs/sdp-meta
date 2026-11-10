---
id: kafka-eventhub
title: Kafka & Event Hubs
sidebar_position: 2
---

# Kafka & Event Hubs

SDP-META supports streaming ingestion from Apache Kafka and Azure Event Hubs. Event Hubs uses the Kafka protocol endpoint, so both share the same underlying Spark Structured Streaming reader.

## Apache Kafka

### Prerequisites

- A running Kafka broker reachable from your Databricks cluster
- A source topic plus two sink topics already created. With the example below, create `sdp-meta-source`, `sdp-meta-sink_1`, and `sdp-meta-sink_2`; `--kafka_sink_topic` is the prefix used for both sink topic names.
- The broker address stored in Databricks Secrets:

```bash
databricks secrets create-scope kafka_creds
databricks secrets put-secret kafka_creds broker --string-value "host:9092"
```

### Onboarding configuration

```json
{
  "data_flow_id": "1",
  "data_flow_group": "streaming_group",
  "source_format": "kafka",
  "source_details": {
    "source_schema_path": "/Volumes/my_catalog/my_schema/my_volume/schema/events.ddl",
    "kafka.bootstrap.servers": "broker-host:9092",
    "subscribe": "my-kafka-topic"
  },
  "bronze_catalog_dev": "my_catalog",
  "bronze_database_dev": "streaming_bronze",
  "bronze_table": "events_bronze",
  "bronze_reader_options": {
    "startingOffsets": "latest",
    "failOnDataLoss": "false"
  }
}
```

### Key `source_details` fields for Kafka

| Field | Description |
|---|---|
| `source_schema_path` | Path to the Spark DDL schema file |
| `kafka.bootstrap.servers` | Comma-separated broker addresses |
| `subscribe` | Kafka topic name |
| `kafka.sasl.mechanism` | SASL mechanism, e.g. `PLAIN`, `SCRAM-SHA-256` |
| `kafka.security.protocol` | Security protocol, e.g. `SASL_SSL`, `PLAINTEXT` |

### Running the Kafka demo

```bash
python integration_tests/run_integration_tests.py \
  --source=kafka \
  --uc_catalog_name=<your_catalog> \
  --kafka_source_topic=sdp-meta-source \
  --kafka_sink_topic=sdp-meta-sink \
  --kafka_source_servers_secrets_scope_name=kafka_creds \
  --kafka_source_servers_secrets_scope_key=broker \
  --kafka_sink_servers_secret_scope_name=kafka_creds \
  --kafka_sink_servers_secret_scope_key=broker \
  --profile=<profile-name>
```

## Azure Event Hubs

Azure Event Hubs exposes a Kafka-compatible endpoint. SDP-META uses Databricks Secrets to store the connection key.

### Prerequisites

1. An Azure Event Hubs namespace with the source, append-flow, and sink Event Hubs used below
2. One shared SAS policy named `sdp-meta-access` with both `Listen` and `Send` permissions
3. That policy's SAS key stored in Databricks Secrets:

```bash
databricks secrets create-scope eventhubs_creds
databricks secrets put-secret eventhubs_creds sdp-meta-access --string-value "<shared-policy-sas-key>"
```

### Onboarding configuration

```json
{
  "data_flow_id": "1",
  "data_flow_group": "iot_group",
  "source_format": "eventhub",
  "source_details": {
    "source_schema_path": "/Volumes/my_catalog/my_schema/my_volume/schema/iot.ddl",
    "eventhub.accessKeyName": "sdp-meta-access",
    "eventhub.accessKeySecretName": "sdp-meta-access",
    "eventhub.name": "iot-events",
    "eventhub.secretsScopeName": "eventhubs_creds",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol": "SASL_SSL",
    "eventhub.namespace": "my-eventhubs-namespace",
    "eventhub.port": "9093"
  },
  "bronze_catalog_dev": "my_catalog",
  "bronze_database_dev": "iot_bronze",
  "bronze_table": "iot_bronze"
}
```

### Key `source_details` fields for Event Hubs

| Field | Description |
|---|---|
| `source_schema_path` | Path to the Spark DDL schema file |
| `eventhub.accessKeyName` | Name of the SAS policy |
| `eventhub.accessKeySecretName` | Databricks Secrets key name containing the SAS key value |
| `eventhub.name` | Event Hub topic name |
| `eventhub.secretsScopeName` | Databricks Secrets scope |
| `kafka.sasl.mechanism` | Always `PLAIN` for Event Hubs |
| `kafka.security.protocol` | Always `SASL_SSL` for Event Hubs |
| `eventhub.namespace` | Namespace (without `.servicebus.windows.net`) |
| `eventhub.port` | Always `9093` for Event Hubs |

### Event Hubs demo output

![Event Hubs demo result](/img/af_eh_demo.png)

### Running the Event Hubs demo

```bash
python integration_tests/run_integration_tests.py \
  --source=eventhub \
  --uc_catalog_name=<your_catalog> \
  --eventhub_name=iot \
  --eventhub_name_append_flow=iot-af \
  --eventhub_sink_name=iot-sink \
  --eventhub_secrets_scope_name=eventhubs_creds \
  --eventhub_accesskey_secret_name=sdp-meta-access \
  --eventhub_namespace=my-eventhubs-namespace \
  --eventhub_port=9093 \
  --eventhub_producer_accesskey_name=sdp-meta-access \
  --eventhub_consumer_accesskey_name=sdp-meta-access \
  --profile=<profile-name>
```

## Related

- [Onboarding File Fields — eventhub source_details](../reference/onboarding-fields#source_details--eventhub)
- [Onboarding File Fields — kafka source_details](../reference/onboarding-fields#source_details--kafka)
- [Integration Tests](../operations/integration-tests)
