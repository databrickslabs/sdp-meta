---
id: integration-tests
title: Integration Tests
sidebar_position: 2
---

# Integration Tests

The SDP-META integration test suite runs end-to-end pipeline tests against a real Databricks workspace, covering cloudFiles, Event Hubs, and Kafka source types.

## Initial setup

**Prerequisites:**

- [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/profiles.html) installed and authenticated
- Python 3.10–3.12
- A Databricks workspace with Unity Catalog enabled

```bash
git clone https://github.com/databrickslabs/sdp-meta.git
cd sdp-meta
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements-dev.txt
```

## Running integration tests

Pass `--profile=<profile-name>` for a named Databricks CLI profile. For service-principal or token authentication, omit `--profile` and configure the standard Databricks SDK environment variables such as `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, and `DATABRICKS_CLIENT_SECRET` (or `DATABRICKS_TOKEN`).

### cloudFiles

```bash
python integration_tests/run_integration_tests.py \
  --source=cloudfiles \
  --uc_catalog_name=<your_catalog> \
  --profile=<profile-name>
```

### Event Hubs

**Prerequisites:** The source, append-flow, and sink Event Hubs plus one shared SAS policy named `sdp-meta-access` with both `Listen` and `Send` permissions. Store that policy's key in Databricks Secrets:

```bash
databricks secrets create-scope eventhubs_creds
databricks secrets put-secret eventhubs_creds sdp-meta-access --string-value "<shared-policy-sas-key>"
```

```bash
python integration_tests/run_integration_tests.py \
  --source=eventhub \
  --uc_catalog_name=<your_catalog> \
  --eventhub_name=iot \
  --eventhub_name_append_flow=iot-af \
  --eventhub_sink_name=iot-sink \
  --eventhub_secrets_scope_name=eventhubs_creds \
  --eventhub_accesskey_secret_name=sdp-meta-access \
  --eventhub_namespace=<your-namespace> \
  --eventhub_port=9093 \
  --eventhub_producer_accesskey_name=sdp-meta-access \
  --eventhub_consumer_accesskey_name=sdp-meta-access \
  --profile=<profile-name>
```

**Required arguments for Event Hubs:**

| Argument | Description |
|---|---|
| `--eventhub_name` | Event Hub topic name |
| `--eventhub_name_append_flow` | Event Hub topic used by the append-flow test |
| `--eventhub_sink_name` | Event Hub sink name used by the integration test |
| `--eventhub_namespace` | Namespace (without `.servicebus.windows.net`) |
| `--eventhub_port` | Always `9093` |
| `--eventhub_secrets_scope_name` | Databricks Secrets scope name |
| `--eventhub_accesskey_secret_name` | Secret key containing the shared SAS policy key |
| `--eventhub_producer_accesskey_name` | SAS policy name used to publish source and append-flow events |
| `--eventhub_consumer_accesskey_name` | SAS policy name used to read source events |

### Kafka

**Prerequisites:** A running Kafka broker reachable from Databricks, one source topic, and two sink topics. With the example below, create `sdp-meta-source`, `sdp-meta-sink_1`, and `sdp-meta-sink_2`; `--kafka_sink_topic` supplies the sink-topic prefix. Store the broker address in Databricks Secrets:

```bash
databricks secrets create-scope kafka_creds
databricks secrets put-secret kafka_creds broker --string-value "host:9092"
```

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

**Required arguments for Kafka:**

| Argument | Description |
|---|---|
| `--kafka_source_topic` | Kafka topic read by the integration test |
| `--kafka_sink_topic` | Prefix for the two sink topics (`<prefix>_1` and `<prefix>_2`) |
| `--kafka_source_servers_secrets_scope_name` | Secret scope containing the source broker address |
| `--kafka_source_servers_secrets_scope_key` | Secret key containing the source broker address |
| `--kafka_sink_servers_secret_scope_name` | Secret scope containing the sink broker address |
| `--kafka_sink_servers_secret_scope_key` | Secret key containing the sink broker address |

## Testing backward-compatible upgrades

`integration_tests/run_backward_compat_tests.py` verifies that an existing
pipeline continues working when its installed package is upgraded. It runs
both versions against the same pipeline IDs and checkpoints:

1. Phase 1 builds and installs the source wheel, onboards the legacy
   configuration, runs Bronze and Silver, and records baseline row counts.
2. Phase 2 swaps the existing pipelines to the target install specification,
   adds an incremental input batch, reruns Bronze and Silver, and verifies data
   preservation, incremental growth, package resolution, and legacy imports.

The default run tests the released legacy-to-current upgrade:

```bash
python integration_tests/run_backward_compat_tests.py \
  --uc_catalog_name=<your_catalog> \
  --profile=<profile-name>
```

Use `--build_target_from_worktree` while testing uncommitted target-side
changes. The source wheel still comes from the pinned source Git ref:

```bash
python integration_tests/run_backward_compat_tests.py \
  --uc_catalog_name=<your_catalog> \
  --install_mode=local \
  --build_target_from_worktree \
  --profile=<profile-name>
```

### Testing the `dlt-meta` compatibility redirect

The `compat_wheelhouse` target surface verifies that installing the legacy
`dlt-meta` distribution resolves the new `databricks-labs-sdp-meta`
distribution and keeps `dlt_meta` and `src.*` compatibility available:

```bash
python integration_tests/run_backward_compat_tests.py \
  --uc_catalog_name=<your_catalog> \
  --install_mode=local \
  --build_target_from_worktree \
  --target_install_surface=compat_wheelhouse \
  --profile=<profile-name>
```

This mode is limited to local-wheel, legacy-to-current upgrades. Before
creating workspace resources, the runner:

- builds the target primary and redirect wheels;
- derives their shared package version and fails if they disagree;
- downloads a complete binary runtime wheelhouse for the target interpreter;
- verifies the primary wheel's unconditional runtime dependencies are present;
- uploads the wheelhouse to the run's Unity Catalog volume.

Phase 1 uses the source notebook's original wheel install. At the phase
boundary, only the uploaded notebook copy is replaced with a Phase 2 install
that uses `--force-reinstall --no-index --find-links`. The pipeline therefore
resolves `dlt-meta==<derived-version>` entirely from the uploaded wheelhouse
without requiring PyPI access from serverless compute.

`--target_package_version=<version>` is optional. When omitted, the version is
derived from the built target wheels. When supplied, it acts as an assertion
and the run fails before upload if it does not match both wheels.

`--compat_python_version` selects the CPython minor version used when
downloading binary dependencies and defaults to `3.12`. Override it when the
target Databricks runtime uses another supported Python minor version:

```bash
--compat_python_version=3.11
```

The dependency download runs on the machine launching the test and requires
PyPI or package-mirror access there. It times out after 600 seconds with a
diagnostic instead of hanging indefinitely.

## Test output

Results are downloaded locally as `integration_test_output_<run_id>.csv`. A successful run:

```
0,Completed Bronze Lakeflow Spark Declarative Pipeline.
1,Completed Silver Lakeflow Spark Declarative Pipeline.
2,Validating Lakeflow Spark Declarative Pipeline Bronze and Silver Table Counts...
3,Validating Counts for Table bronze_7d1d3ccc9e144a85b07c23110ea50133.transactions.
4,Expected: 10002 Actual: 10002. Passed!
5,Validating Counts for Table bronze_7d1d3ccc9e144a85b07c23110ea50133.transactions_quarantine.
6,Expected: 7 Actual: 7. Passed!
...
```

## Running unit tests locally

Unit tests do not require a Databricks workspace:

```bash
python -m pip install -r requirements-dev.txt
pytest tests/
coverage run -m pytest tests/
coverage report
```

:::warning
Unit tests require Python 3.10–3.12. See [Troubleshooting — Python version issues](./troubleshooting#python-version-issues).
:::
