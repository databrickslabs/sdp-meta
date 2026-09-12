"""Dataflow Spec related utilities."""
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, row_number
from pyspark.sql.session import SparkSession
from pyspark.sql.window import Window

logger = logging.getLogger("sdp-meta")
logger.setLevel(logging.INFO)

DQE_CONTRACT_VERSION = "v2"


def _coerce_scd_type_to_str(payload: dict) -> dict:
    """Coerce an integer ``scd_type`` in ``payload`` to its string form.

    Onboarding files written for v0.0.10 and earlier carried
    ``"scd_type": 1`` as an int (issue #370), and dataflowspec tables
    onboarded with those versions persisted the int inside the CDC JSON
    payloads. The pipeline compares ``scd_type == "2"`` and passes the
    value to ``stored_as_scd_type=...`` which expects a string, so an
    int would silently disable SCD2 handling. Normalizing here — at the
    parse boundary — covers both freshly onboarded specs and specs
    already stored by older versions. ``bool`` is excluded because it is
    an ``int`` subclass but was never a valid SCD type.

    Mutates and returns ``payload`` for call-site convenience.
    """
    scd_type = payload.get("scd_type")
    if isinstance(scd_type, int) and not isinstance(scd_type, bool):
        payload["scd_type"] = str(scd_type)
    return payload


@dataclass
class BronzeDataflowSpec:
    """A schema to hold a dataflow spec used for writing to the bronze layer."""

    dataFlowId: str
    dataFlowGroup: str
    sourceFormat: str
    sourceDetails: map
    readerConfigOptions: map
    targetFormat: str
    targetDetails: map
    tableProperties: map
    schema: str
    partitionColumns: list
    cdcApplyChanges: str
    applyChangesFromSnapshot: str
    dataQualityExpectations: str
    quarantineTargetDetails: map
    quarantineTableProperties: map
    appendFlows: str
    appendFlowsSchemas: map
    version: str
    createDate: datetime
    createdBy: str
    updateDate: datetime
    updatedBy: str
    clusterBy: list
    clusterByAuto: bool
    sinks: str
    # Multi-source AUTO CDC (issue #294): JSON-encoded
    # ``CDCApplyChangesFlowGroup`` describing N create_auto_cdc_flow calls
    # targeting this bronze table, plus a per-flow schema map keyed by
    # ``flow.name`` (mirrors ``appendFlowsSchemas``). Older dataflowspec
    # Delta tables that pre-date this feature are forward-compatible via
    # :attr:`DataflowSpecUtils.additional_bronze_df_columns`, which fills
    # missing columns with ``None`` at read time.
    cdcApplyChangesFlows: str
    cdcApplyChangesFlowsSchemas: map
    # UC row-level security (issue #303): canonical
    # ``ROW FILTER cat.schema.func ON (col)`` clause attached to the main
    # bronze table and (optionally and independently) the quarantine table.
    # Silently dropped on non-UC pipelines via
    # :meth:`DataflowPipeline._get_row_filter` /
    # :meth:`DataflowPipeline._get_quarantine_row_filter`. Forward-compatible
    # with legacy Delta dataflowspec tables via
    # :attr:`DataflowSpecUtils.additional_bronze_df_columns`.
    rowFilter: str
    quarantineRowFilter: str
    # Validated, immutable quality-engine snapshot. ``None`` selects the
    # existing legacy/standard runtime paths.
    qualityConfig: str = None
    # ``None`` identifies rows onboarded before strict legacy-DQE validation.
    # New/re-onboarded rows are stamped with :data:`DQE_CONTRACT_VERSION`.
    dqeContract: str = None


@dataclass
class SilverDataflowSpec:
    """A schema to hold a dataflow spec used for writing to the silver layer."""

    dataFlowId: str
    dataFlowGroup: str
    sourceFormat: str
    sourceDetails: map
    readerConfigOptions: map
    targetFormat: str
    targetDetails: map
    tableProperties: map
    selectExp: list
    whereClause: list
    partitionColumns: list
    cdcApplyChanges: str
    applyChangesFromSnapshot: str
    dataQualityExpectations: str
    quarantineTargetDetails: map
    quarantineTableProperties: map
    appendFlows: str
    appendFlowsSchemas: map
    version: str
    createDate: datetime
    createdBy: str
    updateDate: datetime
    updatedBy: str
    clusterBy: list
    clusterByAuto: bool
    sinks: str
    # Multi-source AUTO CDC (issue #294): JSON-encoded
    # ``CDCApplyChangesFlowGroup`` describing N create_auto_cdc_flow calls
    # targeting this silver table. Silver has no per-flow schema map
    # because silver flows always read from Delta upstream where schema
    # is inferred from the source table. Older dataflowspec Delta tables
    # are forward-compatible via
    # :attr:`DataflowSpecUtils.additional_silver_df_columns`.
    cdcApplyChangesFlows: str
    # UC row-level security (issue #303). See the bronze docstring above for
    # the full rationale; the silver version is wired through the same
    # :meth:`DataflowPipeline._get_row_filter` /
    # :meth:`DataflowPipeline._get_quarantine_row_filter` helpers.
    rowFilter: str
    quarantineRowFilter: str
    # Validated, immutable quality-engine snapshot. ``None`` selects the
    # existing legacy/standard runtime paths.
    qualityConfig: str = None
    dqeContract: str = None


@dataclass
class CDCApplyChanges:
    """CDC ApplyChanges structure."""

    keys: list
    sequence_by: str
    where: str
    ignore_null_updates: bool
    apply_as_deletes: str
    apply_as_truncates: str
    column_list: list
    except_column_list: list
    scd_type: str
    track_history_column_list: list
    track_history_except_column_list: list
    flow_name: str
    once: bool
    ignore_null_updates_column_list: list
    ignore_null_updates_except_column_list: list


@dataclass
class ApplyChangesFromSnapshot:
    """CDC ApplyChangesFromSnapshot structure."""
    keys: list
    scd_type: str
    track_history_column_list: list
    track_history_except_column_list: list


@dataclass
class AppendFlow:
    """Append Flow structure."""
    name: str
    comment: str
    create_streaming_table: bool
    source_format: str
    source_details: map
    reader_options: map
    spark_conf: map
    once: bool


@dataclass
class CDCApplyChangesFlow:
    """One AUTO CDC flow inside a multi-source CDC group.

    Each flow becomes a separate dp.create_auto_cdc_flow call against the
    same target streaming table. The CDC config (keys/sequence_by/scd_type
    etc.) is shared across flows at the group level — DLT requires those
    to be identical when multiple create_auto_cdc_flow calls target one
    streaming table — so only the read-side and per-flow normalization
    fields live here.
    """
    name: str
    source_format: str
    source_details: map
    reader_options: map
    select_exp: list
    where_clause: list
    once: bool


@dataclass
class CDCApplyChangesFlowGroup:
    """N AUTO CDC flows landing in one streaming table.

    The shared CDC config lives at the group level because DLT requires
    identical keys / sequence_by / scd_type / etc. across all
    create_auto_cdc_flow calls targeting the same streaming table. Flow
    definitions in :attr:`flows` carry only the read-side surface that
    legitimately varies per source (source_format, source_details,
    reader_options, select_exp, where_clause, once).
    """
    keys: list
    sequence_by: str
    scd_type: str
    where: str
    ignore_null_updates: bool
    apply_as_deletes: str
    apply_as_truncates: str
    column_list: list
    except_column_list: list
    track_history_column_list: list
    track_history_except_column_list: list
    ignore_null_updates_column_list: list
    ignore_null_updates_except_column_list: list
    flows: list  # list[CDCApplyChangesFlow]


@dataclass
class DLTSink:
    name: str
    format: str
    options: map
    select_exp: list
    where_clause: str


class DataflowSpecUtils:
    """A collection of methods for working with DataflowSpec."""

    cdc_applychanges_api_mandatory_attributes = ["keys", "sequence_by", "scd_type"]
    cdc_applychanges_api_attributes = [
        "keys",
        "sequence_by",
        "where",
        "ignore_null_updates",
        "apply_as_deletes",
        "apply_as_truncates",
        "column_list",
        "except_column_list",
        "scd_type",
        "track_history_column_list",
        "track_history_except_column_list",
        "flow_name",
        "once",
        "ignore_null_updates_column_list",
        "ignore_null_updates_except_column_list"
    ]

    cdc_applychanges_api_attributes_defaults = {
        "where": None,
        "ignore_null_updates": False,
        "apply_as_deletes": None,
        "apply_as_truncates": None,
        "column_list": None,
        "except_column_list": None,
        "track_history_column_list": None,
        "track_history_except_column_list": None,
        "flow_name": None,
        "once": False,
        "ignore_null_updates_column_list": None,
        "ignore_null_updates_except_column_list": None
    }

    append_flow_mandatory_attributes = ["name", "source_format", "create_streaming_table", "source_details"]
    sink_mandatory_attributes = ["name", "format", "options"]
    supported_sink_formats = ["delta", "kafka", "eventhub"]

    append_flow_api_attributes_defaults = {
        "comment": None,
        "create_streaming_table": False,
        "reader_options": None,
        "spark_conf": None,
        "once": False
    }

    # Multi-source AUTO CDC (issue #294). Per-flow fields mirror
    # ``AppendFlow`` (name + source_format + source_details +
    # reader_options + once) plus the per-flow select_exp / where_clause
    # used for normalizing each source's schema before the merge. The
    # group-level CDC config (keys / sequence_by / scd_type / etc.) reuses
    # ``cdc_applychanges_api_attributes`` and the same defaults map above.
    cdc_apply_changes_flow_mandatory_attributes = [
        "name",
        "source_format",
        "source_details",
    ]
    cdc_apply_changes_flow_attributes_defaults = {
        "reader_options": None,
        "select_exp": None,
        "where_clause": None,
        "once": False,
    }
    cdc_apply_changes_flows_group_mandatory_attributes = [
        "keys",
        "sequence_by",
        "scd_type",
        "flows",
    ]

    sink_attributes_defaults = {
        "select_exp": None,
        "where_clause": None
    }

    additional_bronze_df_columns = [
        "appendFlows",
        "appendFlowsSchemas",
        "applyChangesFromSnapshot",
        "clusterBy",
        "clusterByAuto",
        "sinks",
        # Multi-source AUTO CDC (issue #294). Both default to ``None`` on
        # spec rows written before v0.1.0 so old dataflowspec tables load
        # without rewriting.
        "cdcApplyChangesFlows",
        "cdcApplyChangesFlowsSchemas",
        # UC row-level security (issue #303). Both default to ``None`` for
        # legacy dataflowspec rows.
        "rowFilter",
        "quarantineRowFilter",
        "qualityConfig",
        "dqeContract",
    ]
    additional_silver_df_columns = [
        "dataQualityExpectations",
        "quarantineTargetDetails",
        "quarantineTableProperties",
        "appendFlows",
        "appendFlowsSchemas",
        "applyChangesFromSnapshot",
        "clusterBy",
        "clusterByAuto",
        "sinks",
        # Multi-source AUTO CDC (issue #294). Silver has no per-flow
        # schemas map; defaults to ``None`` for spec rows written before
        # v0.1.0.
        "cdcApplyChangesFlows",
        # UC row-level security (issue #303). See bronze entry above.
        "rowFilter",
        "quarantineRowFilter",
        "qualityConfig",
        "dqeContract",
    ]
    additional_cdc_apply_changes_columns = ["flow_name", "once"]
    apply_changes_from_snapshot_api_attributes = [
        "keys",
        "scd_type",
        "track_history_column_list",
        "track_history_except_column_list"
    ]
    apply_changes_from_snapshot_api_mandatory_attributes = ["keys", "scd_type"]
    additional_apply_changes_from_snapshot_columns = ["track_history_column_list", "track_history_except_column_list"]
    apply_changes_from_snapshot_api_attributes_defaults = {
        "track_history_column_list": None,
        "track_history_except_column_list": None
    }

    @staticmethod
    def _get_dataflow_spec(
        spark: SparkSession,
        layer: str,
        dataflow_spec_df: DataFrame = None,
        group: str = None,
        dataflow_ids: str = None,
    ) -> DataFrame:
        """Get DataflowSpec for given parameters.

        Can be configured using spark config values, used for optionally filtering
        the returned data to a group or list of DataflowIDs
        """
        if not group:
            group = spark.conf.get(f"{layer}.group", None)

        if not dataflow_ids:
            dataflow_ids = spark.conf.get(f"{layer}.dataflowIds", None)

        if not dataflow_spec_df:
            dataflow_spec_table = spark.conf.get(f"{layer}.dataflowspecTable")
            dataflow_spec_df = spark.read.table(dataflow_spec_table)

        if group:
            dataflow_spec_df = dataflow_spec_df.where(col("dataFlowGroup") == lit(group))
        elif dataflow_ids:
            # Parse the comma-separated dataflowIds and filter with a typed
            # ``isin(...)`` instead of splicing the raw conf string into a SQL
            # predicate (``dataFlowId in (<raw>)``). This keeps operator-supplied
            # input out of the SQL text entirely — no injection surface — while
            # preserving the previous membership-filter behaviour. Surrounding
            # quotes/whitespace are stripped so both ``100,101`` and ``'a','b'``
            # styles work.
            id_list = [
                token.strip().strip("'\"")
                for token in dataflow_ids.split(",")
                if token.strip()
            ]
            if id_list:
                dataflow_spec_df = dataflow_spec_df.where(col("dataFlowId").isin(*id_list))

        version_history = Window.partitionBy(col("dataFlowGroup"), col("dataFlowId")).orderBy(col("version").desc())
        dataflow_spec_df = (
            dataflow_spec_df.withColumn("row_num", row_number().over(version_history))
            .where(col("row_num") == lit(1))  # latest version
            .drop(col("row_num"))
        )

        return dataflow_spec_df

    @staticmethod
    def get_bronze_dataflow_spec(spark) -> List[BronzeDataflowSpec]:
        """Get bronze dataflow spec."""
        DataflowSpecUtils.check_spark_dataflowpipeline_conf_params(spark, "bronze")
        dataflow_spec_rows = DataflowSpecUtils._get_dataflow_spec(spark, "bronze").collect()
        bronze_dataflow_spec_list: list[BronzeDataflowSpec] = []
        for row in dataflow_spec_rows:
            target_row = DataflowSpecUtils.populate_additional_df_cols(
                row.asDict(),
                DataflowSpecUtils.additional_bronze_df_columns
            )
            bronze_dataflow_spec_list.append(BronzeDataflowSpec(**target_row))
        logger.info(f"bronze_dataflow_spec_list={bronze_dataflow_spec_list}")
        return bronze_dataflow_spec_list

    @staticmethod
    def populate_additional_df_cols(onboarding_row_dict, additional_columns):
        for column in additional_columns:
            if column not in onboarding_row_dict.keys():
                onboarding_row_dict[column] = None
        return onboarding_row_dict

    @staticmethod
    def get_silver_dataflow_spec(spark) -> List[SilverDataflowSpec]:
        """Get silver dataflow spec list."""
        DataflowSpecUtils.check_spark_dataflowpipeline_conf_params(spark, "silver")

        dataflow_spec_rows = DataflowSpecUtils._get_dataflow_spec(spark, "silver").collect()
        silver_dataflow_spec_list: list[SilverDataflowSpec] = []
        for row in dataflow_spec_rows:
            target_row = DataflowSpecUtils.populate_additional_df_cols(
                row.asDict(),
                DataflowSpecUtils.additional_silver_df_columns
            )
            silver_dataflow_spec_list.append(SilverDataflowSpec(**target_row))
        return silver_dataflow_spec_list

    @staticmethod
    def check_spark_dataflowpipeline_conf_params(spark, layer_arg):
        """Check dataflowpipine config params."""
        layer = spark.conf.get("layer", None)
        if layer is None:
            raise Exception(
                f"""parameter {layer_arg} is missing in spark.conf.
                 Please set spark.conf.set({layer_arg},'silver') """
            )
        dataflow_spec_table = spark.conf.get(f"{layer_arg}.dataflowspecTable", None)
        if dataflow_spec_table is None:
            raise Exception(
                f"""parameter {layer_arg}.dataflowspecTable is missing in sparkConf
                Please set spark.conf.set('{layer_arg}.dataflowspecTable'='database.dataflowSpecTableName')"""
            )

        group = spark.conf.get(f"{layer_arg}.group", None)
        dataflow_ids = spark.conf.get(f"{layer_arg}.dataflowIds", None)

        if group is None and dataflow_ids is None:
            raise Exception(
                f"""please provide {layer_arg}.group or {layer}.dataflowIds in spark.conf
                 Please set spark.conf.set('{layer}.group'='groupName')
                 OR
                 spark.conf.set('{layer_arg}.dataflowIds'='comma seperated dataflowIds')
                 """
            )

    @staticmethod
    def get_partition_cols(partition_columns):
        """Get partition columns."""
        partition_cols = None
        if partition_columns:
            if isinstance(partition_columns, str):
                # quarantineTableProperties cluster by
                partition_cols = partition_columns.split(',')
            else:
                if len(partition_columns) == 1:
                    if partition_columns[0] == "" or partition_columns[0].strip() == "":
                        partition_cols = None
                    else:
                        partition_cols = partition_columns
                else:
                    partition_cols = list(filter(None, partition_columns))
        return partition_cols

    @staticmethod
    def get_apply_changes_from_snapshot(apply_changes_from_snapshot) -> ApplyChangesFromSnapshot:
        """Get Apply changes from snapshot metadata."""
        logger.info(apply_changes_from_snapshot)
        json_apply_changes_from_snapshot = _coerce_scd_type_to_str(
            json.loads(apply_changes_from_snapshot)
        )
        logger.info(f"actual mergeInfo={json_apply_changes_from_snapshot}")
        payload_keys = json_apply_changes_from_snapshot.keys()
        missing_apply_changes_from_snapshot_payload_keys = set(
            DataflowSpecUtils.apply_changes_from_snapshot_api_mandatory_attributes).difference(payload_keys)
        logger.info(
            f"missing apply changes from snapshot payload keys:"
            f"{missing_apply_changes_from_snapshot_payload_keys}"
        )
        if set(DataflowSpecUtils.apply_changes_from_snapshot_api_mandatory_attributes) - set(payload_keys):
            missing_mandatory_attr = set(DataflowSpecUtils.apply_changes_from_snapshot_api_mandatory_attributes) - set(
                payload_keys)
            logger.info(f"mandatory missing keys= {missing_mandatory_attr}")
            raise Exception(f"mandatory missing keys= {missing_mandatory_attr} for merge info")
        else:
            logger.info(
                f"""all mandatory keys
                {DataflowSpecUtils.apply_changes_from_snapshot_api_mandatory_attributes} exists"""
            )

        for missing_apply_changes_from_snapshot_payload_key in missing_apply_changes_from_snapshot_payload_keys:
            json_apply_changes_from_snapshot[
                missing_apply_changes_from_snapshot_payload_key
            ] = DataflowSpecUtils.cdc_applychanges_api_attributes_defaults[
                missing_apply_changes_from_snapshot_payload_key]

        logger.info(f"final mergeInfo={json_apply_changes_from_snapshot}")
        json_apply_changes_from_snapshot = DataflowSpecUtils.populate_additional_df_cols(
            json_apply_changes_from_snapshot,
            DataflowSpecUtils.additional_apply_changes_from_snapshot_columns
        )
        return ApplyChangesFromSnapshot(**json_apply_changes_from_snapshot)

    @staticmethod
    def get_cdc_apply_changes(cdc_apply_changes) -> CDCApplyChanges:
        """Get CDC Apply changes metadata."""
        logger.info(cdc_apply_changes)
        json_cdc_apply_changes = _coerce_scd_type_to_str(json.loads(cdc_apply_changes))
        logger.info(f"actual mergeInfo={json_cdc_apply_changes}")
        payload_keys = json_cdc_apply_changes.keys()
        missing_cdc_payload_keys = set(DataflowSpecUtils.cdc_applychanges_api_attributes).difference(payload_keys)
        logger.info(f"missing cdc payload keys:{missing_cdc_payload_keys}")
        if set(DataflowSpecUtils.cdc_applychanges_api_mandatory_attributes) - set(payload_keys):
            missing_mandatory_attr = set(DataflowSpecUtils.cdc_applychanges_api_mandatory_attributes) - set(
                payload_keys
            )
            logger.info(f"mandatory missing keys= {missing_mandatory_attr}")
            raise Exception(f"mandatory missing keys= {missing_mandatory_attr} for merge info")
        else:
            logger.info(
                f"""all mandatory keys
                {DataflowSpecUtils.cdc_applychanges_api_mandatory_attributes} exists"""
            )

        for missing_cdc_payload_key in missing_cdc_payload_keys:
            json_cdc_apply_changes[
                missing_cdc_payload_key
            ] = DataflowSpecUtils.cdc_applychanges_api_attributes_defaults[missing_cdc_payload_key]

        logger.info(f"final mergeInfo={json_cdc_apply_changes}")
        json_cdc_apply_changes = DataflowSpecUtils.populate_additional_df_cols(
            json_cdc_apply_changes,
            DataflowSpecUtils.additional_cdc_apply_changes_columns
        )
        return CDCApplyChanges(**json_cdc_apply_changes)

    @staticmethod
    def get_append_flows(append_flows) -> list[AppendFlow]:
        """Get append flow metadata."""
        logger.info(append_flows)
        json_append_flows = json.loads(append_flows)
        logger.info(f"actual appendFlow={json_append_flows}")
        list_append_flows = []
        for json_append_flow in json_append_flows:
            payload_keys = json_append_flow.keys()
            missing_append_flow_payload_keys = (
                set(DataflowSpecUtils.append_flow_api_attributes_defaults)
                .difference(payload_keys)
            )
            logger.info(f"missing append flow payload keys:{missing_append_flow_payload_keys}")
            if set(DataflowSpecUtils.append_flow_mandatory_attributes) - set(payload_keys):
                missing_mandatory_attr = (
                    set(DataflowSpecUtils.append_flow_mandatory_attributes)
                    - set(payload_keys)
                )
                logger.info(f"mandatory missing keys= {missing_mandatory_attr}")
                raise Exception(f"mandatory missing keys= {missing_mandatory_attr} for append flow")
            else:
                logger.info(
                    f"""all mandatory keys
                    {DataflowSpecUtils.append_flow_mandatory_attributes} exists"""
                )

            for missing_append_flow_payload_key in missing_append_flow_payload_keys:
                json_append_flow[
                    missing_append_flow_payload_key
                ] = DataflowSpecUtils.append_flow_api_attributes_defaults[missing_append_flow_payload_key]

            logger.info(f"final appendFlow={json_append_flow}")
            list_append_flows.append(AppendFlow(**json_append_flow))
        return list_append_flows

    @staticmethod
    def get_cdc_apply_changes_flows(cdc_apply_changes_flows) -> 'CDCApplyChangesFlowGroup':
        """Parse multi-source AUTO CDC group payload (issue #294).

        Accepts a JSON string or already-parsed dict shaped like::

            {
              "keys": [...], "sequence_by": "...", "scd_type": "1",
              "where": "...", "ignore_null_updates": false,
              "apply_as_deletes": "...", "apply_as_truncates": "...",
              "column_list": [...], "except_column_list": [...],
              ...,
              "flows": [
                {"name": "...", "source_format": "...",
                 "source_details": {...}, "reader_options": {...},
                 "select_exp": [...], "where_clause": [...],
                 "once": false},
                ...
              ]
            }

        Validates:
          * Group-level mandatory fields (``keys``, ``sequence_by``,
            ``scd_type``, ``flows``) are present.
          * ``flows`` is a non-empty list.
          * Each flow has mandatory ``name`` / ``source_format`` /
            ``source_details``.
          * ``flow.name`` values are unique within the group (the runtime
            uses them as DLT view names and ``flow_name``, so duplicates
            would silently collide).

        Fills defaults from
        :attr:`cdc_applychanges_api_attributes_defaults` for the group
        and :attr:`cdc_apply_changes_flow_attributes_defaults` per flow.

        Returns:
            CDCApplyChangesFlowGroup instance.

        Raises:
            Exception: When any of the validations above fail.
        """
        logger.info(cdc_apply_changes_flows)
        if isinstance(cdc_apply_changes_flows, str):
            json_group = json.loads(cdc_apply_changes_flows)
        else:
            json_group = dict(cdc_apply_changes_flows)
        json_group = _coerce_scd_type_to_str(json_group)
        logger.info(f"actual cdc_apply_changes_flows group={json_group}")

        # Group-level mandatory keys.
        payload_keys = set(json_group.keys())
        missing_mandatory = (
            set(DataflowSpecUtils.cdc_apply_changes_flows_group_mandatory_attributes)
            - payload_keys
        )
        if missing_mandatory:
            raise Exception(
                f"mandatory missing keys= {missing_mandatory} for "
                f"cdc_apply_changes_flows group"
            )

        flows_raw = json_group.get("flows") or []
        if not isinstance(flows_raw, list) or len(flows_raw) == 0:
            raise Exception(
                "cdc_apply_changes_flows.flows must be a non-empty list"
            )

        # Default-fill the group-level CDC config. We share the existing
        # CDC defaults map so the runtime semantics stay identical to
        # single-flow cdcApplyChanges (e.g. ``ignore_null_updates``
        # defaults to False, ``where`` to None, etc.).
        group_only_keys = (
            set(DataflowSpecUtils.cdc_applychanges_api_attributes_defaults.keys())
        )
        for k in group_only_keys:
            if k not in json_group:
                json_group[k] = (
                    DataflowSpecUtils.cdc_applychanges_api_attributes_defaults[k]
                )

        # Per-flow parse + dedupe.
        seen_names = set()
        list_flows = []
        for idx, raw_flow in enumerate(flows_raw):
            if not isinstance(raw_flow, dict):
                raise Exception(
                    f"cdc_apply_changes_flows.flows[{idx}] must be an object"
                )
            flow_keys = set(raw_flow.keys())
            missing_flow_mandatory = (
                set(DataflowSpecUtils.cdc_apply_changes_flow_mandatory_attributes)
                - flow_keys
            )
            if missing_flow_mandatory:
                raise Exception(
                    f"mandatory missing keys= {missing_flow_mandatory} for "
                    f"cdc_apply_changes_flows.flows[{idx}]"
                )

            name = raw_flow["name"]
            if name in seen_names:
                raise Exception(
                    f"duplicate flow name {name!r} in cdc_apply_changes_flows; "
                    f"each flow name must be unique within a group"
                )
            seen_names.add(name)

            # Per-flow defaults. Only keep fields that belong to
            # ``CDCApplyChangesFlow`` so any extra keys at the call site
            # raise a clear TypeError ("unexpected keyword argument").
            flow_payload = {
                "name": raw_flow["name"],
                "source_format": raw_flow["source_format"],
                "source_details": raw_flow["source_details"],
                "reader_options": raw_flow.get(
                    "reader_options",
                    DataflowSpecUtils.cdc_apply_changes_flow_attributes_defaults[
                        "reader_options"
                    ],
                ),
                "select_exp": raw_flow.get(
                    "select_exp",
                    DataflowSpecUtils.cdc_apply_changes_flow_attributes_defaults[
                        "select_exp"
                    ],
                ),
                "where_clause": raw_flow.get(
                    "where_clause",
                    DataflowSpecUtils.cdc_apply_changes_flow_attributes_defaults[
                        "where_clause"
                    ],
                ),
                "once": raw_flow.get(
                    "once",
                    DataflowSpecUtils.cdc_apply_changes_flow_attributes_defaults[
                        "once"
                    ],
                ),
            }
            list_flows.append(CDCApplyChangesFlow(**flow_payload))

        group_payload = {
            "keys": json_group["keys"],
            "sequence_by": json_group["sequence_by"],
            "scd_type": json_group["scd_type"],
            "where": json_group.get("where"),
            "ignore_null_updates": json_group.get("ignore_null_updates", False),
            "apply_as_deletes": json_group.get("apply_as_deletes"),
            "apply_as_truncates": json_group.get("apply_as_truncates"),
            "column_list": json_group.get("column_list"),
            "except_column_list": json_group.get("except_column_list"),
            "track_history_column_list": json_group.get("track_history_column_list"),
            "track_history_except_column_list": json_group.get(
                "track_history_except_column_list"
            ),
            "ignore_null_updates_column_list": json_group.get(
                "ignore_null_updates_column_list"
            ),
            "ignore_null_updates_except_column_list": json_group.get(
                "ignore_null_updates_except_column_list"
            ),
            "flows": list_flows,
        }
        logger.info(f"final cdc_apply_changes_flows group={group_payload}")
        return CDCApplyChangesFlowGroup(**group_payload)

    def get_db_utils(spark):
        """Get databricks utils using DBUtils package."""
        try:
            from pyspark.dbutils import DBUtils
            return DBUtils(spark)
        except ImportError:
            raise RuntimeError(
                "DBUtils is not available. "
                "Secret management features (Kafka/EventHub with secrets) require Databricks runtime."
            )

    def get_sinks(sinks, spark) -> list[DLTSink]:
        """Get sink metadata."""
        logger.info(sinks)
        json_sinks = json.loads(sinks)
        dlt_sinks = []
        for json_sink in json_sinks:
            logger.info(f"actual sink={json_sink}")
            payload_keys = json_sink.keys()
            missing_sink_payload_keys = set(DataflowSpecUtils.sink_mandatory_attributes).difference(payload_keys)
            logger.info(f"missing sink payload keys:{missing_sink_payload_keys}")
            if set(DataflowSpecUtils.sink_mandatory_attributes) - set(payload_keys):
                missing_mandatory_attr = set(DataflowSpecUtils.sink_mandatory_attributes) - set(payload_keys)
                logger.info(f"mandatory missing keys= {missing_mandatory_attr}")
                raise Exception(f"mandatory missing keys= {missing_mandatory_attr} for sink")
            else:
                logger.info(
                    f"""all mandatory keys
                    {DataflowSpecUtils.sink_mandatory_attributes} exists"""
                )
            format = json_sink['format']
            if format not in DataflowSpecUtils.supported_sink_formats:
                raise Exception(f"Unsupported sink format: {format}")
            if 'options' in json_sink.keys():
                json_sink['options'] = json.loads(json_sink['options'])
            if format == "kafka" and 'options' in json_sink.keys():
                kafka_options_json = json_sink['options']
                dbutils = DataflowSpecUtils.get_db_utils(spark)
                if "kafka_sink_servers_secret_scope_name" in kafka_options_json.keys() and \
                   "kafka_sink_servers_secret_scope_key" in kafka_options_json.keys():
                    kbs_secrets_scope = kafka_options_json["kafka_sink_servers_secret_scope_name"]
                    kbs_secrets_key = kafka_options_json["kafka_sink_servers_secret_scope_key"]
                    json_sink['options']["kafka.bootstrap.servers"] = \
                        dbutils.secrets.get(kbs_secrets_scope, kbs_secrets_key)
                    del json_sink['options']['kafka_sink_servers_secret_scope_name']
                    del json_sink['options']['kafka_sink_servers_secret_scope_key']
                    ssl_truststore_location = kafka_options_json.get("kafka.ssl.truststore.location", None)
                    ssl_keystore_location = kafka_options_json.get("kafka.ssl.keystore.location", None)
                    if ssl_truststore_location and ssl_keystore_location:
                        truststore_scope = kafka_options_json.get("kafka.ssl.truststore.secrets.scope", None)
                        truststore_key = kafka_options_json.get("kafka.ssl.truststore.secrets.key", None)
                        keystore_scope = kafka_options_json.get("kafka.ssl.keystore.secrets.scope", None)
                        keystore_key = kafka_options_json.get("kafka.ssl.keystore.secrets.key", None)
                        if (truststore_scope and truststore_key and keystore_scope and keystore_key):
                            dbutils = DataflowSpecUtils.get_db_utils(spark)
                            json_sink['options']['kafka.ssl.truststore.location'] = ssl_truststore_location
                            json_sink['options']['kafka.ssl.keystore.location'] = ssl_keystore_location
                            json_sink['options']['kafka.ssl.keystore.password'] = dbutils.secrets.get(
                                keystore_scope, keystore_key
                            )
                            json_sink['options']['kafka.ssl.truststore.password'] = dbutils.secrets.get(
                                truststore_scope, truststore_key)
                            del json_sink['options']['kafka.ssl.truststore.secrets.scope']
                            del json_sink['options']['kafka.ssl.truststore.secrets.key']
                            del json_sink['options']['kafka.ssl.keystore.secrets.scope']
                            del json_sink['options']['kafka.ssl.keystore.secrets.key']
                        else:
                            params = ["kafka.ssl.truststore.secrets.scope",
                                      "kafka.ssl.truststore.secrets.key",
                                      "kafka.ssl.keystore.secrets.scope",
                                      "kafka.ssl.keystore.secrets.key"
                                      ]
                            raise Exception(
                                f"Kafka ssl required params are: {params}! provided options are :{kafka_options_json}"
                            )
            if format == "eventhub" and 'options' in json_sink.keys():
                dbutils = DataflowSpecUtils.get_db_utils(spark)
                kafka_options_json = json_sink['options']
                eh_namespace = kafka_options_json["eventhub.namespace"]
                eh_port = kafka_options_json["eventhub.port"]
                eh_name = kafka_options_json["eventhub.name"]
                eh_shared_key_name = kafka_options_json["eventhub.accessKeyName"]
                secret_name = kafka_options_json["eventhub.accessKeySecretName"]
                if not secret_name:
                    # set default value if "eventhub.accessKeySecretName" is not specified
                    secret_name = eh_shared_key_name
                secret_scope = kafka_options_json.get("eventhub.secretsScopeName")
                eh_shared_key_value = dbutils.secrets.get(secret_scope, secret_name)
                eh_shared_key_value = f"SharedAccessKeyName={eh_shared_key_name};SharedAccessKey={eh_shared_key_value}"
                eh_conn_str = f"Endpoint=sb://{eh_namespace}.servicebus.windows.net/;{eh_shared_key_value}"
                eh_kafka_str = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule"
                sasl_config = f"{eh_kafka_str} required username=\"$ConnectionString\" password=\"{eh_conn_str}\";"

                eh_conn_options = {
                    "kafka.bootstrap.servers": f"{eh_namespace}.servicebus.windows.net:{eh_port}",
                    "topic": eh_name,
                    "kafka.sasl.mechanism": "PLAIN",
                    "kafka.security.protocol": "SASL_SSL",
                    "kafka.sasl.jaas.config": sasl_config
                }
                json_sink['options']['kafka.bootstrap.servers'] = eh_conn_options['kafka.bootstrap.servers']
                json_sink['options']['kafka.sasl.mechanism'] = eh_conn_options['kafka.sasl.mechanism']
                json_sink['options']['kafka.security.protocol'] = eh_conn_options['kafka.security.protocol']
                json_sink['options']['kafka.sasl.jaas.config'] = eh_conn_options['kafka.sasl.jaas.config']
                json_sink['options']['topic'] = eh_conn_options['topic']
                del json_sink['options']['eventhub.namespace']
                del json_sink['options']['eventhub.port']
                del json_sink['options']['eventhub.name']
                del json_sink['options']['eventhub.accessKeyName']
                del json_sink['options']['eventhub.accessKeySecretName']
                del json_sink['options']['eventhub.secretsScopeName']
                # DLT interacts with EventHub API as Kafka, change format before invoking sink.
                json_sink['format'] = 'kafka'
            if 'select_exp' in json_sink.keys():
                json_sink['select_exp'] = json_sink['select_exp']
            if 'where_clause' in json_sink.keys():
                json_sink['where_clause'] = json_sink['where_clause']
            for missing_sink_payload_key in missing_sink_payload_keys:
                json_sink[
                    missing_sink_payload_key
                ] = DataflowSpecUtils.sink_attributes_defaults[missing_sink_payload_key]
            logger.info(f"final sink={json_sink}")
            dlt_sinks.append(DLTSink(**json_sink))
        return dlt_sinks
