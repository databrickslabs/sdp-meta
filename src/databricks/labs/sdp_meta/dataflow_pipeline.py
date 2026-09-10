"""DataflowPipeline provide generic code using dataflowspec."""
import json
import logging
from typing import Callable, Optional
import ast
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr, struct
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from pyspark.sql.utils import AnalysisException
from databricks.labs.sdp_meta.dataflow_spec import BronzeDataflowSpec, SilverDataflowSpec, DataflowSpecUtils
from databricks.labs.sdp_meta.pipeline_writers import AppendFlowWriter, DLTSinkWriter
from databricks.labs.sdp_meta.__about__ import __version__
from databricks.labs.sdp_meta.pipeline_readers import PipelineReaders
from databricks.labs.sdp_meta.identifiers import parse_sequence_by_columns

logger = logging.getLogger('databricks.labs.sdp_meta')
logger.setLevel(logging.INFO)


def _as_plain_dict(obj):
    """Coerce a Spark map / Row / dict-like into a plain ``dict`` ({} on None)."""
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    try:
        return dict(obj)
    except (TypeError, ValueError):
        return {}


def _file_metadata_struct():
    """The standard Spark file-source ``_metadata`` struct.

    Used to type the autoloader metadata column (and any ``select_metadata_cols``
    that project one of its fields) when augmenting a bronze declared schema
    with the columns the reader injects into the materialised target.
    """
    return StructType([
        StructField("file_path", StringType(), True),
        StructField("file_name", StringType(), True),
        StructField("file_size", LongType(), True),
        StructField("file_block_start", LongType(), True),
        StructField("file_block_length", LongType(), True),
        StructField("file_modification_time", TimestampType(), True),
    ])


def augment_bronze_schema_with_reader_columns(bronze_spec, declared_schema):
    """Return ``declared_schema`` augmented with the columns the bronze reader
    injects into the *materialised* bronze target table.

    A bronze table's on-disk schema is the declared source schema
    (``source_schema_path``) PLUS columns the reader/pipeline adds — so a
    downstream consumer that only has the declared schema (a combined-run silver
    transform, or the bronze column-policy DDL for issue #2) cannot resolve
    against the true target shape. This helper reproduces those additions,
    mirroring :class:`PipelineReaders`:

      * ``cloudFiles`` sources gain the rescued-data column — name from the
        ``cloudFiles.rescuedDataColumn`` / ``rescuedDataColumn`` reader option,
        default ``_rescued_data`` — as a ``StringType``.
      * When ``source_details['source_metadata']`` enables
        ``include_autoloader_metadata_column``, the file-metadata column (custom
        ``autoloader_metadata_col_name`` or ``source_metadata``) is added as the
        standard file-metadata struct. Any ``select_metadata_cols`` are added
        too (typed from the ``_metadata`` struct when the expression projects one
        of its fields, else ``StringType``).

    Columns already present in ``declared_schema`` are never duplicated. Returns
    ``None`` unchanged when ``declared_schema`` is ``None``. This function is
    deliberately module-level and reusable — issue #2 (bronze column policies)
    needs the same reader-injected-column augmentation.
    """
    if declared_schema is None:
        return None
    fields = list(declared_schema.fields)
    existing = {f.name for f in fields}

    def _add(name, dtype):
        if name and name not in existing:
            fields.append(StructField(name, dtype, True))
            existing.add(name)

    source_format = (getattr(bronze_spec, "sourceFormat", None) or "").lower()
    reader_opts = _as_plain_dict(getattr(bronze_spec, "readerConfigOptions", None))
    source_details = _as_plain_dict(getattr(bronze_spec, "sourceDetails", None))

    if source_format == "cloudfiles":
        rescued = (
            reader_opts.get("cloudFiles.rescuedDataColumn")
            or reader_opts.get("rescuedDataColumn")
            or "_rescued_data"
        )
        _add(rescued, StringType())

        source_metadata_raw = source_details.get("source_metadata")
        if source_metadata_raw:
            try:
                meta = (
                    json.loads(source_metadata_raw)
                    if isinstance(source_metadata_raw, str)
                    else _as_plain_dict(source_metadata_raw)
                )
            except (ValueError, TypeError):
                meta = {}
            file_meta_struct = _file_metadata_struct()
            subfield_types = {f.name: f.dataType for f in file_meta_struct.fields}
            # Mirror ``PipelineReaders.add_cloudfiles_metadata`` EXACTLY, both in
            # column ORDER and in false-flag handling:
            #   * ``selectExpr("*", "_metadata")`` adds the ``_metadata`` struct
            #     FIRST (right after the reader's ``_rescued_data``), then the
            #     ``select_metadata_cols`` projections are appended — so the
            #     struct column precedes the projected columns in the target.
            #   * ``_metadata`` is only DROPPED when the
            #     ``include_autoloader_metadata_column`` key is ABSENT. When the
            #     key is present-and-false the reader keeps it as ``_metadata``;
            #     present-and-true renames it (custom name, or ``source_metadata``).
            if "include_autoloader_metadata_column" in meta:
                flag = str(meta.get("include_autoloader_metadata_column", "")).lower() == "true"
                if flag and "autoloader_metadata_col_name" in meta:
                    # Reader renames ``_metadata`` -> custom name (a custom name
                    # equal to ``_metadata`` is a no-op — same column name here).
                    meta_col = meta["autoloader_metadata_col_name"]
                elif flag:
                    meta_col = "source_metadata"
                else:
                    # present-and-false: reader keeps the struct as ``_metadata``.
                    meta_col = "_metadata"
                _add(meta_col, file_meta_struct)
            # ``select_metadata_cols`` are projected regardless of the
            # include-metadata flag, and always AFTER the ``_metadata`` column.
            for new_col, source_expr in (_as_plain_dict(meta.get("select_metadata_cols"))).items():
                dtype = StringType()
                if isinstance(source_expr, str) and source_expr.startswith("_metadata."):
                    dtype = subfield_types.get(source_expr.split(".", 1)[1], StringType())
                _add(new_col, dtype)

    return StructType(fields)


class DataflowPipeline:
    """This class uses dataflowSpec to launch Lakeflow Spark Declarative Pipelines.

    Raises:
        Exception: "Dataflow not supported!"

    Returns:
        [type]: [description]
    """

    def __init__(self, spark, dataflow_spec, view_name, view_name_quarantine=None,
                 custom_transform_func: Optional[Callable] = None,
                 next_snapshot_and_version: Optional[Callable] = None,
                 source_schema_map: Optional[dict] = None,
                 combined_run: bool = False):
        """Initialize Constructor.

        ``source_schema_map`` maps an upstream source table's fully-qualified
        name to its (reader-augmented) bronze target schema — a ``StructType``,
        or a StructType-JSON string/dict. It is populated by
        :meth:`invoke_dlt_pipeline` for the combined ``bronze_silver`` topology
        so a silver spec can derive its schema from the bronze spec's declared
        schema in-process — without reading the not-yet-materialised bronze
        table (issue #1).

        ``combined_run`` is ``True`` only for the silver flow of a combined
        ``bronze_silver`` run. When set, silver column-policy schema resolution
        that cannot be satisfied from ``source_schema_map`` FAILS FAST with an
        actionable error naming the split-pipeline workaround, instead of
        falling back to a live bronze-table read that would raise the opaque
        ``TABLE_OR_VIEW_NOT_FOUND`` (the bronze table does not exist yet).
        """
        logger.info(
            f"""dataflowSpec={dataflow_spec} ,
                view_name={view_name},
                view_name_quarantine={view_name_quarantine}"""
        )
        self.source_schema_map = source_schema_map or {}
        self.combined_run = combined_run
        if isinstance(dataflow_spec, BronzeDataflowSpec) or isinstance(dataflow_spec, SilverDataflowSpec):
            self.__initialize_dataflow_pipeline(
                spark, dataflow_spec, view_name, view_name_quarantine, custom_transform_func, next_snapshot_and_version
            )
        else:
            raise Exception("Dataflow not supported!")

    # Type-safe helper methods for dictionary access
    def _safe_dict_access(self, dict_obj, key, default=None):
        """Safely access dictionary-like objects with proper type casting."""
        if dict_obj is None:
            return default
        dict_data = dict(dict_obj) if hasattr(dict_obj, '__iter__') else dict_obj
        return dict_data.get(key, default)

    def _safe_dict_get_item(self, dict_obj, key):
        """Safely get item from dictionary-like objects with proper type casting."""
        if dict_obj is None:
            raise KeyError(f"Dictionary is None, cannot access key: {key}")
        dict_data = dict(dict_obj) if hasattr(dict_obj, '__iter__') else dict_obj
        return dict_data[key]

    def _get_dict_as_dict(self, dict_obj):
        """Convert map-type objects to proper dictionaries."""
        if dict_obj is None:
            return {}
        return dict(dict_obj) if hasattr(dict_obj, '__iter__') else dict_obj

    def _get_source_details(self):
        """Get source details as a proper dictionary."""
        return self._get_dict_as_dict(self.dataflowSpec.sourceDetails)

    def _get_target_details(self):
        """Get target details as a proper dictionary."""
        return self._get_dict_as_dict(self.dataflowSpec.targetDetails)

    def _get_quarantine_target_details(self):
        """Get quarantine target details as a proper dictionary."""
        if hasattr(self.dataflowSpec, 'quarantineTargetDetails'):
            return self._get_dict_as_dict(self.dataflowSpec.quarantineTargetDetails)
        return {}

    def _get_reader_config_options(self):
        """Get reader config options as a proper dictionary."""
        return self._get_dict_as_dict(self.dataflowSpec.readerConfigOptions)

    def _get_table_properties(self):
        """Get table properties as a proper dictionary."""
        return self._get_dict_as_dict(self.dataflowSpec.tableProperties)

    def __initialize_dataflow_pipeline(
        self, spark, dataflow_spec, view_name, view_name_quarantine, custom_transform_func: Callable,
        next_snapshot_and_version: Callable
    ):
        """Initialize dataflow pipeline state."""
        self.spark = spark
        uc_enabled_str = spark.conf.get("spark.databricks.unityCatalog.enabled", "False")
        spark.conf.set("databrickslab.sdp-meta.version", f"{__version__}")
        uc_enabled_str = uc_enabled_str.lower()
        self.uc_enabled = True if uc_enabled_str == "true" else False
        self.dpm_enabled = self._is_dpm_enabled(spark)
        self.is_legacy_publishing_mode = not self.dpm_enabled
        if self.is_legacy_publishing_mode:
            logger.info(
                "Pipeline is using legacy publishing mode (pipelines.schema is unset). "
                "Table names will be unqualified to comply with LIVE schema restrictions.",
            )
        self.dataflowSpec = dataflow_spec
        self.view_name = view_name
        if view_name_quarantine:
            self.view_name_quarantine = view_name_quarantine
        self.custom_transform_func = custom_transform_func
        if dataflow_spec.cdcApplyChanges:
            self.cdcApplyChanges = DataflowSpecUtils.get_cdc_apply_changes(self.dataflowSpec.cdcApplyChanges)
        else:
            self.cdcApplyChanges = None
        # Multi-source AUTO CDC (issue #294). Parse the group once at init
        # so downstream read/write paths get a typed CDCApplyChangesFlowGroup
        # rather than a JSON string. We use ``getattr`` defensively because
        # older dataflowspec rows (pre-issue-#294) deserialize without this
        # field via ``populate_additional_df_cols``, but a hand-built spec
        # object in a unit test could legitimately omit the attribute.
        if getattr(dataflow_spec, "cdcApplyChangesFlows", None):
            # Mutual exclusion is mirrored by onboarding pre-flight; this
            # second check catches programmatic spec construction that
            # bypasses onboarding (e.g. tests, custom pipelines).
            if dataflow_spec.cdcApplyChanges:
                raise Exception(
                    f"Both cdcApplyChanges and cdcApplyChangesFlows are set "
                    f"on dataFlowId={dataflow_spec.dataFlowId}; use one or "
                    f"the other."
                )
            self.cdcApplyChangesFlows = DataflowSpecUtils.get_cdc_apply_changes_flows(
                dataflow_spec.cdcApplyChangesFlows
            )
        else:
            self.cdcApplyChangesFlows = None
        if dataflow_spec.appendFlows:
            self.appendFlows = DataflowSpecUtils.get_append_flows(dataflow_spec.appendFlows)
        else:
            self.appendFlows = None
        if dataflow_spec.applyChangesFromSnapshot:
            self.applyChangesFromSnapshot = DataflowSpecUtils.get_apply_changes_from_snapshot(
                self.dataflowSpec.applyChangesFromSnapshot
            )
        if isinstance(dataflow_spec, BronzeDataflowSpec):
            if dataflow_spec.schema is not None:
                self.schema_json = json.loads(dataflow_spec.schema)
            else:
                self.schema_json = None
        elif isinstance(dataflow_spec, SilverDataflowSpec):
            self.schema_json = None
        self.next_snapshot_and_version = None
        self.next_snapshot_and_version = next_snapshot_and_version
        self.next_snapshot_and_version_from_source_view = False
        if self.dataflowSpec.sourceDetails and self.dataflowSpec.sourceDetails.get("snapshot_format", None):
            self.snapshot_source_format = self.dataflowSpec.sourceDetails["snapshot_format"]
        else:
            self.snapshot_source_format = None
        self.silver_schema = None

    def table_has_expectations(self):
        """Table has expectations check."""
        return self.dataflowSpec.dataQualityExpectations is not None

    def _get_row_filter(self):
        """Return rowFilter when UC is enabled — row filters are a UC-only feature.

        Returns:
            str | None: The row filter SQL clause, or None if not set or UC is disabled.
        """
        if not self.uc_enabled:
            return None
        return getattr(self.dataflowSpec, "rowFilter", None)

    def _get_quarantine_row_filter(self):
        """Return quarantineRowFilter when UC is enabled — UC-only feature.

        The quarantine table holds rows that failed DQE expectations. Without
        a filter the quarantine becomes a PII-bypass channel for restricted
        main-table data; with the same filter as the main table ops loses
        visibility into rejected rows. Operators must therefore opt in
        explicitly via `bronze_quarantine_row_filter` /
        `silver_quarantine_row_filter` in the onboarding spec, choosing the
        right policy for their deployment (e.g. a looser filter that grants
        the ops group full visibility while still hiding PII from end users).

        Returns:
            str | None: The quarantine row filter SQL clause, or None if not
                set or UC is disabled.
        """
        if not self.uc_enabled:
            return None
        return getattr(self.dataflowSpec, "quarantineRowFilter", None)

    def _get_column_comments(self):
        """Return the column-comment map (``{column: text}``) or ``None``.

        Comments are NOT a UC-only feature — they annotate any Delta table —
        so this is not gated on ``self.uc_enabled`` (unlike masks / row
        filters). Stored as a JSON string on the spec; parsed here.
        """
        raw = getattr(self.dataflowSpec, "columnComments", None)
        if not raw:
            return None
        return json.loads(raw)

    def _get_column_masks(self):
        """Return the column-mask map (``{column: mask_clause}``) or ``None``.

        Column masks are a Unity Catalog feature (like row filters), so they
        are silently dropped on non-UC pipelines. Stored as a JSON string on
        the spec; parsed here.
        """
        if not self.uc_enabled:
            return None
        raw = getattr(self.dataflowSpec, "columnMasks", None)
        if not raw:
            return None
        return json.loads(raw)

    def _apply_column_policies(self, struct_schema):
        """Resolve the ``schema=`` argument for a DLT table creation call,
        splicing UC column comments / masks into a DDL-string schema when set.

        Returns:
            * the original ``struct_schema`` unchanged (``StructType`` or
              ``None``) when no comments/masks are configured — zero behaviour
              change for pipelines that don't use the feature;
            * a DDL string (built by
              :meth:`DataflowSpecUtils.build_schema_ddl`) when comments/masks
              are set and a schema is available.

        Raises:
            ValueError: if masks are configured but no schema is available to
                attach them to (e.g. a bronze table with an inferred schema).
                Masks fail closed — see ``build_schema_ddl``.
        """
        comments = self._get_column_comments()
        masks = self._get_column_masks()
        if not comments and not masks:
            return struct_schema
        if struct_schema is None:
            if masks:
                raise ValueError(
                    "column_masks are configured for "
                    f"{self._get_target_table_name()} but no schema is "
                    "available to attach them to. Column masks require a "
                    "declared schema (set the bronze source schema, or use "
                    "them on a silver table whose schema is derived from its "
                    "transform)."
                )
            logger.warning(
                "column_comments are configured for %s but no schema is "
                "available to attach them to; skipping comments.",
                self._get_target_table_name(),
            )
            return None
        ddl = DataflowSpecUtils.build_schema_ddl(struct_schema, comments, masks)
        return ddl if ddl is not None else struct_schema

    def _resolve_policy_schema(self):
        """Materialise the table's StructType for column-policy rendering,
        only when comments/masks are actually configured (avoids the cost of
        ``get_silver_schema`` when the feature is unused).

        Returns ``None`` when no policies are set, or when the schema can't be
        determined (bronze without a declared schema).
        """
        if not self._get_column_comments() and not self._get_column_masks():
            return None
        if isinstance(self.dataflowSpec, SilverDataflowSpec):
            # Silver derives its schema from the transform; cache it on the
            # first (and only) production use of get_silver_schema().
            if self.silver_schema is None:
                self.silver_schema = self.get_silver_schema()
            return self.silver_schema
        # Bronze: schema is known only when a schema_json was supplied.
        if self.schema_json:
            return StructType.fromJson(self.schema_json)
        return None

    def is_create_view(self):
        """Determine if a view should be created based on source details and snapshot configuration.

        Returns:
            bool: True if a view should be created, False otherwise.
        """
        # if sourceDetails is provided and snapshot_format is delta, then create a view
        # if next_snapshot_and_version is provided, then do not create a view
        # otherwise create a view
        if (self.dataflowSpec.sourceDetails and self.dataflowSpec.sourceDetails.get("snapshot_format") == "delta"):
            self.next_snapshot_and_version_from_source_view = True
            return True
        elif self.next_snapshot_and_version:
            return False
        return True

    def read(self):
        """Read DLT."""
        logger.info("In read function")
        # When the spec uses multi-source CDC (cdcApplyChangesFlows), the
        # primary view is irrelevant — every CDC flow has its own view
        # built by ``read_cdc_flows``. We skip the primary
        # ``dp.temporary_view`` to avoid declaring an unused view, but
        # still register the per-flow views below. Append flows on the
        # same spec are still honoured (they're orthogonal to CDC flows).
        if self.cdcApplyChangesFlows:
            self.read_cdc_flows()
            if self.appendFlows:
                self.read_append_flows()
            return
        if isinstance(self.dataflowSpec, BronzeDataflowSpec) and self.is_create_view():
            dp.temporary_view(
                self.read_bronze,
                name=self.view_name,
                comment=f"input dataset view for {self.view_name}",
            )
        elif isinstance(self.dataflowSpec, SilverDataflowSpec) and self.is_create_view():
            dp.temporary_view(
                self.read_silver,
                name=self.view_name,
                comment=f"input dataset view for {self.view_name}",
            )
        else:
            if not self.next_snapshot_and_version:
                raise Exception("Dataflow read not supported for {}".format(type(self.dataflowSpec)))
        if self.appendFlows:
            self.read_append_flows()

    def read_append_flows(self):
        if self.dataflowSpec.appendFlows:
            append_flows_schema_map = self.dataflowSpec.appendFlowsSchemas
            for append_flow in self.appendFlows:
                flow_schema = None
                if append_flows_schema_map:
                    flow_schema = append_flows_schema_map.get(append_flow.name)
                pipeline_reader = PipelineReaders(
                    self.spark,
                    append_flow.source_format,
                    append_flow.source_details,
                    append_flow.reader_options,
                    json.loads(flow_schema) if flow_schema else None
                )
                if append_flow.source_format == "cloudFiles":
                    dp.temporary_view(pipeline_reader.read_dlt_cloud_files,
                                      name=f"{append_flow.name}_view",
                                      comment=f"append flow input dataset view for {append_flow.name}_view"
                                      )
                elif append_flow.source_format == "delta":
                    dp.temporary_view(pipeline_reader.read_dlt_delta,
                                      name=f"{append_flow.name}_view",
                                      comment=f"append flow input dataset view for {append_flow.name}_view"
                                      )
                elif append_flow.source_format == "eventhub" or append_flow.source_format == "kafka":
                    dp.temporary_view(pipeline_reader.read_kafka,
                                      name=f"{append_flow.name}_view",
                                      comment=f"append flow input dataset view for {append_flow.name}_view"
                                      )
        else:
            raise Exception(f"Append Flows not found for dataflowSpec={self.dataflowSpec}")

    def read_cdc_flows(self):
        """Create a DLT temporary view per CDC flow (issue #294).

        Each flow becomes ``{flow.name}_cdc_view``. We use the same
        ``PipelineReaders`` dispatcher as append flows so per-flow source
        formats (cloudFiles / delta / kafka / eventhub) behave identically.
        Per-flow ``select_exp`` runs as ``selectExpr(*flow.select_exp)``
        and per-flow ``where_clause`` runs as a chain of ``.where(...)``
        calls — the same normalization shape silver dataflows use today,
        but pinned per source so each flow can normalize its own schema
        into the shared target shape before the merge.

        For bronze, per-flow source schemas are looked up in
        ``dataflowSpec.cdcApplyChangesFlowsSchemas`` (mirrors
        ``appendFlowsSchemas`` and is keyed by ``flow.name``). Silver has
        no per-flow schema map because silver flows always read Delta
        upstream where schema is inferred.

        The user's ``custom_transform_func`` is applied to each flow's
        view, consistent with how primary reads and append flows are
        treated, so per-source transformations stay composable.
        """
        if not self.cdcApplyChangesFlows:
            return
        is_bronze = isinstance(self.dataflowSpec, BronzeDataflowSpec)
        schemas_map = (
            self.dataflowSpec.cdcApplyChangesFlowsSchemas if is_bronze else None
        )
        for flow in self.cdcApplyChangesFlows.flows:
            flow_schema_json = None
            if schemas_map and schemas_map.get(flow.name):
                flow_schema_json = json.loads(schemas_map[flow.name])

            pipeline_reader = PipelineReaders(
                self.spark,
                flow.source_format,
                flow.source_details,
                flow.reader_options or {},
                flow_schema_json,
            )
            sf = flow.source_format.lower()
            if sf == "cloudfiles":
                base_reader = pipeline_reader.read_dlt_cloud_files
            elif sf == "delta":
                base_reader = pipeline_reader.read_dlt_delta
            elif sf in ("kafka", "eventhub"):
                base_reader = pipeline_reader.read_kafka
            else:
                # Should be unreachable — onboarding pre-flight rejects
                # other formats. Surface a clear runtime error rather than
                # an opaque AttributeError if a stale spec snuck through.
                raise Exception(
                    f"cdcApplyChangesFlows.flows[{flow.name}].source_format"
                    f"={flow.source_format!r} is not supported by the "
                    f"runtime; allowed: cloudFiles, delta, kafka, eventhub"
                )

            def _make_view_factory(reader=base_reader, f=flow):
                # Per-flow factory closure. Capturing ``reader`` and ``f``
                # as default args pins each closure to its own flow — the
                # late-binding bug of capturing the loop variable would
                # otherwise have every closure read the LAST flow.
                def _view_factory():
                    df = reader()
                    if f.select_exp:
                        df = df.selectExpr(*f.select_exp)
                    if f.where_clause:
                        for clause in f.where_clause:
                            df = df.where(clause)
                    return self.apply_custom_transform_fun(df)
                return _view_factory

            dp.temporary_view(
                _make_view_factory(),
                name=f"{flow.name}_cdc_view",
                comment=f"cdc flow input view for {flow.name}",
            )

    def write(self):
        """Write DLT."""
        if self.dataflowSpec.sinks:
            dlt_sinks = DataflowSpecUtils.get_sinks(self.dataflowSpec.sinks, self.spark)
            for dlt_sink in dlt_sinks:
                DLTSinkWriter(self.spark, dlt_sink, self.view_name).write_to_sink()
        if isinstance(self.dataflowSpec, BronzeDataflowSpec):
            self.write_bronze()
        elif isinstance(self.dataflowSpec, SilverDataflowSpec):
            self.write_silver()
        else:
            raise Exception(f"Dataflow write not supported for type= {type(self.dataflowSpec)}")

    def _get_target_table_info(self):
        """Extract target table information from dataflow spec."""
        target_details = self._get_target_details()
        target_path = None if self.uc_enabled else target_details.get("path")
        target_cl = target_details.get('catalog', None)
        target_db_name = target_details['database']
        target_table_name = target_details['table']
        target_table = self._build_table_name(target_cl, target_db_name, target_table_name)
        return target_path, target_table, target_table_name

    def _get_table_comment(self, target_table, is_bronze=True):
        """Generate appropriate comment for the table."""
        layer_name = "bronze" if is_bronze else "silver"
        target_details = self._get_target_details()
        if 'comment' in target_details:
            return target_details.get('comment')
        return f"{layer_name} dlt table{target_table}"

    def _write_standard_table(self, is_bronze=True):
        """Write standard DLT table for bronze or silver layer."""
        target_path, target_table, target_table_name = self._get_target_table_info()
        comment = self._get_table_comment(target_table, is_bronze)

        # Get cluster_by_auto from dataflowSpec, default to False if not present
        cluster_by_auto = (
            self.dataflowSpec.clusterByAuto
            if hasattr(self.dataflowSpec, 'clusterByAuto')
            and self.dataflowSpec.clusterByAuto is not None
            else False
        )

        # Resolve the column-policy schema (``None`` unless comments/masks are
        # configured — the feature gate). On the standard bronze write path DLT
        # infers the query schema, which includes the reader-injected columns
        # (``_rescued_data`` from ``cloudFiles.rescuedDataColumn`` and the
        # autoloader metadata columns). The declared ``source_schema_path`` does
        # NOT list those, so forcing the declared schema alone would fail table
        # creation with a schema-incompatibility error (issue #2). Augment the
        # policy schema with the SAME reader columns
        # ``PipelineReaders.add_cloudfiles_metadata`` injects so the explicit
        # schema matches DLT's inferred query schema. Strictly behind the
        # policies-configured gate (``struct_schema is None`` when unused) and
        # only for bronze — silver derives its schema from the transform.
        struct_schema = self._resolve_policy_schema()
        if is_bronze and struct_schema is not None:
            struct_schema = augment_bronze_schema_with_reader_columns(
                self.dataflowSpec, struct_schema
            )

        dp.table(
            self.write_to_delta,
            name=f"{target_table}",
            partition_cols=DataflowSpecUtils.get_partition_cols(self.dataflowSpec.partitionColumns),
            cluster_by=DataflowSpecUtils.get_partition_cols(self.dataflowSpec.clusterBy),
            cluster_by_auto=cluster_by_auto,
            table_properties=self.dataflowSpec.tableProperties,
            path=target_path,
            comment=comment,
            row_filter=self._get_row_filter(),
            schema=self._apply_column_policies(struct_schema),
        )

    def write_layer_table(self):
        """Write Bronze or Silver tables using unified logic."""
        # Materialise the derived silver schema up-front when UC column
        # comments/masks are configured, so every downstream path (standard,
        # DQE, CDC apply-changes) can render them into a DDL-string schema —
        # in particular ``modify_schema_for_cdc_changes`` reads
        # ``self.silver_schema``. No-op (and no ``get_silver_schema`` cost)
        # when the feature is unused.
        self._resolve_policy_schema()
        is_bronze = isinstance(self.dataflowSpec, BronzeDataflowSpec)
        # Handle special cases first
        if is_bronze:
            bronze_spec = self.dataflowSpec
            # Handle snapshot format for bronze
            if bronze_spec.sourceFormat and bronze_spec.sourceFormat.lower() == "snapshot":
                if self.next_snapshot_and_version:
                    self.apply_changes_from_snapshot()
                else:
                    raise Exception("Snapshot reader function not provided!")
                self._handle_append_flows()
                return
            # Handle data quality expectations for bronze
            if bronze_spec.dataQualityExpectations:
                self.write_layer_with_dqe()
                self._handle_append_flows()
                return
        else:
            # Handle apply changes from snapshot for silver
            silver_spec = self.dataflowSpec
            if silver_spec.applyChangesFromSnapshot:
                self.apply_changes_from_snapshot()
                self._handle_append_flows()
                return
            # Handle data quality expectations for silver
            if silver_spec.dataQualityExpectations:
                self.write_layer_with_dqe()
                self._handle_append_flows()
                return
        # Multi-source AUTO CDC (issue #294) takes precedence over the
        # single-flow ``cdcApplyChanges`` branch and the standard write —
        # the init-time mutual-exclusion check guarantees only one of the
        # two CDC modes is set, but we still check first so the standard
        # write branch never fires when CDC flows are present.
        if self.cdcApplyChangesFlows:
            self.cdc_apply_changes_flows()
        elif self.dataflowSpec.cdcApplyChanges and not self.dataflowSpec.dataQualityExpectations:
            self.cdc_apply_changes()
        else:
            # Write standard table
            self._write_standard_table(is_bronze)
        # Handle append flows (common to both)
        self._handle_append_flows()

    def _handle_append_flows(self):
        """Handle append flows if they exist."""
        if self.dataflowSpec.appendFlows:
            self.write_append_flows()

    def write_bronze(self):
        """Write Bronze tables."""
        self.write_layer_table()

    def write_silver(self):
        """Write silver tables."""
        self.write_layer_table()

    def read_bronze(self) -> DataFrame:
        """Read Bronze Table."""
        logger.info("In read_bronze func")
        pipeline_reader = PipelineReaders(
            self.spark,
            self.dataflowSpec.sourceFormat,
            self.dataflowSpec.sourceDetails,
            self.dataflowSpec.readerConfigOptions,
            self.schema_json
        )
        bronze_dataflow_spec: BronzeDataflowSpec = self.dataflowSpec
        input_df = None
        if bronze_dataflow_spec.sourceFormat == "cloudFiles":
            input_df = pipeline_reader.read_dlt_cloud_files()
        elif bronze_dataflow_spec.sourceFormat == "delta" or bronze_dataflow_spec.sourceFormat == "snapshot":
            input_df = pipeline_reader.read_dlt_delta()
        elif bronze_dataflow_spec.sourceFormat == "eventhub" or bronze_dataflow_spec.sourceFormat == "kafka":
            input_df = pipeline_reader.read_kafka()
        else:
            raise Exception(f"{bronze_dataflow_spec.sourceFormat} source format not supported")
        return self.apply_custom_transform_fun(input_df)

    def apply_custom_transform_fun(self, input_df):
        if self.custom_transform_func:
            input_df = self.custom_transform_func(input_df, self.dataflowSpec)
        return input_df

    def _get_inprocess_source_schema(self, source_fqn):
        """Return the upstream source's (reader-augmented) bronze target schema
        as a ``StructType`` when it was threaded in-process via
        ``source_schema_map`` (combined ``bronze_silver`` runs), else ``None``.

        This lets ``get_silver_schema`` derive the silver schema from the
        bronze dataflowspec's declared schema instead of a live
        ``spark.readStream.table(<bronze fqn>)`` — the bronze table is produced
        in the SAME run and does not exist in UC at graph-construction time, so
        a physical read raises ``TABLE_OR_VIEW_NOT_FOUND`` (issue #1). Accepts a
        ``StructType`` (as built by ``_build_bronze_target_schema_map``) or a
        StructType-JSON string / dict (as tests and hand-built maps may supply).
        """
        schema_map = getattr(self, "source_schema_map", None)
        if not schema_map or not source_fqn:
            return None
        schema = schema_map.get(source_fqn)
        if schema is None:
            return None
        if isinstance(schema, StructType):
            return schema
        if isinstance(schema, str):
            schema = json.loads(schema)
        if isinstance(schema, dict):
            return StructType.fromJson(schema)
        return None

    @staticmethod
    def _flow_source_fqn(source_details):
        """Build a multi-source CDC flow's source FQN from its
        ``source_catalog`` / ``source_database`` / ``source_table`` keys (the
        naming ``PipelineReaders.read_dlt_delta`` uses), matching how
        ``_build_bronze_target_schema_map`` keys bronze targets. Returns
        ``None`` when the flow source is not a catalog/db.table (e.g. a
        cloudFiles path)."""
        sd = _as_plain_dict(source_details)
        db = sd.get("source_database")
        table = sd.get("source_table")
        if not db or not table:
            return None
        catalog = sd.get("source_catalog")
        catalog_prefix = f"{catalog}." if catalog else ''
        return f"{catalog_prefix}{db}.{table}"

    def _combined_topology_schema_error(self, source_label):
        """Actionable error for a combined-run silver policy schema that cannot
        be resolved in-process (schemaless bronze, or a non-bronze source)."""
        return (
            f"Silver column policies (columnComments/columnMasks) on "
            f"{self._get_target_table_name()} need the upstream schema at "
            f"graph-construction time, but the source '{source_label}' has no "
            f"declared schema available in-process during this combined "
            f"'bronze_silver' run (the bronze table is produced in the same run "
            f"and does not exist yet). Declare the bronze source schema "
            f"(source_schema_path) for that source, or run bronze and silver as "
            f"separate pipelines (the split topology, which reads the "
            f"already-materialised bronze table)."
        )

    def _derive_schema_from_struct(self, source_struct, select_exp, where_clause, source_label):
        """Apply a silver ``select_exp`` / ``where_clause`` to an EMPTY frame of
        ``source_struct`` and return the resulting schema — deriving the silver
        schema in-process with no physical read (issue #1). A referenced column
        missing from the in-process bronze schema (e.g. added by a bronze
        ``custom_transform_func``, or a reader column not captured by the
        declared schema) surfaces as an ``AnalysisException`` here, which we
        translate into an actionable combined-topology error rather than letting
        it fall through to an opaque failure."""
        df = self.spark.createDataFrame([], source_struct)
        try:
            if select_exp:
                df = df.selectExpr(*select_exp)
            df = self.__apply_where_clause(where_clause, df)
        except AnalysisException as ae:
            raise ValueError(
                f"Silver column policies on {self._get_target_table_name()} in a "
                f"combined 'bronze_silver' run: the silver transform references a "
                f"column not present in the in-process bronze schema for source "
                f"'{source_label}'. This happens when the column is added by a "
                f"bronze custom_transform_func or a reader option not reflected "
                f"in the declared bronze schema. Declare the column in the bronze "
                f"source schema, or run bronze and silver as separate pipelines "
                f"(split topology). Original error: {ae}"
            ) from ae
        return df.schema

    def _read_flow_source_df(self, flow):
        """Live-read one multi-source CDC flow's source into a DataFrame (split
        topology only — the source table/files already exist). Mirrors the
        reader dispatch in ``read_cdc_flows``."""
        pipeline_reader = PipelineReaders(
            self.spark,
            flow.source_format,
            flow.source_details,
            flow.reader_options or {},
            None,
        )
        sf = flow.source_format.lower()
        if sf == "cloudfiles":
            return pipeline_reader.read_dlt_cloud_files()
        elif sf == "delta":
            return pipeline_reader.read_dlt_delta()
        elif sf in ("kafka", "eventhub"):
            return pipeline_reader.read_kafka()
        raise Exception(
            f"cdcApplyChangesFlows.flows[{flow.name}].source_format"
            f"={flow.source_format!r} is not supported by the runtime; "
            f"allowed: cloudFiles, delta, kafka, eventhub"
        )

    def _merge_flow_schemas(self, per_flow_schemas):
        """Verify every multi-source flow projects the same (name, type) columns
        and return the merged schema. All flows land in ONE streaming table, so
        their post-transform schemas must be compatible; a mismatch is a config
        error surfaced clearly rather than a confusing DLT failure downstream.

        Nullability is merged by SAFE WIDENING: a field is nullable in the
        result when ANY flow projects it as nullable. This is order-independent
        (the flow list order never changes the result) and never emits a
        spurious ``NOT NULL`` — the policy DDL adds ``NOT NULL`` from
        ``field.nullable`` (dataflow_spec.build_schema_ddl), so a column is
        constrained ``NOT NULL`` only when every flow guarantees it is
        non-null."""
        if not per_flow_schemas:
            return None
        ref_name, ref_schema = per_flow_schemas[0]
        ref_fields = [(f.name, f.dataType) for f in ref_schema.fields]
        # nullable[i] widened across flows (any-nullable -> nullable).
        nullable = [f.nullable for f in ref_schema.fields]
        for name, schema in per_flow_schemas[1:]:
            fields = [(f.name, f.dataType) for f in schema.fields]
            if fields != ref_fields:
                raise ValueError(
                    f"Multi-source AUTO CDC flows for {self._get_target_table_name()} "
                    f"produce incompatible schemas after per-flow "
                    f"select_exp/where_clause: flow '{ref_name}' yields "
                    f"{ref_fields} but flow '{name}' yields {fields}. Every flow "
                    f"landing in one streaming table must project the same "
                    f"columns and types."
                )
            for i, f in enumerate(schema.fields):
                nullable[i] = nullable[i] or f.nullable
        return StructType([
            StructField(f.name, f.dataType, nullable[i], f.metadata)
            for i, f in enumerate(ref_schema.fields)
        ])

    def _get_silver_schema_from_cdc_flows(self):
        """Derive the target schema for a multi-source AUTO CDC silver spec
        (issue #294). Pure multi-source silver specs carry empty
        ``sourceDetails`` and null ``selectExp`` — their real sources live in
        ``cdcApplyChangesFlows`` — so single-source ``get_silver_schema`` would
        build the FQN ``"."`` and fail. Resolve EACH flow's source against the
        in-process bronze schema map (combined run) or a live read (split
        topology), apply that flow's ``select_exp`` / ``where_clause``, and merge
        the compatible per-flow schemas into the shared target schema (issue
        #1 / BLOCKING: multi-source combined policy support)."""
        group = self.cdcApplyChangesFlows
        per_flow_schemas = []
        for flow in group.flows:
            fqn = self._flow_source_fqn(flow.source_details)
            in_proc = self._get_inprocess_source_schema(fqn)
            if in_proc is not None:
                schema = self._derive_schema_from_struct(
                    in_proc, flow.select_exp, flow.where_clause, fqn
                )
            elif self.combined_run:
                raise ValueError(self._combined_topology_schema_error(
                    fqn or f"flow '{flow.name}' (source_format={flow.source_format})"
                ))
            else:
                # Split topology: the flow's source already exists — read it.
                df = self._read_flow_source_df(flow)
                if flow.select_exp:
                    df = df.selectExpr(*flow.select_exp)
                df = self.__apply_where_clause(flow.where_clause, df)
                schema = df.schema
            per_flow_schemas.append((flow.name, schema))
        return self._merge_flow_schemas(per_flow_schemas)

    def get_silver_schema(self):
        """Get Silver table Schema."""
        # Multi-source AUTO CDC silver specs (issue #294) carry their real
        # sources in ``cdcApplyChangesFlows`` (empty sourceDetails / null
        # selectExp), so resolve them per-flow.
        if self.cdcApplyChangesFlows:
            return self._get_silver_schema_from_cdc_flows()
        silver_dataflow_spec: SilverDataflowSpec = self.dataflowSpec
        source_details = self._get_source_details()
        source_cl = source_details.get('catalog', None)
        source_cl_name = f"{source_cl}." if source_cl is not None else ''
        source_database = source_details["database"]
        source_table = source_details["table"]
        select_exp = silver_dataflow_spec.selectExp
        where_clause = silver_dataflow_spec.whereClause
        source_fqn = f"{source_cl_name}{source_database}.{source_table}"
        # In a combined ``bronze_silver`` run the upstream bronze table is
        # produced in the SAME pipeline run and does not yet exist in UC at
        # graph-construction time. When the bronze dataflowspec's declared
        # schema was threaded in-process (see ``invoke_dlt_pipeline``), derive
        # the silver schema from it — applying the same ``selectExpr`` /
        # ``where`` transform against an empty frame — instead of issuing a live
        # ``spark.readStream.table(<bronze fqn>)`` that would raise
        # TABLE_OR_VIEW_NOT_FOUND. This removes the bronze→silver ordering
        # dependency entirely (issue #1).
        source_struct = self._get_inprocess_source_schema(source_fqn)
        if source_struct is not None:
            return self._derive_schema_from_struct(
                source_struct, select_exp, where_clause, source_fqn
            )
        # No in-process schema. In a combined run the bronze table does not exist
        # yet, so a live read would raise TABLE_OR_VIEW_NOT_FOUND — fail fast with
        # an actionable message instead (schemaless bronze / non-mapped source).
        if self.combined_run:
            raise ValueError(self._combined_topology_schema_error(source_fqn))
        # Split bronze-then-silver topology: the bronze table already exists,
        # read it live, exactly as before.
        if self.uc_enabled:
            raw_delta_table_stream = self.spark.readStream.table(
                source_fqn
            ).selectExpr(*select_exp)
        else:
            raw_delta_table_stream = self.spark.readStream.load(
                path=source_details.get("path"),
                format="delta"
            ).selectExpr(*select_exp)
        raw_delta_table_stream = self.__apply_where_clause(where_clause, raw_delta_table_stream)
        return raw_delta_table_stream.schema

    def __apply_where_clause(self, where_clause, raw_delta_table_stream):
        """This method apply where clause provided in silver transformations

        Args:
            where_clause (_type_): _description_
            raw_delta_table_stream (_type_): _description_

        Returns:
            _type_: _description_
        """
        if where_clause:
            where_clause_str = " ".join(where_clause)
            if len(where_clause_str.strip()) > 0:
                for clause in where_clause:
                    raw_delta_table_stream = raw_delta_table_stream.where(clause)
        return raw_delta_table_stream

    def read_silver(self) -> DataFrame:
        """Read Silver tables."""
        silver_dataflow_spec: SilverDataflowSpec = self.dataflowSpec
        source_details = self._get_source_details()
        reader_config_opts = self._get_reader_config_options()
        source_cl = source_details.get('catalog', None)
        source_cl_name = f"{source_cl}." if source_cl is not None else ''
        source_database = source_details["database"]
        source_table = source_details["table"]
        select_exp = silver_dataflow_spec.selectExp
        where_clause = silver_dataflow_spec.whereClause
        if reader_config_opts:
            if silver_dataflow_spec.sourceFormat == "snapshot":
                bronze_df = self.spark.read.options(**reader_config_opts).table(
                    f"{source_cl_name}{source_database}.{source_table}"
                ) if self.uc_enabled else self.spark.read.options(
                    **reader_config_opts
                ).load(
                    path=source_details.get("path"),
                    format="delta"
                )
            else:
                bronze_df = self.spark.readStream.options(**reader_config_opts).table(
                    f"{source_cl_name}{source_database}.{source_table}"
                ) if self.uc_enabled else self.spark.readStream.options(
                    **reader_config_opts
                ).load(
                    path=source_details.get("path"),
                    format="delta"
                )
        else:
            if silver_dataflow_spec.sourceFormat == "snapshot":
                bronze_df = self.spark.read.table(
                    f"{source_cl_name}{source_database}.{source_table}"
                ) if self.uc_enabled else self.spark.read.load(
                    path=source_details.get("path"),
                    format="delta"
                )
            else:
                bronze_df = self.spark.readStream.table(
                    f"{source_cl_name}{source_database}.{source_table}"
                ) if self.uc_enabled else self.spark.readStream.load(
                    path=source_details.get("path"),
                    format="delta"
                )
        bronze_df = self._apply_transformations(bronze_df, select_exp, where_clause)
        bronze_df = self.apply_custom_transform_fun(bronze_df)
        return bronze_df

    def write_to_delta(self):
        """Write to Delta.

        Uses ``dp.read_stream`` (the LDP equivalent of the legacy
        ``dlt.read_stream``) to resolve the view from the pipeline's internal
        dataset graph.  ``spark.readStream.table`` is intentionally NOT used
        here: in legacy publishing mode it falls back to a catalog lookup and
        raises TABLE_OR_VIEW_NOT_FOUND instead of finding the LIVE-schema view.
        """
        return dp.read_stream(self.view_name)

    def apply_changes_from_snapshot(self):
        target_path = None if self.uc_enabled else self.dataflowSpec.targetDetails["path"]
        # Fail closed for SCD2 snapshot targets when column policies are
        # CONFIGURED. An SCD2 table carries DLT-managed ``__START_AT`` /
        # ``__END_AT`` system columns typed to the snapshot *version*. Unlike
        # the regular CDC path (``modify_schema_for_cdc_changes``),
        # ``ApplyChangesFromSnapshot`` has no ``sequence_by`` from which to
        # derive that version dtype — it is determined at runtime by the
        # ``next_snapshot_and_version`` return / snapshot source — so we cannot
        # build a complete explicit schema. Emitting an explicit schema that
        # omits those system columns can break target-table creation, and
        # silently dropping a mask is a security regression.
        #
        # The check is based on whether policies are CONFIGURED, not on
        # whether a schema was resolved: ``_resolve_policy_schema`` returns
        # ``None`` for an inferred-schema Bronze target, so a comments-only +
        # inferred-schema SCD2 target would otherwise slip past (comments
        # merely warned/skipped) and the table would still be created,
        # violating the "SCD2 snapshot with policies must raise and create no
        # table" contract. We therefore reject BEFORE ``create_streaming_table``
        # whenever comments and/or masks are configured for this target. SCD1
        # snapshot targets have no such system columns and keep working; SCD2
        # with no policies is unaffected. See docs/docs/guides/column-policies.md.
        has_policies = bool(self._get_column_comments()) or bool(self._get_column_masks())
        struct_schema = self._resolve_policy_schema()
        if has_policies and str(self.applyChangesFromSnapshot.scd_type) == "2":
            # Determine the snapshot version type for the DLT-managed
            # __START_AT / __END_AT system columns. It is an EXPLICIT contract
            # (declared snapshot_version_type), or LONG for the first-party
            # Delta snapshot-source mode that contractually guarantees the
            # Delta commit version. When neither is available we keep the
            # original fail-closed error (below).
            version_type = self._resolve_snapshot_version_type()
            if version_type is not None:
                # INJECT the system columns into the explicit schema via the
                # SAME build-new-StructType mechanism as the regular CDC path
                # (Fix 1): __END_AT nullable, __START_AT following the version
                # nullability. When the policy schema is unavailable (e.g. an
                # inferred-schema Bronze target) there is nothing to attach the
                # policies to; fall through to the fail-closed error so masks
                # are never silently dropped.
                if struct_schema is not None:
                    struct_schema = self._with_scd2_system_columns(
                        struct_schema, version_type, start_at_nullable=True
                    )
                    self.create_streaming_table(struct_schema, target_path)
                    self._create_auto_cdc_from_snapshot_flow()
                    return
            raise ValueError(
                "column_comments / column_masks are not supported on an SCD2 "
                f"apply_changes_from_snapshot target ({self._get_target_table_name()}) "
                "without a declared snapshot_version_type. "
                "SCD2 snapshot targets require DLT-managed __START_AT / __END_AT "
                "system columns whose type is the snapshot version and cannot be "
                "derived here (apply_changes_from_snapshot has no sequence_by), so "
                "an explicit schema carrying the policies would omit them and break "
                "target-table creation. Declare the version type via "
                "apply_changes_from_snapshot.snapshot_version_type (e.g. 'long' or "
                "'timestamp') AND provide a schema (Bronze source schema, or a "
                "Silver transform-derived schema), use SCD type 1 for column "
                "policies on a snapshot target, or apply the COMMENT / MASK with a "
                "separate ALTER TABLE after the pipeline creates the table."
            )
        # Wire in the declared (Bronze ``schema_json``) / derived (Silver
        # transform) schema so column comments/masks are applied to the
        # snapshot-CDC target table. ``_resolve_policy_schema`` returns
        # ``None`` when the feature is unused, so ``_apply_column_policies``
        # (inside ``create_streaming_table``) preserves the previous
        # ``create_streaming_table(None, ...)`` behaviour — a masks-only
        # inferred-schema SCD1 target still fails closed via
        # ``_apply_column_policies(None)``.
        self.create_streaming_table(struct_schema, target_path)
        self._create_auto_cdc_from_snapshot_flow()

    def _resolve_snapshot_version_type(self):
        """Return the SCD2 snapshot version ``DataType`` to declare, or ``None``.

        Precedence:
          1. An explicit ``snapshot_version_type`` declared on the spec — the
             general contract. Parsed to a real Spark ``DataType`` (validated
             at onboarding). The runtime version MUST conform to it; a mismatch
             surfaces as an explicit create/insert failure.
          2. ``LongType`` for the first-party Delta snapshot-source mode
             (``snapshot_format == "delta"``), which CONTRACTUALLY guarantees
             the version is the Delta commit version (a ``long``). We do NOT
             infer ``LONG`` merely because a ``next_snapshot_and_version``
             callback happens to read Delta — only this declared source mode.
          3. ``None`` otherwise — the caller keeps the fail-closed error.
        """
        declared = getattr(self.applyChangesFromSnapshot, "snapshot_version_type", None)
        if declared:
            from pyspark.sql.types import _parse_datatype_string
            return _parse_datatype_string(declared)
        if self.snapshot_source_format == "delta":
            return LongType()
        return None

    def _create_auto_cdc_from_snapshot_flow(self):
        """Register the snapshot CDC flow against the (already created) target."""
        target_cl = self.dataflowSpec.targetDetails.get('catalog', None)
        target_db_name = self.dataflowSpec.targetDetails['database']
        target_table_name = self.dataflowSpec.targetDetails['table']
        target_table = self._build_table_name(target_cl, target_db_name, target_table_name)
        source = (
            (lambda latest_snapshot_version: self.next_snapshot_and_version(
                latest_snapshot_version, self.dataflowSpec
            ))
            if self.next_snapshot_and_version and not self.next_snapshot_and_version_from_source_view
            else self.view_name
        )

        dp.create_auto_cdc_from_snapshot_flow(
            target=target_table,
            source=source,
            keys=self.applyChangesFromSnapshot.keys,
            stored_as_scd_type=self.applyChangesFromSnapshot.scd_type,
            track_history_column_list=self.applyChangesFromSnapshot.track_history_column_list,
            track_history_except_column_list=self.applyChangesFromSnapshot.track_history_except_column_list,
        )

    def write_layer_with_dqe(self):
        """Write Bronze or Silver table with data quality expectations."""
        is_bronze = isinstance(self.dataflowSpec, BronzeDataflowSpec)
        data_quality_expectations_json = json.loads(self.dataflowSpec.dataQualityExpectations)

        dlt_table_with_expectation = None
        expect_or_quarantine_dict = None
        expect_all_dict, expect_all_or_drop_dict, expect_all_or_fail_dict = self.get_dq_expectations()
        # Both bronze and silver layers support quarantine tables
        if "expect_or_quarantine" in data_quality_expectations_json:
            expect_or_quarantine_dict = data_quality_expectations_json["expect_or_quarantine"]
        if self.dataflowSpec.cdcApplyChanges:
            self.cdc_apply_changes()
        else:
            target_path, target_table, target_table_name = self._get_target_table_info()
            target_comment = self._get_table_comment(target_table, is_bronze)

            # Get cluster_by_auto from dataflowSpec, default to False if not present
            cluster_by_auto = (
                self.dataflowSpec.clusterByAuto
                if hasattr(self.dataflowSpec, 'clusterByAuto')
                and self.dataflowSpec.clusterByAuto is not None
                else False
            )
            # Resolve the DDL-string schema carrying any UC column comments /
            # masks once (falls back to the derived StructType / None when the
            # feature is unused).
            column_policy_schema = self._apply_column_policies(self._resolve_policy_schema())

            # Create base table with expectations
            if expect_all_dict:
                dlt_table_with_expectation = dp.expect_all(expect_all_dict)(
                    dp.table(
                        self.write_to_delta,
                        name=f"{target_table}",
                        table_properties=self.dataflowSpec.tableProperties,
                        partition_cols=DataflowSpecUtils.get_partition_cols(self.dataflowSpec.partitionColumns),
                        cluster_by=DataflowSpecUtils.get_partition_cols(self.dataflowSpec.clusterBy),
                        cluster_by_auto=cluster_by_auto,
                        path=target_path,
                        comment=target_comment,
                        row_filter=self._get_row_filter(),
                        schema=column_policy_schema,
                    )
                )
            if expect_all_or_fail_dict:
                if expect_all_dict is None:
                    dlt_table_with_expectation = dp.expect_all_or_fail(expect_all_or_fail_dict)(
                        dp.table(
                            self.write_to_delta,
                            name=f"{target_table}",
                            table_properties=self.dataflowSpec.tableProperties,
                            partition_cols=DataflowSpecUtils.get_partition_cols(self.dataflowSpec.partitionColumns),
                            cluster_by=DataflowSpecUtils.get_partition_cols(self.dataflowSpec.clusterBy),
                            cluster_by_auto=cluster_by_auto,
                            path=target_path,
                            comment=target_comment,
                            row_filter=self._get_row_filter(),
                            schema=column_policy_schema,
                        )
                    )
                else:
                    dlt_table_with_expectation = dp.expect_all_or_fail(expect_all_or_fail_dict)(
                        dlt_table_with_expectation)
            if expect_all_or_drop_dict:
                if expect_all_dict is None and expect_all_or_fail_dict is None:
                    dlt_table_with_expectation = dp.expect_all_or_drop(expect_all_or_drop_dict)(
                        dp.table(
                            self.write_to_delta,
                            name=f"{target_table}",
                            table_properties=self.dataflowSpec.tableProperties,
                            partition_cols=DataflowSpecUtils.get_partition_cols(self.dataflowSpec.partitionColumns),
                            cluster_by=DataflowSpecUtils.get_partition_cols(self.dataflowSpec.clusterBy),
                            cluster_by_auto=cluster_by_auto,
                            path=target_path,
                            comment=target_comment,
                            row_filter=self._get_row_filter(),
                            schema=column_policy_schema,
                        )
                    )
                else:
                    dlt_table_with_expectation = dp.expect_all_or_drop(expect_all_or_drop_dict)(
                        dlt_table_with_expectation)
            # Handle quarantine table (Bronze and Silver layers)
        if expect_or_quarantine_dict:
            q_partition_cols = None
            q_cluster_by = None
            quarantine_target_details = self._get_quarantine_target_details()
            if quarantine_target_details.get("partition_columns"):
                q_partition_cols = [quarantine_target_details["partition_columns"]]

            if quarantine_target_details.get("cluster_by"):
                # Parse cluster_by if it's a string representation of a list
                cluster_by_value = quarantine_target_details['cluster_by']
                if isinstance(cluster_by_value, str) and cluster_by_value.strip().startswith(('[', "[")):
                    # Handle string representations like "['id', 'email']" or '["id", "email"]'
                    try:
                        parsed_cluster_by = ast.literal_eval(cluster_by_value)
                        if isinstance(parsed_cluster_by, list):
                            cluster_by_value = parsed_cluster_by
                    except (ValueError, SyntaxError):
                        # If parsing fails, keep as string and let get_partition_cols handle it
                        quarantine_table_name = quarantine_target_details.get('table', '')
                        msg = f"Invalid cluster_by {cluster_by_value} for {quarantine_table_name}"
                        logger.error(msg)
                q_cluster_by = DataflowSpecUtils.get_partition_cols(cluster_by_value)

            quarantine_path = None if self.uc_enabled else quarantine_target_details.get("path")
            quarantine_cl = quarantine_target_details.get('catalog', None)
            quarantine_db = quarantine_target_details.get('database', '')
            quarantine_table_name = quarantine_target_details.get('table', '')

            # Check if quarantine_table_name is not empty (handles both None and empty string)
            if not quarantine_table_name or quarantine_table_name.strip() == '':
                logger.warning("Quarantine table name is empty or None. Skipping quarantine table creation.")
                return

            quarantine_table = self._build_table_name(quarantine_cl, quarantine_db, quarantine_table_name)
            layer_name = "bronze" if is_bronze else "silver"
            quarantine_comment = (
                quarantine_target_details.get('comment')
                if 'comment' in quarantine_target_details
                else f"{layer_name} dlt quarantine table {quarantine_table}"
            )

            # Get cluster_by_auto from quarantine configuration, default to False
            # Handle both string and boolean values since quarantineTargetDetails uses StringType
            q_cluster_by_auto_value = quarantine_target_details.get("cluster_by_auto", False)
            if isinstance(q_cluster_by_auto_value, str):
                q_cluster_by_auto = q_cluster_by_auto_value.lower().strip() == 'true'
            else:
                q_cluster_by_auto = bool(q_cluster_by_auto_value) if q_cluster_by_auto_value else False

            dp.expect_all_or_drop(expect_or_quarantine_dict)(
                dp.table(
                    self.write_to_delta,
                    name=f"{quarantine_table}",
                    table_properties=self.dataflowSpec.quarantineTableProperties,
                    partition_cols=q_partition_cols,
                    cluster_by=q_cluster_by,
                    cluster_by_auto=q_cluster_by_auto,
                    path=quarantine_path,
                    comment=quarantine_comment,
                    row_filter=self._get_quarantine_row_filter(),
                )
            )

    def write_append_flows(self):
        """Creates an append flow for the target specified in the dataflowSpec.

        This method creates a streaming table with the given schema and target path.
        It then appends the flow to the table using the specified parameters.

        Args:
            None

        Returns:
            None
        """
        if self.appendFlows is None:
            return
        for append_flow in self.appendFlows:
            struct_schema = None
            if self.schema_json:
                struct_schema = (
                    StructType.fromJson(self.schema_json)
                    if isinstance(self.dataflowSpec, BronzeDataflowSpec)
                    else self.silver_schema
                )
            elif isinstance(self.dataflowSpec, SilverDataflowSpec):
                # Silver has no ``schema_json`` (its schema is derived from the
                # transform). Resolve it lazily so column comments/masks can be
                # attached; returns ``None`` when the feature is unused, which
                # preserves the previous "no explicit schema" behaviour.
                struct_schema = self._resolve_policy_schema()
            target_details = self._get_target_details()

            append_flow_writer = AppendFlowWriter(
                self.spark, append_flow,
                target_details['table'],
                self._apply_column_policies(struct_schema),
                self.dataflowSpec.tableProperties,
                self.dataflowSpec.partitionColumns,
                self.dataflowSpec.clusterBy,
                self.dataflowSpec.clusterByAuto,
                self._get_row_filter()
            )
            append_flow_writer.write_flow()

    def cdc_apply_changes(self):
        """CDC Apply Changes against dataflowspec."""
        cdc_apply_changes = self.cdcApplyChanges
        if cdc_apply_changes is None:
            raise Exception("cdcApplychanges is None! ")

        # Silver has no ``schema_json`` (its schema comes from the transform),
        # so conditioning only on ``schema_json`` previously passed ``None``
        # here — dropping Silver comments and failing Silver masks with "no
        # schema is available". ``_resolve_policy_schema`` materialises the
        # derived schema on ``self.silver_schema`` ONLY when comments/masks
        # are configured (returns ``None`` otherwise), so we pass the modified
        # schema on the CDC path when policies are set and otherwise preserve
        # the previous inferred-schema behaviour.
        # Parse sequence_by ONCE here; this single list is reused for BOTH the
        # explicit-schema derivation (modify_schema_for_cdc_changes) and the
        # apply-time struct(*cols) below — the contract's single source of truth.
        sequence_cols = parse_sequence_by_columns(cdc_apply_changes.sequence_by)

        policy_schema = self._resolve_policy_schema()
        struct_schema = None
        if self.schema_json or policy_schema is not None:
            struct_schema = self.modify_schema_for_cdc_changes(cdc_apply_changes, sequence_cols)

        target_path = None if self.uc_enabled else self.dataflowSpec.targetDetails["path"]

        self.create_streaming_table(struct_schema, target_path)

        apply_as_deletes = None
        if cdc_apply_changes.apply_as_deletes:
            apply_as_deletes = expr(cdc_apply_changes.apply_as_deletes)

        apply_as_truncates = None
        if cdc_apply_changes.apply_as_truncates:
            apply_as_truncates = expr(cdc_apply_changes.apply_as_truncates)

        target_cl = self.dataflowSpec.targetDetails.get('catalog', None)
        target_db_name = self.dataflowSpec.targetDetails['database']
        target_table_name = self.dataflowSpec.targetDetails['table']
        target_table = self._build_table_name(target_cl, target_db_name, target_table_name)

        # Composite sequence_by => struct(*cols); single => the bare column.
        # ``sequence_cols`` was parsed once above and also fed to the schema
        # derivation, so the declared __START_AT/__END_AT type matches this
        # value exactly.
        sequence_by = (
            struct(*sequence_cols) if len(sequence_cols) > 1 else sequence_cols[0]
        )

        dp.create_auto_cdc_flow(
            target=target_table,
            source=self.view_name,
            keys=cdc_apply_changes.keys,
            sequence_by=sequence_by,
            where=cdc_apply_changes.where,
            ignore_null_updates=cdc_apply_changes.ignore_null_updates,
            apply_as_deletes=apply_as_deletes,
            apply_as_truncates=apply_as_truncates,
            column_list=cdc_apply_changes.column_list,
            except_column_list=cdc_apply_changes.except_column_list,
            stored_as_scd_type=cdc_apply_changes.scd_type,
            track_history_column_list=cdc_apply_changes.track_history_column_list,
            track_history_except_column_list=cdc_apply_changes.track_history_except_column_list,
            flow_name=cdc_apply_changes.flow_name,
            once=cdc_apply_changes.once,
            ignore_null_updates_column_list=cdc_apply_changes.ignore_null_updates_column_list,
            ignore_null_updates_except_column_list=cdc_apply_changes.ignore_null_updates_except_column_list
        )

    def cdc_apply_changes_flows(self):
        """Run N AUTO CDC flows into a single target table (issue #294).

        Creates the target streaming table ONCE — DLT mandates a single
        ``create_streaming_table`` call per target — then registers one
        ``dp.create_auto_cdc_flow`` per flow in the group. Every
        flow-specific call inherits the group's CDC configuration
        (``keys`` / ``sequence_by`` / ``scd_type`` / etc.) since DLT
        requires those to be identical across flows targeting the same
        streaming table. Per-flow overrides are limited to ``flow_name``
        and ``once``, which DLT supports on a per-call basis.

        Schema derivation reuses :meth:`modify_schema_for_cdc_changes`
        which duck-types on ``except_column_list``, ``sequence_by``,
        and ``scd_type`` — all present on
        :class:`CDCApplyChangesFlowGroup` — so the SCD2 ``__START_AT`` /
        ``__END_AT`` append logic stays identical.

        Source views are produced by :meth:`read_cdc_flows` at read-time
        and consumed here by name (``{flow.name}_cdc_view``).

        Row filters: UC row filters bind at table-creation time, not at
        write time. ``dp.create_auto_cdc_flow`` therefore has no
        ``row_filter`` knob — every flow targeting the same streaming
        table inherits the table-level filter. The spec-level
        ``rowFilter`` is wired through :meth:`create_streaming_table`
        below, so all N flows respect a single consistent policy on the
        merged target. This is the correct shape: row filters express
        WHO can read WHICH rows of the merged dataset, not how each
        upstream flow should filter on write.
        """
        group = self.cdcApplyChangesFlows
        if group is None:
            raise Exception("cdcApplyChangesFlows is None! ")

        # Parse sequence_by ONCE; reused for BOTH the schema derivation and the
        # apply-time struct(*cols) below (single source of truth).
        sequence_cols = parse_sequence_by_columns(group.sequence_by)

        struct_schema = None
        # Bronze derives the streaming-table schema from
        # ``self.schema_json`` if set; silver from ``self.silver_schema``.
        # The single-source path conditions on ``self.schema_json``;
        # ``modify_schema_for_cdc_changes`` then checks both possibilities
        # internally and returns ``None`` when no schema is available.
        if self.schema_json or self.silver_schema:
            struct_schema = self.modify_schema_for_cdc_changes(group, sequence_cols)

        target_path = None if self.uc_enabled else self.dataflowSpec.targetDetails["path"]
        self.create_streaming_table(struct_schema, target_path)

        apply_as_deletes = expr(group.apply_as_deletes) if group.apply_as_deletes else None
        apply_as_truncates = expr(group.apply_as_truncates) if group.apply_as_truncates else None

        # Composite sequence_by ("ts,id") => struct(ts, id), same as the
        # single-flow path. ``sequence_cols`` (parsed once above and also fed to
        # the schema derivation) is the single source of truth, so the declared
        # __START_AT/__END_AT type mirrors exactly what DLT materialises here.
        # (Previously the schema path looked up only the first column's scalar
        # type, which did NOT match struct(ts,id).)
        sequence_by = (
            struct(*sequence_cols) if len(sequence_cols) > 1 else sequence_cols[0]
        )

        target_table = self._get_target_table_name()

        for flow in group.flows:
            dp.create_auto_cdc_flow(
                target=target_table,
                source=f"{flow.name}_cdc_view",
                keys=group.keys,
                sequence_by=sequence_by,
                where=group.where,
                ignore_null_updates=group.ignore_null_updates,
                apply_as_deletes=apply_as_deletes,
                apply_as_truncates=apply_as_truncates,
                column_list=group.column_list,
                except_column_list=group.except_column_list,
                stored_as_scd_type=group.scd_type,
                track_history_column_list=group.track_history_column_list,
                track_history_except_column_list=group.track_history_except_column_list,
                flow_name=flow.name,
                once=flow.once,
                ignore_null_updates_column_list=group.ignore_null_updates_column_list,
                ignore_null_updates_except_column_list=group.ignore_null_updates_except_column_list,
            )

    def modify_schema_for_cdc_changes(self, cdc_apply_changes, sequence_cols=None):
        """Build the explicit target schema for a CDC/SCD2 table.

        ``sequence_cols`` is the ALREADY-parsed bare-column list (single source
        of truth). The apply-time callers parse ``sequence_by`` once and pass it
        here so the derived ``__START_AT``/``__END_AT`` type and the apply-time
        ``struct(*cols)`` provably operate on the identical list. When called
        directly (e.g. from tests) ``None`` means "parse it here".
        """
        if isinstance(self.dataflowSpec, BronzeDataflowSpec) and self.schema_json is None:
            return None
        if isinstance(self.dataflowSpec, SilverDataflowSpec) and self.silver_schema is None:
            return None

        struct_schema = None
        if isinstance(self.dataflowSpec, BronzeDataflowSpec) and self.schema_json is not None:
            struct_schema = StructType.fromJson(self.schema_json)
        elif isinstance(self.dataflowSpec, SilverDataflowSpec):
            struct_schema = self.silver_schema

        if struct_schema is None:
            return None

        # Single source of truth: reuse the caller's already-parsed bare-column
        # list so the SCD2 system-column type and the apply-time struct(*cols)
        # provably operate on the identical columns. Only parse here when called
        # without one (direct/test calls). The derivation below uses the full
        # (pre-prune) schema, so it still resolves the type even when a sequence
        # column is itself listed in except_column_list.
        if sequence_cols is None:
            sequence_cols = parse_sequence_by_columns(cdc_apply_changes.sequence_by)

        # Prune except_column_list off a COPY of the fields. NEVER mutate
        # ``struct_schema`` in place: on the silver path it is
        # ``self.silver_schema`` (shared and cached across refreshes/targets),
        # so an in-place ``.add()`` would corrupt it for every later use.
        if cdc_apply_changes.except_column_list:
            except_set = set(cdc_apply_changes.except_column_list)
            pruned_fields = [f for f in struct_schema.fields if f.name not in except_set]
        else:
            pruned_fields = list(struct_schema.fields)
        pruned_schema = StructType(pruned_fields)

        if cdc_apply_changes.scd_type != "2":
            return pruned_schema

        # Derive the __START_AT/__END_AT type from the (pre-prune) source
        # schema so it matches the apply-time value exactly. Dotted references
        # are rejected here (see _derive_scd2_sequence_type). ``None`` means a
        # plain top-level sequence column is simply absent from the schema — we
        # skip the system columns rather than declare a wrong one (preserving
        # the long-standing behaviour for a missing sequence column).
        derived = self._derive_scd2_sequence_type(
            struct_schema, sequence_cols, target_name=self._get_target_table_name()
        )
        if derived is None:
            return pruned_schema
        start_at_type, start_at_nullable = derived
        return self._with_scd2_system_columns(
            pruned_schema, start_at_type, start_at_nullable=start_at_nullable
        )

    @staticmethod
    def _derive_scd2_sequence_type(struct_schema, sequence_cols, *, target_name=""):
        """Return ``(dataType, nullable)`` for the SCD2 ``__START_AT`` column,
        mirroring EXACTLY what the apply-time sequence expression materialises.

        * 1 column -> the source ``StructField``'s ``dataType`` OBJECT copied
          directly (not rebuilt), following its nullability. Apply time passes
          the bare column name (a scalar reference), so DLT stamps that column's
          own type — which may itself be a struct/array/map, whose nested
          nullability and metadata are preserved because we reuse the very same
          ``DataType`` object.
        * N columns -> a ``StructType`` built from the corresponding source
          ``StructField``s in DECLARED order (names / types / nested
          nullability / metadata copied verbatim), because apply time wraps
          them in ``struct(*cols)`` and Spark's ``struct()`` preserves each
          referenced field verbatim. The struct expression itself is
          non-nullable.

        Dotted sequence references (``a.b``) are REJECTED with a clear error:
        we must emit a COMPLETE explicit schema (comments/masks require it), but
        a dotted path resolves against nested schema and ``struct()`` renames it
        to the last segment, so a top-level lookup here cannot faithfully mirror
        what DLT materialises. Silently skipping would emit a schema missing the
        system columns and break CREATE — so we fail loudly and actionably
        instead. ``validate_sequence_by`` still accepts dotted refs for the
        (non-explicit-schema) SCD1 / ordering-only paths.

        Returns ``None`` if a plain top-level sequence column is simply absent
        from the schema, signalling the caller to skip the system columns
        (long-standing behaviour for a missing sequence column).
        """
        dotted = [col for col in sequence_cols if "." in col]
        if dotted:
            raise ValueError(
                f"SCD2 apply_changes target ({target_name}) declares a dotted "
                f"sequence_by column {dotted!r}, which is not supported when an "
                f"explicit schema is required (column comments/masks, or a "
                f"declared bronze/silver schema). The DLT-managed __START_AT / "
                f"__END_AT columns must be typed from a top-level schema field, "
                f"but a dotted reference resolves against nested schema and is "
                f"renamed by struct(...). Use a top-level column for sequence_by "
                f"on an SCD2 target, or drop the explicit schema / column "
                f"policies."
            )
        name_to_field = {f.name: f for f in struct_schema.fields}
        try:
            seq_fields = [name_to_field[col] for col in sequence_cols]
        except KeyError:
            return None
        if len(seq_fields) == 1:
            # Copy the source dataType OBJECT directly so a non-scalar sequence
            # column keeps its nested nullability/metadata intact.
            field = seq_fields[0]
            return field.dataType, field.nullable
        nested = StructType([
            StructField(f.name, f.dataType, f.nullable, f.metadata)
            for f in seq_fields
        ])
        return nested, False

    @staticmethod
    def _with_scd2_system_columns(struct_schema, start_at_type, *, start_at_nullable):
        """Return a NEW ``StructType`` with SCD2 ``__START_AT`` / ``__END_AT``
        appended to ``struct_schema``.

        Never mutates ``struct_schema`` (it may be a shared/cached schema such
        as ``self.silver_schema``); a fresh ``StructType`` is always built.
        ``__END_AT`` is DELIBERATELY nullable — current/open records carry
        ``NULL`` there — regardless of the sequence/version nullability;
        ``__START_AT`` follows the sequence/version nullability. Shared by the
        regular CDC path (Fix 1) and the snapshot-CDC path (Fix 2).
        """
        return StructType(
            list(struct_schema.fields)
            + [
                StructField("__START_AT", start_at_type, start_at_nullable),
                StructField("__END_AT", start_at_type, True),
            ]
        )

    def create_streaming_table(self, struct_schema, target_path=None):
        expect_all_dict, expect_all_or_drop_dict, expect_all_or_fail_dict = self.get_dq_expectations()

        target_cl = self.dataflowSpec.targetDetails.get('catalog', None)
        target_db_name = self.dataflowSpec.targetDetails['database']
        target_table_name = self.dataflowSpec.targetDetails['table']
        target_table = self._build_table_name(target_cl, target_db_name, target_table_name)

        # Get cluster_by_auto from dataflowSpec, default to False if not present
        cluster_by_auto = (
            self.dataflowSpec.clusterByAuto
            if hasattr(self.dataflowSpec, 'clusterByAuto')
            and self.dataflowSpec.clusterByAuto is not None
            else False
        )

        dp.create_streaming_table(
            name=target_table,
            table_properties=self.dataflowSpec.tableProperties,
            partition_cols=DataflowSpecUtils.get_partition_cols(self.dataflowSpec.partitionColumns),
            cluster_by=DataflowSpecUtils.get_partition_cols(self.dataflowSpec.clusterBy),
            cluster_by_auto=cluster_by_auto,
            path=target_path,
            schema=self._apply_column_policies(struct_schema),
            expect_all=expect_all_dict,
            expect_all_or_drop=expect_all_or_drop_dict,
            expect_all_or_fail=expect_all_or_fail_dict,
            row_filter=self._get_row_filter(),
        )

    def get_dq_expectations(self):
        """
        Retrieves the data quality expectations for the table.

        Returns:
            A tuple containing three dictionaries:
            - expect_all_dict: A dictionary containing the 'expect_all' data quality expectations.
            - expect_all_or_drop_dict: A dictionary containing the 'expect_all_or_drop' data quality expectations.
            - expect_all_or_fail_dict: A dictionary containing the 'expect_all_or_fail' data quality expectations.
        """
        expect_all_dict = None
        expect_all_or_drop_dict = None
        expect_all_or_fail_dict = None
        if self.table_has_expectations():
            data_quality_expectations_json = json.loads(self.dataflowSpec.dataQualityExpectations)
            if "expect_all" in data_quality_expectations_json:
                expect_all_dict = data_quality_expectations_json["expect_all"]
            if "expect" in data_quality_expectations_json:
                expect_all_dict = data_quality_expectations_json["expect"]
            if "expect_all_or_drop" in data_quality_expectations_json:
                expect_all_or_drop_dict = data_quality_expectations_json["expect_all_or_drop"]
            if "expect_or_drop" in data_quality_expectations_json:
                expect_all_or_drop_dict = data_quality_expectations_json["expect_or_drop"]
            if "expect_all_or_fail" in data_quality_expectations_json:
                expect_all_or_fail_dict = data_quality_expectations_json["expect_all_or_fail"]
            if "expect_or_fail" in data_quality_expectations_json:
                expect_all_or_fail_dict = data_quality_expectations_json["expect_or_fail"]
        return expect_all_dict, expect_all_or_drop_dict, expect_all_or_fail_dict

    def run_dlt(self):
        """Run DLT."""
        logger.info("in run_dlt function")
        self.read()
        self.write()

    @staticmethod
    def invoke_dlt_pipeline(spark,
                            layer,
                            bronze_custom_transform_func: Callable = None,
                            silver_custom_transform_func: Callable = None,
                            bronze_next_snapshot_and_version: Callable = None,
                            silver_next_snapshot_and_version: Callable = None):
        """Invoke dlt pipeline will launch dlt with given dataflowspec.

        Args:
            spark (_type_): _description_
            layer (_type_): _description_
        """

        dataflowspec_list = None
        if "bronze" == layer.lower():
            dataflowspec_list = DataflowSpecUtils.get_bronze_dataflow_spec(spark)
            DataflowPipeline._launch_dlt_flow(
                spark, "bronze", dataflowspec_list, bronze_custom_transform_func, bronze_next_snapshot_and_version
            )
        elif "silver" == layer.lower():
            dataflowspec_list = DataflowSpecUtils.get_silver_dataflow_spec(spark)
            DataflowPipeline._launch_dlt_flow(
                spark, "silver", dataflowspec_list, silver_custom_transform_func, silver_next_snapshot_and_version
            )
        elif "bronze_silver" == layer.lower():
            bronze_dataflowspec_list = DataflowSpecUtils.get_bronze_dataflow_spec(spark)
            DataflowPipeline._launch_dlt_flow(
                spark, "bronze", bronze_dataflowspec_list, bronze_custom_transform_func,
                bronze_next_snapshot_and_version
            )
            silver_dataflowspec_list = DataflowSpecUtils.get_silver_dataflow_spec(spark)
            # Thread each bronze target's declared schema into the silver flow
            # so silver column-policy schema resolution never depends on the
            # bronze table already existing in UC (issue #1). In a combined run
            # the bronze table is produced in this same run, so a live read of
            # it at graph-construction time raises TABLE_OR_VIEW_NOT_FOUND.
            source_schema_map = DataflowPipeline._build_bronze_target_schema_map(
                bronze_dataflowspec_list
            )
            DataflowPipeline._launch_dlt_flow(
                spark, "silver", silver_dataflowspec_list, silver_custom_transform_func,
                silver_next_snapshot_and_version, source_schema_map=source_schema_map,
                combined_run=True
            )

    @staticmethod
    def _build_bronze_target_schema_map(bronze_dataflowspec_list):
        """Map each bronze target's fully-qualified table name to its
        reader-augmented TARGET schema (a ``StructType``), for in-process silver
        schema resolution during a combined ``bronze_silver`` run.

        The mapped schema is the declared source schema AUGMENTED with the
        columns the bronze reader injects into the materialised target
        (``_rescued_data``, autoloader metadata columns — see
        :func:`augment_bronze_schema_with_reader_columns`), so a silver
        ``selectExp`` that references such a valid bronze-target column resolves
        against the in-process schema exactly as it would against the physical
        table (BLOCKING #2: mapped schema must be the TARGET schema, not the raw
        input schema).

        Keyed to match how ``get_silver_schema`` / ``_flow_source_fqn`` build
        the silver source FQN (``catalog.database.table``, catalog omitted when
        absent). Bronze specs with no declared schema are skipped — silver
        schema resolution then fails fast in a combined run (there is no schema
        to resolve against) or reads the live table in the split topology.
        """
        schema_map = {}
        for spec in bronze_dataflowspec_list:
            if not isinstance(spec, BronzeDataflowSpec):
                continue
            raw_schema = getattr(spec, "schema", None)
            if not raw_schema:
                continue
            target_details = spec.targetDetails
            if not target_details:
                continue
            declared = StructType.fromJson(
                json.loads(raw_schema) if isinstance(raw_schema, str) else raw_schema
            )
            augmented = augment_bronze_schema_with_reader_columns(spec, declared)
            catalog = target_details.get('catalog', None)
            catalog_prefix = f"{catalog}." if catalog is not None else ''
            key = f"{catalog_prefix}{target_details['database']}.{target_details['table']}"
            schema_map[key] = augmented
        return schema_map

    @staticmethod
    def _launch_dlt_flow(
        spark, layer, dataflowspec_list, custom_transform_func=None, next_snapshot_and_version: Callable = None,
        source_schema_map=None, combined_run=False
    ):
        for dataflowSpec in dataflowspec_list:
            logger.info("Printing Dataflow Spec")
            logger.info(dataflowSpec)
            quarantine_input_view_name = None
            if hasattr(dataflowSpec, 'quarantineTargetDetails') and dataflowSpec.quarantineTargetDetails is not None \
                    and dataflowSpec.quarantineTargetDetails != {}:

                qrt_cl = dataflowSpec.quarantineTargetDetails.get('catalog', None)
                qrt_db = dataflowSpec.quarantineTargetDetails['database'].replace('.', '_')
                qrt_table = dataflowSpec.quarantineTargetDetails['table']
                if not DataflowPipeline._is_dpm_enabled(spark):
                    quarantine_input_view_name = f"{qrt_table}_{layer}_quarantine_inputview"
                else:
                    qrt_cl_str = f"{qrt_cl}_" if qrt_cl is not None else ''
                    quarantine_input_view_name = (
                        f"{qrt_cl_str}{qrt_db}_{qrt_table}"
                        f"_{layer}_quarantine_inputview"
                    )
                    quarantine_input_view_name = quarantine_input_view_name.replace(".", "").lower()
            else:
                logger.info("quarantine_input_view_name set to None")
            target_cl = dataflowSpec.targetDetails.get('catalog', None)
            target_db = dataflowSpec.targetDetails['database'].replace('.', '_')
            target_table = dataflowSpec.targetDetails['table']
            # In legacy publishing mode all tables live in the LIVE virtual schema,
            # so view names must NOT include the catalog/database prefix — they must
            # match exactly what dp.temporary_view registered. In DPM, keep the full
            # prefix so names stay unique across catalogs/schemas.
            if not DataflowPipeline._is_dpm_enabled(spark):
                target_view_name = f"{target_table}_{layer}_inputview"
            else:
                target_cl_str = f"{target_cl}_" if target_cl is not None else ''
                target_view_name = f"{target_cl_str}{target_db}_{target_table}_{layer}_inputview"
                target_view_name = target_view_name.replace(".", "").lower()
            dlt_data_flow = DataflowPipeline(
                spark,
                dataflowSpec,
                target_view_name,
                quarantine_input_view_name,
                custom_transform_func,
                next_snapshot_and_version,
                source_schema_map=source_schema_map,
                combined_run=combined_run
            )
            dlt_data_flow.run_dlt()

    # Additional optimization methods for common patterns
    @staticmethod
    def _is_dpm_enabled(spark):
        """Return True when default publishing mode exposes ``pipelines.schema``."""
        return bool(spark.conf.get("pipelines.schema", "").strip())

    @staticmethod
    def _build_fully_qualified_table_name(catalog, database, table):
        """Build catalog.schema.table (or schema.table without catalog).

        Source table reads are not Lakeflow dataset declarations, so they must
        stay qualified even when output datasets run in legacy publishing mode.
        """
        catalog_prefix = f"{catalog}." if catalog else ''
        return f"{catalog_prefix}{database}.{table}"

    def _build_table_name(self, catalog, database, table):
        """Build a table name appropriate for the pipeline's publishing mode.

        Legacy publishing mode (pipelines.target is set): returns the bare table
        name only — LDP rejects schema-qualified names in the LIVE virtual schema
        and raises DLTAnalysisException.

        Default publishing mode (DPM): returns fully-qualified catalog.schema.table.
        """
        if self.is_legacy_publishing_mode:
            return table
        catalog_prefix = f"{catalog}." if catalog else ''
        return f"{catalog_prefix}{database}.{table}"

    def _get_source_table_info(self):
        """Extract source table information."""
        source_details = self._get_source_details()
        catalog = source_details.get('catalog', None)
        database = source_details["database"]
        table = source_details["table"]
        return self._build_fully_qualified_table_name(catalog, database, table), source_details

    def _get_target_table_name(self):
        """Get the fully qualified target table name."""
        target_details = self._get_target_details()
        catalog = target_details.get('catalog', None)
        database = target_details['database']
        table = target_details['table']
        return self._build_table_name(catalog, database, table)

    def _create_dataframe_reader(self, is_streaming=True, reader_options=None):
        """Create a DataFrame reader with common configuration."""
        if reader_options is None:
            reader_options = {}
        if is_streaming:
            reader = self.spark.readStream
        else:
            reader = self.spark.read
        if reader_options:
            reader = reader.options(**reader_options)
        return reader

    def _read_from_source(self, source_format, is_streaming=True):
        """Generic method to read from different source formats."""
        source_table_name, source_details = self._get_source_table_info()
        reader_options = self._get_reader_config_options()
        reader = self._create_dataframe_reader(is_streaming, reader_options)
        if source_format == "snapshot" or not is_streaming:
            if self.uc_enabled:
                return reader.table(source_table_name)
            else:
                return reader.load(path=source_details.get("path"), format="delta")
        else:
            if self.uc_enabled:
                return reader.table(source_table_name)
            else:
                return reader.load(path=source_details.get("path"), format="delta")

    def _apply_transformations(self, df, select_exp=None, where_clause=None):
        """Apply common transformations (select and where) to a DataFrame."""
        if select_exp:
            df = df.selectExpr(*select_exp)
        if where_clause:
            where_clause_str = " ".join(where_clause)
            if len(where_clause_str.strip()) > 0:
                for clause in where_clause:
                    df = df.where(clause)
        return df
