"""OnboardDataflowSpec class provides bronze/silver onboarding features."""

import copy
import dataclasses
import json
import yaml
import logging
import ast
import os
import re
import tempfile

import pyspark.sql.types as T
from pyspark.sql import functions as f
from pyspark.sql.types import ArrayType, MapType, StringType, StructField, StructType

from databricks.labs.sdp_meta.dataflow_spec import (
    BronzeDataflowSpec,
    DataflowSpecUtils,
    SilverDataflowSpec,
    _coerce_scd_type_to_str,
)
from databricks.labs.sdp_meta.identifiers import (
    SUPPORTED_SOURCE_FORMATS,
    validate_column_comments,
    validate_column_masks,
    validate_scd_type,
    validate_sequence_by,
    validate_source_format,
    validate_sql_where_clause,
    validate_uc_column_list,
    validate_uc_full_name,
    validate_uc_identifier,
)
from databricks.labs.sdp_meta.metastore_ops import DeltaPipelinesInternalTableOps, DeltaPipelinesMetaStoreOps

logger = logging.getLogger("databricks.labs.sdp_meta")
logger.setLevel(logging.INFO)


# Column-name fields inside ``<layer>_cdc_apply_changes`` that drive
# DLT's ``apply_changes`` column-projection logic. Each entry must
# resolve to (a list of) regular SQL identifier(s); a hyphenated entry
# would fail at DLT runtime, so onboarding pre-flight catches it. The
# expression-valued fields (``apply_as_deletes`` / ``apply_as_truncates``
# / ``where``) are deliberately NOT in this list -- they go through
# ``expr(...)`` rather than identifier slots. ``sequence_by`` is also
# NOT here: it supports comma-separated multi-column ordering and
# dotted column references (see ``validate_sequence_by``), so it gets
# its own dedicated check below.
_CDC_COL_FIELDS = (
    "keys",
    "column_list",
    "except_column_list",
    "track_history_column_list",
    "track_history_except_column_list",
    "ignore_null_updates_column_list",
    "ignore_null_updates_except_column_list",
)

# Same idea, but the ``<layer>_apply_changes_from_snapshot`` block
# accepts a smaller set of column-name fields. Keeping the two tuples
# separate -- rather than reusing _CDC_COL_FIELDS -- mirrors what DLT
# itself accepts in each call.
_SNAPSHOT_COL_FIELDS = (
    "keys",
    "track_history_column_list",
    "track_history_except_column_list",
)

# All pre-flight validation error messages start with the same anchor
# prefix ``flow <id> <field>: ...`` (or ``segment N of flow <id>
# <field> ...`` when validate_uc_full_name expands the kind), so we can
# extract the flow id at aggregation time to group errors by row. Kept
# at module scope so ``__pre_validate_onboarding_uc_names`` doesn't
# re-compile the regex per raise.
_FLOW_PREFIX_RE = re.compile(r"^(?:segment \d+ of )?flow (\S+)\b")

# Lower-cased mirror of :data:`SUPPORTED_SOURCE_FORMATS` for the
# case-insensitive runtime check inside
# ``__get_bronze_dataflow_spec_dataframe``. The canonical set
# (``cloudFiles``, ``delta``, ``kafka``, ``eventhub``, ``snapshot``)
# is case-sensitive because that's how DLT and Spark spell these
# strings, but historically the per-row check here has been lenient
# about ``"CloudFiles"`` / ``"cloudfiles"`` / ``"CLOUDFILES"`` and we
# don't want to break callers who relied on that. Built once at module
# scope so we're not rebuilding the set on every (row x layer) pass.
_SUPPORTED_SOURCE_FORMATS_LOWER = frozenset(
    s.lower() for s in SUPPORTED_SOURCE_FORMATS
)


class OnboardDataflowspec:
    """OnboardDataflowSpec class provides bronze/silver onboarding features."""

    def __init__(self, spark, dict_obj, bronze_schema_mapper=None, uc_enabled=False):
        """Onboard Dataflowspec Constructor."""
        self.spark = spark
        self.dict_obj = dict_obj
        self.bronze_dict_obj = copy.deepcopy(dict_obj)
        self.silver_dict_obj = copy.deepcopy(dict_obj)
        self.uc_enabled = uc_enabled
        self.__initialize_paths(uc_enabled)
        self.bronze_schema_mapper = bronze_schema_mapper
        self.deltaPipelinesMetaStoreOps = DeltaPipelinesMetaStoreOps(self.spark)
        self.deltaPipelinesInternalTableOps = DeltaPipelinesInternalTableOps(self.spark)
        self.onboard_file_type = None
        self._onboarding_files_processed: set = set()

    def __initialize_paths(self, uc_enabled):
        if "silver_dataflowspec_table" in self.bronze_dict_obj:
            del self.bronze_dict_obj["silver_dataflowspec_table"]
        if "silver_dataflowspec_path" in self.bronze_dict_obj:
            del self.bronze_dict_obj["silver_dataflowspec_path"]

        if "bronze_dataflowspec_table" in self.silver_dict_obj:
            del self.silver_dict_obj["bronze_dataflowspec_table"]
        if "bronze_dataflowspec_path" in self.silver_dict_obj:
            del self.silver_dict_obj["bronze_dataflowspec_path"]
        if uc_enabled:
            if "bronze_dataflowspec_path" in self.bronze_dict_obj:
                del self.bronze_dict_obj["bronze_dataflowspec_path"]
            if "silver_dataflowspec_path" in self.silver_dict_obj:
                del self.silver_dict_obj["silver_dataflowspec_path"]

    @staticmethod
    def __validate_row_uc_names(onboarding_row, env, layer):
        """Validate every UC identifier in a single onboarding row.

        ``database`` may be 1- or 2-part (`schema` or `catalog.schema`);
        ``table`` and ``catalog`` are always single identifiers. We
        reject anything that isn't a regular SQL identifier here so the
        deployed pipeline can splice these names directly into Spark
        SQL without further escaping (issue #261). ``layer`` is either
        ``"bronze"`` or ``"silver"`` and only affects the field prefix /
        error message.
        """
        flow_id = onboarding_row["data_flow_id"] if "data_flow_id" in onboarding_row else "<unknown>"
        db_field = f"{layer}_database_{env}"
        table_field = f"{layer}_table"
        catalog_field = f"{layer}_catalog_{env}"
        validate_uc_full_name(
            onboarding_row[db_field],
            kind=f"flow {flow_id} {db_field}",
            max_parts=2,
        )
        validate_uc_identifier(
            onboarding_row[table_field],
            kind=f"flow {flow_id} {table_field}",
        )
        if catalog_field in onboarding_row and onboarding_row[catalog_field]:
            validate_uc_identifier(
                onboarding_row[catalog_field],
                kind=f"flow {flow_id} {catalog_field}",
            )
        # Quarantine UC identifiers ride the same SQL-splice path as the
        # main bronze/silver targets (see __get_quarantine_details ->
        # dataflow_pipeline.py:570 where they're concatenated into the
        # dlt.create_streaming_table name unquoted), so any hyphens / dots
        # / leading digits there fail the same way (issue #261). Optional
        # fields — only validate when present and non-empty.
        q_db_field = f"{layer}_database_quarantine_{env}"
        q_table_field = f"{layer}_quarantine_table"
        q_catalog_field = f"{layer}_catalog_quarantine_{env}"
        if q_db_field in onboarding_row and onboarding_row[q_db_field]:
            validate_uc_full_name(
                onboarding_row[q_db_field],
                kind=f"flow {flow_id} {q_db_field}",
                max_parts=2,
            )
        if q_table_field in onboarding_row and onboarding_row[q_table_field]:
            validate_uc_identifier(
                onboarding_row[q_table_field],
                kind=f"flow {flow_id} {q_table_field}",
            )
        if q_catalog_field in onboarding_row and onboarding_row[q_catalog_field]:
            validate_uc_identifier(
                onboarding_row[q_catalog_field],
                kind=f"flow {flow_id} {q_catalog_field}",
            )

    @staticmethod
    def __validate_dict_attributes(attributes, dict_obj):
        """Validate dict attributes method will validate dict attributes keys.

        Args:
            attributes ([type]): [description]
            dict_obj ([type]): [description]

        Raises:
            ValueError: [description]
        """
        if sorted(set(attributes)) != sorted(set(dict_obj.keys())):
            attributes_keys = set(dict_obj.keys())
            logger.info("In validate dict attributes")
            logger.info(f"expected: {set(attributes)}, actual: {attributes_keys}")
            logger.info(
                "missing attributes : {}".format(
                    set(attributes).difference(attributes_keys)
                )
            )
            raise ValueError(
                f"missing attributes : {set(attributes).difference(attributes_keys)}"
            )

    def onboard_dataflow_specs(self):
        """
        Onboard_dataflow_specs method will onboard dataFlowSpecs for bronze and silver.

        This method takes in a SparkSession object and a dictionary object containing the following attributes:
        - onboarding_file_path: The path to the onboarding file.
        - database: The name of the database to onboard the dataflow specs to.
        - env: The environment to onboard the dataflow specs to.
        - bronze_dataflowspec_table: The name of the bronze dataflow specs table.
        - bronze_dataflowspec_path: The path to the bronze dataflow specs.
        - silver_dataflowspec_table: The name of the silver dataflow specs table.
        - silver_dataflowspec_path: The path to the silver dataflow specs.
        - import_author: The author of the import.
        - version: The version of the import.
        - overwrite: Whether to overwrite existing dataflow specs or not.

        If the `uc_enabled` flag is set to True, the dictionary object must contain all the attributes listed above.
        If the `uc_enabled` flag is set to False, the dictionary object must contain all the attributes listed above
        except for `bronze_dataflowspec_path` and `silver_dataflowspec_path`.

        This method calls the `onboard_bronze_dataflow_spec` and `onboard_silver_dataflow_spec` methods to onboard
        the bronze and silver dataflow specs respectively.
        """
        attributes = [
            "onboarding_file_path",
            "database",
            "env",
            "bronze_dataflowspec_table",
            "silver_dataflowspec_table",
            "import_author",
            "version",
            "overwrite",
        ]
        if self.uc_enabled:
            if "bronze_dataflowspec_path" in self.dict_obj:
                del self.dict_obj["bronze_dataflowspec_path"]
            if "silver_dataflowspec_path" in self.dict_obj:
                del self.dict_obj["silver_dataflowspec_path"]
            self.__validate_dict_attributes(attributes, self.dict_obj)
        else:
            attributes.append("bronze_dataflowspec_path")
            attributes.append("silver_dataflowspec_path")
            self.__validate_dict_attributes(attributes, self.dict_obj)
        # Validate the metadata-table identifiers up-front. These get
        # spliced unquoted into CREATE DATABASE / saveAsTable / register-
        # in-metastore calls below, so a hyphenated catalog or schema
        # would only fail much later with a confusing Spark error
        # (issue #261). `database` may be 1- or 2-part (`schema` or
        # `catalog.schema`); the table names are single identifiers.
        validate_uc_full_name(
            self.dict_obj["database"], kind="dict_obj['database']", max_parts=2,
        )
        validate_uc_identifier(
            self.dict_obj["bronze_dataflowspec_table"],
            kind="dict_obj['bronze_dataflowspec_table']",
        )
        validate_uc_identifier(
            self.dict_obj["silver_dataflowspec_table"],
            kind="dict_obj['silver_dataflowspec_table']",
        )
        # Walk the onboarding file once and surface ALL UC-identifier
        # errors before doing any Spark side-effects (CREATE DATABASE,
        # saveAsTable, register-in-metastore). Otherwise a hyphenated
        # catalog on row 5 would fail mid-onboarding with the bronze
        # dataflowspec table already half-written, which is hard to clean
        # up. Aggregating errors also gives the user the full picture in
        # one shot instead of fix-rerun-fix-rerun (issue #261).
        self.__pre_validate_onboarding_uc_names()
        self.onboard_bronze_dataflow_spec()
        self.onboard_silver_dataflow_spec()

    def __pre_validate_onboarding_uc_names(self, layers=("bronze", "silver")):
        """Pre-flight validate every UC identifier in the onboarding file.

        Loads the onboarding file once, walks every row, and validates:

        * Per layer in ``layers`` (default ``("bronze", "silver")``):
            - ``<layer>_database_<env>`` (**required**, 1- or 2-part full name)
            - ``<layer>_table`` (**required**, single identifier)
            - ``<layer>_catalog_<env>`` (optional, single identifier)
            - ``<layer>_database_quarantine_<env>`` (optional, full name)
            - ``<layer>_quarantine_table`` (optional, single identifier)
            - ``<layer>_catalog_quarantine_<env>`` (optional, single identifier)
            - ``<layer>_partition_columns`` (optional, column-name list)
            - ``<layer>_quarantine_table_partitions`` (optional, column-name list)
            - ``<layer>_cluster_by`` (optional, column-name list)
            - ``<layer>_quarantine_table_cluster_by`` (optional, column-name list)

        Aggregates all failures into a single ``ValueError`` so the user
        sees every bad name in one shot instead of fixing them one at a
        time. Runs *before* any Spark side-effect (CREATE DATABASE,
        saveAsTable) so a bad row never half-onboards (issue #261).

        ``layers`` scopes the walk to the layer(s) being onboarded. The
        wrapper :py:meth:`onboard_dataflow_specs` passes the default
        ``("bronze", "silver")``; the single-layer entry points
        :py:meth:`onboard_bronze_dataflow_spec` and
        :py:meth:`onboard_silver_dataflow_spec` pass just their own
        layer so a legitimately-bronze-only (or silver-only) onboarding
        file doesn't get false-positived on the empty *other-layer*
        fields (issue #343 finding #1).

        **Required vs optional fields** (issue #343 finding #2): the two
        layer-anchor fields ``<layer>_database_<env>`` and
        ``<layer>_table`` are treated as *required* — an empty string
        is a validation error, matching the defence-in-depth
        :py:meth:`__validate_row_uc_names` invocation that runs later
        inside the Spark job. Every other check remains optional
        (skipped when the field is absent or falsy) because those
        represent legitimately-absent configuration (no quarantine, no
        cluster_by, no partition columns, …).

        **Idempotency:** the method tracks which layers it has already
        validated for this instance in ``self._prevalidated_layers``.
        Calling it a second time with the same (or a subset) of layers
        is a no-op, so ``onboard_dataflow_specs`` (which pre-validates
        both layers up front) can safely coexist with each single-layer
        method also calling pre-validate at the top of its own body —
        no double work, no double error report.

        The per-row validators (:py:meth:`__validate_row_uc_names`)
        inside :py:meth:`onboard_bronze_dataflow_spec` and
        :py:meth:`onboard_silver_dataflow_spec` are still kept as
        defence-in-depth in case a caller reaches for the Spark work
        via some other internal path.
        """
        # ── Idempotency guard ────────────────────────────────────────
        # Skip if we've already validated the requested layers on this
        # instance. Handles the common composition where the wrapper
        # pre-validates both layers up front and then the single-layer
        # methods also call this at the top of their own bodies.
        already = getattr(self, "_prevalidated_layers", frozenset())
        requested = frozenset(layers)
        to_check = requested - already
        if not to_check:
            return

        # ── Required-field policy ────────────────────────────────────
        # ``<layer>_database_<env>`` and ``<layer>_table`` are the two
        # fields the Spark job splices unquoted into CREATE DATABASE /
        # saveAsTable / register-in-metastore. An empty string there
        # produces a confusing runtime failure ("null.null" or the
        # dataflowspec table half-written). Fail pre-flight instead.
        env = self.dict_obj["env"]
        required_fields = {
            f"{layer}_{suffix}"
            for layer in to_check
            for suffix in (f"database_{env}", "table")
        }
        # Field-purpose + fix-example hints keyed by field. When a
        # required field is absent-or-empty, we render the message with
        # the field's own hint so the user knows what the field is FOR
        # and what a valid value looks like, not just that "something
        # is missing". Kept as a plain dict (not a class-level constant)
        # because the ``_database_<env>`` names are env-dependent.
        field_hints = {
            f"bronze_database_{env}": (
                "the target schema (1- or 2-part ``catalog.schema``) "
                "where the bronze streaming table will be created",
                f"bronze_database_{env}: main.my_bronze_schema",
            ),
            "bronze_table": (
                "the bronze streaming table name",
                "bronze_table: my_bronze_table",
            ),
            f"silver_database_{env}": (
                "the target schema (1- or 2-part ``catalog.schema``) "
                "where the silver streaming table will be created",
                f"silver_database_{env}: main.my_silver_schema",
            ),
            "silver_table": (
                "the silver streaming table name",
                "silver_table: my_silver_table",
            ),
        }

        onboarding_df = self.__get_onboarding_file_dataframe(
            self.dict_obj["onboarding_file_path"]
        )
        errors = []

        def _check(validator, value, kind, **kwargs):
            """Run ``validator(value, kind=kind, **kwargs)`` and stash the
            error string instead of raising, so we can collect every
            problem in one pass."""
            try:
                validator(value, kind=kind, **kwargs)
            except ValueError as exc:
                errors.append(str(exc))

        for row in onboarding_df.collect():
            # Recursive=True so nested Row objects (cdc_apply_changes,
            # apply_changes_from_snapshot, append_flows entries) come
            # through as plain dicts; the enum checks below need to peek
            # inside them without re-doing asDict() at every level.
            if hasattr(row, "asDict"):
                row_dict = row.asDict(recursive=True)
            else:
                row_dict = dict(row)
            flow_id = row_dict.get("data_flow_id", "<unknown>")

            # Top-level source_format applies to the bronze read path
            # regardless of layer; check it once per row.
            if row_dict.get("source_format"):
                _check(
                    validate_source_format,
                    row_dict["source_format"],
                    kind=f"flow {flow_id} source_format",
                )

            for layer in to_check:
                # ── Layer participation check ────────────────────────
                # An onboarding row can be bronze-only, silver-only, or
                # both. The runtime onboarding methods pick which rows
                # to process by these exact predicates:
                #   * bronze row: has non-empty ``source_details``
                #     (see onboard_bronze_dataflow_spec:1243).
                #   * silver row: has non-empty
                #     ``silver_database_{env}`` (see
                #     onboard_silver_dataflow_spec:2157).
                # We mirror those predicates here so pre-flight only
                # enforces layer-required fields on rows that will
                # ACTUALLY be onboarded into that layer. Otherwise a
                # legitimate bronze-only row would fail the silver
                # required-field check just for not having silver
                # fields (issue #343 finding #2 corner case).
                if layer == "bronze":
                    participates = bool(row_dict.get("source_details"))
                elif layer == "silver":
                    participates = bool(row_dict.get(f"silver_database_{env}"))
                else:
                    participates = False
                if not participates:
                    continue

                # (field, validator, validator_kwargs) tuples for every UC
                # identifier on this layer. Listed in roughly the order they
                # show up in the onboarding row so error messages read
                # naturally top-to-bottom.
                checks = [
                    (
                        f"{layer}_database_{env}",
                        validate_uc_full_name,
                        {"max_parts": 2},
                    ),
                    (f"{layer}_table", validate_uc_identifier, {}),
                    (f"{layer}_catalog_{env}", validate_uc_identifier, {}),
                    (
                        f"{layer}_database_quarantine_{env}",
                        validate_uc_full_name,
                        {"max_parts": 2},
                    ),
                    (f"{layer}_quarantine_table", validate_uc_identifier, {}),
                    (
                        f"{layer}_catalog_quarantine_{env}",
                        validate_uc_identifier,
                        {},
                    ),
                    # allow_empty_entries: the v0.0.10 ``[""]`` no-columns
                    # idiom is tolerated on these four fields only,
                    # because their runtime parser (``get_partition_cols``)
                    # drops blank entries. CDC column lists below stay
                    # strict — they're persisted verbatim into DLT calls.
                    (
                        f"{layer}_partition_columns",
                        validate_uc_column_list,
                        {"allow_empty_entries": True},
                    ),
                    (
                        f"{layer}_quarantine_table_partitions",
                        validate_uc_column_list,
                        {"allow_empty_entries": True},
                    ),
                    (
                        f"{layer}_cluster_by",
                        validate_uc_column_list,
                        {"allow_empty_entries": True},
                    ),
                    (
                        f"{layer}_quarantine_table_cluster_by",
                        validate_uc_column_list,
                        {"allow_empty_entries": True},
                    ),
                    # UC row-level-security clauses (issue #303/#306).
                    # These are the ``ROW FILTER cat.schema.func ON
                    # (cols)`` strings read verbatim in
                    # onboard_{bronze,silver}_dataflow_spec, stored on
                    # the spec, and later spliced into the generated
                    # streaming-table DDL via ``dp.table(row_filter=...)``
                    # / ``dp.create_streaming_table(row_filter=...)``.
                    # Because they land in a SQL/DDL position that the
                    # pipelines API cannot parameterise, run them through
                    # the same denylist guard the App's Metadata Browse
                    # WHERE clause uses \u2014 blocks statement separators,
                    # comments, and DDL/DML keywords while accepting a
                    # legitimate ``ROW FILTER ... ON (...)`` clause.
                    (f"{layer}_row_filter", validate_sql_where_clause, {}),
                    (
                        f"{layer}_quarantine_row_filter",
                        validate_sql_where_clause,
                        {},
                    ),
                    # UC column-level governance. Both are JSON objects keyed
                    # by column name: comments carry free text emitted as an
                    # escaped ``COMMENT '...'`` literal; masks carry a
                    # ``cat.schema.fn USING COLUMNS (...)`` clause spliced
                    # after ``MASK`` in the generated DDL-string schema. The
                    # mask validator runs the same denylist guard as the row
                    # filters plus a strict function-name / USING-COLUMNS
                    # parse (see identifiers.validate_column_masks).
                    (
                        f"{layer}_column_comments",
                        validate_column_comments,
                        {},
                    ),
                    (f"{layer}_column_masks", validate_column_masks, {}),
                ]
                for field, validator, kwargs in checks:
                    value = row_dict.get(field)
                    is_required = field in required_fields
                    if is_required and not value:
                        # Distinguish absent (key not present in the
                        # onboarding row at all) from present-but-empty
                        # (``""``) so the user knows whether to ADD the
                        # key or FILL IN a value \u2014 different fixes
                        # entirely. ``row_dict.get(field)`` alone can't
                        # tell those apart (both return falsy), so peek
                        # at ``in row_dict`` explicitly.
                        purpose, example = field_hints.get(
                            field,
                            (
                                f"the {field} required for {layer} "
                                f"onboarding",
                                f"{field}: <value>",
                            ),
                        )
                        if field not in row_dict or value is None:
                            state = (
                                "required key is missing from this "
                                "onboarding row"
                            )
                        else:
                            state = (
                                "required key is present but empty "
                                f"(value={value!r})"
                            )
                        errors.append(
                            f"flow {flow_id} {field}: {state}. "
                            f"This field is {purpose}. Add e.g. "
                            f"`{example}` to the row and re-run."
                        )
                        continue
                    if not value:
                        continue
                    _check(
                        validator,
                        value,
                        kind=f"flow {flow_id} {field}",
                        **kwargs,
                    )

                # scd_type lives inside the cdc_apply_changes /
                # apply_changes_from_snapshot blocks; bad values silently
                # flow into ``dlt.apply_changes(stored_as_scd_type=...)``
                # without this check. The column-name fields in those
                # blocks (keys, sequence_by, column_list, etc.) drive
                # DLT's apply_changes / apply_changes_from_snapshot
                # column-projection logic, so a hyphenated entry there
                # fails at DLT runtime — pre-flight catches it here.
                #
                # ``apply_as_deletes`` / ``apply_as_truncates`` / ``where``
                # are deliberately NOT validated: they are SQL expressions
                # that go through ``expr(...)``, not column names, so
                # regular-identifier rules do not apply. The two field
                # tuples consumed below live at module scope (above the
                # class) so we don't rebuild them on every (row x layer)
                # iteration.
                cdc_blocks = (
                    (f"{layer}_cdc_apply_changes", _CDC_COL_FIELDS),
                    (f"{layer}_apply_changes_from_snapshot", _SNAPSHOT_COL_FIELDS),
                )
                for cdc_field, col_fields in cdc_blocks:
                    cdc_block = row_dict.get(cdc_field)
                    if not isinstance(cdc_block, dict):
                        continue
                    if cdc_block.get("scd_type") is not None:
                        _check(
                            validate_scd_type,
                            cdc_block["scd_type"],
                            kind=f"flow {flow_id} {cdc_field}.scd_type",
                        )
                    # sequence_by gets its own lenient check (CSV +
                    # dotted refs) instead of the strict identifier
                    # rules -- see validate_sequence_by.
                    if cdc_block.get("sequence_by"):
                        _check(
                            validate_sequence_by,
                            cdc_block["sequence_by"],
                            kind=f"flow {flow_id} {cdc_field}.sequence_by",
                        )
                    for col_field in col_fields:
                        if cdc_block.get(col_field):
                            _check(
                                validate_uc_column_list,
                                cdc_block[col_field],
                                kind=f"flow {flow_id} {cdc_field}.{col_field}",
                            )

                # Each append flow has its own source_format which drives
                # a separate read path; validate every one.
                append_flows = row_dict.get(f"{layer}_append_flows")
                if isinstance(append_flows, list):
                    for af_idx, af in enumerate(append_flows):
                        if isinstance(af, dict) and af.get("source_format"):
                            _check(
                                validate_source_format,
                                af["source_format"],
                                kind=(
                                    f"flow {flow_id} {layer}_append_flows[{af_idx}]"
                                    f".source_format"
                                ),
                            )

                # Multi-source AUTO CDC (issue #294). Validates:
                #   * Mutual exclusion against ``<layer>_cdc_apply_changes``.
                #   * Group-level mandatory column-name fields (``keys``,
                #     ``sequence_by``, ``scd_type``) — same rules as the
                #     single-flow ``<layer>_cdc_apply_changes`` block.
                #   * Per-flow mandatory ``name`` / ``source_format`` /
                #     ``source_details``, supported per-layer source format,
                #     and uniqueness of ``flow.name`` within the group.
                cdc_flows_field = f"{layer}_cdc_apply_changes_flows"
                cdc_flows_block = row_dict.get(cdc_flows_field)
                if isinstance(cdc_flows_block, dict):
                    legacy_cdc_block = row_dict.get(f"{layer}_cdc_apply_changes")
                    if isinstance(legacy_cdc_block, dict):
                        errors.append(
                            f"flow {flow_id} {cdc_flows_field}: both "
                            f"{layer}_cdc_apply_changes and {cdc_flows_field} "
                            f"are set; use one or the other"
                        )
                    if cdc_flows_block.get("scd_type") is not None:
                        _check(
                            validate_scd_type,
                            cdc_flows_block["scd_type"],
                            kind=f"flow {flow_id} {cdc_flows_field}.scd_type",
                        )
                    if cdc_flows_block.get("sequence_by"):
                        _check(
                            validate_sequence_by,
                            cdc_flows_block["sequence_by"],
                            kind=f"flow {flow_id} {cdc_flows_field}.sequence_by",
                        )
                    for col_field in _CDC_COL_FIELDS:
                        if cdc_flows_block.get(col_field):
                            _check(
                                validate_uc_column_list,
                                cdc_flows_block[col_field],
                                kind=(
                                    f"flow {flow_id} {cdc_flows_field}."
                                    f"{col_field}"
                                ),
                            )
                    raw_flows = cdc_flows_block.get("flows")
                    if not isinstance(raw_flows, list) or len(raw_flows) == 0:
                        errors.append(
                            f"flow {flow_id} {cdc_flows_field}.flows must "
                            f"be a non-empty list"
                        )
                    else:
                        # Per-layer allowed source formats — silver flows
                        # always read from Delta upstream, so we hard-cap
                        # silver to {"delta"}. Bronze gets the full
                        # streaming-CDC-supported set; ``snapshot`` is
                        # excluded because snapshot CDC uses a different
                        # DLT primitive (``create_auto_cdc_from_snapshot_flow``).
                        allowed_formats = (
                            {"delta"}
                            if layer == "silver"
                            else {"cloudfiles", "delta", "kafka", "eventhub"}
                        )
                        seen_names = set()
                        for cf_idx, cf in enumerate(raw_flows):
                            if not isinstance(cf, dict):
                                errors.append(
                                    f"flow {flow_id} {cdc_flows_field}."
                                    f"flows[{cf_idx}] must be an object"
                                )
                                continue
                            for mandatory in (
                                "name",
                                "source_format",
                                "source_details",
                            ):
                                if not cf.get(mandatory):
                                    errors.append(
                                        f"flow {flow_id} {cdc_flows_field}."
                                        f"flows[{cf_idx}].{mandatory} is "
                                        f"required"
                                    )
                            name = cf.get("name")
                            if name:
                                if name in seen_names:
                                    errors.append(
                                        f"flow {flow_id} {cdc_flows_field}: "
                                        f"duplicate flow name {name!r}; "
                                        f"each flow name must be unique "
                                        f"within a group"
                                    )
                                seen_names.add(name)
                            sf = cf.get("source_format")
                            if isinstance(sf, str) and sf.lower() not in allowed_formats:
                                errors.append(
                                    f"flow {flow_id} {cdc_flows_field}."
                                    f"flows[{cf_idx}].source_format={sf!r} "
                                    f"is not supported for {layer}; "
                                    f"allowed: {sorted(allowed_formats)}"
                                )

        if errors:
            # Group errors by data_flow_id so all violations for the
            # same row cluster together in the report \u2014 users fixing
            # a spec file work row-by-row, and interleaved errors are
            # much harder to reason about than a "flow 100 has 3
            # errors, flow 102 has 2" report. Sort key extracts the
            # ``flow <id>`` prefix present on every message; entries
            # that don't match (defensive fallback) sort to the end.
            def _flow_key(msg):
                # ``flow <id> <field>: ...`` \u2014 the prefix is stable across
                # every validator error emitted from this method.
                m = _FLOW_PREFIX_RE.match(msg)
                if m:
                    return (0, m.group(1), msg)
                return (1, "", msg)
            errors.sort(key=_flow_key)

            affected_flows = []
            seen_flows = set()
            for msg in errors:
                m = _FLOW_PREFIX_RE.match(msg)
                if m and m.group(1) not in seen_flows:
                    seen_flows.add(m.group(1))
                    affected_flows.append(m.group(1))

            layers_label = "/".join(sorted(to_check))
            flows_label = (
                ", ".join(affected_flows) if affected_flows else "<unknown>"
            )
            bullets = "\n  - ".join(errors)
            raise ValueError(
                f"Onboarding file "
                f"{self.dict_obj['onboarding_file_path']!r} has "
                f"{len(errors)} pre-flight validation error(s) across "
                f"{len(affected_flows) or 1} flow(s) [{flows_label}] "
                f"for layer(s) [{layers_label}]. Each bullet below "
                f"names the flow, the field, why it failed, and "
                f"(where applicable) how to fix it. Fix all "
                f"{len(errors)} and re-run:\n  - {bullets}"
            )

        # Only mark layers as validated AFTER the walk completes without
        # raising \u2014 otherwise a retry after fixing errors would be
        # silently skipped by the idempotency guard.
        self._prevalidated_layers = already | to_check

    def register_bronze_dataflow_spec_tables(self):
        """Register bronze/silver dataflow specs tables."""
        self.deltaPipelinesMetaStoreOps.create_database(
            self.dict_obj["database"], "sdp-meta database"
        )
        self.deltaPipelinesMetaStoreOps.register_table_in_metastore(
            self.dict_obj["database"],
            self.dict_obj["bronze_dataflowspec_table"],
            self.dict_obj["bronze_dataflowspec_path"],
        )
        logger.info(
            f"""onboarded bronze table={self.dict_obj["database"]}.{self.dict_obj["bronze_dataflowspec_table"]}"""
        )
        self.spark.read.table(
            f"""{self.dict_obj["database"]}.{self.dict_obj["bronze_dataflowspec_table"]}"""
        ).show()

    def register_silver_dataflow_spec_tables(self):
        """Register bronze dataflow specs tables."""
        self.deltaPipelinesMetaStoreOps.create_database(
            self.dict_obj["database"], "sdp-meta database"
        )
        self.deltaPipelinesMetaStoreOps.register_table_in_metastore(
            self.dict_obj["database"],
            self.dict_obj["silver_dataflowspec_table"],
            self.dict_obj["silver_dataflowspec_path"],
        )
        logger.info(
            f"""onboarded silver table={self.dict_obj["database"]}.{self.dict_obj["silver_dataflowspec_table"]}"""
        )
        self.spark.read.table(
            f"""{self.dict_obj["database"]}.{self.dict_obj["silver_dataflowspec_table"]}"""
        ).show()

    def onboard_silver_dataflow_spec(self):
        """
        Onboard silver dataflow spec.

        Args:
            onboarding_df (pyspark.sql.DataFrame): DataFrame containing the onboarding file data.
            dict_obj (dict): Dictionary containing the required attributes for onboarding silver dataflow spec.
                Required attributes:
                    - onboarding_file_path (str): Path of the onboarding file.
                    - database (str): Name of the database.
                    - env (str): Environment name.
                    - silver_dataflowspec_table (str): Name of the silver dataflow spec table.
                    - silver_dataflowspec_path (str): Path of the silver dataflow spec file. if uc_enabled is False
                    - import_author (str): Name of the import author.
                    - version (str): Version of the dataflow spec.
                    - overwrite (str): Whether to overwrite the existing dataflow spec table/file or not.
        """
        attributes = [
            "onboarding_file_path",
            "database",
            "env",
            "silver_dataflowspec_table",
            "import_author",
            "version",
            "overwrite",
        ]
        dict_obj = self.silver_dict_obj
        if self.uc_enabled:
            self.__validate_dict_attributes(attributes, dict_obj)
        else:
            attributes.append("silver_dataflowspec_path")
            self.__validate_dict_attributes(attributes, dict_obj)

        # Pre-flight UC-identifier validation \u2014 walk the onboarding file
        # once for the silver layer and aggregate every violation into a
        # single ValueError before any Spark side-effect. Idempotent:
        # a no-op if ``onboard_dataflow_specs`` already validated silver
        # up front (issue #343 finding #1).
        self.__pre_validate_onboarding_uc_names(layers=("silver",))

        onboarding_df = self.__get_onboarding_file_dataframe(
            dict_obj["onboarding_file_path"]
        )
        silver_data_flow_spec_df = self.__get_silver_dataflow_spec_dataframe(
            onboarding_df, dict_obj["env"]
        )
        columns = StructType(
            [
                StructField("select_exp", ArrayType(StringType(), True), True),
                StructField(
                    "target_partition_cols", ArrayType(StringType(), True), True
                ),
                StructField("target_table", StringType(), True),
                StructField("where_clause", ArrayType(StringType(), True), True),
            ]
        )

        env = dict_obj["env"]
        silver_transformation_file_col = f"silver_transformation_json_{env}"
        # When EVERY row in the onboarding file is multi-source AUTO CDC
        # (issue #294), no row defines ``silver_transformation_json_<env>``
        # and the inferred Spark schema omits the column entirely. Skip
        # the file collection in that case — the LEFT join below still
        # works against an empty silver_transformation_json_df.
        if silver_transformation_file_col in onboarding_df.columns:
            silver_transformation_files = (
                onboarding_df.select(silver_transformation_file_col)
                .dropDuplicates()
                .collect()
            )
        else:
            silver_transformation_files = []

        schema_field_names = [field.name for field in columns.fields]
        silver_transformation_rows = []
        for row in silver_transformation_files:
            file_path = row[silver_transformation_file_col]
            if not file_path:
                continue
            parsed = self._load_structured_file(file_path)
            if parsed is None:
                continue
            if not isinstance(parsed, list):
                raise ValueError(
                    f"Silver transformations file '{file_path}' must contain a list "
                    f"of transformation entries; got {type(parsed).__name__}"
                )
            for entry in parsed:
                if not isinstance(entry, dict):
                    raise ValueError(
                        f"Silver transformations file '{file_path}' contains a "
                        f"non-mapping entry: {entry!r}"
                    )
                silver_transformation_rows.append(
                    {name: entry.get(name) for name in schema_field_names}
                )

        silver_transformation_json_df = self.spark.createDataFrame(
            data=silver_transformation_rows, schema=columns
        )

        logger.info(f"Loaded {len(silver_transformation_rows)} silver transformation rows")

        # Left join from the silver spec side so rows that legitimately
        # have no entry in the silver-transformations JSON (multi-source
        # AUTO CDC, issue #294 — transformations come from per-flow
        # ``select_exp`` inside ``cdcApplyChangesFlows``) survive the
        # join with NULL ``select_exp`` / ``where_clause``. The runtime
        # ignores those two fields when ``cdcApplyChangesFlows`` is set.
        silver_data_flow_spec_df = silver_data_flow_spec_df.join(
            silver_transformation_json_df,
            silver_data_flow_spec_df.targetDetails["table"]
            == silver_transformation_json_df.target_table,
            how="left",
        )
        silver_dataflow_spec_df = (
            silver_data_flow_spec_df.drop("target_table")  # .drop("path")
            .drop("target_partition_cols")
            .withColumnRenamed("select_exp", "selectExp")
            .withColumnRenamed("where_clause", "whereClause")
        )

        silver_dataflow_spec_df = self.__add_audit_columns(
            silver_dataflow_spec_df,
            {
                "import_author": dict_obj["import_author"],
                "version": dict_obj["version"],
            },
        )

        silver_fields = [field.name for field in dataclasses.fields(SilverDataflowSpec)]
        silver_dataflow_spec_df = silver_dataflow_spec_df.select(silver_fields)
        database = dict_obj["database"]
        table = dict_obj["silver_dataflowspec_table"]

        if dict_obj["overwrite"] == "True":
            if self.uc_enabled:
                (
                    silver_dataflow_spec_df.write.format("delta")
                    .mode("overwrite")
                    .option("mergeSchema", "true")
                    .saveAsTable(f"{database}.{table}")
                )
            else:
                silver_dataflow_spec_df.write.mode("overwrite").format("delta").option(
                    "mergeSchema", "true"
                ).save(dict_obj["silver_dataflowspec_path"])
        else:
            if self.uc_enabled:
                original_dataflow_df = self.spark.read.format("delta").table(
                    f"{database}.{table}"
                )
            else:
                self.deltaPipelinesMetaStoreOps.register_table_in_metastore(
                    database, table, dict_obj["silver_dataflowspec_path"]
                )
                original_dataflow_df = self.spark.read.format("delta").load(
                    dict_obj["silver_dataflowspec_path"]
                )
            logger.info("In Merge block for Silver")
            self.deltaPipelinesInternalTableOps.merge(
                silver_dataflow_spec_df,
                f"{database}.{table}",
                ["dataFlowId"],
                original_dataflow_df.columns,
            )
        if not self.uc_enabled:
            self.register_silver_dataflow_spec_tables()

    def onboard_bronze_dataflow_spec(self):
        """
        Onboard bronze dataflow spec.

        This function reads the onboarding file and creates bronze dataflow spec. It adds audit columns to the dataframe
        If overwrite is True, it overwrites the table or file with the new dataframe. If overwrite is False,
        it merges the new dataframe with the existing dataframe.
        dict_obj (dict): Dictionary containing the required attributes for onboarding bronze dataflow spec.
            Required attributes:
                - onboarding_file_path (str): Path of the onboarding file.
                - database (str): Name of the database.
                - env (str): Environment name.
                - bronze_dataflowspec_table (str): Name of the bronze dataflow spec table.
                - bronze_dataflowspec_path (str): Path of the bronze dataflow spec file. if uc_enabled is False
                - import_author (str): Name of the import author.
                - version (str): Version of the dataflow spec.
                - overwrite (str): Whether to overwrite the existing dataflow spec table/file or not.

        Args:
            None

        Returns:
            None
        """
        attributes = [
            "onboarding_file_path",
            "database",
            "env",
            "bronze_dataflowspec_table",
            "import_author",
            "version",
            "overwrite",
        ]
        dict_obj = self.bronze_dict_obj
        if self.uc_enabled:
            self.__validate_dict_attributes(attributes, dict_obj)
        else:
            attributes.append("bronze_dataflowspec_path")
            self.__validate_dict_attributes(attributes, dict_obj)

        # Pre-flight UC-identifier validation \u2014 walk the onboarding file
        # once for the bronze layer and aggregate every violation into a
        # single ValueError before any Spark side-effect. Idempotent:
        # a no-op if ``onboard_dataflow_specs`` already validated bronze
        # up front (issue #343 finding #1).
        self.__pre_validate_onboarding_uc_names(layers=("bronze",))

        onboarding_df = self.__get_onboarding_file_dataframe(
            dict_obj["onboarding_file_path"]
        )

        bronze_dataflow_spec_df = self.__get_bronze_dataflow_spec_dataframe(
            onboarding_df, dict_obj["env"]
        )

        bronze_dataflow_spec_df = self.__add_audit_columns(
            bronze_dataflow_spec_df,
            {
                "import_author": dict_obj["import_author"],
                "version": dict_obj["version"],
            },
        )
        bronze_fields = [field.name for field in dataclasses.fields(BronzeDataflowSpec)]
        bronze_dataflow_spec_df = bronze_dataflow_spec_df.select(bronze_fields)
        database = dict_obj["database"]
        table = dict_obj["bronze_dataflowspec_table"]
        if dict_obj["overwrite"] == "True":
            if self.uc_enabled:
                (
                    bronze_dataflow_spec_df.write.format("delta")
                    .mode("overwrite")
                    .option("mergeSchema", "true")
                    .saveAsTable(f"{database}.{table}")
                )
            else:
                (
                    bronze_dataflow_spec_df.write.mode("overwrite")
                    .format("delta")
                    .option("mergeSchema", "true")
                    .save(path=dict_obj["bronze_dataflowspec_path"])
                )
        else:
            if self.uc_enabled:
                original_dataflow_df = self.spark.read.format("delta").table(
                    f"{database}.{table}"
                )
            else:
                self.deltaPipelinesMetaStoreOps.register_table_in_metastore(
                    database, table, dict_obj["bronze_dataflowspec_path"]
                )
                original_dataflow_df = self.spark.read.format("delta").load(
                    dict_obj["bronze_dataflowspec_path"]
                )

            logger.info("In Merge block for Bronze")
            self.deltaPipelinesInternalTableOps.merge(
                bronze_dataflow_spec_df,
                f"{database}.{table}",
                ["dataFlowId"],
                original_dataflow_df.columns,
            )
        if not self.uc_enabled:
            self.register_bronze_dataflow_spec_tables()

    def __delete_none(self, _dict):
        """Delete None values recursively from all of the dictionaries"""
        filtered = {k: v for k, v in _dict.items() if v is not None}
        _dict.clear()
        _dict.update(filtered)
        return _dict

    def _load_structured_file(self, file_path):
        """Load a JSON or YAML file via Spark and return the parsed Python object.

        Supports ``.json``, ``.yml``, and ``.yaml`` extensions (case-insensitive).
        Reads via ``spark.read.text`` so cloud paths (dbfs, volumes, s3, abfss)
        work the same way they do for the existing JSON code paths.

        Args:
            file_path: Path to a structured config file. ``None`` or empty
                returns ``None``.

        Returns:
            Parsed Python object (typically ``dict`` or ``list``), or ``None``
            if ``file_path`` is falsy.

        Raises:
            ValueError: If the file extension is unsupported, the file is empty,
                or the contents cannot be parsed.
        """
        if not file_path:
            return None
        lower_path = file_path.lower()
        if not lower_path.endswith((".json", ".yml", ".yaml")):
            raise ValueError(
                f"Unsupported file format for '{file_path}'. "
                "Expected one of: .json, .yml, .yaml"
            )

        # Wrap Spark IO errors (AnalysisException for missing paths, etc.)
        # so callers can rely on a single ValueError contract regardless of the
        # backing reader's exception class.
        try:
            rows = self.spark.read.text(file_path, wholetext=True).collect()
        except Exception as e:
            raise ValueError(
                f"Failed to read '{file_path}': {e}"
            ) from e
        if not rows or not rows[0]["value"]:
            raise ValueError(f"File '{file_path}' is empty or unreadable")
        text = rows[0]["value"]

        try:
            if lower_path.endswith((".yml", ".yaml")):
                return yaml.safe_load(text)
            return json.loads(text)
        except (yaml.YAMLError, json.JSONDecodeError) as e:
            raise ValueError(f"Failed to parse '{file_path}': {e}") from e

    def convert_yml_to_json(self, onboarding_file_path):
        """Convert a YAML onboarding file into a JSON file Spark can read.

        Reads the YAML at ``onboarding_file_path`` via
        :py:meth:`_load_structured_file` (which uses ``spark.read.text``, so
        cloud paths like ``/Volumes/...``, ``dbfs:/...``, ``s3://...``,
        ``abfss://...`` all work as *input*), serializes it to JSON, and
        writes the result somewhere ``spark.read.json`` will be able to find
        from any compute type (classic, serverless / Spark Connect).

        Write strategy (in order):

        1. **Sibling on the same FUSE-accessible filesystem** — preferred when
           the source path is on a UC Volume (``/Volumes/...``), DBFS FUSE
           (``/dbfs/...``), or local disk. The converted file is written as a
           regular sibling (``<basename>_yml_converted.json``) so Spark reads
           it back through the same filesystem as the source.

           This is what makes serverless work: a bare ``/tmp/...`` path is
           auto-prefixed with ``dbfs:`` by Spark on serverless compute, so a
           file created via ``tempfile.mkdtemp`` becomes ``PATH_NOT_FOUND``.
           A non-hidden filename (no leading ``.`` or ``_``) is required so
           Spark's input-format listing does not skip it.
        2. **Driver-local temp + ``file://`` prefix** — fallback used when
           writing next to the source fails (e.g. the YAML lives on
           ``s3://``/``abfss://`` and the executor sandbox cannot mount it
           for write). The ``file://`` scheme forces Spark to read from the
           driver's local filesystem instead of DBFS.

        Args:
            onboarding_file_path: Path to YAML onboarding file.

        Returns:
            str: Path to the JSON file in a form ``spark.read.json`` can
            consume — either a same-scheme sibling path or a ``file://`` URL.

        Raises:
            ValueError: If the file cannot be read, is empty, or contains
                invalid YAML.
        """
        yaml_data = self._load_structured_file(onboarding_file_path)
        if yaml_data is None:
            raise ValueError(
                f"YAML onboarding file '{onboarding_file_path}' is empty "
                "or could not be parsed"
            )

        base_name = os.path.splitext(os.path.basename(onboarding_file_path))[0]
        parent_dir = os.path.dirname(onboarding_file_path)
        sibling_path = os.path.join(parent_dir, f"{base_name}_yml_converted.json")

        try:
            with open(sibling_path, "w") as json_file:
                json.dump(yaml_data, json_file, indent=4)
            return sibling_path
        except OSError:
            tmp_dir = tempfile.mkdtemp(prefix="sdp_meta_onboarding_")
            json_file_path = os.path.join(tmp_dir, f"{base_name}_yml_converted.json")
            with open(json_file_path, "w") as json_file:
                json.dump(yaml_data, json_file, indent=4)
            return f"file://{json_file_path}"

    def __get_onboarding_file_dataframe(self, onboarding_file_path):
        """Read the onboarding file (JSON or YAML) into a Spark DataFrame.

        JSON inputs are passed straight to ``spark.read.json``. YAML inputs
        are first materialized to a JSON file by
        :py:meth:`convert_yml_to_json` (which writes to a sibling on the same
        filesystem as the source for serverless compatibility); see that
        method's docstring for the exact write strategy and why bare
        ``/tmp/...`` paths cannot be used on serverless / Spark Connect.

        Subsequent calls with the same path skip the eager ``show()``
        and duplicate-id check (tracked in
        ``self._onboarding_files_processed``) so ``onboard_dataflow_specs``
        doesn't print the dataframe + run the dupe groupBy three times
        (pre-flight + bronze + silver). A fresh DataFrame is still
        returned on each call -- see the field-level comment for why we
        intentionally do not cache the DataFrame object itself.
        """
        if not onboarding_file_path:
            raise Exception("Onboarding file path is empty")
        first_read = onboarding_file_path not in self._onboarding_files_processed
        lower_path = onboarding_file_path.lower()
        if not lower_path.endswith((".json", ".yml", ".yaml")):
            raise Exception(
                "Onboarding file format not supported! "
                "Please provide a .json, .yml, or .yaml file"
            )

        if lower_path.endswith(".json"):
            json_path = onboarding_file_path
        else:
            json_path = self.convert_yml_to_json(onboarding_file_path)

        onboarding_df = self.spark.read.option("multiline", "true").json(json_path)
        self.onboard_file_type = "json"

        if first_read:
            onboarding_df.show()
            onboarding_df_dupes = (
                onboarding_df.groupBy("data_flow_id").count().filter("count > 1")
            )
            if len(onboarding_df_dupes.head(1)) > 0:
                onboarding_df_dupes.show()
                raise Exception("onboarding file have duplicated data_flow_ids! ")
            self._onboarding_files_processed.add(onboarding_file_path)
        return onboarding_df

    def __add_audit_columns(self, df, dict_obj):
        """Add_audit_columns method will add AuditColumns like version, dates, author.

        Args:
            df ([type]): [description]
            dict_obj ([type]): attributes = ["import_author", "version"]

        Returns:
            [type]: attributes = ["import_author", "version"]
        """
        attributes = ["import_author", "version"]
        self.__validate_dict_attributes(attributes, dict_obj)

        df = (
            df.withColumn("version", f.lit(dict_obj["version"]))
            .withColumn("createDate", f.current_timestamp())
            .withColumn("createdBy", f.lit(dict_obj["import_author"]))
            .withColumn("updateDate", f.current_timestamp())
            .withColumn("updatedBy", f.lit(dict_obj["import_author"]))
        )
        return df

    def __get_bronze_schema(self, metadata_file):
        """Get schema from metadafile in json format.

        Args:
            metadata_file ([string]): metadata schema file path
        """
        ddlSchemaStr = self.spark.read.text(
            paths=metadata_file, wholetext=True
        ).collect()[0]["value"]
        spark_schema = T._parse_datatype_string(ddlSchemaStr)
        logger.info(spark_schema)
        schema = json.dumps(spark_schema.jsonValue())
        return schema

    def __validate_mandatory_fields(self, onboarding_row, mandatory_fields):
        for field in mandatory_fields:
            if not onboarding_row[field]:
                raise Exception(f"Missing field={field} in onboarding_row")

    def __get_bronze_dataflow_spec_dataframe(self, onboarding_df, env):
        """Get bronze dataflow spec method will convert onboarding dataframe to Bronze Dataflowspec dataframe.

        Args:
            onboarding_df ([type]): [description]
            spark (SparkSession): [description]

        Returns:
            [type]: [description]
        """
        data_flow_spec_columns = [
            "dataFlowId",
            "dataFlowGroup",
            "sourceFormat",
            "sourceDetails",
            "readerConfigOptions",
            "targetFormat",
            "targetDetails",
            "tableProperties",
            "schema",
            "partitionColumns",
            "cdcApplyChanges",
            "applyChangesFromSnapshot",
            "dataQualityExpectations",
            "quarantineTargetDetails",
            "quarantineTableProperties",
            "appendFlows",
            "appendFlowsSchemas",
            "sinks",
            "clusterBy",
            "clusterByAuto",
            # Multi-source AUTO CDC (issue #294). Bronze carries both
            # the JSON-encoded CDCApplyChangesFlowGroup AND a per-flow
            # schema map so cloudFiles/kafka flows can declare their own
            # source_schema_path the same way append flows do.
            "cdcApplyChangesFlows",
            "cdcApplyChangesFlowsSchemas",
            # UC row-level security (issue #303). Both fields are optional
            # and silently dropped on non-UC pipelines.
            "rowFilter",
            "quarantineRowFilter",
            # UC column-level governance. JSON-object strings keyed by column
            # name; optional. Masks are dropped on non-UC pipelines.
            "columnComments",
            "columnMasks",
        ]
        data_flow_spec_schema = StructType(
            [
                StructField("dataFlowId", StringType(), True),
                StructField("dataFlowGroup", StringType(), True),
                StructField("sourceFormat", StringType(), True),
                StructField(
                    "sourceDetails", MapType(StringType(), StringType(), True), True
                ),
                StructField(
                    "readerConfigOptions",
                    MapType(StringType(), StringType(), True),
                    True,
                ),
                StructField("targetFormat", StringType(), True),
                StructField(
                    "targetDetails", MapType(StringType(), StringType(), True), True
                ),
                StructField(
                    "tableProperties", MapType(StringType(), StringType(), True), True
                ),
                StructField("schema", StringType(), True),
                StructField("partitionColumns", ArrayType(StringType(), True), True),
                StructField("cdcApplyChanges", StringType(), True),
                StructField("applyChangesFromSnapshot", StringType(), True),
                StructField("dataQualityExpectations", StringType(), True),
                StructField(
                    "quarantineTargetDetails",
                    MapType(StringType(), StringType(), True),
                    True,
                ),
                StructField(
                    "quarantineTableProperties",
                    MapType(StringType(), StringType(), True),
                    True,
                ),
                StructField("appendFlows", StringType(), True),
                StructField("appendFlowsSchemas", MapType(StringType(), StringType(), True), True),
                StructField("sinks", StringType(), True),
                StructField("clusterBy", ArrayType(StringType(), True), True),
                StructField("clusterByAuto", T.BooleanType(), True),
                StructField("cdcApplyChangesFlows", StringType(), True),
                StructField(
                    "cdcApplyChangesFlowsSchemas",
                    MapType(StringType(), StringType(), True),
                    True,
                ),
                StructField("rowFilter", StringType(), True),
                StructField("quarantineRowFilter", StringType(), True),
                StructField("columnComments", StringType(), True),
                StructField("columnMasks", StringType(), True),
            ]
        )
        data = []
        onboarding_rows = onboarding_df.collect()
        mandatory_fields = [
            "data_flow_id",
            "data_flow_group",
            "source_details",
            f"bronze_database_{env}",
            "bronze_table"
            # "bronze_reader_options",
        ]  # , f"bronze_table_path_{env}"
        for onboarding_row in onboarding_rows:
            # Multi-source AUTO CDC (issue #294): an onboarding file may
            # contain a mix of bronze rows AND a separate silver-only
            # row (no bronze fields) that merges them via
            # ``silver_cdc_apply_changes_flows``. Skip rows that don't
            # define a bronze target so the silver-only row doesn't
            # trip the bronze mandatory-field check (especially
            # ``source_details`` and ``bronze_database_<env>``).
            bronze_db_field = f"bronze_database_{env}"
            if not (
                bronze_db_field in onboarding_row
                and onboarding_row[bronze_db_field]
            ):
                continue
            # Silver fanout: a single onboarding file can also contain
            # fanout consumer rows that REFERENCE an existing bronze
            # table (``bronze_database_<env>`` + ``bronze_table`` set)
            # but don't produce one (no ``source_details`` /
            # ``source_format``). Those rows are silver-only at the
            # bronze pass \u2014 the silver pass picks them up and reads
            # from the bronze produced by an earlier row in the same
            # file. Without this skip, validators would trip on the
            # missing ``source_details`` and force users into a
            # two-stage onboarding orchestration (e.g. the historical
            # ``launch_silver_fanout_demo.py`` chained
            # bronze_silver-then-silver-overwrite=False pattern).
            if not (
                "source_details" in onboarding_row
                and onboarding_row["source_details"]
            ):
                continue
            try:
                self.__validate_mandatory_fields(onboarding_row, mandatory_fields)
            except ValueError:
                mandatory_fields.append(f"bronze_table_path_{env}")
                self.__validate_mandatory_fields(onboarding_row, mandatory_fields)
            self.__validate_row_uc_names(onboarding_row, env, "bronze")
            bronze_data_flow_spec_id = onboarding_row["data_flow_id"]
            bronze_data_flow_spec_group = onboarding_row["data_flow_group"]
            if "source_format" not in onboarding_row:
                raise Exception(f"Source format not provided for row={onboarding_row}")

            source_format = onboarding_row["source_format"]
            if source_format.lower() not in _SUPPORTED_SOURCE_FORMATS_LOWER:
                raise Exception(
                    f"Source format {source_format} not supported in SDP-META! row={onboarding_row}"
                )
            # v0.0.10 accepted any case here (``.lower()`` check), but
            # the read dispatch in dataflow_pipeline.py compares exactly
            # (``== "cloudFiles"``). Persist the canonical spelling so a
            # v0.0.10-era case variant is healed instead of stored and
            # failing later at pipeline runtime (issue #370 class).
            source_format = validate_source_format(source_format)
            source_details, bronze_reader_config_options, schema = (
                self.get_bronze_source_details_reader_options_schema(
                    onboarding_row, env
                )
            )
            bronze_target_format = "delta"
            bronze_target_details = {
                "database": onboarding_row["bronze_database_{}".format(env)],
                "table": onboarding_row["bronze_table"],
            }
            bronze_cl = (
                onboarding_row["bronze_catalog_{}".format(env)]
                if "bronze_catalog_{}".format(env) in onboarding_row
                else None
            )
            if "bronze_table_comment" in onboarding_row:
                bronze_target_details["comment"] = onboarding_row["bronze_table_comment"]

            if bronze_cl:
                bronze_target_details["catalog"] = bronze_cl
            if not self.uc_enabled:
                if f"bronze_table_path_{env}" in onboarding_row:
                    bronze_target_details["path"] = onboarding_row[f"bronze_table_path_{env}"]
                else:
                    raise Exception(f"bronze_table_path_{env} not provided in onboarding_row={onboarding_row}")
            bronze_table_properties = {}
            if (
                "bronze_table_properties" in onboarding_row
                and onboarding_row["bronze_table_properties"]
            ):
                bronze_table_properties = self.__delete_none(
                    onboarding_row["bronze_table_properties"].asDict()
                )

            partition_columns = [""]
            if (
                "bronze_partition_columns" in onboarding_row
                and onboarding_row["bronze_partition_columns"]
            ):
                # Split if this is a list separated by commas
                if "," in onboarding_row["bronze_partition_columns"]:
                    partition_columns = onboarding_row["bronze_partition_columns"].split(",")
                else:
                    partition_columns = [onboarding_row["bronze_partition_columns"]]

            dlt_sinks = None
            if "bronze_sinks" in onboarding_row and onboarding_row["bronze_sinks"]:
                dlt_sinks = self.get_sink_details(onboarding_row, "bronze")
            cluster_by = self.__get_cluster_by_properties(onboarding_row, bronze_table_properties,
                                                          "bronze_cluster_by")
            cluster_by_auto = self.__get_cluster_by_auto(onboarding_row, "bronze_cluster_by_auto")

            cdc_apply_changes = None
            if (
                "bronze_cdc_apply_changes" in onboarding_row
                and onboarding_row["bronze_cdc_apply_changes"]
            ):
                self.__validate_apply_changes(onboarding_row, "bronze")
                # v0.0.10 onboarding files carried scd_type as int
                # (issue #370); persist the canonical string form.
                cdc_apply_changes = json.dumps(
                    _coerce_scd_type_to_str(
                        self.__delete_none(
                            onboarding_row["bronze_cdc_apply_changes"].asDict()
                        )
                    )
                )
            apply_changes_from_snapshot = None
            if ("bronze_apply_changes_from_snapshot" in onboarding_row
                    and onboarding_row["bronze_apply_changes_from_snapshot"]):
                self.__validate_apply_changes_from_snapshot(onboarding_row, "bronze")
                apply_changes_from_snapshot = json.dumps(
                    _coerce_scd_type_to_str(
                        self.__delete_none(onboarding_row["bronze_apply_changes_from_snapshot"].asDict())
                    )
                )
            data_quality_expectations = None
            quarantine_target_details = {}
            quarantine_table_properties = {}
            if f"bronze_data_quality_expectations_json_{env}" in onboarding_row:
                bronze_data_quality_expectations_json = onboarding_row[
                    f"bronze_data_quality_expectations_json_{env}"
                ]
                if bronze_data_quality_expectations_json:
                    data_quality_expectations = self.__get_data_quality_expecations(
                        bronze_data_quality_expectations_json
                    )
                    if onboarding_row["bronze_quarantine_table"]:
                        quarantine_target_details, quarantine_table_properties = self.__get_quarantine_details(
                            env, "bronze", onboarding_row
                        )

            append_flows, append_flows_schemas = self.get_append_flows_json(
                onboarding_row, "bronze", env
            )
            cdc_apply_changes_flows, cdc_apply_changes_flows_schemas = (
                self.get_cdc_apply_changes_flows_json(onboarding_row, "bronze", env)
            )
            bronze_row_filter = (
                onboarding_row["bronze_row_filter"]
                if "bronze_row_filter" in onboarding_row and onboarding_row["bronze_row_filter"]
                else None
            )
            bronze_quarantine_row_filter = (
                onboarding_row["bronze_quarantine_row_filter"]
                if (
                    "bronze_quarantine_row_filter" in onboarding_row
                    and onboarding_row["bronze_quarantine_row_filter"]
                )
                else None
            )
            bronze_column_comments = self.__get_column_policy_json(
                onboarding_row, "bronze_column_comments"
            )
            bronze_column_masks = self.__get_column_policy_json(
                onboarding_row, "bronze_column_masks", cleaner=validate_column_masks
            )
            bronze_row = (
                bronze_data_flow_spec_id,
                bronze_data_flow_spec_group,
                source_format,
                source_details,
                bronze_reader_config_options,
                bronze_target_format,
                bronze_target_details,
                bronze_table_properties,
                schema,
                partition_columns,
                cdc_apply_changes,
                apply_changes_from_snapshot,
                data_quality_expectations,
                quarantine_target_details,
                quarantine_table_properties,
                append_flows,
                append_flows_schemas,
                dlt_sinks,
                cluster_by,
                cluster_by_auto,
                cdc_apply_changes_flows,
                cdc_apply_changes_flows_schemas,
                bronze_row_filter,
                bronze_quarantine_row_filter,
                bronze_column_comments,
                bronze_column_masks,
            )
            data.append(bronze_row)
            # logger.info(bronze_parition_columns)

        data_flow_spec_rows_df = self.spark.createDataFrame(
            data, data_flow_spec_schema
        ).toDF(*data_flow_spec_columns)

        return data_flow_spec_rows_df

    def __get_column_policy_json(self, onboarding_row, key, cleaner=None):
        """Return the ``*_column_comments`` / ``*_column_masks`` value as a
        JSON string, or ``None`` when absent/empty.

        The onboarding file carries these as JSON objects keyed by column
        name; the dataflowspec table stores them as a ``StringType`` JSON
        string (like ``dataQualityExpectations``). ``spark.read.json`` infers
        a *unified* struct across all rows, so a row that didn't set this key
        still surfaces a struct whose fields (contributed by other rows) are
        all ``None`` — those phantom keys are dropped here so each row keeps
        only its own entries. A plain JSON string is passed through unchanged.

        When ``cleaner`` is supplied it is the field's validator
        (:func:`identifiers.validate_column_masks`), re-run here so the
        *persisted* spec matches what pre-flight accepted — in particular
        empty mask values are dropped rather than stored (they would render
        an invalid bare ``MASK`` and are safely skipped by the renderer, but
        should not be persisted). Without a cleaner, only phantom ``None``
        keys are dropped (comments keep empty values, which render a valid
        ``COMMENT ''``)."""
        if key not in onboarding_row or onboarding_row[key] is None:
            return None
        value = onboarding_row[key]
        if hasattr(value, "asDict"):
            value = value.asDict(recursive=True)
        if cleaner is not None:
            cleaned = cleaner(value)
            return json.dumps(cleaned) if cleaned else None
        if isinstance(value, dict):
            value = {k: v for k, v in value.items() if v is not None}
            return json.dumps(value) if value else None
        if isinstance(value, str):
            return value if value else None
        return json.dumps(value)

    def __parse_cluster_by_string(self, cluster_by_value, cluster_key):
        """Parse string representation of list into actual list."""

        if isinstance(cluster_by_value, list):
            return cluster_by_value

        if isinstance(cluster_by_value, str):
            # Try to parse string representation of a list
            try:
                parsed = ast.literal_eval(cluster_by_value)
                if isinstance(parsed, list):
                    return parsed
                else:
                    raise ValueError(f"Parsed value is not a list: {type(parsed).__name__}")
            except (ValueError, SyntaxError) as e:
                raise Exception(
                    f"Invalid {cluster_key}: Cannot parse string as list. "
                    f"Value: '{cluster_by_value}'. Error: {str(e)}"
                )

        raise Exception(
            f"Invalid {cluster_key}: Expected a list or string representation of list but got "
            f"{type(cluster_by_value).__name__}. Value: {cluster_by_value}"
        )

    def __get_cluster_by_properties(self, onboarding_row, table_properties, cluster_key):
        cluster_by = None
        if cluster_key in onboarding_row and onboarding_row[cluster_key]:
            if table_properties.get('pipelines.autoOptimize.zOrderCols') is not None:
                raise Exception(
                    f"Cannot support zOrder and cluster_by together at {cluster_key} "
                    f"for onboarding_row={onboarding_row}"
                )
            # Parse cluster_by value (handles both lists and string representations)
            cluster_by = self.__parse_cluster_by_string(onboarding_row[cluster_key], cluster_key)

            # Validate that each element in the list is a properly formatted string
            for i, column in enumerate(cluster_by):
                if not isinstance(column, str):
                    raise Exception(
                        f"Invalid {cluster_key}: Element at index {i} must be a string but got "
                        f"{type(column).__name__}. Value: {column}"
                    )

                # Check for common string formatting issues
                if column.strip() != column:
                    raise Exception(
                        f"Invalid {cluster_key}: Element at index {i} contains leading/trailing whitespace. "
                        f"Value: '{column}' (should be '{column.strip()}')"
                    )

                if not column.strip():
                    raise Exception(
                        f"Invalid {cluster_key}: Element at index {i} is empty or contains only whitespace. "
                        f"Value: '{column}'"
                    )

                # Check for unbalanced quotes or malformed strings
                if (column.count('"') % 2 != 0) or (column.count("'") % 2 != 0):
                    raise Exception(
                        f"Invalid {cluster_key}: Element at index {i} contains unbalanced quotes. "
                        f"Value: '{column}'"
                    )
            return cluster_by

    def __get_cluster_by_auto(self, onboarding_row, cluster_by_auto_key):
        """Get cluster_by_auto property from onboarding row."""
        # If key doesn't exist, return False
        if cluster_by_auto_key not in onboarding_row:
            return False

        value = onboarding_row[cluster_by_auto_key]

        # If explicitly set to None, return None
        if value is None:
            return None

        # Handle boolean values
        if isinstance(value, bool):
            return value

        # Handle string values
        if isinstance(value, str):
            value_lower = value.lower().strip()
            if value_lower == 'true':
                return True
            elif value_lower == 'false':
                return False
            else:
                raise Exception(
                    f"Invalid {cluster_by_auto_key}: Expected boolean or string representation of boolean "
                    f"but got '{value}'"
                )

        # Invalid type
        raise Exception(
            f"Invalid {cluster_by_auto_key}: Expected boolean or string representation of boolean "
            f"but got {type(value).__name__}: '{value}'"
        )

    def __get_quarantine_details(self, env, layer, onboarding_row):
        quarantine_table_partition_columns = ""
        quarantine_target_details = {}
        quarantine_table_properties = {}
        quarantine_table_cluster_by = None
        if (
            f"{layer}_quarantine_table_partitions" in onboarding_row
            and onboarding_row[f"{layer}_quarantine_table_partitions"]
        ):
            # Split if this is a list separated by commas
            if "," in onboarding_row[f"{layer}_quarantine_table_partitions"]:
                quarantine_table_partition_columns = onboarding_row[f"{layer}_quarantine_table_partitions"].split(",")
            else:
                quarantine_table_partition_columns = onboarding_row[f"{layer}_quarantine_table_partitions"]
        if (
            f"{layer}_quarantine_table_properties" in onboarding_row
            and onboarding_row[f"{layer}_quarantine_table_properties"]
        ):
            quarantine_table_properties = self.__delete_none(
                onboarding_row[f"{layer}_quarantine_table_properties"].asDict()
            )

        quarantine_table_cluster_by = self.__get_cluster_by_properties(onboarding_row, quarantine_table_properties,
                                                                       f"{layer}_quarantine_table_cluster_by")
        quarantine_table_cluster_by_auto = self.__get_cluster_by_auto(
            onboarding_row, f"{layer}_quarantine_table_cluster_by_auto"
        )
        if (
            f"{layer}_database_quarantine_{env}" in onboarding_row
            and onboarding_row[f"{layer}_database_quarantine_{env}"]
        ):
            quarantine_target_details = {"database": onboarding_row[f"{layer}_database_quarantine_{env}"],
                                         "table": onboarding_row[f"{layer}_quarantine_table"],
                                         "partition_columns": quarantine_table_partition_columns,
                                         "cluster_by": quarantine_table_cluster_by,
                                         "cluster_by_auto": quarantine_table_cluster_by_auto
                                         }
            quarantine_catalog = (
                onboarding_row[f"{layer}_catalog_quarantine_{env}"]
                if f"{layer}_catalog_quarantine_{env}" in onboarding_row
                else None
            )
            if quarantine_catalog:
                quarantine_target_details["catalog"] = quarantine_catalog
            if f"{layer}_quarantine_table_comment" in onboarding_row:
                quarantine_target_details["comment"] = onboarding_row[f"{layer}_quarantine_table_comment"]
        if not self.uc_enabled and f"{layer}_quarantine_table_path_{env}" in onboarding_row:
            quarantine_target_details["path"] = onboarding_row[f"{layer}_quarantine_table_path_{env}"]

        return quarantine_target_details, quarantine_table_properties

    def get_append_flows_json(self, onboarding_row, layer, env):
        append_flows = None
        append_flows_schema = {}
        if (
            f"{layer}_append_flows" in onboarding_row
            and onboarding_row[f"{layer}_append_flows"]
        ):
            self.__validate_append_flow(onboarding_row, layer)
            json_append_flows = onboarding_row[f"{layer}_append_flows"]
            from pyspark.sql.types import Row

            af_list = []
            for json_append_flow in json_append_flows:
                json_append_flow = json_append_flow.asDict()
                append_flow_map = {}
                for key in json_append_flow.keys():
                    if isinstance(json_append_flow[key], Row):
                        fs = json_append_flow[key].__fields__
                        mp = {}
                        for ff in fs:
                            if f"source_path_{env}" == ff:
                                mp["path"] = json_append_flow[key][f"{ff}"]
                            elif "source_schema_path" == ff:
                                source_schema_path = json_append_flow[key][f"{ff}"]
                                if source_schema_path:
                                    schema = self.__get_bronze_schema(
                                        source_schema_path
                                    )
                                    append_flows_schema[json_append_flow["name"]] = (
                                        schema
                                    )
                            else:
                                mp[f"{ff}"] = json_append_flow[key][f"{ff}"]
                        append_flow_map[key] = self.__delete_none(mp)
                    else:
                        append_flow_map[key] = json_append_flow[key]
                # Same canonicalization as the top-level source_format:
                # v0.0.10 onboarded case variants, but read_append_flows
                # dispatches with exact string comparison.
                if append_flow_map.get("source_format"):
                    append_flow_map["source_format"] = validate_source_format(
                        append_flow_map["source_format"],
                        kind=f"{layer}_append_flows source_format",
                    )
                af_list.append(self.__delete_none(append_flow_map))
            append_flows = json.dumps(af_list)
        return append_flows, append_flows_schema

    def get_sink_details(self, onboarding_row, layer):
        sink_details_json = onboarding_row[f"{layer}_sinks"]
        sinks_json = self.get_validated_sinks_details(sink_details_json)
        return sinks_json

    def get_cdc_apply_changes_flows_json(self, onboarding_row, layer, env):
        """Parse the multi-source AUTO CDC group (issue #294).

        Reads ``<layer>_cdc_apply_changes_flows`` from the onboarding row,
        validates it, applies the same per-flow ``source_path_{env}`` ->
        ``path`` remapping and ``source_schema_path`` schema lookup we use
        for append flows, and returns:

            (group_json_str, per_flow_schemas_map)

        ``per_flow_schemas_map`` is keyed by ``flow.name`` and only
        populated for bronze rows (silver flows always read from Delta
        upstream — they don't carry source_schema_path).

        Returns ``(None, {})`` when the row does not declare the field.
        """
        field_name = f"{layer}_cdc_apply_changes_flows"
        if field_name not in onboarding_row or not onboarding_row[field_name]:
            return None, {}

        # Reject both single-flow + multi-flow declared on the same row;
        # the runtime mutual-exclusion check repeats this defence in depth.
        legacy_field = f"{layer}_cdc_apply_changes"
        if legacy_field in onboarding_row and onboarding_row[legacy_field]:
            flow_id = (
                onboarding_row["data_flow_id"]
                if "data_flow_id" in onboarding_row
                else "<unknown>"
            )
            raise Exception(
                f"flow {flow_id}: both {legacy_field} and {field_name} are "
                f"set; use one or the other"
            )

        from pyspark.sql.types import Row

        group_row = onboarding_row[field_name]
        # ``recursive=False`` here: we only flatten the top-level group dict
        # ourselves and walk the nested ``flows`` list explicitly so we can
        # spot non-Row entries (an invalid YAML where ``flows`` is a single
        # object instead of a list) with a clear error.
        if isinstance(group_row, Row):
            group_dict = group_row.asDict()
        elif isinstance(group_row, dict):
            group_dict = dict(group_row)
        else:
            raise Exception(
                f"{field_name} must be an object on flow "
                f"{onboarding_row.get('data_flow_id', '<unknown>')}"
            )

        # Mandatory keys at group level. We use the dataflow_spec
        # constants as the canonical truth so the parser, the onboarding
        # validation, and the runtime stay aligned.
        group_keys = set(group_dict.keys())
        missing_mandatory = (
            set(DataflowSpecUtils.cdc_apply_changes_flows_group_mandatory_attributes)
            - group_keys
        )
        if missing_mandatory:
            raise Exception(
                f"mandatory missing keys= {missing_mandatory} for "
                f"{field_name} on flow "
                f"{onboarding_row.get('data_flow_id', '<unknown>')}"
            )

        raw_flows = group_dict.get("flows") or []
        if not isinstance(raw_flows, list) or len(raw_flows) == 0:
            raise Exception(
                f"{field_name}.flows must be a non-empty list on flow "
                f"{onboarding_row.get('data_flow_id', '<unknown>')}"
            )

        per_flow_schemas = {}
        out_flows = []
        seen_names = set()
        for idx, raw_flow in enumerate(raw_flows):
            if isinstance(raw_flow, Row):
                flow_dict = raw_flow.asDict()
            elif isinstance(raw_flow, dict):
                flow_dict = dict(raw_flow)
            else:
                raise Exception(
                    f"{field_name}.flows[{idx}] must be an object"
                )

            # Per-flow mandatory keys.
            flow_keys = set(flow_dict.keys())
            missing_flow_mandatory = (
                set(DataflowSpecUtils.cdc_apply_changes_flow_mandatory_attributes)
                - flow_keys
            )
            if missing_flow_mandatory:
                raise Exception(
                    f"mandatory missing keys= {missing_flow_mandatory} for "
                    f"{field_name}.flows[{idx}] on flow "
                    f"{onboarding_row.get('data_flow_id', '<unknown>')}"
                )

            # Per-flow source_format must be one the runtime knows how to
            # dispatch through ``dp.create_auto_cdc_flow``. ``snapshot`` is
            # deliberately excluded — snapshot CDC uses
            # ``create_auto_cdc_from_snapshot_flow``, a distinct DLT
            # primitive, and is out of scope for this field per design
            # (issue #294). Silver further restricts to ``delta`` because
            # silver always reads from Delta upstream.
            allowed_formats = (
                {"delta"}
                if layer == "silver"
                else {"cloudfiles", "delta", "kafka", "eventhub"}
            )
            sf = flow_dict["source_format"]
            if not isinstance(sf, str) or sf.lower() not in allowed_formats:
                raise Exception(
                    f"unsupported source_format {sf!r} in "
                    f"{field_name}.flows[{idx}] on flow "
                    f"{onboarding_row.get('data_flow_id', '<unknown>')}; "
                    f"allowed: {sorted(allowed_formats)}"
                )

            # Per-flow name uniqueness — the runtime uses ``flow.name`` as
            # the DLT view name and ``flow_name`` argument; duplicates
            # would silently collide.
            name = flow_dict["name"]
            if name in seen_names:
                raise Exception(
                    f"duplicate flow name {name!r} in {field_name} on flow "
                    f"{onboarding_row.get('data_flow_id', '<unknown>')}; "
                    f"each flow name must be unique within a group"
                )
            seen_names.add(name)

            # Normalize per-flow source_details: source_path_{env} -> path,
            # source_schema_path -> per-flow schemas map (bronze only).
            sd_raw = flow_dict["source_details"]
            if isinstance(sd_raw, Row):
                sd_dict = self.__delete_none(sd_raw.asDict())
            elif isinstance(sd_raw, dict):
                sd_dict = self.__delete_none(dict(sd_raw))
            else:
                raise Exception(
                    f"{field_name}.flows[{idx}].source_details must be an object"
                )

            normalized_sd = {}
            for sd_key, sd_val in sd_dict.items():
                if sd_key == f"source_path_{env}":
                    normalized_sd["path"] = sd_val
                elif sd_key == "source_schema_path":
                    if layer == "bronze" and sd_val:
                        per_flow_schemas[name] = self.__get_bronze_schema(sd_val)
                    # Keep source_schema_path in normalized_sd so
                    # PipelineReaders that re-read it (cloudFiles fallback
                    # path) still see it.
                    normalized_sd[sd_key] = sd_val
                else:
                    normalized_sd[sd_key] = sd_val
            flow_dict["source_details"] = normalized_sd

            # Per-flow reader_options can come through as a Row when read
            # from JSON via Spark; flatten consistently with the append-flow
            # pipeline so downstream JSON-encode produces a flat object.
            if "reader_options" in flow_dict and isinstance(
                flow_dict["reader_options"], Row
            ):
                flow_dict["reader_options"] = self.__delete_none(
                    flow_dict["reader_options"].asDict()
                )

            # Default-fill the per-flow optional fields. We keep only the
            # known per-flow keys so an accidental top-level group field
            # mistakenly nested under a flow doesn't silently flow through.
            normalized_flow = {
                "name": flow_dict["name"],
                "source_format": flow_dict["source_format"],
                "source_details": flow_dict["source_details"],
                "reader_options": flow_dict.get(
                    "reader_options",
                    DataflowSpecUtils.cdc_apply_changes_flow_attributes_defaults[
                        "reader_options"
                    ],
                ),
                "select_exp": flow_dict.get(
                    "select_exp",
                    DataflowSpecUtils.cdc_apply_changes_flow_attributes_defaults[
                        "select_exp"
                    ],
                ),
                "where_clause": flow_dict.get(
                    "where_clause",
                    DataflowSpecUtils.cdc_apply_changes_flow_attributes_defaults[
                        "where_clause"
                    ],
                ),
                "once": flow_dict.get(
                    "once",
                    DataflowSpecUtils.cdc_apply_changes_flow_attributes_defaults[
                        "once"
                    ],
                ),
            }
            out_flows.append(self.__delete_none(normalized_flow))

        # Rebuild group with only known top-level fields so an accidental
        # typo at the group level is visible by its absence rather than
        # silently passed through.
        group_payload = {
            "keys": group_dict["keys"],
            "sequence_by": group_dict["sequence_by"],
            "scd_type": group_dict["scd_type"],
            "where": group_dict.get("where"),
            "ignore_null_updates": group_dict.get("ignore_null_updates", False),
            "apply_as_deletes": group_dict.get("apply_as_deletes"),
            "apply_as_truncates": group_dict.get("apply_as_truncates"),
            "column_list": group_dict.get("column_list"),
            "except_column_list": group_dict.get("except_column_list"),
            "track_history_column_list": group_dict.get("track_history_column_list"),
            "track_history_except_column_list": group_dict.get(
                "track_history_except_column_list"
            ),
            "ignore_null_updates_column_list": group_dict.get(
                "ignore_null_updates_column_list"
            ),
            "ignore_null_updates_except_column_list": group_dict.get(
                "ignore_null_updates_except_column_list"
            ),
            "flows": out_flows,
        }
        group_json = json.dumps(
            _coerce_scd_type_to_str(self.__delete_none(group_payload))
        )
        return group_json, per_flow_schemas

    def get_validated_sinks_details(self, sinks_details_json):
        sink_list = []
        for sink_details_json in sinks_details_json:
            sink = {}
            sink_details = sink_details_json.asDict()
            sink_details_keys = set(sink_details.keys())
            missing_sink_details_keys = set(DataflowSpecUtils.sink_mandatory_attributes).difference(sink_details_keys)
            if missing_sink_details_keys:
                raise Exception(f"Missing sink details keys: {missing_sink_details_keys}")
            if sink_details.get("name", None):
                sink["name"] = sink_details["name"].lower()
            if sink_details.get("format", None):
                sink_format_options = ["delta", "kafka", "eventhub"]
                if sink_details["format"].lower() not in sink_format_options:
                    raise Exception(f"Sink format {sink_details['format']} not supported in SDP-META!")
                sink["format"] = sink_details["format"].lower()
            if sink_details.get("options", None):
                options_dict = self.__delete_none(sink_details["options"].asDict())
                options_json = json.dumps(self.__delete_none(options_dict))
                sink["options"] = options_json
                delta_format_options = ["path", "tablename"]
                dlt_sink_options_keys = set(options_dict.keys())
                if sink["format"] == "delta":
                    if "path" in dlt_sink_options_keys or "tablename" in dlt_sink_options_keys:
                        logger.info("Validated delta sink options")
                    else:
                        raise Exception(f"Missing delta sink options: {delta_format_options}")
            sink["select_exp"] = sink_details.get("select_exp", None)
            sink["where_clause"] = sink_details.get("where_clause", None)
            sink_list.append(sink)
        sinks_json = json.dumps(sink_list)
        logger.info(f"Validated sinks details: {sinks_json}")
        return sinks_json

    def __validate_apply_changes(self, onboarding_row, layer):
        cdc_apply_changes = onboarding_row[f"{layer}_cdc_apply_changes"]
        json_cdc_apply_changes = self.__delete_none(cdc_apply_changes.asDict())
        logger.info(f"actual mergeInfo={json_cdc_apply_changes}")
        payload_keys = json_cdc_apply_changes.keys()
        missing_cdc_payload_keys = set(
            DataflowSpecUtils.cdc_applychanges_api_attributes
        ).difference(payload_keys)
        logger.info(
            f"""missing cdc payload keys:{missing_cdc_payload_keys}
                for onboarding row = {onboarding_row}"""
        )
        if set(DataflowSpecUtils.cdc_applychanges_api_mandatory_attributes) - set(
            payload_keys
        ):
            missing_mandatory_attr = set(
                DataflowSpecUtils.cdc_applychanges_api_mandatory_attributes
            ) - set(payload_keys)
            logger.info(f"mandatory missing keys= {missing_mandatory_attr}")
            raise Exception(
                f"""mandatory missing atrributes for {layer}_cdc_apply_changes = {missing_mandatory_attr}
                for onboarding row = {onboarding_row}"""
            )
        else:
            logger.info(
                f"""all mandatory {layer}_cdc_apply_changes atrributes
                {DataflowSpecUtils.cdc_applychanges_api_mandatory_attributes} exists"""
            )

    def __validate_apply_changes_from_snapshot(self, onboarding_row, layer):
        apply_changes_from_snapshot = onboarding_row[f"{layer}_apply_changes_from_snapshot"]
        json_apply_changes_from_snapshot = self.__delete_none(apply_changes_from_snapshot.asDict())
        logger.info(f"actual applyChangesFromSnapshot={json_apply_changes_from_snapshot}")
        payload_keys = json_apply_changes_from_snapshot.keys()
        missing_apply_changes_from_snapshot_payload_keys = (
            set(DataflowSpecUtils.apply_changes_from_snapshot_api_attributes).difference(payload_keys)
        )
        logger.info(
            f"""missing applyChangesFromSnapshot payload keys:{missing_apply_changes_from_snapshot_payload_keys}
                for onboarding row = {onboarding_row}"""
        )
        if set(DataflowSpecUtils.apply_changes_from_snapshot_api_mandatory_attributes) - set(payload_keys):
            missing_mandatory_attr = set(DataflowSpecUtils.apply_changes_from_snapshot_api_mandatory_attributes) - set(
                payload_keys
            )
            logger.info(f"mandatory missing keys= {missing_mandatory_attr}")
            raise Exception(
                f"""mandatory missing atrributes for {layer}_apply_changes_from_snapshot = {
                    missing_mandatory_attr}
                for onboarding row = {onboarding_row}"""
            )
        else:
            logger.info(
                f"""all mandatory {layer}_apply_changes_from_snapshot atrributes
                 {DataflowSpecUtils.apply_changes_from_snapshot_api_mandatory_attributes} exists"""
            )

    def get_bronze_source_details_reader_options_schema(self, onboarding_row, env):
        """Get bronze source reader options.

        Args:
            onboarding_row ([type]): [description]

        Returns:
            [type]: [description]
        """
        source_details = {}
        bronze_reader_config_options = {}
        schema = None
        source_format = onboarding_row["source_format"]
        bronze_reader_options_json = (
            onboarding_row["bronze_reader_options"]
            if "bronze_reader_options" in onboarding_row
            else {}
        )
        if bronze_reader_options_json:
            bronze_reader_config_options = self.__delete_none(
                bronze_reader_options_json.asDict()
            )
        source_details_json = onboarding_row["source_details"]
        if source_details_json:
            source_details_file = self.__delete_none(source_details_json.asDict())
            if (source_format.lower() == "cloudfiles"
                    or source_format.lower() == "delta"
                    or source_format.lower() == "snapshot"):
                if f"source_path_{env}" in source_details_file:
                    source_details["path"] = source_details_file[f"source_path_{env}"]
                if f"source_catalog_{env}" in source_details_file:
                    source_details["catalog"] = source_details_file[f"source_catalog_{env}"]
                if "source_database" in source_details_file:
                    source_details["source_database"] = source_details_file[
                        "source_database"
                    ]
                if "source_table" in source_details_file:
                    source_details["source_table"] = source_details_file["source_table"]
                if "source_metadata" in source_details_file:
                    source_metadata_dict = self.__delete_none(
                        source_details_file["source_metadata"].asDict()
                    )
                    if "select_metadata_cols" in source_metadata_dict:
                        select_metadata_cols = self.__delete_none(
                            source_metadata_dict["select_metadata_cols"].asDict()
                        )
                        source_metadata_dict["select_metadata_cols"] = select_metadata_cols
                    source_details["source_metadata"] = json.dumps(
                        self.__delete_none(source_metadata_dict)
                    )
            if source_format.lower() == "snapshot":
                snapshot_format = source_details_file.get("snapshot_format", None)
                if snapshot_format is None:
                    raise Exception("snapshot_format is missing in the source_details")
                source_details["snapshot_format"] = snapshot_format
                if f"source_path_{env}" in source_details_file:
                    source_details["path"] = source_details_file[f"source_path_{env}"]
                elif not self.uc_enabled:
                    # A non-UC snapshot source reads from a Delta path, so the
                    # path is mandatory here. (Under Unity Catalog the snapshot
                    # is read from source_database.source_table, so a path is
                    # optional.) This enforcement previously lived in an
                    # unreachable second ``elif ... == "snapshot"`` branch, so a
                    # missing path was silently ignored; enforce it on the
                    # reachable branch instead.
                    raise Exception(
                        f"source_path_{env} is missing in the source_details "
                        f"for snapshot source (required when Unity Catalog is "
                        f"not enabled)"
                    )
            elif source_format.lower() == "eventhub" or source_format.lower() == "kafka":
                source_details = source_details_file
            if "source_schema_path" in source_details_file:
                source_schema_path = source_details_file["source_schema_path"]
                if source_schema_path:
                    if self.bronze_schema_mapper is not None:
                        schema = self.bronze_schema_mapper(
                            source_schema_path, self.spark
                        )
                    else:
                        schema = self.__get_bronze_schema(source_schema_path)
                else:
                    logger.info(f"no input schema provided for row={onboarding_row}")
                logger.info("spark_schema={}".format(schema))

        return source_details, bronze_reader_config_options, schema

    def __validate_append_flow(self, onboarding_row, layer):
        append_flows = onboarding_row[f"{layer}_append_flows"]
        for append_flow in append_flows:
            json_append_flow = append_flow.asDict()
            logger.info(f"actual appendFlow={json_append_flow}")
            payload_keys = json_append_flow.keys()
            missing_append_flow_payload_keys = set(
                DataflowSpecUtils.append_flow_api_attributes_defaults
            ).difference(payload_keys)
            logger.info(
                f"""missing append flow payload keys:{missing_append_flow_payload_keys}
                    for onboarding row = {onboarding_row}"""
            )
            if set(DataflowSpecUtils.append_flow_mandatory_attributes) - set(
                payload_keys
            ):
                missing_mandatory_attr = set(
                    DataflowSpecUtils.append_flow_mandatory_attributes
                ) - set(payload_keys)
                logger.info(f"mandatory missing keys= {missing_mandatory_attr}")
                raise Exception(
                    f"""mandatory missing atrributes for {layer}_append_flow = {missing_mandatory_attr}
                    for onboarding row = {onboarding_row}"""
                )
            else:
                logger.info(
                    f"""all mandatory {layer}_append_flow atrributes
                    {DataflowSpecUtils.append_flow_mandatory_attributes} exists"""
                )

    def __get_data_quality_expecations(self, file_path):
        """Get Data Quality expectations from a JSON or YAML file.

        Returns the expectations serialized as a JSON string so that downstream
        consumers (which call ``json.loads`` on the stored value) can keep using
        the existing format. A YAML source file is parsed and re-serialized to
        JSON; the on-wire dataflow spec is always JSON.

        Args:
            file_path: Path to a ``.json``, ``.yml``, or ``.yaml`` file containing
                DQ expectations. ``None`` or empty returns ``None``.

        Returns:
            JSON string of the parsed expectations, or ``None`` if ``file_path``
            is falsy.

        Raises:
            ValueError: If the file extension is unsupported, the file is empty,
                or the contents cannot be parsed.
        """
        if not file_path:
            return None
        parsed = self._load_structured_file(file_path)
        if parsed is None:
            return None
        return json.dumps(parsed)

    def __get_silver_dataflow_spec_dataframe(self, onboarding_df, env):
        """Get silver_dataflow_spec method transform onboarding dataframe to silver dataflowSpec dataframe.

        Args:
            onboarding_df ([type]): [description]
            spark (SparkSession): [description]

        Returns:
            [type]: [description]
        """
        data_flow_spec_columns = [
            "dataFlowId",
            "dataFlowGroup",
            "sourceFormat",
            "sourceDetails",
            "readerConfigOptions",
            "targetFormat",
            "targetDetails",
            "tableProperties",
            "partitionColumns",
            "cdcApplyChanges",
            "applyChangesFromSnapshot",
            "dataQualityExpectations",
            "quarantineTargetDetails",
            "quarantineTableProperties",
            "quarantineClusterBy",
            "appendFlows",
            "appendFlowsSchemas",
            "clusterBy",
            "clusterByAuto",
            "sinks",
            # Multi-source AUTO CDC (issue #294). Silver omits the
            # per-flow schema map because silver flows always read from
            # Delta upstream, which carries its own schema.
            "cdcApplyChangesFlows",
            # UC row-level security (issue #303). Both fields are optional
            # and silently dropped on non-UC pipelines.
            "rowFilter",
            "quarantineRowFilter",
            # UC column-level governance. JSON-object strings keyed by column
            # name; optional. Masks are dropped on non-UC pipelines.
            "columnComments",
            "columnMasks",
        ]
        data_flow_spec_schema = StructType(
            [
                StructField("dataFlowId", StringType(), True),
                StructField("dataFlowGroup", StringType(), True),
                StructField("sourceFormat", StringType(), True),
                StructField(
                    "sourceDetails", MapType(StringType(), StringType(), True), True
                ),
                StructField(
                    "readerConfigOptions",
                    MapType(StringType(), StringType(), True),
                    True,
                ),
                StructField("targetFormat", StringType(), True),
                StructField(
                    "targetDetails", MapType(StringType(), StringType(), True), True
                ),
                StructField(
                    "tableProperties", MapType(StringType(), StringType(), True), True
                ),
                StructField("partitionColumns", ArrayType(StringType(), True), True),
                StructField("cdcApplyChanges", StringType(), True),
                StructField("applyChangesFromSnapshot", StringType(), True),
                StructField("dataQualityExpectations", StringType(), True),
                StructField("quarantineTargetDetails", MapType(StringType(), StringType(), True), True),
                StructField("quarantineTableProperties", MapType(StringType(), StringType(), True), True),
                StructField("quarantineClusterBy", ArrayType(StringType(), True), True),
                StructField("appendFlows", StringType(), True),
                StructField("appendFlowsSchemas", MapType(StringType(), StringType(), True), True),
                StructField("clusterBy", ArrayType(StringType(), True), True),
                StructField("clusterByAuto", T.BooleanType(), True),
                StructField("sinks", StringType(), True),
                StructField("cdcApplyChangesFlows", StringType(), True),
                StructField("rowFilter", StringType(), True),
                StructField("quarantineRowFilter", StringType(), True),
                StructField("columnComments", StringType(), True),
                StructField("columnMasks", StringType(), True),
            ]
        )
        data = []

        onboarding_rows = onboarding_df.collect()
        base_mandatory_fields = [
            "data_flow_id",
            "data_flow_group",
            f"silver_database_{env}",
            "silver_table",
            f"silver_transformation_json_{env}",
        ]  # f"silver_table_path_{env}",

        for onboarding_row in onboarding_rows:
            # Multi-source AUTO CDC (issue #294): an onboarding file may
            # contain a mix of bronze-only rows (each defining its own
            # bronze CDC table) and a separate silver row that merges
            # them via ``silver_cdc_apply_changes_flows``. Skip rows
            # that don't define a silver target so the bronze-only
            # entries don't trip the silver mandatory-field check.
            silver_db_field = f"silver_database_{env}"
            if not (
                silver_db_field in onboarding_row
                and onboarding_row[silver_db_field]
            ):
                continue

            # When the row uses multi-source AUTO CDC, the per-flow
            # ``select_exp`` inside the ``silver_cdc_apply_changes_flows``
            # group provides the transformation logic — the external
            # silver-transformations JSON is not consulted at runtime,
            # so don't force the user to ship one.
            has_cdc_flows = (
                "silver_cdc_apply_changes_flows" in onboarding_row
                and onboarding_row["silver_cdc_apply_changes_flows"]
            )
            mandatory_fields = [
                f for f in base_mandatory_fields
                if not (
                    has_cdc_flows and f == f"silver_transformation_json_{env}"
                )
            ]

            try:
                self.__validate_mandatory_fields(onboarding_row, mandatory_fields)
            except ValueError:
                mandatory_fields.append(f"silver_table_path_{env}")
                self.__validate_mandatory_fields(onboarding_row, mandatory_fields)
            # Silver rows reference both the bronze source AND the silver
            # target, so validate both. Bronze fields are optional in the
            # silver-only mandatory list above, but if present they must
            # still be safe SQL identifiers.
            self.__validate_row_uc_names(onboarding_row, env, "silver")
            if (
                f"bronze_database_{env}" in onboarding_row
                and onboarding_row[f"bronze_database_{env}"]
            ):
                self.__validate_row_uc_names(onboarding_row, env, "bronze")
            silver_data_flow_spec_id = onboarding_row["data_flow_id"]
            silver_data_flow_spec_group = onboarding_row["data_flow_group"]
            silver_reader_config_options = {}

            silver_target_format = "delta"

            # Bronze source details for the silver read path. Pure
            # multi-source AUTO CDC silver rows (issue #294) read from
            # per-flow ``source_details`` inside the
            # ``silver_cdc_apply_changes_flows`` group, so their bronze
            # fields may be absent — fall back to an empty placeholder
            # in that case. The runtime dispatcher only consults
            # ``bronze_target_details`` on the legacy single-source
            # silver path, which doesn't fire when
            # ``cdcApplyChangesFlows`` is set.
            bronze_db_field = f"bronze_database_{env}"
            if (
                bronze_db_field in onboarding_row
                and onboarding_row[bronze_db_field]
                and "bronze_table" in onboarding_row
                and onboarding_row["bronze_table"]
            ):
                bronze_target_details = {
                    "database": onboarding_row[bronze_db_field],
                    "table": onboarding_row["bronze_table"],
                }
                bronze_cl = (
                    onboarding_row[f"bronze_catalog_{env}"]
                    if f"bronze_catalog_{env}" in onboarding_row
                    else None
                )
                if bronze_cl:
                    bronze_target_details["catalog"] = bronze_cl
            elif has_cdc_flows:
                bronze_target_details = {"database": "", "table": ""}
            else:
                raise Exception(
                    f"Missing bronze source fields "
                    f"({bronze_db_field}/bronze_table) for silver "
                    f"data_flow_id={onboarding_row['data_flow_id']}"
                )
            silver_target_details = {
                "database": onboarding_row["silver_database_{}".format(env)],
                "table": onboarding_row["silver_table"],
            }
            silver_cl = (
                onboarding_row["silver_catalog_{}".format(env)]
                if "silver_catalog_{}".format(env) in onboarding_row
                else None
            )
            if "silver_table_comment" in onboarding_row:
                silver_target_details["comment"] = onboarding_row["silver_table_comment"]
            if silver_cl:
                silver_target_details["catalog"] = silver_cl
            if not self.uc_enabled:
                # HMS (non-UC) targets are external Delta tables addressed by an
                # explicit location, so persist the table path into
                # ``targetDetails`` — the downstream HMS runtime reads
                # ``targetDetails["path"]`` directly (dataflow_pipeline.py), so
                # the key must exist. A missing/empty path is tolerated and
                # stored as ``None`` ("no explicit location"): snapshot-based
                # silver rows and silver-fanout consumer rows that only
                # reference the shared bronze source legitimately omit it. This
                # matches the pre-refactor behaviour, which assigned the raw
                # column value (``None`` when absent) rather than hard-failing.
                #
                # For a multi-source silver CDC row (``has_cdc_flows``),
                # ``bronze_target_details`` is an empty placeholder the runtime
                # never consults, so skip the bronze path there entirely.
                if not has_cdc_flows:
                    bronze_target_details["path"] = (
                        onboarding_row[f"bronze_table_path_{env}"]
                        if f"bronze_table_path_{env}" in onboarding_row
                        else None
                    )
                silver_target_details["path"] = (
                    onboarding_row[f"silver_table_path_{env}"]
                    if f"silver_table_path_{env}" in onboarding_row
                    else None
                )
            silver_reader_options_json = (
                onboarding_row["silver_reader_options"]
                if "silver_reader_options" in onboarding_row
                else {}
            )
            if silver_reader_options_json:
                silver_reader_config_options = self.__delete_none(
                    silver_reader_options_json.asDict()
                )
            silver_table_properties = {}
            if (
                "silver_table_properties" in onboarding_row
                and onboarding_row["silver_table_properties"]
            ):
                silver_table_properties = self.__delete_none(
                    onboarding_row["silver_table_properties"].asDict()
                )

            silver_parition_columns = [""]
            if (
                "silver_partition_columns" in onboarding_row
                and onboarding_row["silver_partition_columns"]
            ):
                # Split if this is a list separated by commas
                if "," in onboarding_row["silver_partition_columns"]:
                    silver_parition_columns = onboarding_row["silver_partition_columns"].split(",")
                else:
                    silver_parition_columns = [onboarding_row["silver_partition_columns"]]

            dlt_sinks = None
            if "silver_sinks" in onboarding_row and onboarding_row["silver_sinks"]:
                dlt_sinks = self.get_sink_details(onboarding_row, "silver")
            silver_cluster_by = self.__get_cluster_by_properties(onboarding_row, silver_table_properties,
                                                                 "silver_cluster_by")
            silver_cluster_by_auto = self.__get_cluster_by_auto(onboarding_row, "silver_cluster_by_auto")

            silver_cdc_apply_changes = None
            if (
                "silver_cdc_apply_changes" in onboarding_row
                and onboarding_row["silver_cdc_apply_changes"]
            ):
                self.__validate_apply_changes(onboarding_row, "silver")
                silver_cdc_apply_changes_row = onboarding_row[
                    "silver_cdc_apply_changes"
                ]
                if self.onboard_file_type == "json":
                    # v0.0.10 onboarding files carried scd_type as int
                    # (issue #370); persist the canonical string form.
                    silver_cdc_apply_changes = json.dumps(
                        _coerce_scd_type_to_str(
                            self.__delete_none(silver_cdc_apply_changes_row.asDict())
                        )
                    )
            data_quality_expectations = None
            silver_quarantine_target_details = None
            silver_quarantine_table_properties = None
            silver_quarantine_cluster_by = None
            if f"silver_data_quality_expectations_json_{env}" in onboarding_row:
                silver_data_quality_expectations_json = onboarding_row[
                    f"silver_data_quality_expectations_json_{env}"
                ]
                if silver_data_quality_expectations_json:
                    data_quality_expectations = self.__get_data_quality_expecations(
                        silver_data_quality_expectations_json
                    )
                silver_quarantine_target_details, silver_quarantine_table_properties = self.__get_quarantine_details(
                    env, "silver", onboarding_row
                )
                silver_quarantine_cluster_by = self.__get_cluster_by_properties(
                    onboarding_row,
                    silver_quarantine_table_properties,
                    "silver_quarantine_cluster_by"
                )
            append_flows, append_flow_schemas = self.get_append_flows_json(
                onboarding_row, layer="silver", env=env
            )
            silver_cdc_apply_changes_flows, _silver_cdc_flow_schemas = (
                self.get_cdc_apply_changes_flows_json(onboarding_row, "silver", env)
            )
            apply_changes_from_snapshot = None
            source_format = "delta"
            if ("silver_apply_changes_from_snapshot" in onboarding_row
                    and onboarding_row["silver_apply_changes_from_snapshot"]):
                self.__validate_apply_changes_from_snapshot(onboarding_row, "silver")
                apply_changes_from_snapshot = json.dumps(
                    _coerce_scd_type_to_str(
                        self.__delete_none(onboarding_row["silver_apply_changes_from_snapshot"].asDict())
                    )
                )
                source_format = "snapshot"
            silver_row_filter = (
                onboarding_row["silver_row_filter"]
                if "silver_row_filter" in onboarding_row and onboarding_row["silver_row_filter"]
                else None
            )
            silver_quarantine_row_filter = (
                onboarding_row["silver_quarantine_row_filter"]
                if (
                    "silver_quarantine_row_filter" in onboarding_row
                    and onboarding_row["silver_quarantine_row_filter"]
                )
                else None
            )
            silver_column_comments = self.__get_column_policy_json(
                onboarding_row, "silver_column_comments"
            )
            silver_column_masks = self.__get_column_policy_json(
                onboarding_row, "silver_column_masks", cleaner=validate_column_masks
            )
            silver_row = (
                silver_data_flow_spec_id,
                silver_data_flow_spec_group,
                source_format,
                bronze_target_details,
                silver_reader_config_options,
                silver_target_format,
                silver_target_details,
                silver_table_properties,
                silver_parition_columns,
                silver_cdc_apply_changes,
                apply_changes_from_snapshot,
                data_quality_expectations,
                silver_quarantine_target_details,
                silver_quarantine_table_properties,
                silver_quarantine_cluster_by,
                append_flows,
                append_flow_schemas,
                silver_cluster_by,
                silver_cluster_by_auto,
                dlt_sinks,
                silver_cdc_apply_changes_flows,
                silver_row_filter,
                silver_quarantine_row_filter,
                silver_column_comments,
                silver_column_masks,
            )
            data.append(silver_row)
            logger.info(f"silver_data ==== {data}")

        data_flow_spec_rows_df = self.spark.createDataFrame(
            data, data_flow_spec_schema
        ).toDF(*data_flow_spec_columns)
        return data_flow_spec_rows_df
