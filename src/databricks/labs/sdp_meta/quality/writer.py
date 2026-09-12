"""Shared main/quarantine graph writer for quality engines."""

import ast
import json

from pyspark import pipelines as dp

from databricks.labs.sdp_meta.dataflow_spec import (
    BronzeDataflowSpec,
    DataflowSpecUtils,
)
from databricks.labs.sdp_meta.quality.registry import (
    resolve_quality_engine,
    validate_plugin_snapshot,
)
from databricks.labs.sdp_meta.quality.validation import (
    InvalidQualityConfiguration,
)


class QualityEngineWriter:
    """Declare one checked view and the engine's two output tables."""

    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.config = json.loads(pipeline.dataflowSpec.qualityConfig)
        self.plugin = self._resolve_plugin()
        self.checked_view_name = f"{pipeline.view_name}_quality_checked"

    def _resolve_plugin(self):
        engine = self.config.get("engine")
        plugin = resolve_quality_engine(engine, self.pipeline.spark)
        validate_plugin_snapshot(plugin, self.config)
        return plugin

    @staticmethod
    def _apply_bindings(dataset, bindings):
        decorators = {
            "expect": dp.expect_all,
            "expect_or_drop": dp.expect_all_or_drop,
            "expect_or_fail": dp.expect_all_or_fail,
        }
        result = dataset
        for binding in bindings:
            result = decorators[binding.action](binding.rules)(result)
        return result

    def _checked_view(self):
        def checked_view():
            input_df = dp.read_stream(self.pipeline.view_name)
            options = self.config.get("engine_options", {})
            rules = self.config["normalized_rules"]
            reserved = set(self.plugin.reserved_columns())
            conflicts = sorted(reserved.intersection(input_df.columns))
            if conflicts:
                raise ValueError(
                    "Input contains reserved quality columns: "
                    + ", ".join(conflicts)
                )
            if options.get("analysis_required"):
                analyzed = self.plugin.analyze(
                    rules, input_df.schema, options
                )
                if analyzed != rules:
                    raise InvalidQualityConfiguration(
                        "Runtime rule analysis differs from onboarding snapshot"
                    )
            return self.plugin.apply(input_df, rules, options)

        checked = dp.temporary_view(
            checked_view,
            name=self.checked_view_name,
            comment=f"quality-checked input for {self.pipeline.view_name}",
        )
        bindings = [
            binding
            for binding in self.plugin.native_expectations(
                self.config["normalized_rules"],
                self.config.get("engine_options", {}),
            )
            if binding.target == "checked"
        ]
        return self._apply_bindings(checked, bindings)

    def _main_table(self):
        target_path, target_table, _ = self.pipeline._get_target_table_info()
        is_bronze = isinstance(
            self.pipeline.dataflowSpec, BronzeDataflowSpec
        )

        def main_table():
            checked = dp.read_stream(self.checked_view_name)
            main_input = self.plugin.get_main_input(checked)
            return main_input.drop(*self.plugin.reserved_columns())

        table = dp.table(
            main_table,
            name=target_table,
            partition_cols=DataflowSpecUtils.get_partition_cols(
                self.pipeline.dataflowSpec.partitionColumns
            ),
            cluster_by=DataflowSpecUtils.get_partition_cols(
                self.pipeline.dataflowSpec.clusterBy
            ),
            cluster_by_auto=bool(
                getattr(self.pipeline.dataflowSpec, "clusterByAuto", False)
            ),
            table_properties=self.pipeline.dataflowSpec.tableProperties,
            path=target_path,
            comment=self.pipeline._get_table_comment(target_table, is_bronze),
            row_filter=self.pipeline._get_row_filter(),
        )
        bindings = [
            binding
            for binding in self.plugin.native_expectations(
                self.config["normalized_rules"],
                self.config.get("engine_options", {}),
            )
            if binding.target == "main"
        ]
        return self._apply_bindings(table, bindings)

    def _quarantine_table(self):
        details = self.pipeline._get_quarantine_target_details()
        quarantine_table = self.pipeline._build_table_name(
            details.get("catalog"),
            details["database"],
            details["table"],
        )
        quarantine_path = None if self.pipeline.uc_enabled else details.get("path")
        cluster_by = details.get("cluster_by")
        if isinstance(cluster_by, str):
            try:
                parsed_cluster_by = ast.literal_eval(cluster_by)
            except (ValueError, SyntaxError):
                parsed_cluster_by = cluster_by
            if isinstance(parsed_cluster_by, (list, tuple)):
                cluster_by = list(parsed_cluster_by)

        def quarantine_rows():
            checked = dp.read_stream(self.checked_view_name)
            return self.plugin.get_invalid(checked)

        return dp.table(
            quarantine_rows,
            name=quarantine_table,
            table_properties=self.pipeline.dataflowSpec.quarantineTableProperties,
            partition_cols=DataflowSpecUtils.get_partition_cols(
                details.get("partition_columns")
            ),
            cluster_by=DataflowSpecUtils.get_partition_cols(
                cluster_by
            ),
            cluster_by_auto=str(
                details.get("cluster_by_auto", "false")
            ).lower() == "true",
            path=quarantine_path,
            comment=details.get("comment")
            or f"quality quarantine table {quarantine_table}",
            row_filter=self.pipeline._get_quarantine_row_filter(),
        )

    def write(self):
        """Declare the checked view before both consuming tables."""
        self._checked_view()
        self._main_table()
        self._quarantine_table()
