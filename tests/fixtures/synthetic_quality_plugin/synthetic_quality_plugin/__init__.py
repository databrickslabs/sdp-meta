"""Independently packaged fixture for quality-engine discovery tests."""

from pyspark.sql import functions as f

from databricks.labs.sdp_meta.quality.spi import QualityEnginePlugin


class SyntheticQualityEngine(QualityEnginePlugin):
    """Small deterministic engine used only by SDP-META contract tests."""

    plugin_package = "sdp-meta-synthetic-quality-plugin"
    plugin_version = "0.1.0"
    engine_version = "1"

    def __init__(self, spark):
        self.spark = spark

    def validate(self, document, options):
        if not isinstance(document, dict) or "rules" not in document:
            raise ValueError("synthetic quality document requires rules")
        return document["rules"], False

    def apply(self, input_df, rules, options):
        expression = next(iter(rules.values()))
        return input_df.withColumn(
            "_synthetic_errors",
            f.when(~f.expr(expression), f.array(f.lit("failed"))).otherwise(
                f.array().cast("array<string>")
            ),
        )

    def get_main_input(self, checked_df):
        return checked_df.filter(f.size("_synthetic_errors") == 0)

    def get_invalid(self, checked_df):
        return checked_df.filter(f.size("_synthetic_errors") > 0)

    def native_expectations(self, rules, options):
        return []

    def reserved_columns(self):
        return ["_synthetic_errors"]
