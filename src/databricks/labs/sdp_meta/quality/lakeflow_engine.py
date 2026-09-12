"""Built-in Lakeflow quality engine."""

from pyspark.sql import functions as f
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from databricks.labs.sdp_meta.quality.spi import (
    ExpectationBinding,
    QualityEnginePlugin,
)
from databricks.labs.sdp_meta.quality.validation import (
    LAKEFLOW_DIAGNOSTIC_SCHEMA_FINGERPRINT,
    analyze_lakeflow_normalized_rules,
    validate_lakeflow_rules,
)


ERRORS_SCHEMA = ArrayType(
    StructType(
        [
            StructField("name", StringType(), False),
            StructField("expression", StringType(), False),
        ]
    )
)


class LakeflowQualityEngine(QualityEnginePlugin):
    """Quality engine using Spark expressions and native Lakeflow metrics."""

    diagnostic_schema_fingerprint = (
        LAKEFLOW_DIAGNOSTIC_SCHEMA_FINGERPRINT
    )

    def __init__(self, spark):
        self.spark = spark

    def validate(self, document, options):
        schema = (options or {}).get("schema")
        return validate_lakeflow_rules(self.spark, document, schema)

    def analyze(self, rules, schema, options):
        return analyze_lakeflow_normalized_rules(
            self.spark, rules, schema
        )

    def apply(self, input_df, rules, options):
        """Attach one stable `_errors` entry per failed quarantine rule."""
        if "_errors" in input_df.columns:
            raise ValueError("Input contains reserved quality column '_errors'")
        quarantine_rules = rules.get("expect_or_quarantine", {})
        if not quarantine_rules:
            return input_df.withColumn(
                "_errors", f.from_json(f.lit("[]"), ERRORS_SCHEMA)
            )
        failures = [
            f.when(
                ~f.expr(expression),
                f.struct(
                    f.lit(name).alias("name"),
                    f.lit(expression).alias("expression"),
                ),
            )
            for name, expression in quarantine_rules.items()
        ]
        return input_df.withColumn(
            "_errors",
            f.filter(f.array(*failures), lambda item: item.isNotNull()),
        )

    def get_main_input(self, checked_df):
        return checked_df

    def get_invalid(self, checked_df):
        return checked_df.filter(f.size("_errors") > 0)

    def native_expectations(self, rules, options):
        bindings = []
        if rules.get("expect"):
            bindings.append(
                ExpectationBinding("main", "expect", rules["expect"])
            )
        if rules.get("expect_or_drop"):
            bindings.append(
                ExpectationBinding(
                    "main", "expect_or_drop", rules["expect_or_drop"]
                )
            )
        if rules.get("expect_or_quarantine"):
            bindings.append(
                ExpectationBinding(
                    "main",
                    "expect_or_drop",
                    rules["expect_or_quarantine"],
                )
            )
        if rules.get("expect_or_fail"):
            bindings.append(
                ExpectationBinding(
                    "checked", "expect_or_fail", rules["expect_or_fail"]
                )
            )
        return bindings

    def reserved_columns(self):
        return ["_errors"]
