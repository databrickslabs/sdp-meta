# Databricks notebook source
sdp_meta_whl = spark.conf.get("sdp_meta_whl")
%pip install $sdp_meta_whl # noqa: E999

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline

layer = spark.conf.get("layer")
dimension_table = spark.conf.get("at_scale.dimensionTable")


def transform_silver(df, spec):
    """Apply custom logic only to the representative orders flow."""
    table = spec.targetDetails["table"]
    if table != "orders":
        return df

    latest_customer = (
        spark.read.table(dimension_table)
        .withColumn(
            "_rank",
            F.row_number().over(
                Window.partitionBy("customer_id").orderBy(
                    F.col("effective_ts").desc()
                )
            ),
        )
        .where(F.col("_rank") == 1)
        .drop("_rank", "effective_ts")
    )
    return (
        df.join(F.broadcast(latest_customer), "customer_id", "left")
        .withColumn("loaded_at", F.current_timestamp())
    )


DataflowPipeline.invoke_dlt_pipeline(
    spark,
    layer=layer,
    silver_custom_transform_func=transform_silver,
)
