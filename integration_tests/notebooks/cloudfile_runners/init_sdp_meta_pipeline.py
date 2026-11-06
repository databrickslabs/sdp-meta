# Databricks notebook source
sdp_meta_whl = spark.conf.get("sdp_meta_whl")
%pip install $sdp_meta_whl # noqa : E999

# COMMAND ----------

layer = spark.conf.get("layer", None)

from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline
from pyspark.sql.functions import current_timestamp


def add_processing_timestamp(input_df, _dataflow_spec):
    return input_df.withColumn("processing_ts", current_timestamp())


DataflowPipeline.invoke_dlt_pipeline(
    spark,
    layer,
    bronze_custom_transform_func=add_processing_timestamp,
)
