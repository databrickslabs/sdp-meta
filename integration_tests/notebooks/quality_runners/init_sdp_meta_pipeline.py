# Databricks notebook source
sdp_meta_whl = spark.conf.get("sdp_meta_whl")
%pip install $sdp_meta_whl # noqa : E999

# COMMAND ----------

layer = spark.conf.get("layer", None)

from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline

DataflowPipeline.invoke_dlt_pipeline(spark, layer)
