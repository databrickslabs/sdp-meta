# Databricks notebook source
sdp_meta_whl = spark.conf.get("sdp_meta_whl")
%pip install --force-reinstall $sdp_meta_whl databricks-labs-dqx==0.16.0  # noqa: E999

# COMMAND ----------

layer = spark.conf.get("layer", None)

from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline

DataflowPipeline.invoke_dlt_pipeline(spark, layer)
