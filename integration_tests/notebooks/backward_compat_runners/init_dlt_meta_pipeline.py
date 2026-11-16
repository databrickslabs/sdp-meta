# Databricks notebook source
# Backward-compatibility integration test runner for LEGACY-shape
# pipelines (dlt-meta v0.0.1 .. v0.0.10).
#
# This notebook retains the v0.0.10 source contract for Phase 1. At the
# phase boundary the harness keeps the pipeline ID/checkpoints but migrates
# the pipeline library to ``init_sdp_meta_pipeline.py`` and its canonical
# ``sdp_meta_whl`` configuration.
#
# Phase 1 installs its v0.0.10 source wheel from the single
# ``dlt_meta_whl`` configuration key.
#
dlt_meta_whl = spark.conf.get("dlt_meta_whl")
%pip install $dlt_meta_whl  # noqa : E999

# COMMAND ----------

layer = spark.conf.get("layer", None)

from src.dataflow_pipeline import DataflowPipeline
DataflowPipeline.invoke_dlt_pipeline(spark, layer)
