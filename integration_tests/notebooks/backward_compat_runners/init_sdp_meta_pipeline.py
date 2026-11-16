# Databricks notebook source
# Backward-compatibility integration test runner for canonical-shape
# pipelines (sdp-meta v0.1.0+) and every Phase 2 migration target.
#
# Mirrors ``integration_tests/notebooks/cloudfile_runners/init_sdp_meta_pipeline.py``
# but lives under ``backward_compat_runners/`` so the orchestrator
# uploads it from a single, consistent directory regardless of which
# profile (LEGACY or CURRENT) is the upgrade source. It is used in
# Phase 1 for CURRENT sources and replaces the legacy ``from src.*``
# runner during Phase 2 upgrades from v0.0.x.
#
# Phase 1: ``sdp_meta_whl`` -> source-version sdp-meta wheel.
# Phase 2: ``sdp_meta_whl`` swapped to the target-version sdp-meta
# wheel via ``pipelines.update()``. This canonical runner replaces
# legacy ``from src.*`` imports during a v0.0.x-to-v0.2.x migration.
sdp_meta_whl = spark.conf.get("sdp_meta_whl")
%pip install $sdp_meta_whl  # noqa : E999

# COMMAND ----------

layer = spark.conf.get("layer", None)

from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline
DataflowPipeline.invoke_dlt_pipeline(spark, layer)
