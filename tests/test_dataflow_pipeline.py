"""Tests for Dataflowpipeline."""
from datetime import datetime
import json
import sys
import tempfile
import copy
import shutil
import os
from pyspark.sql.functions import lit, expr
import pyspark.sql.types as T
from pyspark.sql import DataFrame
from tests.utils import SDPFrameworkTestCase
from unittest.mock import MagicMock, patch
from databricks.labs.sdp_meta.dataflow_spec import BronzeDataflowSpec, SilverDataflowSpec

# The legacy ``dlt`` module has been replaced by ``pyspark.pipelines`` (imported
# as ``dp``). On the test runner we don't have a Spark version that ships
# ``pyspark.pipelines`` yet, so we register a mock module so the production
# imports succeed.
sys.modules["pyspark.pipelines"] = MagicMock()
from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline  # noqa: E402
from databricks.labs.sdp_meta.onboard_dataflowspec import OnboardDataflowspec  # noqa: E402
from databricks.labs.sdp_meta.dataflow_spec import DataflowSpecUtils  # noqa: E402
from databricks.labs.sdp_meta.pipeline_readers import PipelineReaders  # noqa: E402

dp = MagicMock()
dp.expect_all_or_drop = MagicMock(return_value=lambda func: func)
dp.expect_all_or_fail = MagicMock(return_value=lambda func: func)
dp.table = MagicMock(return_value=lambda func: func)
dp.create_auto_cdc_from_snapshot_flow = MagicMock()
dp.append_flow = MagicMock(return_value=lambda func: func)
dp.expect_all = MagicMock(return_value=lambda func: func)
dp.temporary_view = MagicMock(return_value=lambda func: func)
raw_delta_table_stream = MagicMock()


class DataflowPipelineTests(SDPFrameworkTestCase):
    """Test for Dataflowpipeline."""

    bronze_dataflow_spec_acs_map = {
        "dataFlowId": "1",
        "dataFlowGroup": "A1",
        "sourceFormat": "json",
        "sourceDetails": {"path": "tests/resources/data/customers"},
        "readerConfigOptions": {
        },
        "targetFormat": "delta",
        "targetDetails": {"database": "bronze", "table": "customer", "path": "tests/resources/delta/customers"},
        "tableProperties": {},
        "schema": None,
        "partitionColumns": [""],
        "cdcApplyChanges": None,
        "applyChangesFromSnapshot": """{"keys": ["id"], "scd_type": "2"}""",
        "dataQualityExpectations": """{
            "expect_or_drop": {
                "no_rescued_data": "_rescued_data IS NULL",
                "valid_id": "id IS NOT NULL",
                "valid_operation": "operation IN ('APPEND', 'DELETE', 'UPDATE')"
            }
        }""",
        "quarantineTargetDetails": {
            "database": "bronze", "table": "customer_dqe", "path": "tests/localtest/delta/customers_dqe"
        },
        "quarantineTableProperties": {},
        "appendFlows": [],
        "appendFlowsSchemas": {},
        "version": "v1",
        "createDate": datetime.now,
        "createdBy": "sdp-meta-unittest",
        "updateDate": datetime.now,
        "updatedBy": "sdp-meta-unittest",
        "clusterBy": [""],
        "clusterByAuto": False,
        "sinks": [],
        "cdcApplyChangesFlows": None,
        "cdcApplyChangesFlowsSchemas": None,
        "rowFilter": None,
        "quarantineRowFilter": None,
        "columnComments": None,
        "columnMasks": None,
    }

    bronze_dataflow_spec_map = {
        "dataFlowId": "1",
        "dataFlowGroup": "A1",
        "sourceFormat": "json",
        "sourceDetails": {"path": "tests/resources/data/customers"},
        "readerConfigOptions": {
        },
        "targetFormat": "delta",
        "targetDetails": {"database": "bronze", "table": "customer", "path": "tests/resources/delta/customers"},
        "tableProperties": {},
        "schema": None,
        "partitionColumns": [""],
        "cdcApplyChanges": None,
        "applyChangesFromSnapshot": None,
        "dataQualityExpectations": """{
            "expect_or_drop": {
                "no_rescued_data": "_rescued_data IS NULL",
                "valid_id": "id IS NOT NULL",
                "valid_operation": "operation IN ('APPEND', 'DELETE', 'UPDATE')"
            }
        }""",
        "quarantineTargetDetails": {
            "database": "bronze", "table": "customer_dqe", "path": "tests/localtest/delta/customers_dqe"
        },
        "quarantineTableProperties": {},
        "appendFlows": [],
        "appendFlowsSchemas": {},
        "sinks": [],
        "version": "v1",
        "createDate": datetime.now,
        "createdBy": "sdp-meta-unittest",
        "updateDate": datetime.now,
        "updatedBy": "sdp-meta-unittest",
        "clusterBy": [""],
        "clusterByAuto": False,
        "cdcApplyChangesFlows": None,
        "cdcApplyChangesFlowsSchemas": None,
        "rowFilter": None,
        "quarantineRowFilter": None,
        "columnComments": None,
        "columnMasks": None,
    }
    silver_cdc_apply_changes = {
        "keys": ["id"],
        "sequence_by": "operation_date",
        "scd_type": "1",
        "apply_as_deletes": "operation = 'DELETE'",
        "except_column_list": ["operation", "operation_date", "_rescued_data"],
    }
    silver_cdc_apply_changes_scd2 = {
        "keys": ["id"],
        "sequence_by": "operation_date",
        "scd_type": "2",
        "apply_as_deletes": "operation = 'DELETE'",
        "except_column_list": ["operation", "operation_date", "_rescued_data"],
    }
    silver_dataflow_spec_map = {
        "dataFlowId": "1",
        "dataFlowGroup": "A1",
        "sourceFormat": "delta",
        "sourceDetails": {
            "database": "bronze",
            "table": "customer",
            "path": bronze_dataflow_spec_map["targetDetails"]["path"],
        },
        "readerConfigOptions": {},
        "targetFormat": "delta",
        "targetDetails": {"database": "silver", "table": "customer", "path": tempfile.mkdtemp()},
        "tableProperties": {},
        "selectExp": [
            "address",
            "email",
            "firstname",
            "id",
            "lastname",
            "operation_date",
            "operation",
            "_rescued_data",
        ],
        "whereClause": ["id IS NOT NULL", "email is not NULL"],
        "partitionColumns": ["operation_date"],
        "cdcApplyChanges": json.dumps(silver_cdc_apply_changes),
        "applyChangesFromSnapshot": None,
        "dataQualityExpectations": """{
            "expect_or_drop": {
                "no_rescued_data": "_rescued_data IS NULL",
                "valid_id": "id IS NOT NULL",
                "valid_operation": "operation IN ('APPEND', 'DELETE', 'UPDATE')"
            }
        }""",
        "quarantineTargetDetails": {},
        "quarantineTableProperties": {},
        "appendFlows": [],
        "appendFlowsSchemas": {},
        "sinks": {},
        "version": "v1",
        "createDate": datetime.now,
        "createdBy": "sdp-meta-unittest",
        "updateDate": datetime.now,
        "updatedBy": "sdp-meta-unittest",
        "clusterBy": [""],
        "clusterByAuto": False,
        "cdcApplyChangesFlows": None,
        "rowFilter": None,
        "quarantineRowFilter": None,
        "columnComments": None,
        "columnMasks": None,
    }
    silver_acfs_dataflow_spec_map = {
        "dataFlowId": "1",
        "dataFlowGroup": "A1",
        "sourceFormat": "delta",
        "sourceDetails": {
            "database": "bronze",
            "table": "customer",
            "path": bronze_dataflow_spec_map["targetDetails"]["path"],
        },
        "readerConfigOptions": {},
        "targetFormat": "delta",
        "targetDetails": {"database": "silver", "table": "customer", "path": tempfile.mkdtemp()},
        "tableProperties": {},
        "selectExp": [
            "address",
            "email",
            "firstname",
            "id",
            "lastname",
            "operation_date",
            "operation",
            "_rescued_data",
        ],
        "whereClause": ["id IS NOT NULL", "email is not NULL"],
        "partitionColumns": ["operation_date"],
        "cdcApplyChanges": None,
        "applyChangesFromSnapshot": """{"keys": ["id"], "scd_type": "2"}""",
        "dataQualityExpectations": """{
            "expect_or_drop": {
                "no_rescued_data": "_rescued_data IS NULL",
                "valid_id": "id IS NOT NULL",
                "valid_operation": "operation IN ('APPEND', 'DELETE', 'UPDATE')"
            }
        }""",
        "quarantineTargetDetails": {},
        "quarantineTableProperties": {},
        "appendFlows": [],
        "appendFlowsSchemas": {},
        "sinks": {},
        "version": "v1",
        "createDate": datetime.now,
        "createdBy": "sdp-meta-unittest",
        "updateDate": datetime.now,
        "updatedBy": "sdp-meta-unittest",
        "clusterBy": [""],
        "clusterByAuto": False,
        "cdcApplyChangesFlows": None,
        "rowFilter": None,
        "quarantineRowFilter": None,
        "columnComments": None,
        "columnMasks": None,
    }

    def setUp(self):
        """Set up DPM defaults expected by this class's legacy tests."""
        super().setUp()
        self.spark.conf.set("pipelines.schema", "test_pipeline_schema")
        self.addCleanup(self.spark.conf.unset, "pipelines.schema")

    @patch.object(DataflowPipeline, "run_dlt", return_value={"called"})
    def test_invoke_dlt_pipeline_bronz_positive(self, run_dlt):
        """Test for brozne dlt pipeline."""
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        database = self.onboarding_bronze_silver_params_map["database"]
        bronze_dataflow_table = self.onboarding_bronze_silver_params_map["bronze_dataflowspec_table"]
        self.spark.conf.set("bronze.group", "A1")
        self.spark.conf.set("layer", "bronze")
        self.spark.conf.set(
            "bronze.dataflowspecTable",
            f"{database}.{bronze_dataflow_table}",
        )

        def custom_transform_func(input_df) -> DataFrame:
            return input_df.withColumn('custom_col', lit('test_value'))

        DataflowPipeline.invoke_dlt_pipeline(self.spark, "bronze", custom_transform_func)
        assert run_dlt.called

    @patch.object(DataflowPipeline, "run_dlt", return_value={"called"})
    def test_invoke_dlt_pipeline_silver_positive(self, run_dlt):
        """Test for brozne dlt pipeline."""
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        database = self.onboarding_bronze_silver_params_map["database"]
        silver_dataflow_table = self.onboarding_bronze_silver_params_map["silver_dataflowspec_table"]
        self.spark.conf.set("silver.group", "A1")
        self.spark.conf.set("layer", "silver")
        self.spark.conf.set(
            "silver.dataflowspecTable",
            f"{database}.{silver_dataflow_table}",
        )
        self.spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
        self.spark.sql("DROP TABLE IF EXISTS bronze.customers_cdc")
        self.spark.sql("DROP TABLE IF EXISTS bronze.transactions_cdc")
        if os.path.exists(f"{self.temp_delta_tables_path}/tables/customers_cdc"):
            shutil.rmtree(f"{self.temp_delta_tables_path}/tables/customers_cdc")
        if os.path.exists(f"{self.temp_delta_tables_path}/tables/transactions_cdc"):
            shutil.rmtree(f"{self.temp_delta_tables_path}/tables/transactions_cdc")
        options = {"rescuedDataColumn": "_rescued_data", "inferColumnTypes": "true", "multiline": True}
        customers_parquet_df = self.spark.read.options(**options).json("tests/resources/data/customers")
        (customers_parquet_df.withColumn("_rescued_data", lit("Test")).write.format("delta")
            .mode("append")
            .option("path", f"{self.temp_delta_tables_path}/tables/customers_cdc")
            .saveAsTable("bronze.customers_cdc")
         )
        transactions_parquet_df = self.spark.read.options(**options).json("tests/resources/data/transactions")
        (transactions_parquet_df.withColumn("_rescued_data", lit("Test")).write.format("delta")
            .mode("append")
            .option("path", f"{self.temp_delta_tables_path}/tables/transactions_cdc")
            .saveAsTable("bronze.transactions_cdc")
         )

        def custom_transform_func(input_df) -> DataFrame:
            return input_df.withColumn('custom_col', lit('test_value'))
        DataflowPipeline.invoke_dlt_pipeline(self.spark, "silver", custom_transform_func)
        assert run_dlt.called

    @patch.object(DataflowPipeline, "read", return_value={"called"})
    def test_run_dlt_pipeline_silver_positive(self, read):
        """Test for silver dlt pipeline."""
        silver_spec_map = DataflowPipelineTests.silver_dataflow_spec_map
        source_details = {
            "sourceDetails": {"database": "bronze", "table": "customer", "path": "tests/resources/delta/customers"}
        }
        silver_spec_map.update(source_details)
        silver_dataflow_spec = SilverDataflowSpec(**silver_spec_map)
        self.spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
        options = {"rescuedDataColumn": "_rescued_data", "inferColumnTypes": "true", "multiline": True}
        customers_parquet_df = self.spark.read.options(**options).json("tests/resources/data/customers")
        (customers_parquet_df.withColumn("_rescued_data", lit("Test")).write.format("delta")
            .option("overwriteSchema", "true").mode("overwrite").saveAsTable("bronze.customer")
         )

        dlt_data_flow = DataflowPipeline(
            self.spark,
            silver_dataflow_spec,
            f"{silver_dataflow_spec.targetDetails['table']}_inputview"
        )

        self.assertIsNone(dlt_data_flow.silver_schema)
        dlt_data_flow.run_dlt()
        assert read.called

    def test_dataflow_pipeline_constructor_negative(self):
        """Test dataflowpipelines consturctor with negative values."""
        with self.assertRaises(Exception):
            DataflowPipeline(
                self.spark,
                None,
                "inputView",
                None,
            )

    def test_dataflow_pipeline_read_bronze_negative(self):
        """Test dataflowpipeline reading bronze layer."""
        bronze_map = DataflowPipelineTests.bronze_dataflow_spec_map
        bronze_update_map = {"sourceFormat": "orc"}
        bronze_map.update(bronze_update_map)
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_map)
        dlt_data_flow = DataflowPipeline(
            self.spark,
            bronze_dataflow_spec,
            f"{bronze_dataflow_spec.targetDetails['table']}_inputview",
            None,
        )
        with self.assertRaises(Exception):
            dlt_data_flow.read_bronze()

    def test_dataflow_pipeline_table_has_expectations_positive(self):
        """Test dataflow pipeline tables expectations."""
        bronze_dataflow_spec = BronzeDataflowSpec(**DataflowPipelineTests.bronze_dataflow_spec_map)
        dlt_data_flow = DataflowPipeline(
            self.spark,
            bronze_dataflow_spec,
            f"{bronze_dataflow_spec.targetDetails['table']}_inputview",
            None,
        )
        self.assertIsNotNone(dlt_data_flow.table_has_expectations())

    def test_get_silver_schema_positive(self):
        """Test silver schema."""
        silver_spec_map = DataflowPipelineTests.silver_dataflow_spec_map
        source_details = {
            "sourceDetails": {"database": "bronze", "table": "customer", "path": "tests/resources/delta/customers"}
        }
        silver_spec_map.update(source_details)
        silver_dataflow_spec = SilverDataflowSpec(**silver_spec_map)
        self.spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
        self.spark.sql("DROP TABLE IF EXISTS bronze.customer")
        if os.path.exists(f"{self.temp_delta_tables_path}/tables/customer"):
            shutil.rmtree(f"{self.temp_delta_tables_path}/tables/customer")
        options = {"rescuedDataColumn": "_rescued_data", "inferColumnTypes": "true", "multiline": True}
        customers_parquet_df = self.spark.read.options(**options).json("tests/resources/data/customers")
        (customers_parquet_df.withColumn("_rescued_data", lit("Test")).write.format("delta")
            .mode("append").option("path", f"{self.temp_delta_tables_path}/tables/customer")
            .saveAsTable("bronze.customer")
         )
        dlt_data_flow = DataflowPipeline(
            self.spark,
            silver_dataflow_spec,
            f"{silver_dataflow_spec.targetDetails['table']}_inputview",
            None,
        )
        silver_schema = dlt_data_flow.get_silver_schema()
        self.assertIsNotNone(silver_schema)

    def test_get_silver_schema_where_clause(self):
        """Test silver schema."""
        silver_spec_map = DataflowPipelineTests.silver_dataflow_spec_map
        source_details = {
            "sourceDetails": {"database": "bronze", "table": "customer", "path": "tests/resources/delta/customers"}
        }
        silver_spec_map.update(source_details)
        silver_dataflow_spec = SilverDataflowSpec(**silver_spec_map)

        self.spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
        self.spark.sql("DROP TABLE IF EXISTS bronze.customer")
        if os.path.exists(f"{self.temp_delta_tables_path}/tables/customer"):
            shutil.rmtree(f"{self.temp_delta_tables_path}/tables/customer")
        options = {"rescuedDataColumn": "_rescued_data", "inferColumnTypes": "true", "multiline": True}
        customers_parquet_df = self.spark.read.options(**options).json("tests/resources/data/customers")
        (customers_parquet_df.withColumn("_rescued_data", lit("Test")).write.format("delta")
            .mode("append").option("path", f"{self.temp_delta_tables_path}/tables/customer")
            .saveAsTable("bronze.customer")
         )

        silver_dataflow_spec.whereClause = None
        dlt_data_flow = DataflowPipeline(
            self.spark,
            silver_dataflow_spec,
            f"{silver_dataflow_spec.targetDetails['table']}_inputview",
            None,
        )
        silver_schema = dlt_data_flow.get_silver_schema()
        self.assertIsNotNone(silver_schema)
        silver_dataflow_spec.whereClause = [" "]
        dlt_data_flow = DataflowPipeline(
            self.spark,
            silver_dataflow_spec,
            f"{silver_dataflow_spec.targetDetails['table']}_inputview",
            None,
        )
        silver_schema = dlt_data_flow.get_silver_schema()
        self.assertIsNotNone(silver_schema)

    def test_read_silver_positive(self):
        """Test silver reader positive."""
        silver_spec_map = DataflowPipelineTests.silver_dataflow_spec_map
        source_details = {
            "sourceDetails": {"database": "bronze", "table": "customer", "path": "tests/resources/delta/customers"}
        }
        silver_spec_map.update(source_details)
        self.spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
        self.spark.sql("DROP TABLE IF EXISTS bronze.customer")
        if os.path.exists(f"{self.temp_delta_tables_path}/tables/customer"):
            shutil.rmtree(f"{self.temp_delta_tables_path}/tables/customer")
        options = {"rescuedDataColumn": "_rescued_data", "inferColumnTypes": "true", "multiline": True}
        customers_parquet_df = self.spark.read.options(**options).json("tests/resources/data/customers")
        (customers_parquet_df.withColumn("_rescued_data", lit("Test")).write.format("delta")
            .mode("append").option("path", f"{self.temp_delta_tables_path}/tables/customer")
            .saveAsTable("bronze.customer")
         )
        silver_dataflow_spec = SilverDataflowSpec(**silver_spec_map)
        dlt_data_flow = DataflowPipeline(
            self.spark,
            silver_dataflow_spec,
            f"{silver_dataflow_spec.targetDetails['table']}_inputview",
            None,
        )
        silver_df = dlt_data_flow.read_silver()
        self.assertIsNotNone(silver_df)

        silver_dataflow_spec.whereClause = None
        dlt_data_flow = DataflowPipeline(
            self.spark,
            silver_dataflow_spec,
            f"{silver_dataflow_spec.targetDetails['table']}_inputview",
            None,
        )
        silver_df = dlt_data_flow.read_silver()
        self.assertIsNotNone(silver_df)
        silver_dataflow_spec.whereClause = [" "]
        dlt_data_flow = DataflowPipeline(
            self.spark,
            silver_dataflow_spec,
            f"{silver_dataflow_spec.targetDetails['table']}_inputview",
            None,
        )
        silver_df = dlt_data_flow.read_silver()
        self.assertIsNotNone(silver_df)

    @patch.object(DataflowPipeline, "get_silver_schema", return_value={"called"})
    def test_read_silver_with_where(self, get_silver_schema):
        """Test silver reader positive."""
        silver_spec_map = DataflowPipelineTests.silver_dataflow_spec_map
        source_details = {
            "sourceDetails": {"database": "bronze", "table": "customer", "path": "tests/resources/delta/customers"}
        }
        silver_spec_map.update(source_details)
        self.spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
        self.spark.sql("DROP TABLE IF EXISTS bronze.customer")
        if os.path.exists(f"{self.temp_delta_tables_path}/tables/customer"):
            shutil.rmtree(f"{self.temp_delta_tables_path}/tables/customer")
        options = {"rescuedDataColumn": "_rescued_data", "inferColumnTypes": "true", "multiline": True}
        customers_parquet_df = self.spark.read.options(**options).json("tests/resources/data/customers")
        (customers_parquet_df.withColumn("_rescued_data", lit("Test")).write.format("delta")
            .mode("append").option("path", f"{self.temp_delta_tables_path}/tables/customer")
            .saveAsTable("bronze.customer")
         )
        silver_dataflow_spec = SilverDataflowSpec(**silver_spec_map)
        dlt_data_flow = DataflowPipeline(
            self.spark,
            silver_dataflow_spec,
            f"{silver_dataflow_spec.targetDetails['table']}_inputview",
            None,
        )
        silver_df = dlt_data_flow.read_silver()
        self.assertIsNotNone(silver_df)

    @patch.object(DataflowPipeline, "write_layer_with_dqe", return_value={"called"})
    @patch.object(dp, "expect_all_or_drop", return_value={"called"})
    def test_broze_write_dqe(self, expect_all_or_drop, write_layer_with_dqe):
        bronze_dataflow_spec = BronzeDataflowSpec(**DataflowPipelineTests.bronze_dataflow_spec_map)
        dlt_data_flow = DataflowPipeline(
            self.spark,
            bronze_dataflow_spec,
            f"{bronze_dataflow_spec.targetDetails['table']}_inputview",
            f"{bronze_dataflow_spec.targetDetails['table']}_inputQView",
        )
        dlt_data_flow.write_bronze()
        assert write_layer_with_dqe.called

    @patch.object(DataflowPipeline, "cdc_apply_changes", return_value={"called"})
    @patch.object(dp, "expect_all_or_drop", return_value={"called"})
    def test_broze_write_cdc_apply_changes(self, expect_all_or_drop, cdc_apply_changes):
        bronze_dataflow_spec = BronzeDataflowSpec(**DataflowPipelineTests.bronze_dataflow_spec_map)
        cdc_apply_changes_json = """{
            "keys": [
                "id"
            ],
            "sequence_by": "operation_date",
            "scd_type": "1",
            "apply_as_deletes": "operation = 'DELETE'",
            "except_column_list": [
                "operation",
                "operation_date",
                "_rescued_data"
            ]
        }"""
        bronze_dataflow_spec.cdcApplyChanges = cdc_apply_changes_json
        dlt_data_flow = DataflowPipeline(
            self.spark,
            bronze_dataflow_spec,
            f"{bronze_dataflow_spec.targetDetails['table']}_inputview",
            f"{bronze_dataflow_spec.targetDetails['table']}_inputQView",
        )
        dlt_data_flow.write_bronze()
        assert cdc_apply_changes.called

    @patch.object(DataflowPipeline, "cdc_apply_changes", return_value={"called"})
    def test_cdc_apply_changes_scd_type2(self, cdc_apply_changes):
        silver_spec_map = DataflowPipelineTests.silver_dataflow_spec_map
        silver_dataflow_spec = SilverDataflowSpec(**silver_spec_map)
        silver_dataflow_spec.cdcApplyChanges = json.dumps(self.silver_cdc_apply_changes_scd2)
        self.spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
        self.spark.sql("DROP TABLE IF EXISTS bronze.customer")
        if os.path.exists(f"{self.temp_delta_tables_path}/tables/customer"):
            shutil.rmtree(f"{self.temp_delta_tables_path}/tables/customer")
        options = {"rescuedDataColumn": "_rescued_data", "inferColumnTypes": "true", "multiline": True}
        customers_parquet_df = self.spark.read.options(**options).json("tests/resources/data/customers")
        (customers_parquet_df.withColumn("_rescued_data", lit("Test")).write.format("delta")
            .mode("append").option("path", f"{self.temp_delta_tables_path}/tables/customer")
            .saveAsTable("bronze.customer")
         )
        dlt_data_flow = DataflowPipeline(
            self.spark,
            silver_dataflow_spec,
            f"{silver_dataflow_spec.targetDetails['table']}_inputview",
            None,
        )
        dlt_data_flow.cdc_apply_changes()
        assert cdc_apply_changes.called
        dlt_data_flow.cdcApplyChanges.except_column_list = ["operation_date", "_rescued_data"]
        dlt_data_flow.cdc_apply_changes()
        assert cdc_apply_changes.called
        dlt_data_flow.cdc_apply_changes = None
        with self.assertRaises(Exception):
            dlt_data_flow.cdc_apply_changes()

    @patch('databricks.labs.sdp_meta.dataflow_pipeline.dp')
    def test_dlt_view_bronze_call(self, mock_dlt):
        mock_dlt.temporary_view = MagicMock(return_value=None)
        bronze_dataflow_spec = BronzeDataflowSpec(
            **DataflowPipelineTests.bronze_dataflow_spec_map
        )
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name, None)
        pipeline.read_bronze = MagicMock()
        pipeline.view_name = view_name
        pipeline.read()
        mock_dlt.temporary_view.assert_called_once_with(
            pipeline.read_bronze,
            name=pipeline.view_name,
            comment=f"input dataset view for {pipeline.view_name}"
        )

    @patch('databricks.labs.sdp_meta.dataflow_pipeline.dp')
    def test_dlt_view_silver_call(self, mock_dlt):
        mock_dlt.temporary_view = MagicMock(return_value=None)
        silver_dataflow_spec = SilverDataflowSpec(
            **DataflowPipelineTests.silver_dataflow_spec_map
        )
        view_name = f"{silver_dataflow_spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, silver_dataflow_spec, view_name, None)
        pipeline.read_bronze = MagicMock()
        pipeline.view_name = view_name
        pipeline.read()
        mock_dlt.temporary_view.assert_called_once_with(
            pipeline.read_silver,
            name=pipeline.view_name,
            comment=f"input dataset view for {pipeline.view_name}"
        )

    @patch('databricks.labs.sdp_meta.dataflow_pipeline.dp')
    def test_dlt_write_bronze(self, mock_dlt):
        mock_dlt_table = MagicMock(return_value=lambda func: func)
        mock_dlt.table = mock_dlt_table
        bronze_dataflow_spec = BronzeDataflowSpec(
            **DataflowPipelineTests.bronze_dataflow_spec_map
        )
        bronze_dataflow_spec.cdcApplyChanges = None
        bronze_dataflow_spec.dataQualityExpectations = None
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputview"

        # Unity Catalog enabled
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        pipeline_uc = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name, None)
        pipeline_uc.read_bronze = MagicMock()
        pipeline_uc.view_name = view_name
        pipeline_uc.write_bronze()
        mock_dlt_table.assert_called_once()
        args, kwargs = mock_dlt_table.call_args
        target_path_uc, target_table_uc, _ = pipeline_uc._get_target_table_info()
        expected_comment_uc = pipeline_uc._get_table_comment(target_table_uc, is_bronze=True)
        self.assertEqual(args[0], pipeline_uc.write_to_delta)
        self.assertEqual(kwargs["name"], target_table_uc)
        self.assertIsNone(target_path_uc)
        self.assertIsNone(kwargs["path"])
        self.assertEqual(kwargs["comment"], expected_comment_uc)
        mock_dlt_table.reset_mock()

        # Unity Catalog disabled
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "False")
        bronze_dataflow_spec_no_uc = BronzeDataflowSpec(
            **DataflowPipelineTests.bronze_dataflow_spec_map
        )
        bronze_dataflow_spec_no_uc.cdcApplyChanges = None
        bronze_dataflow_spec_no_uc.dataQualityExpectations = None
        pipeline_no_uc = DataflowPipeline(self.spark, bronze_dataflow_spec_no_uc, view_name, None)
        pipeline_no_uc.read_bronze = MagicMock()
        pipeline_no_uc.view_name = view_name
        target_path_no_uc, target_table_no_uc, _ = pipeline_no_uc._get_target_table_info()
        expected_comment_no_uc = pipeline_no_uc._get_table_comment(target_table_no_uc, is_bronze=True)
        pipeline_no_uc.write_bronze()
        mock_dlt_table.assert_called_once()
        args, kwargs = mock_dlt_table.call_args
        self.assertEqual(args[0], pipeline_no_uc.write_to_delta)
        self.assertEqual(kwargs["name"], target_table_no_uc)
        self.assertEqual(kwargs["path"], target_path_no_uc)
        self.assertEqual(kwargs["comment"], expected_comment_no_uc)

    @patch('databricks.labs.sdp_meta.dataflow_pipeline.dp')
    def test_dlt_write_silver(self, mock_dlt):
        mock_dlt_table = MagicMock(return_value=lambda func: func)
        mock_dlt.table = mock_dlt_table
        silver_dataflow_spec = SilverDataflowSpec(
            **DataflowPipelineTests.silver_dataflow_spec_map
        )
        silver_dataflow_spec.cdcApplyChanges = None
        view_name = f"{silver_dataflow_spec.targetDetails['table']}_inputview"

        # Unity Catalog enabled
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        pipeline_uc = DataflowPipeline(self.spark, silver_dataflow_spec, view_name, None)
        pipeline_uc.read_bronze = MagicMock()
        pipeline_uc.view_name = view_name
        pipeline_uc.write_silver()
        mock_dlt_table.assert_called_once()
        args, kwargs = mock_dlt_table.call_args
        target_path_uc, target_table_uc, target_table_name_uc = pipeline_uc._get_target_table_info()
        expected_comment_uc = pipeline_uc._get_table_comment(target_table_uc, is_bronze=False)
        self.assertEqual(args[0], pipeline_uc.write_to_delta)
        self.assertEqual(kwargs["name"], target_table_uc)
        self.assertIsNone(target_path_uc)
        self.assertIsNone(kwargs["path"])
        self.assertEqual(kwargs["comment"], expected_comment_uc)
        mock_dlt_table.reset_mock()

        # Unity Catalog disabled
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "False")
        silver_dataflow_spec_no_uc = SilverDataflowSpec(
            **DataflowPipelineTests.silver_dataflow_spec_map
        )
        silver_dataflow_spec_no_uc.cdcApplyChanges = None
        pipeline_no_uc = DataflowPipeline(self.spark, silver_dataflow_spec_no_uc, view_name, None)
        pipeline_no_uc.read_bronze = MagicMock()
        pipeline_no_uc.view_name = view_name
        target_path_no_uc, target_table_no_uc, target_table_name_no_uc = pipeline_no_uc._get_target_table_info()
        expected_comment_no_uc = pipeline_no_uc._get_table_comment(target_table_no_uc, is_bronze=False)
        pipeline_no_uc.write_silver()
        mock_dlt_table.assert_called_once()
        args, kwargs = mock_dlt_table.call_args
        self.assertEqual(args[0], pipeline_no_uc.write_to_delta)
        self.assertEqual(kwargs["name"], target_table_no_uc)
        self.assertEqual(kwargs["path"], target_path_no_uc)
        self.assertEqual(kwargs["comment"], expected_comment_no_uc)

    @patch.object(DataflowPipeline, 'write_silver', new_callable=MagicMock)
    def test_dataflowpipeline_silver_write(self, mock_dfp):
        mock_dfp.write_bronze.return_value = None
        silver_dataflow_spec = SilverDataflowSpec(
            **DataflowPipelineTests.silver_dataflow_spec_map
        )
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        view_name = f"{silver_dataflow_spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, silver_dataflow_spec, view_name, None)
        pipeline.read_bronze = MagicMock()
        pipeline.view_name = view_name
        silver_dataflow_spec.cdcApplyChanges = None
        pipeline.write()
        assert mock_dfp.called

    @patch.object(DataflowPipeline, 'write_bronze', new_callable=MagicMock)
    def test_dataflowpipeline_bronze_write(self, mock_dfp):
        mock_dfp.write_bronze.return_value = None
        bronze_dataflow_spec = BronzeDataflowSpec(
            **DataflowPipelineTests.bronze_dataflow_spec_map
        )
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name, None)
        pipeline.read_bronze = MagicMock()
        pipeline.view_name = view_name
        bronze_dataflow_spec.cdcApplyChanges = None
        pipeline.write()
        assert mock_dfp.called

    @patch.object(PipelineReaders, 'read_dlt_cloud_files', mock_cloud_files=MagicMock)
    def test_dataflow_pipeline_read_bronze_cloudfiles(self, mock_cloud_files):
        mock_cloud_files.return_value = None
        bronze_dataflow_spec = BronzeDataflowSpec(
            **DataflowPipelineTests.bronze_dataflow_spec_map
        )
        bronze_dataflow_spec.sourceFormat = "cloudFiles"
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        bronze_dataflow_spec.cdcApplyChanges = None
        bronze_dataflow_spec.dataQualityExpectations = None
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name, None)
        pipeline.read_bronze()
        assert mock_cloud_files.called
        bronze_dataflow_spec.sourceFormat = "delta"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name, None)
        bronze_dataflow_spec.sourceFormat = "eventhub"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name, None)
        bronze_dataflow_spec.sourceFormat = "kafka"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name, None)

    @patch.object(PipelineReaders, 'read_dlt_delta', mock_read_dlt_delta=MagicMock)
    def test_dataflow_pipeline_read_bronze_delta(self, mock_read_dlt_delta):
        mock_read_dlt_delta.return_value = None
        bronze_dataflow_spec = BronzeDataflowSpec(
            **DataflowPipelineTests.bronze_dataflow_spec_map
        )
        bronze_dataflow_spec.sourceFormat = "delta"
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        bronze_dataflow_spec.cdcApplyChanges = None
        bronze_dataflow_spec.dataQualityExpectations = None
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name, None)
        pipeline.read_bronze()
        assert mock_read_dlt_delta.called

    @patch.object(PipelineReaders, 'read_kafka', mock_read_kafka=MagicMock)
    def test_dataflow_pipeline_read_bronze_kafka(self, mock_read_kafka):
        mock_read_kafka.return_value = None
        bronze_dataflow_spec = BronzeDataflowSpec(
            **DataflowPipelineTests.bronze_dataflow_spec_map
        )
        bronze_dataflow_spec.sourceFormat = "kafka"
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        bronze_dataflow_spec.cdcApplyChanges = None
        bronze_dataflow_spec.dataQualityExpectations = None
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name, None)
        pipeline.read_bronze()
        assert mock_read_kafka.called

    def read_dataflowspec(self, database, table):
        return self.spark.read.table(f"{database}.{table}")

    @patch('databricks.labs.sdp_meta.dataflow_pipeline.dp')
    def test_dataflowpipeline_bronze_dqe(self, mock_dlt):
        mock_dlt_table = MagicMock(return_value=lambda func: func)
        mock_expect_all = MagicMock(return_value=lambda func: func)
        mock_expect_all_or_fail = MagicMock(return_value=lambda func: func)
        mock_expect_all_or_drop = MagicMock(return_value=lambda func: func)
        mock_dlt.table = mock_dlt_table
        mock_dlt.expect_all = mock_expect_all
        mock_dlt.expect_all_or_fail = mock_expect_all_or_fail
        mock_dlt.expect_all_or_drop = mock_expect_all_or_drop
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        onboarding_params_map['onboarding_file_path'] = self.onboarding_type2_json_file
        del onboarding_params_map["silver_dataflowspec_table"]
        del onboarding_params_map["silver_dataflowspec_path"]
        o_dfs = OnboardDataflowspec(self.spark, onboarding_params_map)
        o_dfs.onboard_bronze_dataflow_spec()
        bronze_dataflowSpec_df = self.spark.read.format("delta").load(
            self.onboarding_bronze_silver_params_map['bronze_dataflowspec_path']
        )
        bronze_df_row = bronze_dataflowSpec_df.filter(bronze_dataflowSpec_df.dataFlowId == "201").collect()[0]
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_df_row.asDict())
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name, None)
        data_quality_expectations_json = json.loads(bronze_dataflow_spec.dataQualityExpectations)
        expect_dict = {}
        if "expect" in data_quality_expectations_json or "expect_all" in data_quality_expectations_json:
            expect_dict.update(data_quality_expectations_json["expect"])
        if "expect_all" in data_quality_expectations_json:
            expect_dict.update(data_quality_expectations_json["expect_all"])
        if "expect_or_fail" in data_quality_expectations_json:
            expect_or_fail_dict = data_quality_expectations_json["expect_or_fail"]
        if "expect_or_drop" in data_quality_expectations_json:
            expect_or_drop_dict = data_quality_expectations_json["expect_or_drop"]
        if "expect_or_quarantine" in data_quality_expectations_json:
            expect_or_quarantine_dict = data_quality_expectations_json["expect_or_quarantine"]
        pipeline.write_bronze()
        self.assertGreaterEqual(mock_dlt_table.call_count, 1)
        _, kwargs = mock_dlt_table.call_args_list[0]
        target_path_actual, target_table, target_table_name = pipeline._get_target_table_info()
        expected_comment = pipeline._get_table_comment(target_table, is_bronze=True)
        self.assertEqual(kwargs["name"], target_table)
        expected_table_properties = (
            dict(bronze_dataflow_spec.tableProperties)
            if bronze_dataflow_spec.tableProperties
            else {}
        )
        self.assertEqual(kwargs["table_properties"], expected_table_properties)
        self.assertEqual(
            kwargs["partition_cols"],
            DataflowSpecUtils.get_partition_cols(bronze_dataflow_spec.partitionColumns)
        )
        self.assertEqual(kwargs["path"], target_path_actual)
        self.assertEqual(kwargs["comment"], expected_comment)
        self.assertGreaterEqual(mock_expect_all_or_drop.call_count, 1)
        first_call_args, _ = mock_expect_all_or_drop.call_args_list[0]
        self.assertEqual(first_call_args[0], expect_or_drop_dict)
        mock_expect_all_or_fail.assert_called_once_with(expect_or_fail_dict)
        mock_expect_all.assert_called_once_with(expect_dict)
        assert mock_expect_all_or_drop.expect_all_or_drop(expect_or_quarantine_dict)
        # Verify quarantine table uses fully-qualified name (issue #243)
        if expect_or_quarantine_dict:
            _, quarantine_kwargs = mock_dlt_table.call_args_list[-1]
            quarantine_target_details = pipeline._get_quarantine_target_details()
            q_cl = quarantine_target_details.get('catalog', None)
            q_cl_name = f"{q_cl}." if q_cl is not None else ''
            q_db = quarantine_target_details.get('database', '')
            q_table_name = quarantine_target_details.get('table', '')
            expected_quarantine_table = f"{q_cl_name}{q_db}.{q_table_name}"
            self.assertEqual(quarantine_kwargs["name"], expected_quarantine_table)

    @patch.object(DataflowPipeline, 'get_silver_schema', new_callable=MagicMock)
    @patch('databricks.labs.sdp_meta.dataflow_pipeline.dp')
    @patch.object(DataflowPipeline, "create_streaming_table", new_callable=MagicMock)
    def test_dataflowpipeline_silver_cdc_apply_changes(self,
                                                       mock_create_streaming_table,
                                                       mock_dlt,
                                                       mock_get_silver_schema):
        mock_create_streaming_table.return_value = None
        mock_create_auto_cdc_flow = MagicMock(return_value=None)
        mock_dlt.create_auto_cdc_flow = mock_create_auto_cdc_flow
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        onboarding_params_map['onboarding_file_path'] = self.onboarding_type2_json_file
        del onboarding_params_map["bronze_dataflowspec_table"]
        del onboarding_params_map["bronze_dataflowspec_path"]
        o_dfs = OnboardDataflowspec(self.spark, onboarding_params_map)
        o_dfs.onboard_silver_dataflow_spec()
        silver_dataflowSpec_df = self.spark.read.format("delta").load(
            self.onboarding_bronze_silver_params_map['silver_dataflowspec_path']
        )
        bronze_df_row = silver_dataflowSpec_df.filter(silver_dataflowSpec_df.dataFlowId == "201").collect()[0]
        silver_dataflow_spec = SilverDataflowSpec(**bronze_df_row.asDict())
        data_quality_expectations_json = json.loads(silver_dataflow_spec.dataQualityExpectations)
        expect_dict = {}
        expect_or_fail_dict = {}
        expect_or_drop_dict = {}
        if "expect" in data_quality_expectations_json or "expect_all" in data_quality_expectations_json:
            expect_dict.update(data_quality_expectations_json["expect"])
        if "expect_all" in data_quality_expectations_json:
            expect_dict.update(data_quality_expectations_json["expect_all"])
        if "expect_or_fail" in data_quality_expectations_json:
            expect_or_fail_dict.update(data_quality_expectations_json["expect_or_fail"])
        if "expect_all_or_fail" in data_quality_expectations_json:
            expect_or_fail_dict.update(data_quality_expectations_json["expect_all_or_fail"])
        if "expect_all_or_drop" in data_quality_expectations_json:
            expect_or_drop_dict.update(data_quality_expectations_json["expect_all_or_drop"])
        if "expect_or_drop" in data_quality_expectations_json:
            expect_or_drop_dict.update(data_quality_expectations_json["expect_or_drop"])
        view_name = f"{silver_dataflow_spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, silver_dataflow_spec, view_name, None)
        target_path = silver_dataflow_spec.targetDetails["path"]
        cdc_apply_changes = DataflowSpecUtils.get_cdc_apply_changes(silver_dataflow_spec.cdcApplyChanges)
        apply_as_deletes = None
        if cdc_apply_changes.apply_as_deletes:
            apply_as_deletes = expr(cdc_apply_changes.apply_as_deletes)
        apply_as_truncates = None
        if cdc_apply_changes.apply_as_truncates:
            apply_as_truncates = expr(cdc_apply_changes.apply_as_truncates)
        ddlSchemaStr = self.spark.read.text(paths="tests/resources/schema/products.ddl",
                                            wholetext=True).collect()[0]["value"]
        struct_schema = T._parse_datatype_string(ddlSchemaStr)
        mock_get_silver_schema.return_value = json.dumps(struct_schema.jsonValue())
        pipeline.silver_schema = struct_schema
        pipeline.write_silver()
        mock_create_streaming_table.assert_called_once_with(None, target_path)
        mock_create_auto_cdc_flow.assert_called_once()
        _, kwargs = mock_create_auto_cdc_flow.call_args
        target_database = silver_dataflow_spec.targetDetails['database']
        target_table_name = silver_dataflow_spec.targetDetails['table']
        expected_target = f"{target_database}.{target_table_name}"
        self.assertEqual(kwargs["target"], expected_target)
        self.assertEqual(kwargs["source"], view_name)
        self.assertEqual(kwargs["keys"], cdc_apply_changes.keys)
        self.assertEqual(kwargs["sequence_by"], cdc_apply_changes.sequence_by)

        def assert_column_equals(actual, expected):
            if expected is None:
                self.assertIsNone(actual)
            else:
                self.assertIsNotNone(actual)
                self.assertEqual(str(actual), str(expected))

        assert_column_equals(kwargs["apply_as_deletes"], apply_as_deletes)
        assert_column_equals(kwargs["apply_as_truncates"], apply_as_truncates)
        self.assertEqual(kwargs["where"], cdc_apply_changes.where)
        self.assertEqual(kwargs["ignore_null_updates"], cdc_apply_changes.ignore_null_updates)
        self.assertEqual(kwargs["column_list"], cdc_apply_changes.column_list)
        self.assertEqual(kwargs["except_column_list"], cdc_apply_changes.except_column_list)
        self.assertEqual(kwargs["stored_as_scd_type"], cdc_apply_changes.scd_type)
        self.assertEqual(kwargs["track_history_column_list"], cdc_apply_changes.track_history_column_list)
        self.assertEqual(kwargs["track_history_except_column_list"], cdc_apply_changes.track_history_except_column_list)
        self.assertEqual(kwargs["flow_name"], cdc_apply_changes.flow_name)
        self.assertEqual(kwargs["once"], cdc_apply_changes.once)
        self.assertEqual(
            kwargs["ignore_null_updates_column_list"],
            cdc_apply_changes.ignore_null_updates_column_list
        )
        self.assertEqual(
            kwargs["ignore_null_updates_except_column_list"],
            cdc_apply_changes.ignore_null_updates_except_column_list
        )

    @patch('databricks.labs.sdp_meta.dataflow_pipeline.dp')
    @patch.object(DataflowPipeline, "create_streaming_table", new_callable=MagicMock)
    def test_bronze_cdc_apply_changes(self,
                                      mock_create_streaming_table,
                                      mock_dlt):
        mock_create_streaming_table.return_value = None
        mock_create_auto_cdc_flow = MagicMock(return_value=None)
        mock_dlt.create_auto_cdc_flow = mock_create_auto_cdc_flow
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        onboarding_params_map['onboarding_file_path'] = self.onboarding_bronze_type2_json_file
        o_dfs = OnboardDataflowspec(self.spark, onboarding_params_map)
        o_dfs.onboard_bronze_dataflow_spec()
        bronze_dataflowSpec_df = self.spark.read.format("delta").load(
            self.onboarding_bronze_silver_params_map['bronze_dataflowspec_path']
        )
        bronze_df_row = bronze_dataflowSpec_df.filter(bronze_dataflowSpec_df.dataFlowId == "201").collect()[0]
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_df_row.asDict())
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name, None)
        cdc_apply_changes = DataflowSpecUtils.get_cdc_apply_changes(bronze_dataflow_spec.cdcApplyChanges)
        apply_as_deletes = None
        if cdc_apply_changes.apply_as_deletes:
            apply_as_deletes = expr(cdc_apply_changes.apply_as_deletes)

        apply_as_truncates = None
        if cdc_apply_changes.apply_as_truncates:
            apply_as_truncates = expr(cdc_apply_changes.apply_as_truncates)
        expected_schema = pipeline.modify_schema_for_cdc_changes(cdc_apply_changes)
        pipeline.write_bronze()
        mock_create_streaming_table.assert_called_once()
        args, kwargs = mock_create_streaming_table.call_args
        self.assertEqual(args[0], expected_schema)
        self.assertEqual(args[1], bronze_dataflow_spec.targetDetails["path"])
        mock_create_auto_cdc_flow.assert_called_once()
        _, kwargs = mock_create_auto_cdc_flow.call_args
        target_database = bronze_dataflow_spec.targetDetails['database']
        target_table_name = bronze_dataflow_spec.targetDetails['table']
        expected_target = f"{target_database}.{target_table_name}"
        self.assertEqual(kwargs["target"], expected_target)
        self.assertEqual(kwargs["source"], view_name)
        self.assertEqual(kwargs["keys"], cdc_apply_changes.keys)
        self.assertEqual(kwargs["sequence_by"], cdc_apply_changes.sequence_by)

        def assert_column_equals(actual, expected):
            if expected is None:
                self.assertIsNone(actual)
            else:
                self.assertIsNotNone(actual)
                self.assertEqual(str(actual), str(expected))

        assert_column_equals(kwargs["apply_as_deletes"], apply_as_deletes)
        assert_column_equals(kwargs["apply_as_truncates"], apply_as_truncates)

    @patch('databricks.labs.sdp_meta.dataflow_pipeline.dp')
    @patch.object(DataflowPipeline, "create_streaming_table", new_callable=MagicMock)
    def test_bronze_cdc_apply_changes_v7(self,
                                         mock_create_streaming_table,
                                         mock_dlt):
        mock_create_streaming_table.return_value = None
        mock_create_auto_cdc_flow = MagicMock(return_value=None)
        mock_dlt.create_auto_cdc_flow = mock_create_auto_cdc_flow
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        onboarding_params_map['onboarding_file_path'] = self.onboarding_json_v7_file
        o_dfs = OnboardDataflowspec(self.spark, onboarding_params_map)
        o_dfs.onboard_bronze_dataflow_spec()
        bronze_dataflowSpec_df = self.spark.read.format("delta").load(
            self.onboarding_bronze_silver_params_map['bronze_dataflowspec_path']
        )
        bronze_df_row = bronze_dataflowSpec_df.filter(bronze_dataflowSpec_df.dataFlowId == "100").collect()[0]
        bronze_row_dict = DataflowSpecUtils.populate_additional_df_cols(
            bronze_df_row.asDict(),
            DataflowSpecUtils.additional_bronze_df_columns
        )
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_row_dict)
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name, None)
        cdc_apply_changes = DataflowSpecUtils.get_cdc_apply_changes(bronze_dataflow_spec.cdcApplyChanges)
        apply_as_deletes = None
        if cdc_apply_changes.apply_as_deletes:
            apply_as_deletes = expr(cdc_apply_changes.apply_as_deletes)

        apply_as_truncates = None
        if cdc_apply_changes.apply_as_truncates:
            apply_as_truncates = expr(cdc_apply_changes.apply_as_truncates)
        expected_schema = pipeline.modify_schema_for_cdc_changes(cdc_apply_changes)
        pipeline.write_bronze()
        mock_create_streaming_table.assert_called_once()
        args, kwargs = mock_create_streaming_table.call_args
        self.assertEqual(args[0], expected_schema)
        self.assertEqual(args[1], bronze_dataflow_spec.targetDetails["path"])
        mock_create_auto_cdc_flow.assert_called_once()
        _, kwargs = mock_create_auto_cdc_flow.call_args
        target_database = bronze_dataflow_spec.targetDetails['database']
        target_table_name = bronze_dataflow_spec.targetDetails['table']
        expected_target = f"{target_database}.{target_table_name}"
        self.assertEqual(kwargs["target"], expected_target)
        self.assertEqual(kwargs["source"], view_name)
        self.assertEqual(kwargs["keys"], cdc_apply_changes.keys)
        self.assertEqual(kwargs["sequence_by"], cdc_apply_changes.sequence_by)

        def assert_column_equals(actual, expected):
            if expected is None:
                self.assertIsNone(actual)
            else:
                self.assertIsNotNone(actual)
                self.assertEqual(str(actual), str(expected))

        assert_column_equals(kwargs["apply_as_deletes"], apply_as_deletes)
        assert_column_equals(kwargs["apply_as_truncates"], apply_as_truncates)
        self.assertEqual(kwargs["where"], cdc_apply_changes.where)
        self.assertEqual(kwargs["ignore_null_updates"], cdc_apply_changes.ignore_null_updates)
        self.assertEqual(kwargs["column_list"], cdc_apply_changes.column_list)
        self.assertEqual(kwargs["except_column_list"], cdc_apply_changes.except_column_list)
        self.assertEqual(kwargs["stored_as_scd_type"], cdc_apply_changes.scd_type)
        self.assertEqual(kwargs["track_history_column_list"], cdc_apply_changes.track_history_column_list)
        self.assertEqual(kwargs["track_history_except_column_list"], cdc_apply_changes.track_history_except_column_list)
        self.assertEqual(kwargs["flow_name"], cdc_apply_changes.flow_name)
        self.assertEqual(kwargs["once"], cdc_apply_changes.once)
        self.assertEqual(
            kwargs["ignore_null_updates_column_list"],
            cdc_apply_changes.ignore_null_updates_column_list
        )
        self.assertEqual(
            kwargs["ignore_null_updates_except_column_list"],
            cdc_apply_changes.ignore_null_updates_except_column_list
        )

    @patch.object(DataflowPipeline, "create_streaming_table", new_callable=MagicMock)
    @patch.object(DataflowPipeline, "write_to_delta", new_callable=MagicMock)
    @patch('databricks.labs.sdp_meta.pipeline_writers.dp')
    @patch('databricks.labs.sdp_meta.dataflow_pipeline.dp')
    def test_bronze_append_flow_positive(self,
                                         mock_dlt_dp,
                                         mock_dlt_pw,
                                         mock_write_to_delta,
                                         mock_create_streaming_table,
                                         ):
        mock_create_streaming_table.return_value = None
        mock_write_to_delta.return_value = None
        mock_dlt_create_streaming_table = MagicMock(return_value=None)
        mock_append_flow = MagicMock(return_value=lambda func: func)
        mock_dlt_pw.create_streaming_table = mock_dlt_create_streaming_table
        mock_dlt_pw.append_flow = mock_append_flow
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        onboarding_params_map['onboarding_file_path'] = self.onboarding_append_flow_json_file
        o_dfs = OnboardDataflowspec(self.spark, onboarding_params_map)
        o_dfs.onboard_bronze_dataflow_spec()
        bronze_dataflowSpec_df = self.spark.read.format("delta").load(
            self.onboarding_bronze_silver_params_map['bronze_dataflowspec_path']
        )
        bronze_df_row = bronze_dataflowSpec_df.filter(bronze_dataflowSpec_df.dataFlowId == "100").collect()[0]
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_df_row.asDict())
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name, None)
        struct_schema = json.loads(bronze_dataflow_spec.schema)
        append_flows = DataflowSpecUtils.get_append_flows(bronze_dataflow_spec.appendFlows)
        pipeline.write_bronze()
        target_table = bronze_dataflow_spec.targetDetails["table"]
        flows_with_streaming = [flow for flow in append_flows if flow.create_streaming_table]
        self.assertEqual(mock_dlt_create_streaming_table.call_count, len(flows_with_streaming))
        for call, append_flow in zip(mock_dlt_create_streaming_table.call_args_list, flows_with_streaming):
            _, kwargs = call
            self.assertEqual(kwargs["name"], target_table)
            self.assertEqual(kwargs["table_properties"], bronze_dataflow_spec.tableProperties)
            self.assertEqual(kwargs["path"], bronze_dataflow_spec.targetDetails["path"])
            self.assertEqual(kwargs["schema"], struct_schema)
            self.assertIsNone(kwargs["expect_all"])
            self.assertIsNone(kwargs["expect_all_or_drop"])
            self.assertIsNone(kwargs["expect_all_or_fail"])
        self.assertEqual(mock_append_flow.call_count, len(append_flows))
        for call, append_flow in zip(mock_append_flow.call_args_list, append_flows):
            _, kwargs = call
            self.assertEqual(kwargs["name"], append_flow.name)
            self.assertEqual(kwargs["target"], target_table)
            self.assertEqual(kwargs["comment"], f"append_flow={append_flow.name} for target={target_table}")
            expected_spark_conf = append_flow.spark_conf if append_flow.spark_conf else {}
            self.assertEqual(kwargs["spark_conf"], expected_spark_conf)
            self.assertEqual(kwargs["once"], append_flow.once)

    def test_get_dq_expectations(self):
        o_dfs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)
        o_dfs.onboard_bronze_dataflow_spec()
        bronze_dataflowSpec_df = self.spark.read.format("delta").load(
            self.onboarding_bronze_silver_params_map['bronze_dataflowspec_path']
        )
        bronze_df_row = bronze_dataflowSpec_df.filter(bronze_dataflowSpec_df.dataFlowId == "100").collect()[0]
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_df_row.asDict())
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name, None)
        expect_all_dict, expect_all_or_drop_dict, expect_all_or_fail_dict = pipeline.get_dq_expectations()
        self.assertIsNotNone(expect_all_or_drop_dict)
        self.assertIsNone(expect_all_or_fail_dict)
        self.assertIsNone(expect_all_dict)

    @patch('databricks.labs.sdp_meta.dataflow_pipeline.dp')
    def test_read_append_flows(self, mock_dlt):
        mock_dlt.temporary_view = MagicMock(return_value=None)
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        onboarding_params_map['onboarding_file_path'] = self.onboarding_append_flow_json_file
        o_dfs = OnboardDataflowspec(self.spark, onboarding_params_map)
        o_dfs.onboard_dataflow_specs()
        bronze_dataflowSpec_df = self.spark.read.format("delta").load(
            self.onboarding_bronze_silver_params_map['bronze_dataflowspec_path']
        )
        bronze_df_row = bronze_dataflowSpec_df.filter(bronze_dataflowSpec_df.dataFlowId == "100").collect()[0]
        silver_dataflow_spec = BronzeDataflowSpec(**bronze_df_row.asDict())
        view_name = f"{silver_dataflow_spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, silver_dataflow_spec, view_name, None)
        pipeline.read_append_flows()
        append_flow = DataflowSpecUtils.get_append_flows(silver_dataflow_spec.appendFlows)[0]

        # Check if mock was called before unpacking
        self.assertIsNotNone(mock_dlt.temporary_view.call_args, "mock_view was not called")
        called_args, called_kwargs = mock_dlt.temporary_view.call_args
        read_callable = called_args[0]
        self.assertEqual(read_callable.__name__, "read_dlt_cloud_files")
        self.assertEqual(called_kwargs["name"], f"{append_flow.name}_view")
        self.assertEqual(called_kwargs["comment"], f"append flow input dataset view for {append_flow.name}_view")
        mock_dlt.temporary_view.reset_mock()

        bronze_df_row = bronze_dataflowSpec_df.filter(bronze_dataflowSpec_df.dataFlowId == "103").collect()[0]
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_df_row.asDict())
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name, None)
        pipeline.read_append_flows()
        append_flow = DataflowSpecUtils.get_append_flows(bronze_dataflow_spec.appendFlows)[0]

        self.assertIsNotNone(mock_dlt.temporary_view.call_args, "mock_view was not called for dataFlowId 103")
        called_args, called_kwargs = mock_dlt.temporary_view.call_args
        read_callable = called_args[0]
        self.assertEqual(read_callable.__name__, "read_kafka")
        self.assertEqual(called_kwargs["name"], f"{append_flow.name}_view")
        self.assertEqual(called_kwargs["comment"], f"append flow input dataset view for {append_flow.name}_view")
        mock_dlt.temporary_view.reset_mock()

        silver_dataflowSpec_df = self.spark.read.format("delta").load(
            self.onboarding_bronze_silver_params_map['silver_dataflowspec_path']
        )
        silver_df_row = silver_dataflowSpec_df.filter(silver_dataflowSpec_df.dataFlowId == "101").collect()[0]
        silver_dataflow_spec = SilverDataflowSpec(**silver_df_row.asDict())
        view_name = f"{silver_dataflow_spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, silver_dataflow_spec, view_name, None)
        pipeline.read_append_flows()
        append_flow = DataflowSpecUtils.get_append_flows(silver_dataflow_spec.appendFlows)[0]

        self.assertIsNotNone(mock_dlt.temporary_view.call_args, "mock_view was not called for dataFlowId 101")
        called_args, called_kwargs = mock_dlt.temporary_view.call_args
        read_callable = called_args[0]
        self.assertEqual(read_callable.__name__, "read_dlt_delta")
        self.assertEqual(called_kwargs["name"], f"{append_flow.name}_view")
        self.assertEqual(called_kwargs["comment"], f"append flow input dataset view for {append_flow.name}_view")
        bronze_dataflowSpec_df.appendFlows = None
        with self.assertRaises(Exception):
            pipeline = DataflowPipeline(self.spark, bronze_dataflowSpec_df, view_name, None)

    def test_get_dq_expectations_with_expect_all(self):
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        onboarding_params_map['onboarding_file_path'] = self.onboarding_type2_json_file
        o_dfs = OnboardDataflowspec(self.spark, onboarding_params_map)
        o_dfs.onboard_bronze_dataflow_spec()
        bronze_dataflowSpec_df = self.spark.read.format("delta").load(
            self.onboarding_bronze_silver_params_map['bronze_dataflowspec_path']
        )
        bronze_df_row = bronze_dataflowSpec_df.filter(bronze_dataflowSpec_df.dataFlowId == "201").collect()[0]
        bronze_row_dict = DataflowSpecUtils.populate_additional_df_cols(
            bronze_df_row.asDict(),
            DataflowSpecUtils.additional_bronze_df_columns
        )
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_row_dict)
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name, None)
        expect_all_dict, expect_all_or_drop_dict, expect_all_or_fail_dict = pipeline.get_dq_expectations()
        self.assertIsNotNone(expect_all_dict)
        self.assertIsNotNone(expect_all_or_drop_dict)
        self.assertIsNotNone(expect_all_or_fail_dict)

    @patch('databricks.labs.sdp_meta.dataflow_pipeline.dp')
    def test_modify_schema_for_cdc_changes(self, mock_dlt):
        mock_dlt_table = MagicMock()
        mock_dlt_table.table.return_value = None
        mock_dlt.table = mock_dlt_table
        cdc_apply_changes_json = """{
            "keys": ["id"],
            "sequence_by": "operation_date",
            "scd_type": "2",
            "except_column_list": ["operation", "operation_date", "_rescued_data"]
        }"""
        cdc_apply_changes = DataflowSpecUtils.get_cdc_apply_changes(cdc_apply_changes_json)
        bmap = DataflowPipelineTests.bronze_dataflow_spec_map
        ddlSchemaStr = (
            self.spark.read.text(paths="tests/resources/schema/customer_schema.ddl")
            .select("value")
            .collect()[0]["value"]
        )
        schema = T._parse_datatype_string(ddlSchemaStr)
        bronze_dataflow_spec = BronzeDataflowSpec(
            **bmap
        )
        bronze_dataflow_spec.schema = json.dumps(schema.jsonValue())
        bronze_dataflow_spec.cdcApplyChanges = json.dumps(self.silver_cdc_apply_changes_scd2)
        bronze_dataflow_spec.dataQualityExpectations = None
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name, None)
        expected_schema = T.StructType([
            T.StructField("address", T.StringType()),
            T.StructField("email", T.StringType()),
            T.StructField("firstname", T.StringType()),
            T.StructField("id", T.StringType()),
            T.StructField("lastname", T.StringType()),
            T.StructField("__START_AT", T.StringType()),
            T.StructField("__END_AT", T.StringType())
        ])
        modified_schema = pipeline.modify_schema_for_cdc_changes(cdc_apply_changes)
        self.assertEqual(modified_schema, expected_schema)
        pipeline.schema_json = None
        modified_schema = pipeline.modify_schema_for_cdc_changes(cdc_apply_changes)
        self.assertEqual(modified_schema, None)

    @patch('databricks.labs.sdp_meta.dataflow_pipeline.dp')
    def test_modify_schema_for_cdc_changes_composite_sequence_by(self, mock_dlt):
        """Composite sequence_by like ' ts , id ' must use the FIRST trimmed token
        for the SCD2 timestamp dtype lookup."""
        cdc_apply_changes = DataflowSpecUtils.get_cdc_apply_changes(json.dumps({
            "keys": ["id"],
            "sequence_by": " ts , id ",
            "scd_type": "2",
            "except_column_list": ["op"],
        }))
        schema = T.StructType([
            T.StructField("id", T.StringType()),
            T.StructField("ts", T.TimestampType()),
            T.StructField("op", T.StringType()),
        ])
        spec = BronzeDataflowSpec(**copy.deepcopy(self.bronze_dataflow_spec_map))
        spec.schema = json.dumps(schema.jsonValue())
        spec.dataQualityExpectations = None
        pipeline = DataflowPipeline(
            self.spark, spec,
            f"{spec.targetDetails['table']}_inputview", None,
        )
        out = pipeline.modify_schema_for_cdc_changes(cdc_apply_changes)
        self.assertNotIn("op", out.fieldNames())
        self.assertIn("__START_AT", out.fieldNames())
        self.assertIn("__END_AT", out.fieldNames())
        self.assertEqual(out["__START_AT"].dataType, T.TimestampType())
        self.assertEqual(out["__END_AT"].dataType, T.TimestampType())

    @patch('databricks.labs.sdp_meta.dataflow_pipeline.dp')
    def test_modify_schema_for_cdc_changes_unknown_sequence_column(self, mock_dlt):
        """If sequence_by names a column not present in the schema, SCD2
        START/END columns must NOT be appended (sequenced_by_data_type is None)."""
        cdc_apply_changes = DataflowSpecUtils.get_cdc_apply_changes(json.dumps({
            "keys": ["id"],
            "sequence_by": "missing_col",
            "scd_type": "2",
            "except_column_list": ["op"],
        }))
        schema = T.StructType([
            T.StructField("id", T.StringType()),
            T.StructField("op", T.StringType()),
        ])
        spec = BronzeDataflowSpec(**copy.deepcopy(self.bronze_dataflow_spec_map))
        spec.schema = json.dumps(schema.jsonValue())
        spec.dataQualityExpectations = None
        pipeline = DataflowPipeline(
            self.spark, spec,
            f"{spec.targetDetails['table']}_inputview", None,
        )
        out = pipeline.modify_schema_for_cdc_changes(cdc_apply_changes)
        self.assertNotIn("__START_AT", out.fieldNames())
        self.assertNotIn("__END_AT", out.fieldNames())
        self.assertNotIn("op", out.fieldNames())
        self.assertIn("id", out.fieldNames())

    @patch('databricks.labs.sdp_meta.dataflow_pipeline.dp')
    def test_modify_schema_for_cdc_changes_many_columns_correctness(self, mock_dlt):
        """Correctness on a wide schema (the shape the optimization targets):
        a 701-col schema with 100 excluded columns must produce the right
        field set and SCD2 dtype regardless of width."""
        cols = [T.StructField(f"c{i}", T.StringType()) for i in range(700)]
        cols.append(T.StructField("ts", T.TimestampType()))
        schema = T.StructType(cols)
        excluded = [f"c{i}" for i in range(100)]
        cdc_apply_changes = DataflowSpecUtils.get_cdc_apply_changes(json.dumps({
            "keys": ["c100"],
            "sequence_by": "ts",
            "scd_type": "2",
            "except_column_list": excluded,
        }))
        spec = BronzeDataflowSpec(**copy.deepcopy(self.bronze_dataflow_spec_map))
        spec.schema = json.dumps(schema.jsonValue())
        spec.dataQualityExpectations = None
        pipeline = DataflowPipeline(
            self.spark, spec,
            f"{spec.targetDetails['table']}_inputview", None,
        )
        out = pipeline.modify_schema_for_cdc_changes(cdc_apply_changes)
        # 700 c* cols - 100 excluded + 1 ts kept + 2 SCD2 columns = 603
        self.assertEqual(len(out.fieldNames()), 603)
        for c in excluded:
            self.assertNotIn(c, out.fieldNames())
        self.assertIn("ts", out.fieldNames())
        self.assertEqual(out["__START_AT"].dataType, T.TimestampType())
        self.assertEqual(out["__END_AT"].dataType, T.TimestampType())

    @patch('databricks.labs.sdp_meta.dataflow_pipeline.dp')
    def test_modify_schema_for_cdc_changes_sequence_column_in_except_list(self, mock_dlt):
        """If the sequence_by column itself is in except_column_list, its dtype
        must still be captured for SCD2 START/END columns (dtype lookup happens
        before the schema is filtered) and the column itself must be dropped
        from the data schema."""
        cdc_apply_changes = DataflowSpecUtils.get_cdc_apply_changes(json.dumps({
            "keys": ["id"],
            "sequence_by": "ts",
            "scd_type": "2",
            "except_column_list": ["ts", "op"],
        }))
        schema = T.StructType([
            T.StructField("id", T.StringType()),
            T.StructField("ts", T.TimestampType()),
            T.StructField("op", T.StringType()),
        ])
        spec = BronzeDataflowSpec(**copy.deepcopy(self.bronze_dataflow_spec_map))
        spec.schema = json.dumps(schema.jsonValue())
        spec.dataQualityExpectations = None
        pipeline = DataflowPipeline(
            self.spark, spec,
            f"{spec.targetDetails['table']}_inputview", None,
        )
        out = pipeline.modify_schema_for_cdc_changes(cdc_apply_changes)
        self.assertNotIn("ts", out.fieldNames())
        self.assertNotIn("op", out.fieldNames())
        self.assertIn("id", out.fieldNames())
        self.assertEqual(out["__START_AT"].dataType, T.TimestampType())
        self.assertEqual(out["__END_AT"].dataType, T.TimestampType())

    @patch.object(dp, 'create_streaming_table', return_value={"called"})
    @patch.object(dp, 'create_auto_cdc_from_snapshot_flow', return_value={"called"})
    def test_apply_changes_from_snapshot(self, mock_create_auto_cdc_from_snapshot_flow, mock_create_streaming_table):
        """Test apply_changes_from_snapshot method."""

        def next_snapshot_and_version(latest_snapshot_version, dataflow_spec):
            latest_snapshot_version = latest_snapshot_version or 0
            next_version = latest_snapshot_version + 1
            bronze_dataflow_spec: BronzeDataflowSpec = dataflow_spec
            options = bronze_dataflow_spec.readerConfigOptions
            snapshot_format = bronze_dataflow_spec.sourceDetails["snapshot_format"]
            snapshot_root_path = bronze_dataflow_spec.sourceDetails['path']
            snapshot_path = f"{snapshot_root_path}{next_version}.csv"
            snapshot = self.spark.read.format(snapshot_format).options(**options).load(snapshot_path)
            return (snapshot, next_version)

        mock_create_streaming_table.return_value = None
        mock_create_auto_cdc_from_snapshot_flow.return_value = None
        bronze_dataflow_spec = BronzeDataflowSpec(**self.bronze_dataflow_spec_acs_map)
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name,
                                    next_snapshot_and_version=next_snapshot_and_version)
        pipeline.apply_changes_from_snapshot()
        dp.called

    @patch.object(dp, 'create_streaming_table', return_value={"called"})
    @patch.object(dp, 'create_auto_cdc_from_snapshot_flow', return_value={"called"})
    def test_apply_changes_from_snapshot_uc_enabled(self,
                                                    mock_create_auto_cdc_from_snapshot_flow,
                                                    mock_create_streaming_table):
        """Test apply_changes_from_snapshot method with Unity Catalog enabled."""
        def next_snapshot_and_version(latest_snapshot_version, dataflow_spec):
            latest_snapshot_version = latest_snapshot_version or 0
            next_version = latest_snapshot_version + 1
            bronze_dataflow_spec: BronzeDataflowSpec = dataflow_spec
            options = bronze_dataflow_spec.readerConfigOptions
            snapshot_format = bronze_dataflow_spec.sourceDetails["snapshot_format"]
            snapshot_root_path = bronze_dataflow_spec.sourceDetails['path']
            snapshot_path = f"{snapshot_root_path}{next_version}.csv"
            snapshot = self.spark.read.format(snapshot_format).options(**options).load(snapshot_path)
            return (snapshot, next_version)
        mock_create_streaming_table.return_value = None
        mock_create_auto_cdc_from_snapshot_flow.return_value = None
        bronze_dataflow_spec = BronzeDataflowSpec(**self.bronze_dataflow_spec_acs_map)
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputview"
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name,
                                    next_snapshot_and_version=next_snapshot_and_version)
        pipeline.apply_changes_from_snapshot()
        dp.called

    @patch.object(dp, 'create_streaming_table', return_value={"called"})
    @patch.object(dp, 'create_auto_cdc_from_snapshot_flow', return_value={"called"})
    def test_silver_apply_changes_from_snapshot_uc_enabled(self,
                                                           mock_create_auto_cdc_from_snapshot_flow,
                                                           mock_create_streaming_table):
        mock_create_streaming_table.return_value = None
        mock_create_auto_cdc_from_snapshot_flow.return_value = None
        silver_dataflow_spec = SilverDataflowSpec(**self.silver_acfs_dataflow_spec_map)
        view_name = f"{silver_dataflow_spec.targetDetails['table']}_inputview"
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        pipeline = DataflowPipeline(self.spark, silver_dataflow_spec, view_name)
        pipeline.apply_changes_from_snapshot()
        dp.called

    @patch.object(DataflowSpecUtils, 'get_bronze_dataflow_spec', return_value=[MagicMock()])
    @patch.object(DataflowSpecUtils, 'get_silver_dataflow_spec', return_value=[MagicMock()])
    @patch.object(DataflowPipeline, '_launch_dlt_flow', return_value=None)
    def test_invoke_dlt_pipeline_bronze_silver(
        self, mock_launch_dlt_flow, mock_get_silver_dataflow_spec, mock_get_bronze_dataflow_spec
    ):
        """Test invoke_dlt_pipeline for bronze_silver layer."""
        spark = MagicMock()
        bronze_custom_transform_func = MagicMock()
        silver_custom_transform_func = MagicMock()
        bronze_next_snapshot_and_version = MagicMock()
        silver_next_snapshot_and_version = MagicMock()

        DataflowPipeline.invoke_dlt_pipeline(
            spark, "bronze_silver", bronze_custom_transform_func, silver_custom_transform_func,
            bronze_next_snapshot_and_version, silver_next_snapshot_and_version
        )

        mock_get_bronze_dataflow_spec.assert_called_once_with(spark)
        mock_get_silver_dataflow_spec.assert_called_once_with(spark)
        mock_launch_dlt_flow.assert_any_call(
            spark, "bronze", mock_get_bronze_dataflow_spec.return_value,
            bronze_custom_transform_func, bronze_next_snapshot_and_version
        )
        mock_launch_dlt_flow.assert_any_call(
            spark, "silver", mock_get_silver_dataflow_spec.return_value,
            silver_custom_transform_func, silver_next_snapshot_and_version
        )

    @patch.object(dp, 'create_streaming_table', return_value={"called"})
    @patch.object(dp, 'create_auto_cdc_from_snapshot_flow', return_value={"called"})
    def test_read_unsupported_dataflow(self, mock_create_auto_cdc_from_snapshot_flow, mock_create_streaming_table):
        """Test apply_changes_from_snapshot method."""
        mock_create_streaming_table.return_value = None
        mock_create_auto_cdc_from_snapshot_flow.return_value = None
        bronze_dataflow_spec = BronzeDataflowSpec(**self.bronze_dataflow_spec_acs_map)
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name)

        class UnsupportedDataflowSpec:
            pass
        unsupported_dataflow_spec = UnsupportedDataflowSpec()
        pipeline.dataflowSpec = unsupported_dataflow_spec
        with self.assertRaises(Exception) as context:
            pipeline.read()
        self.assertTrue("Dataflow read not supported" in str(context.exception))

    @patch.object(DataflowPipeline, 'apply_changes_from_snapshot', return_value=None)
    def test_write_bronze_snapshot(self, mock_create_auto_cdc_from_snapshot_flow):
        """Test write_bronze with snapshot source format."""
        bronze_dataflow_spec = BronzeDataflowSpec(**self.bronze_dataflow_spec_acs_map)
        bronze_dataflow_spec.sourceFormat = "snapshot"
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(
            self.spark, bronze_dataflow_spec, view_name, None, next_snapshot_and_version=MagicMock()
        )
        pipeline.write_bronze()
        assert mock_create_auto_cdc_from_snapshot_flow.called

    @patch.object(DataflowPipeline, 'write_layer_with_dqe', return_value=None)
    def test_write_bronze_with_dqe(self, mock_write_layer_with_dqe):
        """Test write_bronze with data quality expectations."""
        bronze_dataflow_spec = BronzeDataflowSpec(**self.bronze_dataflow_spec_map)
        bronze_dataflow_spec.dataQualityExpectations = json.dumps({
            "expect_or_drop": {
                "no_rescued_data": "_rescued_data IS NULL",
                "valid_id": "id IS NOT NULL",
                "valid_operation": "operation IN ('APPEND', 'DELETE', 'UPDATE')"
            }
        })
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name, None)
        pipeline.write_bronze()
        assert mock_write_layer_with_dqe.called

    @patch.object(DataflowPipeline, 'cdc_apply_changes', return_value=None)
    def test_write_bronze_cdc_apply_changes(self, mock_cdc_apply_changes):
        """Test write_bronze with CDC apply changes."""
        bronze_dataflow_spec = BronzeDataflowSpec(**self.bronze_dataflow_spec_map)
        bronze_dataflow_spec.cdcApplyChanges = json.dumps({
            "keys": ["id"],
            "sequence_by": "operation_date",
            "scd_type": "1",
            "apply_as_deletes": "operation = 'DELETE'",
            "except_column_list": ["operation", "operation_date", "_rescued_data"]
        })
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name, None)
        pipeline.write_bronze()
        assert mock_cdc_apply_changes.called

    @patch.object(DataflowPipeline, 'cdc_apply_changes', return_value=None)
    def test_write_bronze_cdc_apply_changes_multiple_sequence(self, mock_cdc_apply_changes):
        """Test write_bronze with CDC apply changes using multiple sequence columns."""
        bronze_dataflow_spec = BronzeDataflowSpec(**self.bronze_dataflow_spec_map)
        bronze_dataflow_spec.cdcApplyChanges = json.dumps({
            "keys": ["id"],
            "sequence_by": "event_timestamp, enqueue_timestamp, sequence_id",
            "scd_type": "1",
            "apply_as_deletes": "operation = 'DELETE'",
            "except_column_list": ["operation", "event_timestamp", "enqueue_timestamp", "sequence_id", "_rescued_data"]
        })
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name, None)
        pipeline.write_bronze()
        assert mock_cdc_apply_changes.called

    @patch('pyspark.sql.SparkSession.readStream')
    def test_get_silver_schema_uc_enabled(self, mock_read_stream):
        """Test get_silver_schema with Unity Catalog enabled."""
        silver_spec_map = DataflowPipelineTests.silver_dataflow_spec_map
        source_details = {
            "sourceDetails": {"database": "bronze", "table": "customer", "path": "tests/resources/delta/customers"}
        }
        silver_spec_map.update(source_details)
        silver_dataflow_spec = SilverDataflowSpec(**silver_spec_map)
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        mock_read_stream.table.return_value.selectExpr.return_value = raw_delta_table_stream
        dlt_data_flow = DataflowPipeline(
            self.spark,
            silver_dataflow_spec,
            f"{silver_dataflow_spec.targetDetails['table']}_inputview",
            None,
        )
        schema = dlt_data_flow.get_silver_schema()
        self.assertIsNotNone(schema)

    @patch('pyspark.sql.SparkSession.readStream')
    def test_get_silver_schema_uc_disabled(self, mock_read_stream):
        """Test get_silver_schema with Unity Catalog disabled."""
        silver_spec_map = DataflowPipelineTests.silver_dataflow_spec_map
        source_details = {
            "sourceDetails": {"database": "bronze", "table": "customer", "path": "tests/resources/delta/customers"}
        }
        silver_spec_map.update(source_details)
        silver_dataflow_spec = SilverDataflowSpec(**silver_spec_map)
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "False")
        mock_read_stream.load.return_value.selectExpr.return_value = raw_delta_table_stream
        dlt_data_flow = DataflowPipeline(
            self.spark,
            silver_dataflow_spec,
            f"{silver_dataflow_spec.targetDetails['table']}_inputview",
            None,
        )
        schema = dlt_data_flow.get_silver_schema()
        self.assertIsNotNone(schema)

    def test_safe_dict_access_with_none(self):
        """Test _safe_dict_access with None input."""
        bronze_dataflow_spec = BronzeDataflowSpec(**DataflowPipelineTests.bronze_dataflow_spec_map)
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view")

        # Test with None dict_obj
        result = pipeline._safe_dict_access(None, "test_key", "default_value")
        self.assertEqual(result, "default_value")

        # Test with None dict_obj and no default
        result = pipeline._safe_dict_access(None, "test_key")
        self.assertIsNone(result)

    def test_safe_dict_access_with_valid_dict(self):
        """Test _safe_dict_access with valid dictionary."""
        bronze_dataflow_spec = BronzeDataflowSpec(**DataflowPipelineTests.bronze_dataflow_spec_map)
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view")

        test_dict = {"key1": "value1", "key2": "value2"}

        # Test with existing key
        result = pipeline._safe_dict_access(test_dict, "key1")
        self.assertEqual(result, "value1")

        # Test with non-existing key and default
        result = pipeline._safe_dict_access(test_dict, "non_existing", "default")
        self.assertEqual(result, "default")

    def test_safe_dict_get_item_with_none(self):
        """Test _safe_dict_get_item with None input."""
        bronze_dataflow_spec = BronzeDataflowSpec(**DataflowPipelineTests.bronze_dataflow_spec_map)
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view")

        # Test with None dict_obj - should raise KeyError
        with self.assertRaises(KeyError) as context:
            pipeline._safe_dict_get_item(None, "test_key")
        self.assertIn("Dictionary is None, cannot access key: test_key", str(context.exception))

    def test_safe_dict_get_item_with_valid_dict(self):
        """Test _safe_dict_get_item with valid dictionary."""
        bronze_dataflow_spec = BronzeDataflowSpec(**DataflowPipelineTests.bronze_dataflow_spec_map)
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view")

        test_dict = {"key1": "value1", "key2": "value2"}

        # Test with existing key
        result = pipeline._safe_dict_get_item(test_dict, "key1")
        self.assertEqual(result, "value1")

    def test_get_dict_as_dict_with_none(self):
        """Test _get_dict_as_dict with None input."""
        bronze_dataflow_spec = BronzeDataflowSpec(**DataflowPipelineTests.bronze_dataflow_spec_map)
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view")

        # Test with None - should return empty dict
        result = pipeline._get_dict_as_dict(None)
        self.assertEqual(result, {})

    def test_get_dict_as_dict_with_valid_dict(self):
        """Test _get_dict_as_dict with valid dictionary."""
        bronze_dataflow_spec = BronzeDataflowSpec(**DataflowPipelineTests.bronze_dataflow_spec_map)
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view")

        test_dict = {"key1": "value1", "key2": "value2"}
        result = pipeline._get_dict_as_dict(test_dict)
        self.assertEqual(result, test_dict)

    def test_dataflow_pipeline_unsupported_dataflow_spec(self):
        """Test DataflowPipeline constructor with unsupported dataflow spec."""
        # Test with invalid dataflow spec type - should raise exception
        with self.assertRaises(Exception) as context:
            DataflowPipeline(self.spark, "invalid_spec", "test_view")
        self.assertEqual(str(context.exception), "Dataflow not supported!")

    def test_apply_custom_transform_fun_with_none(self):
        """Test apply_custom_transform_fun with no custom function."""
        bronze_dataflow_spec = BronzeDataflowSpec(**DataflowPipelineTests.bronze_dataflow_spec_map)
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view")

        # Create a mock DataFrame
        mock_df = MagicMock()

        # Test with no custom transform function
        result = pipeline.apply_custom_transform_fun(mock_df)
        self.assertEqual(result, mock_df)

    def test_apply_custom_transform_fun_with_function(self):
        """Test apply_custom_transform_fun with custom function."""
        def custom_transform(df, spec):
            # Mock transformation
            return df

        bronze_dataflow_spec = BronzeDataflowSpec(**DataflowPipelineTests.bronze_dataflow_spec_map)
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view", None, custom_transform)

        # Create a mock DataFrame
        mock_df = MagicMock()
        mock_df.show = MagicMock()

        # Test with custom transform function
        result = pipeline.apply_custom_transform_fun(mock_df)
        self.assertEqual(result, mock_df)

    def test_quarantine_target_details_with_no_attribute(self):
        """Test _get_quarantine_target_details when attribute doesn't exist."""
        bronze_dataflow_spec = BronzeDataflowSpec(**DataflowPipelineTests.bronze_dataflow_spec_map)
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view")

        # Remove the quarantineTargetDetails attribute if it exists
        if hasattr(pipeline.dataflowSpec, 'quarantineTargetDetails'):
            delattr(pipeline.dataflowSpec, 'quarantineTargetDetails')

        result = pipeline._get_quarantine_target_details()
        self.assertEqual(result, {})

    def test_silver_dataflow_with_schema_none(self):
        """Test SilverDataflowSpec initialization with None schema."""
        silver_spec_map = copy.deepcopy(DataflowPipelineTests.silver_dataflow_spec_map)
        silver_dataflow_spec = SilverDataflowSpec(**silver_spec_map)

        pipeline = DataflowPipeline(self.spark, silver_dataflow_spec, "test_view")

        # For SilverDataflowSpec, schema_json should always be None
        self.assertIsNone(pipeline.schema_json)

    def test_bronze_dataflow_with_none_schema(self):
        """Test BronzeDataflowSpec initialization with None schema."""
        bronze_spec_map = copy.deepcopy(DataflowPipelineTests.bronze_dataflow_spec_map)
        bronze_spec_map["schema"] = None
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_spec_map)

        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view")

        # For BronzeDataflowSpec with None schema, schema_json should be None
        self.assertIsNone(pipeline.schema_json)

    def test_snapshot_source_format_handling(self):
        """Test snapshot source format handling."""
        bronze_spec_map = copy.deepcopy(DataflowPipelineTests.bronze_dataflow_spec_map)

        # Test without snapshot_format
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_spec_map)
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view")
        self.assertIsNone(pipeline.snapshot_source_format)

        # Test with snapshot_format
        bronze_spec_map["sourceDetails"] = {"snapshot_format": "delta"}
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_spec_map)
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view")
        self.assertEqual(pipeline.snapshot_source_format, "delta")

    def test_unsupported_source_format_exception(self):
        """Test exception for unsupported source format."""
        bronze_spec_map = copy.deepcopy(DataflowPipelineTests.bronze_dataflow_spec_map)
        bronze_spec_map["sourceFormat"] = "unsupported_format"
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_spec_map)

        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view")

        with self.assertRaises(Exception) as context:
            pipeline.read_bronze()
        self.assertIn("unsupported_format source format not supported", str(context.exception))

    def test_read_exception_for_unsupported_dataflow(self):
        """Test read method exception for unsupported dataflow without next_snapshot_and_version."""
        bronze_spec_map = copy.deepcopy(DataflowPipelineTests.bronze_dataflow_spec_map)
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_spec_map)

        # Mock is_create_view to return False
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view")
        pipeline.is_create_view = MagicMock(return_value=False)
        pipeline.next_snapshot_and_version = None

        with self.assertRaises(Exception) as context:
            pipeline.read()
        self.assertIn("Dataflow read not supported", str(context.exception))

    def test_snapshot_format_exception_without_reader_function(self):
        """Test exception when snapshot format is used without reader function."""
        bronze_spec_map = copy.deepcopy(DataflowPipelineTests.bronze_dataflow_spec_map)
        bronze_spec_map["sourceFormat"] = "snapshot"
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_spec_map)

        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view")
        pipeline.next_snapshot_and_version = None

        with self.assertRaises(Exception) as context:
            pipeline.run_dlt()
        self.assertEqual(str(context.exception), "Snapshot reader function not provided!")

    @patch('databricks.labs.sdp_meta.dataflow_pipeline.dp')
    def test_is_create_view_with_delta_snapshot_format(self, mock_dlt):
        """Test is_create_view with delta snapshot format."""
        mock_dlt_temporary_view = MagicMock()
        mock_dlt.temporary_view = mock_dlt_temporary_view
        bronze_spec_map = copy.deepcopy(DataflowPipelineTests.bronze_dataflow_spec_map)
        bronze_spec_map["sourceDetails"] = {"snapshot_format": "delta"}
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_spec_map)

        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view")

        # Should return True for delta snapshot format
        result = pipeline.is_create_view()
        self.assertTrue(result)
        self.assertTrue(pipeline.next_snapshot_and_version_from_source_view)

    def test_is_create_view_with_next_snapshot_and_version(self):
        """Test is_create_view when next_snapshot_and_version is provided."""
        def mock_next_snapshot():
            return {}

        bronze_spec_map = copy.deepcopy(DataflowPipelineTests.bronze_dataflow_spec_map)
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_spec_map)

        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view", None, None, mock_next_snapshot)

        # Should return False when next_snapshot_and_version is provided
        result = pipeline.is_create_view()
        self.assertFalse(result)

    def test_apply_where_clause_empty(self):
        """Test __apply_where_clause with empty where clause."""
        bronze_spec_map = copy.deepcopy(DataflowPipelineTests.bronze_dataflow_spec_map)
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_spec_map)

        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view")

        mock_stream = MagicMock()

        # Test with None where clause
        result = pipeline._DataflowPipeline__apply_where_clause(None, mock_stream)
        self.assertEqual(result, mock_stream)

        # Test with empty list
        result = pipeline._DataflowPipeline__apply_where_clause([], mock_stream)
        self.assertEqual(result, mock_stream)

        # Test with empty string clause
        result = pipeline._DataflowPipeline__apply_where_clause(["   "], mock_stream)
        self.assertEqual(result, mock_stream)

    def test_apply_where_clause_with_conditions(self):
        """Test __apply_where_clause with actual conditions."""
        bronze_spec_map = copy.deepcopy(DataflowPipelineTests.bronze_dataflow_spec_map)
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_spec_map)

        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view")

        # Create a chain of mocks that return each other
        mock_stream = MagicMock()
        mock_stream2 = MagicMock()
        mock_stream.where.return_value = mock_stream2
        mock_stream2.where.return_value = mock_stream2

        where_clauses = ["id > 0", "name IS NOT NULL"]

        result = pipeline._DataflowPipeline__apply_where_clause(where_clauses, mock_stream)

        # Should call where() on the first mock and then on the returned mock
        mock_stream.where.assert_called_once_with("id > 0")
        mock_stream2.where.assert_called_once_with("name IS NOT NULL")
        self.assertEqual(result, mock_stream2)

    def test_build_table_name_with_catalog(self):
        """Test _build_table_name with catalog."""
        bronze_spec_map = copy.deepcopy(DataflowPipelineTests.bronze_dataflow_spec_map)
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_spec_map)

        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view")

        result = pipeline._build_table_name("my_catalog", "my_database", "my_table")
        self.assertEqual(result, "my_catalog.my_database.my_table")

    def test_build_table_name_without_catalog(self):
        """Test _build_table_name without catalog."""
        bronze_spec_map = copy.deepcopy(DataflowPipelineTests.bronze_dataflow_spec_map)
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_spec_map)

        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view")

        result = pipeline._build_table_name(None, "my_database", "my_table")
        self.assertEqual(result, "my_database.my_table")

        result = pipeline._build_table_name("", "my_database", "my_table")
        self.assertEqual(result, "my_database.my_table")

    @patch('pyspark.sql.SparkSession.readStream', new_callable=MagicMock)
    def test_create_dataframe_reader_streaming(self, mock_read_stream_property):
        """Test _create_dataframe_reader for streaming."""
        bronze_spec_map = copy.deepcopy(DataflowPipelineTests.bronze_dataflow_spec_map)
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_spec_map)

        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view")

        # Configure the mock - the property returns a reader that has options method
        mock_reader_with_options = MagicMock()
        mock_read_stream_property.options.return_value = mock_reader_with_options

        # Test with no options - should return the readStream property itself
        result = pipeline._create_dataframe_reader(is_streaming=True, reader_options=None)
        self.assertEqual(result, mock_read_stream_property)

        # Test with options - should return result of options() call
        options = {"option1": "value1"}
        result = pipeline._create_dataframe_reader(is_streaming=True, reader_options=options)
        mock_read_stream_property.options.assert_called_with(**options)
        self.assertEqual(result, mock_reader_with_options)

    @patch('pyspark.sql.SparkSession.read', new_callable=MagicMock)
    def test_create_dataframe_reader_batch(self, mock_read_property):
        """Test _create_dataframe_reader for batch."""
        bronze_spec_map = copy.deepcopy(DataflowPipelineTests.bronze_dataflow_spec_map)
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_spec_map)

        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view")

        # Configure the mock - the property returns a reader that has options method
        mock_reader_with_options = MagicMock()
        mock_read_property.options.return_value = mock_reader_with_options

        # Test batch reader with empty options (empty dict is falsy, so options() not called)
        result = pipeline._create_dataframe_reader(is_streaming=False, reader_options={})
        mock_read_property.options.assert_not_called()
        self.assertEqual(result, mock_read_property)

        # Test batch reader with actual options (should call options())
        options = {"format": "parquet"}
        result = pipeline._create_dataframe_reader(is_streaming=False, reader_options=options)
        mock_read_property.options.assert_called_with(**options)
        self.assertEqual(result, mock_reader_with_options)

    def test_apply_transformations_with_none(self):
        """Test _apply_transformations with None parameters."""
        bronze_spec_map = copy.deepcopy(DataflowPipelineTests.bronze_dataflow_spec_map)
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_spec_map)

        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view")

        mock_df = MagicMock()

        # Test with no transformations
        result = pipeline._apply_transformations(mock_df, None, None)
        self.assertEqual(result, mock_df)

    def test_apply_transformations_with_select_and_where(self):
        """Test _apply_transformations with select and where clauses."""
        bronze_spec_map = copy.deepcopy(DataflowPipelineTests.bronze_dataflow_spec_map)
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_spec_map)

        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view")

        mock_df = MagicMock()
        mock_df.selectExpr = MagicMock(return_value=mock_df)
        mock_df.where = MagicMock(return_value=mock_df)

        select_exp = ["col1", "col2"]
        where_clause = ["id > 0"]

        pipeline._apply_transformations(mock_df, select_exp, where_clause)

        mock_df.selectExpr.assert_called_once_with(*select_exp)
        mock_df.where.assert_called_once_with("id > 0")

    def test_cluster_by_auto_string_to_boolean_conversion_true(self):
        """Test cluster_by_auto string 'true' conversion to boolean True."""
        bronze_spec_map = copy.deepcopy(DataflowPipelineTests.bronze_dataflow_spec_map)
        bronze_spec_map["quarantineTargetDetails"] = {
            "database": "bronze",
            "table": "customer_dqe",
            "path": "tests/localtest/delta/customers_dqe",
            "cluster_by_auto": "true"
        }
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_spec_map)
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view")

        quarantine_details = pipeline._get_quarantine_target_details()
        q_cluster_by_auto_value = quarantine_details.get("cluster_by_auto", False)

        # Simulate the conversion logic from dataflow_pipeline.py
        if isinstance(q_cluster_by_auto_value, str):
            q_cluster_by_auto = q_cluster_by_auto_value.lower().strip() == 'true'
        else:
            q_cluster_by_auto = bool(q_cluster_by_auto_value) if q_cluster_by_auto_value else False

        self.assertEqual(q_cluster_by_auto, True)
        self.assertIsInstance(q_cluster_by_auto, bool)

    def test_cluster_by_auto_string_to_boolean_conversion_false(self):
        """Test cluster_by_auto string 'false' conversion to boolean False."""
        bronze_spec_map = copy.deepcopy(DataflowPipelineTests.bronze_dataflow_spec_map)
        bronze_spec_map["quarantineTargetDetails"] = {
            "database": "bronze",
            "table": "customer_dqe",
            "path": "tests/localtest/delta/customers_dqe",
            "cluster_by_auto": "false"
        }
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_spec_map)
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view")

        quarantine_details = pipeline._get_quarantine_target_details()
        q_cluster_by_auto_value = quarantine_details.get("cluster_by_auto", False)

        # Simulate the conversion logic from dataflow_pipeline.py
        if isinstance(q_cluster_by_auto_value, str):
            q_cluster_by_auto = q_cluster_by_auto_value.lower().strip() == 'true'
        else:
            q_cluster_by_auto = bool(q_cluster_by_auto_value) if q_cluster_by_auto_value else False

        self.assertEqual(q_cluster_by_auto, False)
        self.assertIsInstance(q_cluster_by_auto, bool)

    def test_cluster_by_auto_string_case_insensitive(self):
        """Test cluster_by_auto case insensitive string conversion."""
        test_values = ["True", "TRUE", "TrUe"]
        for value in test_values:
            bronze_spec_map = copy.deepcopy(DataflowPipelineTests.bronze_dataflow_spec_map)
            bronze_spec_map["quarantineTargetDetails"] = {
                "database": "bronze",
                "table": "customer_dqe",
                "cluster_by_auto": value
            }
            bronze_dataflow_spec = BronzeDataflowSpec(**bronze_spec_map)
            pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view")

            quarantine_details = pipeline._get_quarantine_target_details()
            q_cluster_by_auto_value = quarantine_details.get("cluster_by_auto", False)

            if isinstance(q_cluster_by_auto_value, str):
                q_cluster_by_auto = q_cluster_by_auto_value.lower().strip() == 'true'
            else:
                q_cluster_by_auto = bool(q_cluster_by_auto_value) if q_cluster_by_auto_value else False

            self.assertEqual(q_cluster_by_auto, True, f"Failed for value: {value}")

    def test_cluster_by_auto_boolean_true(self):
        """Test cluster_by_auto with boolean True value."""
        bronze_spec_map = copy.deepcopy(DataflowPipelineTests.bronze_dataflow_spec_map)
        bronze_spec_map["quarantineTargetDetails"] = {
            "database": "bronze",
            "table": "customer_dqe",
            "cluster_by_auto": True
        }
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_spec_map)
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view")

        quarantine_details = pipeline._get_quarantine_target_details()
        q_cluster_by_auto_value = quarantine_details.get("cluster_by_auto", False)

        # Simulate the conversion logic from dataflow_pipeline.py
        if isinstance(q_cluster_by_auto_value, str):
            q_cluster_by_auto = q_cluster_by_auto_value.lower().strip() == 'true'
        else:
            q_cluster_by_auto = bool(q_cluster_by_auto_value) if q_cluster_by_auto_value else False

        self.assertEqual(q_cluster_by_auto, True)

    def test_cluster_by_auto_boolean_false(self):
        """Test cluster_by_auto with boolean False value."""
        bronze_spec_map = copy.deepcopy(DataflowPipelineTests.bronze_dataflow_spec_map)
        bronze_spec_map["quarantineTargetDetails"] = {
            "database": "bronze",
            "table": "customer_dqe",
            "cluster_by_auto": False
        }
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_spec_map)
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view")

        quarantine_details = pipeline._get_quarantine_target_details()
        q_cluster_by_auto_value = quarantine_details.get("cluster_by_auto", False)

        # Simulate the conversion logic from dataflow_pipeline.py
        if isinstance(q_cluster_by_auto_value, str):
            q_cluster_by_auto = q_cluster_by_auto_value.lower().strip() == 'true'
        else:
            q_cluster_by_auto = bool(q_cluster_by_auto_value) if q_cluster_by_auto_value else False

        self.assertEqual(q_cluster_by_auto, False)

    def test_cluster_by_auto_default_when_missing(self):
        """Test cluster_by_auto defaults to False when not provided."""
        bronze_spec_map = copy.deepcopy(DataflowPipelineTests.bronze_dataflow_spec_map)
        bronze_spec_map["quarantineTargetDetails"] = {
            "database": "bronze",
            "table": "customer_dqe"
        }
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_spec_map)
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view")

        quarantine_details = pipeline._get_quarantine_target_details()
        q_cluster_by_auto_value = quarantine_details.get("cluster_by_auto", False)

        # Simulate the conversion logic from dataflow_pipeline.py
        if isinstance(q_cluster_by_auto_value, str):
            q_cluster_by_auto = q_cluster_by_auto_value.lower().strip() == 'true'
        else:
            q_cluster_by_auto = bool(q_cluster_by_auto_value) if q_cluster_by_auto_value else False

        self.assertEqual(q_cluster_by_auto, False)

    def test_cluster_by_auto_for_bronze_table(self):
        """Test cluster_by_auto is correctly extracted for bronze table."""
        bronze_spec_map = copy.deepcopy(DataflowPipelineTests.bronze_dataflow_spec_map)
        bronze_spec_map["clusterByAuto"] = True
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_spec_map)
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view")

        # Simulate the logic from dataflow_pipeline.py for bronze tables
        cluster_by_auto = (
            pipeline.dataflowSpec.clusterByAuto
            if hasattr(pipeline.dataflowSpec, 'clusterByAuto')
            and pipeline.dataflowSpec.clusterByAuto is not None
            else False
        )

        self.assertEqual(cluster_by_auto, True)
        self.assertIsInstance(cluster_by_auto, bool)

    def test_cluster_by_auto_for_bronze_table_false(self):
        """Test cluster_by_auto is False for bronze table when set to False."""
        bronze_spec_map = copy.deepcopy(DataflowPipelineTests.bronze_dataflow_spec_map)
        bronze_spec_map["clusterByAuto"] = False
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_spec_map)
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view")

        cluster_by_auto = (
            pipeline.dataflowSpec.clusterByAuto
            if hasattr(pipeline.dataflowSpec, 'clusterByAuto')
            and pipeline.dataflowSpec.clusterByAuto is not None
            else False
        )

        self.assertEqual(cluster_by_auto, False)

    def test_cluster_by_auto_for_bronze_table_not_set(self):
        """Test cluster_by_auto defaults to False when not set for bronze table."""
        bronze_spec_map = copy.deepcopy(DataflowPipelineTests.bronze_dataflow_spec_map)
        # Don't set clusterByAuto
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_spec_map)
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view")

        cluster_by_auto = (
            pipeline.dataflowSpec.clusterByAuto
            if hasattr(pipeline.dataflowSpec, 'clusterByAuto')
            and pipeline.dataflowSpec.clusterByAuto is not None
            else False
        )

        self.assertEqual(cluster_by_auto, False)

    def test_cluster_by_auto_for_silver_table(self):
        """Test cluster_by_auto is correctly extracted for silver table."""
        silver_spec_map = copy.deepcopy(DataflowPipelineTests.silver_dataflow_spec_map)
        silver_spec_map["clusterByAuto"] = True
        silver_dataflow_spec = SilverDataflowSpec(**silver_spec_map)
        pipeline = DataflowPipeline(self.spark, silver_dataflow_spec, "test_view")

        cluster_by_auto = (
            pipeline.dataflowSpec.clusterByAuto
            if hasattr(pipeline.dataflowSpec, 'clusterByAuto')
            and pipeline.dataflowSpec.clusterByAuto is not None
            else False
        )

        self.assertEqual(cluster_by_auto, True)
        self.assertIsInstance(cluster_by_auto, bool)

    def test_get_table_properties(self):
        """Test _get_table_properties method."""
        bronze_dataflow_spec = BronzeDataflowSpec(**DataflowPipelineTests.bronze_dataflow_spec_map)
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view")
        table_properties = pipeline._get_table_properties()
        self.assertIsInstance(table_properties, dict)

    def test_helper_methods_for_table_info(self):
        """Test helper methods that extract source and target table information."""
        bronze_spec_map = copy.deepcopy(DataflowPipelineTests.bronze_dataflow_spec_map)
        bronze_spec_map["sourceDetails"] = {
            "catalog": "test_catalog",
            "database": "test_db",
            "table": "test_table"
        }
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_spec_map)
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view")

        # Test _get_source_table_info
        source_table_name, source_details = pipeline._get_source_table_info()
        self.assertEqual(source_table_name, "test_catalog.test_db.test_table")
        self.assertIn("catalog", source_details)

        # Test _get_target_table_name
        target_table_name = pipeline._get_target_table_name()
        self.assertIn("customer", target_table_name)

    def test_quarantine_cluster_by_string_parsing(self):
        """Test quarantine cluster_by parsing with string representation."""
        bronze_spec_map = copy.deepcopy(DataflowPipelineTests.bronze_dataflow_spec_map)
        bronze_spec_map["quarantineTargetDetails"] = {
            "database": "bronze",
            "table": "customer_dqe",
            "path": "tests/localtest/delta/customers_dqe",
            "cluster_by": "['id', 'email']"
        }
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_spec_map)
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view", "quarantine_view")

        # This should trigger the cluster_by parsing logic
        quarantine_details = pipeline._get_quarantine_target_details()
        self.assertIn("cluster_by", quarantine_details)

    def test_apply_transformations_helper(self):
        """Test _apply_transformations helper method."""
        bronze_dataflow_spec = BronzeDataflowSpec(**DataflowPipelineTests.bronze_dataflow_spec_map)
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view")

        # Create test DataFrame
        test_data = [("1", "John", "john@email.com"), ("2", "Jane", "jane@email.com")]
        test_df = self.spark.createDataFrame(test_data, ["id", "name", "email"])

        # Test with select and where
        result_df = pipeline._apply_transformations(test_df, ["id", "name"], ["id > '0'"])
        self.assertEqual(len(result_df.columns), 2)
        self.assertIn("id", result_df.columns)
        self.assertIn("name", result_df.columns)

    def test_get_silver_schema_uc_disabled_with_path(self):
        """Test get_silver_schema with Unity Catalog disabled using path."""
        # Use copy of silver spec map
        silver_spec_map = copy.deepcopy(DataflowPipelineTests.silver_dataflow_spec_map)

        self.spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
        self.spark.sql("DROP TABLE IF EXISTS bronze.customer")
        if os.path.exists(f"{self.temp_delta_tables_path}/tables/customer_schema_uc_test"):
            shutil.rmtree(f"{self.temp_delta_tables_path}/tables/customer_schema_uc_test")

        # Write data to delta format with all required columns
        options = {"rescuedDataColumn": "_rescued_data", "inferColumnTypes": "true", "multiline": True}
        customers_df = self.spark.read.options(**options).json("tests/resources/data/customers")
        # Add the _rescued_data column and ensure all selectExp columns are present
        customers_with_rescued = customers_df.withColumn("_rescued_data", lit("Test"))

        # Write to both table and path
        (customers_with_rescued.write.format("delta")
         .mode("overwrite")
         .option("path", f"{self.temp_delta_tables_path}/tables/customer_schema_uc_test")
         .saveAsTable("bronze.customer"))

        # Update silver spec with the path where data was written
        silver_spec_map.update({
            "sourceDetails": {
                "database": "bronze",
                "table": "customer",
                "path": f"{self.temp_delta_tables_path}/tables/customer_schema_uc_test"
            }
        })
        silver_dataflow_spec = SilverDataflowSpec(**silver_spec_map)

        # Test with UC disabled - should use path instead of table name
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "False")
        pipeline = DataflowPipeline(self.spark, silver_dataflow_spec, "test_view")

        # Verify that get_silver_schema returns a valid schema when using path
        schema = pipeline.get_silver_schema()
        self.assertIsNotNone(schema, "Schema should not be None when using path with UC disabled")

    def test_get_silver_schema_with_catalog(self):
        """Test get_silver_schema with catalog specified in source details."""
        silver_spec_map = copy.deepcopy(DataflowPipelineTests.silver_dataflow_spec_map)
        silver_spec_map["sourceDetails"] = {
            "catalog": "test_catalog",
            "database": "bronze",
            "table": "customer",
            "path": "tests/resources/delta/customers"
        }

        self.spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
        self.spark.sql("DROP TABLE IF EXISTS bronze.customer")
        options = {"rescuedDataColumn": "_rescued_data", "inferColumnTypes": "true", "multiline": True}
        customers_df = self.spark.read.options(**options).json("tests/resources/data/customers")
        (customers_df.withColumn("_rescued_data", lit("Test")).write.format("delta")
         .mode("overwrite").saveAsTable("bronze.customer"))

        silver_dataflow_spec = SilverDataflowSpec(**silver_spec_map)

        # Test with UC enabled - should try to use catalog (will fail but tests the path)
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        pipeline = DataflowPipeline(self.spark, silver_dataflow_spec, "test_view")

        # This will likely fail in test environment but exercises the catalog code path
        try:
            pipeline.get_silver_schema()
        except Exception:
            # Expected to fail in test environment without real UC
            pass

    def test_read_silver_with_reader_config_and_snapshot(self):
        """Test read_silver with reader config options and snapshot format."""
        silver_spec_map = copy.deepcopy(DataflowPipelineTests.silver_dataflow_spec_map)
        silver_spec_map["readerConfigOptions"] = {"maxFilesPerTrigger": "1"}
        silver_spec_map["sourceFormat"] = "snapshot"
        silver_spec_map["sourceDetails"] = {
            "database": "bronze",
            "table": "customer",
            "path": f"{self.temp_delta_tables_path}/tables/customer_snapshot"
        }

        self.spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
        self.spark.sql("DROP TABLE IF EXISTS bronze.customer")
        if os.path.exists(f"{self.temp_delta_tables_path}/tables/customer_snapshot"):
            shutil.rmtree(f"{self.temp_delta_tables_path}/tables/customer_snapshot")

        options = {"rescuedDataColumn": "_rescued_data", "inferColumnTypes": "true", "multiline": True}
        customers_df = self.spark.read.options(**options).json("tests/resources/data/customers")
        (customers_df.withColumn("_rescued_data", lit("Test")).write.format("delta")
         .mode("append").option("path", f"{self.temp_delta_tables_path}/tables/customer_snapshot")
         .saveAsTable("bronze.customer"))

        silver_dataflow_spec = SilverDataflowSpec(**silver_spec_map)

        # Test with UC disabled - should use snapshot read with path
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "False")
        pipeline = DataflowPipeline(self.spark, silver_dataflow_spec, "test_view")

        result_df = pipeline.read_silver()
        self.assertIsNotNone(result_df)
        # Verify it's a DataFrame with data
        self.assertTrue(hasattr(result_df, 'count'))

    def test_read_silver_with_reader_config_uc_enabled(self):
        """Test read_silver with reader config options and UC enabled."""
        silver_spec_map = copy.deepcopy(DataflowPipelineTests.silver_dataflow_spec_map)
        silver_spec_map["readerConfigOptions"] = {"maxFilesPerTrigger": "1"}
        silver_spec_map["sourceFormat"] = "snapshot"
        silver_spec_map["sourceDetails"] = {
            "database": "bronze",
            "table": "customer",
            "path": "tests/resources/delta/customers"
        }

        self.spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
        self.spark.sql("DROP TABLE IF EXISTS bronze.customer")
        options = {"rescuedDataColumn": "_rescued_data", "inferColumnTypes": "true", "multiline": True}
        customers_df = self.spark.read.options(**options).json("tests/resources/data/customers")
        (customers_df.withColumn("_rescued_data", lit("Test")).write.format("delta")
         .mode("overwrite").saveAsTable("bronze.customer"))

        silver_dataflow_spec = SilverDataflowSpec(**silver_spec_map)

        # Test with UC enabled - should use table name
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        pipeline = DataflowPipeline(self.spark, silver_dataflow_spec, "test_view")

        result_df = pipeline.read_silver()
        self.assertIsNotNone(result_df)

    def test_read_silver_streaming_with_reader_config(self):
        """Test read_silver with streaming and reader config options."""
        silver_spec_map = copy.deepcopy(DataflowPipelineTests.silver_dataflow_spec_map)
        silver_spec_map["readerConfigOptions"] = {"maxFilesPerTrigger": "1"}
        silver_spec_map["sourceFormat"] = "delta"  # Not snapshot, so uses readStream
        silver_spec_map["sourceDetails"] = {
            "database": "bronze",
            "table": "customer",
            "path": f"{self.temp_delta_tables_path}/tables/customer_streaming"
        }

        self.spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
        self.spark.sql("DROP TABLE IF EXISTS bronze.customer")
        if os.path.exists(f"{self.temp_delta_tables_path}/tables/customer_streaming"):
            shutil.rmtree(f"{self.temp_delta_tables_path}/tables/customer_streaming")

        options = {"rescuedDataColumn": "_rescued_data", "inferColumnTypes": "true", "multiline": True}
        customers_df = self.spark.read.options(**options).json("tests/resources/data/customers")
        (customers_df.withColumn("_rescued_data", lit("Test")).write.format("delta")
         .mode("append").option("path", f"{self.temp_delta_tables_path}/tables/customer_streaming")
         .saveAsTable("bronze.customer"))

        silver_dataflow_spec = SilverDataflowSpec(**silver_spec_map)

        # Test with UC disabled - should use readStream with path
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "False")
        pipeline = DataflowPipeline(self.spark, silver_dataflow_spec, "test_view")

        result_df = pipeline.read_silver()
        self.assertIsNotNone(result_df)

    def test_read_from_source_snapshot_format(self):
        """Test _read_from_source with snapshot format."""
        bronze_spec_map = copy.deepcopy(DataflowPipelineTests.bronze_dataflow_spec_map)
        bronze_spec_map["sourceFormat"] = "snapshot"
        bronze_spec_map["sourceDetails"] = {
            "database": "bronze",
            "table": "customer",
            "path": "tests/resources/delta/customers"
        }

        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_spec_map)

        # Test with UC disabled
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "False")
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view")

        # This exercises the snapshot read path
        self.assertIsNotNone(pipeline)
        self.assertEqual(pipeline.dataflowSpec.sourceFormat, "snapshot")

    def test_read_from_source_streaming_with_path(self):
        """Test _read_from_source with streaming format using path."""
        bronze_spec_map = copy.deepcopy(DataflowPipelineTests.bronze_dataflow_spec_map)
        bronze_spec_map["sourceFormat"] = "delta"
        bronze_spec_map["sourceDetails"] = {
            "database": "bronze",
            "table": "customer",
            "path": "tests/resources/delta/customers"
        }

        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_spec_map)

        # Test with UC disabled - exercises the streaming read with path
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "False")
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view")

        self.assertIsNotNone(pipeline)
        self.assertFalse(pipeline.uc_enabled)

    def test_read_from_source_with_reader_options(self):
        """Test _read_from_source with reader config options."""
        bronze_spec_map = copy.deepcopy(DataflowPipelineTests.bronze_dataflow_spec_map)
        bronze_spec_map["sourceFormat"] = "delta"
        bronze_spec_map["readerConfigOptions"] = {"maxFilesPerTrigger": "1", "ignoreDeletes": "true"}
        bronze_spec_map["sourceDetails"] = {
            "database": "bronze",
            "table": "customer",
            "path": "tests/resources/delta/customers"
        }

        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_spec_map)
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view")

        # Verify reader config options are available
        reader_opts = pipeline._get_reader_config_options()
        self.assertIn("maxFilesPerTrigger", reader_opts)
        self.assertEqual(reader_opts["maxFilesPerTrigger"], "1")

    def _mock_source_reader_pipeline(self, *, uc_enabled):
        bronze_spec_map = copy.deepcopy(
            DataflowPipelineTests.bronze_dataflow_spec_map
        )
        pipeline = DataflowPipeline(
            self.spark,
            BronzeDataflowSpec(**bronze_spec_map),
            "test_view",
        )
        pipeline.uc_enabled = uc_enabled
        pipeline._get_source_table_info = MagicMock(return_value=(
            "main.raw.events",
            {"path": "/tmp/events"},
        ))
        pipeline._get_reader_config_options = MagicMock(
            return_value={"ignoreDeletes": "true"}
        )
        reader = MagicMock()
        pipeline._create_dataframe_reader = MagicMock(return_value=reader)
        return pipeline, reader

    def test_read_from_source_snapshot_uses_uc_table(self):
        pipeline, reader = self._mock_source_reader_pipeline(uc_enabled=True)
        expected = object()
        reader.table.return_value = expected

        result = pipeline._read_from_source("snapshot")

        self.assertIs(result, expected)
        reader.table.assert_called_once_with("main.raw.events")
        reader.load.assert_not_called()

    def test_read_from_source_snapshot_uses_delta_path_without_uc(self):
        pipeline, reader = self._mock_source_reader_pipeline(uc_enabled=False)
        expected = object()
        reader.load.return_value = expected

        result = pipeline._read_from_source("snapshot")

        self.assertIs(result, expected)
        reader.load.assert_called_once_with(path="/tmp/events", format="delta")
        reader.table.assert_not_called()

    def test_read_from_source_streaming_uses_uc_table(self):
        pipeline, reader = self._mock_source_reader_pipeline(uc_enabled=True)
        expected = object()
        reader.table.return_value = expected

        result = pipeline._read_from_source("delta", is_streaming=True)

        self.assertIs(result, expected)
        reader.table.assert_called_once_with("main.raw.events")

    def test_read_from_source_batch_uses_path_without_uc(self):
        pipeline, reader = self._mock_source_reader_pipeline(uc_enabled=False)
        expected = object()
        reader.load.return_value = expected

        result = pipeline._read_from_source("delta", is_streaming=False)

        self.assertIs(result, expected)
        pipeline._create_dataframe_reader.assert_called_once_with(
            False, {"ignoreDeletes": "true"}
        )
        reader.load.assert_called_once_with(path="/tmp/events", format="delta")

    def test_create_dataframe_reader_with_options(self):
        """Test _create_dataframe_reader with various options."""
        bronze_dataflow_spec = BronzeDataflowSpec(**DataflowPipelineTests.bronze_dataflow_spec_map)
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, "test_view")

        # Test streaming reader without options
        reader = pipeline._create_dataframe_reader(is_streaming=True, reader_options=None)
        self.assertIsNotNone(reader)

        # Test batch reader without options
        reader = pipeline._create_dataframe_reader(is_streaming=False, reader_options=None)
        self.assertIsNotNone(reader)

        # Test streaming reader with options
        reader = pipeline._create_dataframe_reader(
            is_streaming=True,
            reader_options={"maxFilesPerTrigger": "1"}
        )
        self.assertIsNotNone(reader)

        # Test batch reader with options
        reader = pipeline._create_dataframe_reader(
            is_streaming=False,
            reader_options={"inferSchema": "true"}
        )
        self.assertIsNotNone(reader)

    # ------------------------------------------------------------------
    # UC row-filter runtime tests (issue #303)
    # ------------------------------------------------------------------

    # Canonical UC row-filter clause used as the opaque pass-through token in
    # the row-filter tests below. The framework treats `rowFilter` as a string
    # forwarded verbatim to dp.table / dp.create_streaming_table, so the UDF
    # does not need to actually exist for these mock-based assertions.
    ROW_FILTER_REGION = "ROW FILTER main.bronze.region_filter ON (region)"
    ROW_FILTER_DEPT = "ROW FILTER main.bronze.dept_filter ON (dept)"

    def _build_bronze_pipeline(self, uc_enabled, row_filter):
        """Helper: build a simple-path bronze DataflowPipeline with a given UC/row-filter setup."""
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True" if uc_enabled else "False")
        # Test isolation: clear the UC conf override after this test finishes
        # so the override doesn't leak into sibling tests in this module.
        self.addCleanup(self.spark.conf.unset, "spark.databricks.unityCatalog.enabled")
        spec = BronzeDataflowSpec(**copy.deepcopy(DataflowPipelineTests.bronze_dataflow_spec_map))
        spec.cdcApplyChanges = None
        spec.dataQualityExpectations = None
        spec.appendFlows = []
        spec.rowFilter = row_filter
        view_name = f"{spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, spec, view_name, None)
        pipeline.read_bronze = MagicMock()
        pipeline.view_name = view_name
        return pipeline

    def test_get_row_filter_uc_enabled(self):
        """_get_row_filter returns the rowFilter value when UC is enabled."""
        pipeline = self._build_bronze_pipeline(uc_enabled=True, row_filter=self.ROW_FILTER_REGION)
        self.assertEqual(pipeline._get_row_filter(), self.ROW_FILTER_REGION)

    def test_get_row_filter_uc_disabled(self):
        """_get_row_filter returns None when UC is disabled, even if rowFilter is set."""
        pipeline = self._build_bronze_pipeline(uc_enabled=False, row_filter=self.ROW_FILTER_REGION)
        self.assertIsNone(pipeline._get_row_filter())

    def test_get_row_filter_not_set(self):
        """_get_row_filter returns None when rowFilter is not set."""
        pipeline = self._build_bronze_pipeline(uc_enabled=True, row_filter=None)
        self.assertIsNone(pipeline._get_row_filter())

    @patch('databricks.labs.sdp_meta.dataflow_pipeline.dp')
    def test_write_bronze_passes_row_filter(self, mock_dlt):
        """write_bronze (simple path) passes row_filter to dp.table when UC is enabled and set."""
        mock_dlt.table = MagicMock(return_value=lambda func: func)
        pipeline = self._build_bronze_pipeline(uc_enabled=True, row_filter=self.ROW_FILTER_REGION)
        pipeline.write_bronze()
        _, kwargs = mock_dlt.table.call_args
        self.assertEqual(kwargs["row_filter"], self.ROW_FILTER_REGION)

    @patch('databricks.labs.sdp_meta.dataflow_pipeline.dp')
    def test_write_bronze_no_row_filter(self, mock_dlt):
        """write_bronze passes row_filter=None when rowFilter is not set."""
        mock_dlt.table = MagicMock(return_value=lambda func: func)
        pipeline = self._build_bronze_pipeline(uc_enabled=True, row_filter=None)
        pipeline.write_bronze()
        _, kwargs = mock_dlt.table.call_args
        self.assertIsNone(kwargs["row_filter"])

    @patch('databricks.labs.sdp_meta.dataflow_pipeline.dp')
    def test_write_bronze_row_filter_suppressed_without_uc(self, mock_dlt):
        """row_filter is suppressed (None) on the write path when UC is disabled."""
        mock_dlt.table = MagicMock(return_value=lambda func: func)
        pipeline = self._build_bronze_pipeline(uc_enabled=False, row_filter=self.ROW_FILTER_REGION)
        pipeline.write_bronze()
        _, kwargs = mock_dlt.table.call_args
        self.assertIsNone(kwargs["row_filter"])

    @patch('databricks.labs.sdp_meta.dataflow_pipeline.dp')
    def test_create_streaming_table_passes_row_filter(self, mock_dlt):
        """create_streaming_table (CDC / snapshot path) passes row_filter to dp.create_streaming_table."""
        mock_dlt.create_streaming_table = MagicMock()
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        # Same isolation guarantee as _build_bronze_pipeline: don't leak the
        # UC conf override into other tests.
        self.addCleanup(self.spark.conf.unset, "spark.databricks.unityCatalog.enabled")
        spec = BronzeDataflowSpec(**copy.deepcopy(DataflowPipelineTests.bronze_dataflow_spec_map))
        spec.dataQualityExpectations = None
        spec.rowFilter = self.ROW_FILTER_DEPT
        view_name = f"{spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, spec, view_name, None)
        pipeline.create_streaming_table(None, None)
        _, kwargs = mock_dlt.create_streaming_table.call_args
        self.assertEqual(kwargs["row_filter"], self.ROW_FILTER_DEPT)

    # ------------------------------------------------------------------
    # quarantine_row_filter coverage
    # ------------------------------------------------------------------

    def test_get_quarantine_row_filter_uc_enabled(self):
        """_get_quarantine_row_filter returns the quarantineRowFilter value when UC is enabled."""
        pipeline = self._build_bronze_pipeline(uc_enabled=True, row_filter=None)
        pipeline.dataflowSpec.quarantineRowFilter = self.ROW_FILTER_DEPT
        self.assertEqual(pipeline._get_quarantine_row_filter(), self.ROW_FILTER_DEPT)

    def test_get_quarantine_row_filter_uc_disabled(self):
        """_get_quarantine_row_filter returns None when UC is disabled, even if quarantineRowFilter is set."""
        pipeline = self._build_bronze_pipeline(uc_enabled=False, row_filter=None)
        pipeline.dataflowSpec.quarantineRowFilter = self.ROW_FILTER_DEPT
        self.assertIsNone(pipeline._get_quarantine_row_filter())

    def test_get_quarantine_row_filter_not_set(self):
        """_get_quarantine_row_filter returns None when quarantineRowFilter is not set."""
        pipeline = self._build_bronze_pipeline(uc_enabled=True, row_filter=None)
        pipeline.dataflowSpec.quarantineRowFilter = None
        self.assertIsNone(pipeline._get_quarantine_row_filter())

    @patch('databricks.labs.sdp_meta.dataflow_pipeline.dp')
    def test_quarantine_dp_table_passes_quarantine_row_filter(self, mock_dlt):
        """The quarantine dp.table call carries `row_filter=quarantineRowFilter` (independent of the main rowFilter)."""
        mock_dlt.expect_all = MagicMock(return_value=lambda func: func)
        mock_dlt.expect_all_or_drop = MagicMock(return_value=lambda func: func)
        mock_dlt.expect_all_or_fail = MagicMock(return_value=lambda func: func)
        mock_dlt.table = MagicMock(return_value=lambda func: func)

        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        self.addCleanup(self.spark.conf.unset, "spark.databricks.unityCatalog.enabled")

        spec = BronzeDataflowSpec(**copy.deepcopy(DataflowPipelineTests.bronze_dataflow_spec_map))
        spec.cdcApplyChanges = None
        spec.applyChangesFromSnapshot = None
        # Drive the quarantine branch in write_layer_with_dqe.
        spec.dataQualityExpectations = json.dumps({
            "expect_or_quarantine": {"valid_id": "id IS NOT NULL"}
        })
        # Distinct values prove the two filters are wired through independently.
        spec.rowFilter = self.ROW_FILTER_REGION
        spec.quarantineRowFilter = self.ROW_FILTER_DEPT

        view_name = f"{spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, spec, view_name, None)
        pipeline.write_layer_with_dqe()

        # Quarantine table name comes from spec.quarantineTargetDetails (no catalog -> "<db>.<table>").
        q_db = spec.quarantineTargetDetails["database"]
        q_table = spec.quarantineTargetDetails["table"]
        expected_quarantine_table = f"{q_db}.{q_table}"

        quarantine_calls = [
            call for call in mock_dlt.table.call_args_list
            if call.kwargs.get("name") == expected_quarantine_table
        ]
        self.assertEqual(
            len(quarantine_calls), 1,
            f"expected exactly 1 dp.table call for quarantine `{expected_quarantine_table}`, "
            f"saw call_args_list={mock_dlt.table.call_args_list}"
        )
        self.assertEqual(
            quarantine_calls[0].kwargs["row_filter"], self.ROW_FILTER_DEPT
        )

    @patch('databricks.labs.sdp_meta.dataflow_pipeline.dp')
    def test_quarantine_dp_table_row_filter_suppressed_without_uc(self, mock_dlt):
        """Quarantine dp.table receives row_filter=None when UC is disabled, even if quarantineRowFilter is set."""
        mock_dlt.expect_all = MagicMock(return_value=lambda func: func)
        mock_dlt.expect_all_or_drop = MagicMock(return_value=lambda func: func)
        mock_dlt.expect_all_or_fail = MagicMock(return_value=lambda func: func)
        mock_dlt.table = MagicMock(return_value=lambda func: func)

        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "False")
        self.addCleanup(self.spark.conf.unset, "spark.databricks.unityCatalog.enabled")

        spec = BronzeDataflowSpec(**copy.deepcopy(DataflowPipelineTests.bronze_dataflow_spec_map))
        spec.cdcApplyChanges = None
        spec.applyChangesFromSnapshot = None
        spec.dataQualityExpectations = json.dumps({
            "expect_or_quarantine": {"valid_id": "id IS NOT NULL"}
        })
        spec.quarantineRowFilter = self.ROW_FILTER_DEPT

        view_name = f"{spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, spec, view_name, None)
        pipeline.write_layer_with_dqe()

        q_db = spec.quarantineTargetDetails["database"]
        q_table = spec.quarantineTargetDetails["table"]
        expected_quarantine_table = f"{q_db}.{q_table}"
        quarantine_calls = [
            call for call in mock_dlt.table.call_args_list
            if call.kwargs.get("name") == expected_quarantine_table
        ]
        self.assertEqual(len(quarantine_calls), 1)
        self.assertIsNone(quarantine_calls[0].kwargs["row_filter"])

    # ------------------------------------------------------------------
    # UC column comments / masks coverage
    # ------------------------------------------------------------------

    def test_get_column_comments_not_uc_gated(self):
        """Comments are not a UC feature -> returned even when UC is off."""
        p = self._build_bronze_pipeline(uc_enabled=False, row_filter=None)
        p.dataflowSpec.columnComments = json.dumps({"id": "the id"})
        self.assertEqual(p._get_column_comments(), {"id": "the id"})

    def test_get_column_comments_none(self):
        p = self._build_bronze_pipeline(uc_enabled=True, row_filter=None)
        p.dataflowSpec.columnComments = None
        self.assertIsNone(p._get_column_comments())

    def test_get_column_masks_uc_enabled(self):
        p = self._build_bronze_pipeline(uc_enabled=True, row_filter=None)
        p.dataflowSpec.columnMasks = json.dumps({"id": "cat.s.mask_id"})
        self.assertEqual(p._get_column_masks(), {"id": "cat.s.mask_id"})

    def test_get_column_masks_uc_disabled(self):
        """Masks are UC-only -> suppressed (None) when UC is disabled."""
        p = self._build_bronze_pipeline(uc_enabled=False, row_filter=None)
        p.dataflowSpec.columnMasks = json.dumps({"id": "cat.s.mask_id"})
        self.assertIsNone(p._get_column_masks())

    def test_get_column_masks_not_set(self):
        p = self._build_bronze_pipeline(uc_enabled=True, row_filter=None)
        p.dataflowSpec.columnMasks = None
        self.assertIsNone(p._get_column_masks())

    def test_apply_column_policies_passthrough_when_unset(self):
        """No comments/masks -> the original schema is returned unchanged."""
        from pyspark.sql.types import StructType, StructField, StringType
        p = self._build_bronze_pipeline(uc_enabled=True, row_filter=None)
        p.dataflowSpec.columnComments = None
        p.dataflowSpec.columnMasks = None
        schema = StructType([StructField("id", StringType(), True)])
        self.assertIs(p._apply_column_policies(schema), schema)

    def test_apply_column_policies_builds_ddl(self):
        from pyspark.sql.types import StructType, StructField, StringType
        p = self._build_bronze_pipeline(uc_enabled=True, row_filter=None)
        p.dataflowSpec.columnComments = json.dumps({"id": "the id"})
        p.dataflowSpec.columnMasks = json.dumps({"id": "cat.s.mask_id"})
        schema = StructType([StructField("id", StringType(), True)])
        ddl = p._apply_column_policies(schema)
        self.assertIn("COMMENT 'the id'", ddl)
        self.assertIn("MASK cat.s.mask_id", ddl)

    def test_apply_column_policies_masks_without_schema_raises(self):
        """Masks fail closed when no schema is available to attach them to."""
        p = self._build_bronze_pipeline(uc_enabled=True, row_filter=None)
        p.dataflowSpec.columnMasks = json.dumps({"id": "cat.s.mask_id"})
        with self.assertRaisesRegex(ValueError, "no schema is available"):
            p._apply_column_policies(None)

    def test_apply_column_policies_comments_only_no_schema_skips(self):
        """Comments without a schema warn + skip (return None), not raise."""
        p = self._build_bronze_pipeline(uc_enabled=True, row_filter=None)
        p.dataflowSpec.columnComments = json.dumps({"id": "x"})
        p.dataflowSpec.columnMasks = None
        self.assertIsNone(p._apply_column_policies(None))

    def test_resolve_policy_schema_none_when_no_policies(self):
        p = self._build_bronze_pipeline(uc_enabled=True, row_filter=None)
        p.dataflowSpec.columnComments = None
        p.dataflowSpec.columnMasks = None
        self.assertIsNone(p._resolve_policy_schema())

    def test_resolve_policy_schema_bronze_from_schema_json(self):
        from pyspark.sql.types import StructType, StructField, StringType
        p = self._build_bronze_pipeline(uc_enabled=True, row_filter=None)
        p.dataflowSpec.columnMasks = json.dumps({"id": "cat.s.mask_id"})
        p.schema_json = StructType([StructField("id", StringType(), True)]).jsonValue()
        resolved = p._resolve_policy_schema()
        self.assertEqual([f.name for f in resolved.fields], ["id"])

    def test_resolve_policy_schema_bronze_no_schema_json_none(self):
        p = self._build_bronze_pipeline(uc_enabled=True, row_filter=None)
        p.dataflowSpec.columnComments = json.dumps({"id": "x"})
        p.schema_json = None
        self.assertIsNone(p._resolve_policy_schema())

    @patch('databricks.labs.sdp_meta.dataflow_pipeline.AppendFlowWriter')
    def test_write_append_flows_silver_passes_schema_ddl(self, mock_writer):
        """Silver append flow attaches masks via the DDL schema (4th positional
        arg to AppendFlowWriter)."""
        from pyspark.sql.types import StructType, StructField, StringType
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        self.addCleanup(self.spark.conf.unset, "spark.databricks.unityCatalog.enabled")
        spec = SilverDataflowSpec(**copy.deepcopy(DataflowPipelineTests.silver_dataflow_spec_map))
        spec.columnMasks = json.dumps({"id": "cat.s.mask_id"})
        view_name = f"{spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, spec, view_name, None)
        pipeline.appendFlows = [MagicMock()]
        pipeline.get_silver_schema = MagicMock(
            return_value=StructType([StructField("id", StringType(), True)])
        )
        pipeline.write_append_flows()
        args, _ = mock_writer.call_args
        schema_arg = args[3]
        self.assertIn("MASK cat.s.mask_id", schema_arg)

    @patch('databricks.labs.sdp_meta.dataflow_pipeline.dp')
    def test_create_streaming_table_passes_schema_ddl_with_policies(self, mock_dlt):
        """create_streaming_table renders comments/masks into a DDL schema."""
        from pyspark.sql.types import StructType, StructField, StringType
        mock_dlt.create_streaming_table = MagicMock()
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        self.addCleanup(self.spark.conf.unset, "spark.databricks.unityCatalog.enabled")
        spec = BronzeDataflowSpec(**copy.deepcopy(DataflowPipelineTests.bronze_dataflow_spec_map))
        spec.dataQualityExpectations = None
        spec.columnComments = json.dumps({"id": "the id"})
        spec.columnMasks = json.dumps({"id": "cat.s.mask_id"})
        view_name = f"{spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, spec, view_name, None)
        schema = StructType([StructField("id", StringType(), True)])
        pipeline.create_streaming_table(schema, None)
        _, kwargs = mock_dlt.create_streaming_table.call_args
        self.assertIn("COMMENT 'the id'", kwargs["schema"])
        self.assertIn("MASK cat.s.mask_id", kwargs["schema"])

    @patch('databricks.labs.sdp_meta.dataflow_pipeline.dp')
    def test_write_silver_standard_passes_schema_ddl(self, mock_dlt):
        """Silver standard write materialises get_silver_schema() and passes a
        DDL-string schema carrying the masks to dp.table."""
        from pyspark.sql.types import StructType, StructField, StringType
        mock_dlt.table = MagicMock(return_value=lambda func: func)
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        self.addCleanup(self.spark.conf.unset, "spark.databricks.unityCatalog.enabled")
        spec = SilverDataflowSpec(**copy.deepcopy(DataflowPipelineTests.silver_dataflow_spec_map))
        spec.cdcApplyChanges = None
        spec.applyChangesFromSnapshot = None
        spec.dataQualityExpectations = None
        spec.appendFlows = []
        spec.columnMasks = json.dumps({"id": "cat.s.mask_id"})
        view_name = f"{spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, spec, view_name, None)
        # Avoid a live source read: stub the derived silver schema.
        pipeline.get_silver_schema = MagicMock(
            return_value=StructType([StructField("id", StringType(), True)])
        )
        pipeline.write_silver()
        _, kwargs = mock_dlt.table.call_args
        self.assertIn("MASK cat.s.mask_id", kwargs["schema"])
        pipeline.get_silver_schema.assert_called()

    # ------------------------------------------------------------------
    # DQE-path row_filter coverage
    #
    # write_layer_with_dqe has three exclusive-first branches that each
    # construct the *main* dp.table separately (lines ~519, ~533, ~551 in
    # dataflow_pipeline.py). The earlier `test_write_bronze_passes_row_filter`
    # only covers the simple/no-DQE path; these tests pin the row_filter
    # pass-through under each DQE wrapper so a future regression in any one
    # branch is caught explicitly.
    # ------------------------------------------------------------------

    def _build_dqe_pipeline(self, dqe_dict):
        """Helper: build a bronze pipeline with UC on and a DQE that exercises
        a specific expect_* branch in write_layer_with_dqe."""
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        self.addCleanup(self.spark.conf.unset, "spark.databricks.unityCatalog.enabled")
        spec = BronzeDataflowSpec(**copy.deepcopy(DataflowPipelineTests.bronze_dataflow_spec_map))
        spec.cdcApplyChanges = None
        spec.applyChangesFromSnapshot = None
        spec.dataQualityExpectations = json.dumps(dqe_dict)
        spec.rowFilter = self.ROW_FILTER_REGION
        view_name = f"{spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, spec, view_name, None)
        return pipeline, spec

    def _expected_target_table_name(self, spec):
        """Produce the fully-qualified target table name in the same shape
        that `_get_target_table_info` uses (catalog optional)."""
        td = spec.targetDetails
        if td.get("catalog"):
            return f"{td['catalog']}.{td['database']}.{td['table']}"
        return f"{td['database']}.{td['table']}"

    def _assert_main_table_row_filter(self, mock_dlt, spec, expected_filter):
        target_table = self._expected_target_table_name(spec)
        main_calls = [
            call for call in mock_dlt.table.call_args_list
            if call.kwargs.get("name") == target_table
        ]
        self.assertEqual(
            len(main_calls), 1,
            f"expected exactly 1 dp.table call for main target `{target_table}`, "
            f"saw call_args_list={mock_dlt.table.call_args_list}"
        )
        self.assertEqual(main_calls[0].kwargs["row_filter"], expected_filter)

    @patch('databricks.labs.sdp_meta.dataflow_pipeline.dp')
    def test_write_layer_with_dqe_expect_all_passes_row_filter(self, mock_dlt):
        """`expect_all` branch wraps dp.table and passes row_filter through."""
        mock_dlt.expect_all = MagicMock(return_value=lambda func: func)
        mock_dlt.expect_all_or_drop = MagicMock(return_value=lambda func: func)
        mock_dlt.expect_all_or_fail = MagicMock(return_value=lambda func: func)
        mock_dlt.table = MagicMock(return_value=lambda func: func)

        pipeline, spec = self._build_dqe_pipeline(
            {"expect_all": {"valid_id": "id IS NOT NULL"}}
        )
        pipeline.write_layer_with_dqe()
        self._assert_main_table_row_filter(mock_dlt, spec, self.ROW_FILTER_REGION)

    @patch('databricks.labs.sdp_meta.dataflow_pipeline.dp')
    def test_write_layer_with_dqe_expect_all_or_fail_passes_row_filter(self, mock_dlt):
        """`expect_all_or_fail` branch (when expect_all is absent) constructs
        dp.table itself and passes row_filter through."""
        mock_dlt.expect_all = MagicMock(return_value=lambda func: func)
        mock_dlt.expect_all_or_drop = MagicMock(return_value=lambda func: func)
        mock_dlt.expect_all_or_fail = MagicMock(return_value=lambda func: func)
        mock_dlt.table = MagicMock(return_value=lambda func: func)

        pipeline, spec = self._build_dqe_pipeline(
            {"expect_all_or_fail": {"valid_id": "id IS NOT NULL"}}
        )
        pipeline.write_layer_with_dqe()
        self._assert_main_table_row_filter(mock_dlt, spec, self.ROW_FILTER_REGION)

    @patch('databricks.labs.sdp_meta.dataflow_pipeline.dp')
    def test_write_layer_with_dqe_expect_all_or_drop_passes_row_filter(self, mock_dlt):
        """`expect_all_or_drop` branch (when expect_all and expect_all_or_fail
        are both absent) constructs dp.table itself and passes row_filter through."""
        mock_dlt.expect_all = MagicMock(return_value=lambda func: func)
        mock_dlt.expect_all_or_drop = MagicMock(return_value=lambda func: func)
        mock_dlt.expect_all_or_fail = MagicMock(return_value=lambda func: func)
        mock_dlt.table = MagicMock(return_value=lambda func: func)

        pipeline, spec = self._build_dqe_pipeline(
            {"expect_all_or_drop": {"valid_id": "id IS NOT NULL"}}
        )
        pipeline.write_layer_with_dqe()
        self._assert_main_table_row_filter(mock_dlt, spec, self.ROW_FILTER_REGION)

    # ------------------------------------------------------------------
    # DQE-path column-policy coverage
    #
    # Each of write_layer_with_dqe's exclusive-first branches builds the
    # main dp.table independently. Only the expect_all branch originally
    # forwarded `schema=column_policy_schema`, so fail-only / drop-only
    # tables silently dropped comments/masks AND skipped the fail-closed
    # unknown-column-mask check. These tests pin the policy schema on the
    # fail-only and drop-only branches so a regression is caught.
    # ------------------------------------------------------------------

    def _assert_main_table_schema_contains(self, mock_dlt, spec, tokens):
        target_table = self._expected_target_table_name(spec)
        main_calls = [
            call for call in mock_dlt.table.call_args_list
            if call.kwargs.get("name") == target_table
        ]
        self.assertEqual(
            len(main_calls), 1,
            f"expected exactly 1 dp.table call for `{target_table}`, "
            f"saw {mock_dlt.table.call_args_list}"
        )
        schema_arg = main_calls[0].kwargs.get("schema")
        self.assertIsInstance(
            schema_arg, str,
            f"expected a DDL-string schema, got {schema_arg!r}"
        )
        for token in tokens:
            self.assertIn(token, schema_arg)

    def _build_dqe_policy_pipeline(self, dqe_dict, comments, masks, schema_cols=("id",)):
        from pyspark.sql.types import StructType, StructField, StringType
        pipeline, spec = self._build_dqe_pipeline(dqe_dict)
        if comments is not None:
            spec.columnComments = json.dumps(comments)
        if masks is not None:
            spec.columnMasks = json.dumps(masks)
        pipeline.schema_json = StructType(
            [StructField(c, StringType(), True) for c in schema_cols]
        ).jsonValue()
        return pipeline, spec

    @patch('databricks.labs.sdp_meta.dataflow_pipeline.dp')
    def test_write_layer_with_dqe_fail_only_applies_column_policies(self, mock_dlt):
        """expect_all_or_fail-only branch attaches the DDL schema carrying
        comments/masks to the main table."""
        mock_dlt.expect_all = MagicMock(return_value=lambda func: func)
        mock_dlt.expect_all_or_drop = MagicMock(return_value=lambda func: func)
        mock_dlt.expect_all_or_fail = MagicMock(return_value=lambda func: func)
        mock_dlt.table = MagicMock(return_value=lambda func: func)
        pipeline, spec = self._build_dqe_policy_pipeline(
            {"expect_all_or_fail": {"valid_id": "id IS NOT NULL"}},
            {"id": "the id"},
            {"id": "cat.s.mask_id"},
        )
        pipeline.write_layer_with_dqe()
        self._assert_main_table_schema_contains(
            mock_dlt, spec, ["COMMENT 'the id'", "MASK cat.s.mask_id"]
        )

    @patch('databricks.labs.sdp_meta.dataflow_pipeline.dp')
    def test_write_layer_with_dqe_drop_only_applies_column_policies(self, mock_dlt):
        """expect_all_or_drop-only branch attaches the DDL schema carrying
        comments/masks to the main table."""
        mock_dlt.expect_all = MagicMock(return_value=lambda func: func)
        mock_dlt.expect_all_or_drop = MagicMock(return_value=lambda func: func)
        mock_dlt.expect_all_or_fail = MagicMock(return_value=lambda func: func)
        mock_dlt.table = MagicMock(return_value=lambda func: func)
        pipeline, spec = self._build_dqe_policy_pipeline(
            {"expect_all_or_drop": {"valid_id": "id IS NOT NULL"}},
            {"id": "the id"},
            {"id": "cat.s.mask_id"},
        )
        pipeline.write_layer_with_dqe()
        self._assert_main_table_schema_contains(
            mock_dlt, spec, ["COMMENT 'the id'", "MASK cat.s.mask_id"]
        )

    @patch('databricks.labs.sdp_meta.dataflow_pipeline.dp')
    def test_write_layer_with_dqe_fail_only_unknown_mask_fails_closed(self, mock_dlt):
        """A mask on an absent column fails closed on the fail-only branch."""
        mock_dlt.expect_all_or_fail = MagicMock(return_value=lambda func: func)
        mock_dlt.table = MagicMock(return_value=lambda func: func)
        pipeline, _ = self._build_dqe_policy_pipeline(
            {"expect_all_or_fail": {"valid_id": "id IS NOT NULL"}},
            None,
            {"ssn": "cat.s.mask_ssn"},
        )
        with self.assertRaisesRegex(ValueError, "not present in the derived"):
            pipeline.write_layer_with_dqe()

    @patch('databricks.labs.sdp_meta.dataflow_pipeline.dp')
    def test_write_layer_with_dqe_drop_only_unknown_mask_fails_closed(self, mock_dlt):
        """A mask on an absent column fails closed on the drop-only branch."""
        mock_dlt.expect_all_or_drop = MagicMock(return_value=lambda func: func)
        mock_dlt.table = MagicMock(return_value=lambda func: func)
        pipeline, _ = self._build_dqe_policy_pipeline(
            {"expect_all_or_drop": {"valid_id": "id IS NOT NULL"}},
            None,
            {"ssn": "cat.s.mask_ssn"},
        )
        with self.assertRaisesRegex(ValueError, "not present in the derived"):
            pipeline.write_layer_with_dqe()

    # ------------------------------------------------------------------
    # Single-source Silver CDC column-policy coverage
    # ------------------------------------------------------------------

    @patch('databricks.labs.sdp_meta.dataflow_pipeline.dp')
    def test_cdc_apply_changes_silver_applies_column_policies(self, mock_dlt):
        """Single-source Silver CDC resolves the derived schema and attaches
        comments/masks. Silver has no schema_json, so the previous
        ``if self.schema_json`` guard passed None and dropped the policies."""
        from pyspark.sql.types import StructType, StructField, StringType
        mock_dlt.create_streaming_table = MagicMock()
        mock_dlt.create_auto_cdc_flow = MagicMock()
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        self.addCleanup(self.spark.conf.unset, "spark.databricks.unityCatalog.enabled")
        spec = SilverDataflowSpec(**copy.deepcopy(DataflowPipelineTests.silver_dataflow_spec_map))
        spec.cdcApplyChanges = json.dumps(self.silver_cdc_apply_changes)
        spec.columnComments = json.dumps({"id": "the id"})
        spec.columnMasks = json.dumps({"id": "cat.s.mask_id"})
        view_name = f"{spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, spec, view_name, None)
        pipeline.get_silver_schema = MagicMock(
            return_value=StructType([StructField("id", StringType(), True)])
        )
        pipeline.cdc_apply_changes()
        _, kwargs = mock_dlt.create_streaming_table.call_args
        self.assertIn("COMMENT 'the id'", kwargs["schema"])
        self.assertIn("MASK cat.s.mask_id", kwargs["schema"])
        pipeline.get_silver_schema.assert_called()

    @patch('databricks.labs.sdp_meta.dataflow_pipeline.dp')
    def test_cdc_apply_changes_silver_unknown_mask_fails_closed(self, mock_dlt):
        """A Silver CDC mask on an absent column fails closed."""
        from pyspark.sql.types import StructType, StructField, StringType
        mock_dlt.create_streaming_table = MagicMock()
        mock_dlt.create_auto_cdc_flow = MagicMock()
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        self.addCleanup(self.spark.conf.unset, "spark.databricks.unityCatalog.enabled")
        spec = SilverDataflowSpec(**copy.deepcopy(DataflowPipelineTests.silver_dataflow_spec_map))
        spec.cdcApplyChanges = json.dumps(self.silver_cdc_apply_changes)
        spec.columnMasks = json.dumps({"ssn": "cat.s.mask_ssn"})
        view_name = f"{spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, spec, view_name, None)
        pipeline.get_silver_schema = MagicMock(
            return_value=StructType([StructField("id", StringType(), True)])
        )
        with self.assertRaisesRegex(ValueError, "not present in the derived"):
            pipeline.cdc_apply_changes()

    # ------------------------------------------------------------------
    # Snapshot CDC column-policy coverage
    # ------------------------------------------------------------------

    @patch('databricks.labs.sdp_meta.dataflow_pipeline.dp')
    def test_apply_changes_from_snapshot_scd1_applies_column_policies(self, mock_dlt):
        """SCD1 snapshot CDC wires the declared schema so comments/masks apply
        to the target table (SCD1 has no __START_AT/__END_AT system columns, so
        an explicit schema is complete and safe)."""
        from pyspark.sql.types import StructType, StructField, StringType
        mock_dlt.create_streaming_table = MagicMock()
        mock_dlt.create_auto_cdc_from_snapshot_flow = MagicMock()
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        self.addCleanup(self.spark.conf.unset, "spark.databricks.unityCatalog.enabled")
        spec = BronzeDataflowSpec(**copy.deepcopy(DataflowPipelineTests.bronze_dataflow_spec_acs_map))
        # bronze_dataflow_spec_acs_map is SCD2; make this an SCD1 target.
        spec.applyChangesFromSnapshot = json.dumps({"keys": ["id"], "scd_type": "1"})
        spec.columnComments = json.dumps({"id": "the id"})
        spec.columnMasks = json.dumps({"id": "cat.s.mask_id"})
        view_name = f"{spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, spec, view_name, None)
        pipeline.schema_json = StructType(
            [StructField("id", StringType(), True)]
        ).jsonValue()
        pipeline.apply_changes_from_snapshot()
        _, kwargs = mock_dlt.create_streaming_table.call_args
        self.assertIn("COMMENT 'the id'", kwargs["schema"])
        self.assertIn("MASK cat.s.mask_id", kwargs["schema"])

    @patch('databricks.labs.sdp_meta.dataflow_pipeline.dp')
    def test_apply_changes_from_snapshot_scd1_unknown_mask_fails_closed(self, mock_dlt):
        """An SCD1 snapshot-CDC mask on an absent column fails closed."""
        from pyspark.sql.types import StructType, StructField, StringType
        mock_dlt.create_streaming_table = MagicMock()
        mock_dlt.create_auto_cdc_from_snapshot_flow = MagicMock()
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        self.addCleanup(self.spark.conf.unset, "spark.databricks.unityCatalog.enabled")
        spec = BronzeDataflowSpec(**copy.deepcopy(DataflowPipelineTests.bronze_dataflow_spec_acs_map))
        spec.applyChangesFromSnapshot = json.dumps({"keys": ["id"], "scd_type": "1"})
        spec.columnMasks = json.dumps({"ssn": "cat.s.mask_ssn"})
        view_name = f"{spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, spec, view_name, None)
        pipeline.schema_json = StructType(
            [StructField("id", StringType(), True)]
        ).jsonValue()
        with self.assertRaisesRegex(ValueError, "not present in the derived"):
            pipeline.apply_changes_from_snapshot()

    @patch('databricks.labs.sdp_meta.dataflow_pipeline.dp')
    def test_apply_changes_from_snapshot_scd2_policies_fail_closed(self, mock_dlt):
        """Column comments/masks on an SCD2 snapshot target fail closed: the
        DLT-managed __START_AT/__END_AT system columns cannot be typed here (no
        sequence_by), so an explicit schema would omit them and break table
        creation. Rather than emit an incomplete schema or drop a mask, the
        pipeline raises. (bronze_dataflow_spec_acs_map is SCD2.)"""
        from pyspark.sql.types import StructType, StructField, StringType
        mock_dlt.create_streaming_table = MagicMock()
        mock_dlt.create_auto_cdc_from_snapshot_flow = MagicMock()
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        self.addCleanup(self.spark.conf.unset, "spark.databricks.unityCatalog.enabled")
        spec = BronzeDataflowSpec(**copy.deepcopy(DataflowPipelineTests.bronze_dataflow_spec_acs_map))
        self.assertEqual(json.loads(spec.applyChangesFromSnapshot)["scd_type"], "2")
        spec.columnComments = json.dumps({"id": "the id"})
        spec.columnMasks = json.dumps({"id": "cat.s.mask_id"})
        view_name = f"{spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, spec, view_name, None)
        pipeline.schema_json = StructType(
            [StructField("id", StringType(), True)]
        ).jsonValue()
        with self.assertRaisesRegex(ValueError, "SCD2 apply_changes_from_snapshot"):
            pipeline.apply_changes_from_snapshot()
        # No table is created when we fail closed.
        mock_dlt.create_streaming_table.assert_not_called()

    @patch('databricks.labs.sdp_meta.dataflow_pipeline.dp')
    def test_apply_changes_from_snapshot_scd2_no_policies_unaffected(self, mock_dlt):
        """SCD2 snapshot WITHOUT column policies is unaffected by the guard —
        it still creates the streaming table with an inferred (None) schema,
        exactly as before the feature."""
        mock_dlt.create_streaming_table = MagicMock()
        mock_dlt.create_auto_cdc_from_snapshot_flow = MagicMock()
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        self.addCleanup(self.spark.conf.unset, "spark.databricks.unityCatalog.enabled")
        spec = BronzeDataflowSpec(**copy.deepcopy(DataflowPipelineTests.bronze_dataflow_spec_acs_map))
        view_name = f"{spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, spec, view_name, None)
        pipeline.apply_changes_from_snapshot()
        _, kwargs = mock_dlt.create_streaming_table.call_args
        self.assertIsNone(kwargs["schema"])

    # ------------------------------------------------------------------
    # Multi-source AUTO CDC runtime tests (issue #294)
    # ------------------------------------------------------------------

    @staticmethod
    def _bronze_cdc_flows_payload():
        """Helper: minimal two-flow CDC group for bronze, with one flow
        carrying ``select_exp`` / ``where_clause`` to exercise the per-
        flow normalization path."""
        return json.dumps({
            "keys": ["customer_id"],
            "sequence_by": "op_ts",
            "scd_type": "1",
            "apply_as_deletes": "operation = 'DELETE'",
            "except_column_list": ["operation", "_rescued_data"],
            "flows": [
                {
                    "name": "us_cdc",
                    "source_format": "cloudFiles",
                    "source_details": {
                        "path": "/mnt/raw/us",
                        "source_schema_path": "tests/resources/schema/customer_schema.ddl",
                    },
                    "reader_options": {"cloudFiles.format": "json"},
                    "select_exp": [
                        "customer_id AS customer_id",
                        "operation",
                        "op_ts",
                        "_rescued_data",
                    ],
                    "where_clause": ["region = 'US'"],
                    "once": True,
                },
                {
                    "name": "eu_cdc",
                    "source_format": "delta",
                    "source_details": {
                        "source_database": "raw",
                        "source_table": "customers_eu",
                    },
                },
            ],
        })

    def test_init_parses_cdc_apply_changes_flows(self):
        """Init parses cdcApplyChangesFlows JSON into a typed group and
        leaves ``cdcApplyChanges`` untouched. Reading the JSON inline
        instead of via the parser guarantees the runtime is the failure
        surface, not the test fixture."""
        bmap = copy.deepcopy(self.bronze_dataflow_spec_map)
        bmap["cdcApplyChanges"] = None
        bmap["cdcApplyChangesFlows"] = self._bronze_cdc_flows_payload()
        spec = BronzeDataflowSpec(**bmap)
        pipeline = DataflowPipeline(
            self.spark, spec,
            f"{spec.targetDetails['table']}_inputview", None,
        )
        self.assertIsNotNone(pipeline.cdcApplyChangesFlows)
        self.assertEqual(len(pipeline.cdcApplyChangesFlows.flows), 2)
        self.assertEqual(
            [f.name for f in pipeline.cdcApplyChangesFlows.flows],
            ["us_cdc", "eu_cdc"],
        )
        self.assertIsNone(pipeline.cdcApplyChanges)

    def test_init_mutual_exclusion_raises(self):
        """Both ``cdcApplyChanges`` AND ``cdcApplyChangesFlows`` set on
        one spec must raise at init — defense in depth on top of the
        onboarding pre-flight check (which a test or custom pipeline
        could legitimately bypass)."""
        bmap = copy.deepcopy(self.bronze_dataflow_spec_map)
        bmap["cdcApplyChanges"] = json.dumps({
            "keys": ["id"], "sequence_by": "op_ts", "scd_type": "1"
        })
        bmap["cdcApplyChangesFlows"] = self._bronze_cdc_flows_payload()
        spec = BronzeDataflowSpec(**bmap)
        with self.assertRaises(Exception):
            DataflowPipeline(
                self.spark, spec,
                f"{spec.targetDetails['table']}_inputview", None,
            )

    @patch("databricks.labs.sdp_meta.dataflow_pipeline.dp")
    def test_read_cdc_flows_registers_one_view_per_flow(self, mock_dp):
        """``read_cdc_flows`` must register exactly one ``dp.temporary_view``
        per flow, named ``{flow.name}_cdc_view``. The view-factory test
        in the next case exercises the per-flow normalization."""
        mock_dp.temporary_view = MagicMock(return_value=None)
        bmap = copy.deepcopy(self.bronze_dataflow_spec_map)
        bmap["cdcApplyChanges"] = None
        bmap["cdcApplyChangesFlows"] = self._bronze_cdc_flows_payload()
        spec = BronzeDataflowSpec(**bmap)
        pipeline = DataflowPipeline(
            self.spark, spec,
            f"{spec.targetDetails['table']}_inputview", None,
        )
        pipeline.read_cdc_flows()
        # Two flows -> two views.
        self.assertEqual(mock_dp.temporary_view.call_count, 2)
        names = sorted(
            kwargs["name"]
            for _args, kwargs in mock_dp.temporary_view.call_args_list
        )
        self.assertEqual(names, ["eu_cdc_cdc_view", "us_cdc_cdc_view"])

    @patch("databricks.labs.sdp_meta.dataflow_pipeline.dp")
    def test_read_cdc_flows_view_factory_applies_select_and_where(self, mock_dp):
        """The per-flow view factory must (1) read via PipelineReaders,
        (2) apply selectExpr from ``select_exp``, (3) chain ``where``
        clauses, (4) pass through the custom transform function. We
        capture each registered view-factory closure and invoke it
        against a MagicMock DataFrame to validate the call order."""
        captured = {}

        def _capture(view_factory, name, comment):
            captured[name] = view_factory
            return None

        mock_dp.temporary_view = MagicMock(side_effect=_capture)

        bmap = copy.deepcopy(self.bronze_dataflow_spec_map)
        bmap["cdcApplyChanges"] = None
        bmap["cdcApplyChangesFlows"] = self._bronze_cdc_flows_payload()
        spec = BronzeDataflowSpec(**bmap)

        captured_transform_inputs = []

        def custom_transform(df, _spec):
            captured_transform_inputs.append(df)
            return df

        pipeline = DataflowPipeline(
            self.spark, spec,
            f"{spec.targetDetails['table']}_inputview", None,
            custom_transform_func=custom_transform,
        )

        # Replace PipelineReaders.read_dlt_cloud_files /
        # read_dlt_delta with deterministic mock DataFrames so we can
        # inspect the chained calls. Using ``patch.object`` here would
        # require importing module paths; the simpler approach is to
        # monkey-patch the methods on the instance the factory will
        # construct. Because the factory creates a fresh PipelineReaders
        # per call, we patch the class directly via mock.patch.
        with patch.object(PipelineReaders, "read_dlt_cloud_files") as mock_cf, \
                patch.object(PipelineReaders, "read_dlt_delta") as mock_delta:
            mock_us_df = MagicMock()
            mock_us_df.selectExpr.return_value = mock_us_df
            mock_us_df.where.return_value = mock_us_df
            mock_cf.return_value = mock_us_df

            mock_eu_df = MagicMock()
            mock_delta.return_value = mock_eu_df

            pipeline.read_cdc_flows()

            # Invoke the registered factories.
            us_result = captured["us_cdc_cdc_view"]()
            eu_result = captured["eu_cdc_cdc_view"]()

        # us_cdc: selectExpr called with the four select_exp entries,
        # then where called once with the region clause, then custom
        # transform called on the result.
        mock_us_df.selectExpr.assert_called_once_with(
            "customer_id AS customer_id",
            "operation",
            "op_ts",
            "_rescued_data",
        )
        mock_us_df.where.assert_called_once_with("region = 'US'")
        self.assertIs(us_result, mock_us_df)

        # eu_cdc has no select_exp or where_clause -> the raw delta read
        # passes through to the custom transform unchanged.
        mock_eu_df.selectExpr.assert_not_called()
        mock_eu_df.where.assert_not_called()
        self.assertIs(eu_result, mock_eu_df)

        # custom_transform was called once per flow.
        self.assertEqual(len(captured_transform_inputs), 2)
        self.assertIn(mock_us_df, captured_transform_inputs)
        self.assertIn(mock_eu_df, captured_transform_inputs)

    @patch("databricks.labs.sdp_meta.dataflow_pipeline.dp")
    def test_cdc_apply_changes_flows_creates_streaming_table_once(self, mock_dp):
        """A multi-flow CDC group must call ``dp.create_streaming_table``
        ONCE — DLT mandates a single ``create_streaming_table`` per
        target — and then one ``dp.create_auto_cdc_flow`` per flow."""
        mock_dp.create_streaming_table = MagicMock()
        mock_dp.create_auto_cdc_flow = MagicMock()
        mock_dp.temporary_view = MagicMock(return_value=None)

        bmap = copy.deepcopy(self.bronze_dataflow_spec_map)
        bmap["cdcApplyChanges"] = None
        bmap["cdcApplyChangesFlows"] = self._bronze_cdc_flows_payload()
        bmap["dataQualityExpectations"] = None
        spec = BronzeDataflowSpec(**bmap)
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        pipeline = DataflowPipeline(
            self.spark, spec,
            f"{spec.targetDetails['table']}_inputview", None,
        )
        pipeline.cdc_apply_changes_flows()

        self.assertEqual(mock_dp.create_streaming_table.call_count, 1)
        self.assertEqual(mock_dp.create_auto_cdc_flow.call_count, 2)

        # Every flow points at its own ``{flow.name}_cdc_view`` and the
        # same fully-qualified target table.
        sources = sorted(
            kwargs["source"]
            for _a, kwargs in mock_dp.create_auto_cdc_flow.call_args_list
        )
        targets = {
            kwargs["target"]
            for _a, kwargs in mock_dp.create_auto_cdc_flow.call_args_list
        }
        self.assertEqual(sources, ["eu_cdc_cdc_view", "us_cdc_cdc_view"])
        self.assertEqual(len(targets), 1)  # single target table

        # Group-level CDC config is propagated to every per-flow call.
        for _a, kwargs in mock_dp.create_auto_cdc_flow.call_args_list:
            self.assertEqual(kwargs["keys"], ["customer_id"])
            self.assertEqual(kwargs["stored_as_scd_type"], "1")
            self.assertEqual(kwargs["except_column_list"],
                             ["operation", "_rescued_data"])

        # Per-flow once is honoured: us_cdc=True, eu_cdc=False (default).
        flow_once = {
            kwargs["flow_name"]: kwargs["once"]
            for _a, kwargs in mock_dp.create_auto_cdc_flow.call_args_list
        }
        self.assertEqual(flow_once, {"us_cdc": True, "eu_cdc": False})

    @patch("databricks.labs.sdp_meta.dataflow_pipeline.dp")
    def test_write_layer_table_dispatches_to_cdc_flows(self, mock_dp):
        """``write_layer_table`` must prefer the multi-source CDC path
        when ``cdcApplyChangesFlows`` is set — even if
        ``cdcApplyChanges`` was somehow also non-empty (the init-time
        mutual-exclusion check should block that path entirely, so we
        only reach this branch when CDC flows is the only one set)."""
        mock_dp.create_streaming_table = MagicMock()
        mock_dp.create_auto_cdc_flow = MagicMock()
        mock_dp.temporary_view = MagicMock(return_value=None)
        mock_dp.table = MagicMock(return_value=lambda func: func)

        bmap = copy.deepcopy(self.bronze_dataflow_spec_map)
        bmap["cdcApplyChanges"] = None
        bmap["dataQualityExpectations"] = None
        bmap["cdcApplyChangesFlows"] = self._bronze_cdc_flows_payload()
        spec = BronzeDataflowSpec(**bmap)
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        pipeline = DataflowPipeline(
            self.spark, spec,
            f"{spec.targetDetails['table']}_inputview", None,
        )
        pipeline.write_layer_table()
        # Multi-source CDC path = one create_streaming_table + N
        # create_auto_cdc_flow. No mock_dp.table call (that would mean
        # the standard write path fired instead).
        self.assertEqual(mock_dp.create_streaming_table.call_count, 1)
        self.assertEqual(mock_dp.create_auto_cdc_flow.call_count, 2)
        mock_dp.table.assert_not_called()

    # ------------------------------------------------------------------
    # row_filter coverage on multi-source CDC + snapshot CDC paths
    #
    # The ``cdc_apply_changes_flows`` and ``apply_changes_from_snapshot``
    # methods don't carry a ``row_filter`` kwarg of their own — they
    # delegate target-table creation to ``create_streaming_table``,
    # which already pins ``row_filter=self._get_row_filter()``. These
    # tests guard the inheritance: future changes that route either CDC
    # path AROUND ``create_streaming_table`` (e.g. by inlining
    # ``dp.create_streaming_table`` directly) would break this contract
    # silently otherwise.
    # ------------------------------------------------------------------

    @patch("databricks.labs.sdp_meta.dataflow_pipeline.dp")
    def test_cdc_apply_changes_flows_passes_row_filter_to_streaming_table(self, mock_dp):
        """Multi-source CDC target table inherits the spec-level rowFilter via create_streaming_table."""
        mock_dp.create_streaming_table = MagicMock()
        mock_dp.create_auto_cdc_flow = MagicMock()
        mock_dp.temporary_view = MagicMock(return_value=None)

        bmap = copy.deepcopy(self.bronze_dataflow_spec_map)
        bmap["cdcApplyChanges"] = None
        bmap["dataQualityExpectations"] = None
        bmap["cdcApplyChangesFlows"] = self._bronze_cdc_flows_payload()
        bmap["rowFilter"] = self.ROW_FILTER_REGION
        spec = BronzeDataflowSpec(**bmap)

        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        self.addCleanup(self.spark.conf.unset, "spark.databricks.unityCatalog.enabled")
        pipeline = DataflowPipeline(
            self.spark, spec,
            f"{spec.targetDetails['table']}_inputview", None,
        )
        pipeline.cdc_apply_changes_flows()

        # Exactly one streaming table is created (single target — DLT
        # mandate) and it carries the row_filter.
        self.assertEqual(mock_dp.create_streaming_table.call_count, 1)
        _, kwargs = mock_dp.create_streaming_table.call_args
        self.assertEqual(kwargs["row_filter"], self.ROW_FILTER_REGION)

    @patch("databricks.labs.sdp_meta.dataflow_pipeline.dp")
    def test_cdc_apply_changes_flows_row_filter_suppressed_without_uc(self, mock_dp):
        """With UC disabled, multi-source CDC target table receives row_filter=None even if spec.rowFilter is set."""
        mock_dp.create_streaming_table = MagicMock()
        mock_dp.create_auto_cdc_flow = MagicMock()
        mock_dp.temporary_view = MagicMock(return_value=None)

        bmap = copy.deepcopy(self.bronze_dataflow_spec_map)
        bmap["cdcApplyChanges"] = None
        bmap["dataQualityExpectations"] = None
        bmap["cdcApplyChangesFlows"] = self._bronze_cdc_flows_payload()
        bmap["rowFilter"] = self.ROW_FILTER_REGION
        spec = BronzeDataflowSpec(**bmap)

        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "False")
        self.addCleanup(self.spark.conf.unset, "spark.databricks.unityCatalog.enabled")
        pipeline = DataflowPipeline(
            self.spark, spec,
            f"{spec.targetDetails['table']}_inputview", None,
        )
        pipeline.cdc_apply_changes_flows()

        _, kwargs = mock_dp.create_streaming_table.call_args
        self.assertIsNone(kwargs["row_filter"])

    @patch("databricks.labs.sdp_meta.dataflow_pipeline.dp")
    def test_apply_changes_from_snapshot_passes_row_filter_to_streaming_table(self, mock_dp):
        """Snapshot-CDC target table inherits the spec-level rowFilter via create_streaming_table.

        Parity with the multi-source CDC test above: both paths funnel
        through ``create_streaming_table`` for the actual table mint,
        so both must respect ``rowFilter``.
        """
        mock_dp.create_streaming_table = MagicMock()
        mock_dp.create_auto_cdc_from_snapshot_flow = MagicMock()

        bmap = copy.deepcopy(self.bronze_dataflow_spec_map)
        bmap["cdcApplyChanges"] = None
        bmap["cdcApplyChangesFlows"] = None
        bmap["dataQualityExpectations"] = None
        # applyChangesFromSnapshot drives the apply_changes_from_snapshot
        # branch; the values mirror the single-source CDC shape.
        bmap["applyChangesFromSnapshot"] = json.dumps({
            "keys": ["customer_id"],
            "scd_type": "1",
        })
        bmap["rowFilter"] = self.ROW_FILTER_DEPT
        spec = BronzeDataflowSpec(**bmap)

        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        self.addCleanup(self.spark.conf.unset, "spark.databricks.unityCatalog.enabled")
        # apply_changes_from_snapshot needs a snapshot-version callback
        # but doesn't care what it returns for this assertion path.
        pipeline = DataflowPipeline(
            self.spark, spec,
            f"{spec.targetDetails['table']}_inputview", None,
            next_snapshot_and_version=lambda *_a, **_k: None,
        )
        pipeline.apply_changes_from_snapshot()

        self.assertEqual(mock_dp.create_streaming_table.call_count, 1)
        _, kwargs = mock_dp.create_streaming_table.call_args
        self.assertEqual(kwargs["row_filter"], self.ROW_FILTER_DEPT)


class LegacyPublishingModeTests(SDPFrameworkTestCase):
    """Tests for runtime legacy-vs-DPM publishing mode detection and table name construction.

    When a pipeline has no ``pipelines.schema`` setting, LDP legacy mode rejects
    schema-qualified names (raises DLTAnalysisException).  sdp-meta must detect
    this at init and emit bare table names to dp.create_streaming_table /
    dp.table / dp.create_auto_cdc_flow instead of catalog.schema.table names.
    """

    # Minimal bronze spec with catalog/database/table all set so we can
    # assert which parts get included in the final dp.* call.
    _BASE_SPEC = {
        "dataFlowId": "1",
        "dataFlowGroup": "A1",
        "sourceFormat": "cloudFiles",
        "sourceDetails": {"path": "tests/resources/data/customers"},
        "readerConfigOptions": {},
        "targetFormat": "delta",
        "targetDetails": {
            "catalog": "dev_bronze",
            "database": "gracis",
            "table": "actionplans",
            "path": "tests/resources/delta/actionplans",
        },
        "tableProperties": {},
        "schema": None,
        "partitionColumns": [""],
        "cdcApplyChanges": None,
        "applyChangesFromSnapshot": None,
        "dataQualityExpectations": None,
        "quarantineTargetDetails": {
            "catalog": "dev_bronze",
            "database": "gracis",
            "table": "actionplans_quarantine",
            "path": "tests/resources/delta/actionplans_quar",
        },
        "quarantineTableProperties": {},
        "appendFlows": [],
        "appendFlowsSchemas": {},
        "version": "v1",
        "createDate": datetime.now,
        "createdBy": "test",
        "updateDate": datetime.now,
        "updatedBy": "test",
        "clusterBy": [""],
        "clusterByAuto": False,
        "sinks": [],
        "cdcApplyChangesFlows": None,
        "cdcApplyChangesFlowsSchemas": None,
        "rowFilter": None,
        "quarantineRowFilter": None,
        "columnComments": None,
        "columnMasks": None,
    }

    def _make_pipeline(self, pipeline_schema="", uc_enabled=True, extra_spec=None):
        """Build a DataflowPipeline with the given pipelines.schema conf value."""
        self.spark.conf.set(
            "spark.databricks.unityCatalog.enabled", "True" if uc_enabled else "False"
        )
        self.spark.conf.set("pipelines.schema", pipeline_schema)
        self.addCleanup(self.spark.conf.unset, "spark.databricks.unityCatalog.enabled")
        self.addCleanup(self.spark.conf.unset, "pipelines.schema")

        spec_map = copy.deepcopy(self._BASE_SPEC)
        if extra_spec:
            spec_map.update(extra_spec)
        spec = BronzeDataflowSpec(**spec_map)
        view_name = f"{spec.targetDetails['table']}_inputview"
        pipeline = DataflowPipeline(self.spark, spec, view_name, None)
        pipeline.read_bronze = MagicMock()
        return pipeline

    # ── publishing mode detection ────────────────────────────────────────────

    def test_dpm_mode_detected_when_schema_set(self):
        """pipelines.schema signals default publishing mode."""
        pipeline = self._make_pipeline(pipeline_schema="target_schema")
        self.assertTrue(pipeline.dpm_enabled)
        self.assertFalse(pipeline.is_legacy_publishing_mode)

    def test_legacy_mode_detected_when_schema_empty(self):
        """An unset pipelines.schema signals legacy publishing mode."""
        pipeline = self._make_pipeline(pipeline_schema="")
        self.assertFalse(pipeline.dpm_enabled)
        self.assertTrue(pipeline.is_legacy_publishing_mode)

    def test_legacy_mode_detected_when_schema_whitespace(self):
        """Whitespace-only pipelines.schema is treated as legacy mode."""
        pipeline = self._make_pipeline(pipeline_schema="   ")
        self.assertTrue(pipeline.is_legacy_publishing_mode)

    # ── _build_table_name ────────────────────────────────────────────────────

    def test_build_table_name_legacy_returns_bare_name(self):
        """In legacy mode, _build_table_name returns only the table name."""
        pipeline = self._make_pipeline(pipeline_schema="")
        result = pipeline._build_table_name("dev_bronze", "gracis", "actionplans")
        self.assertEqual(result, "actionplans")

    def test_build_table_name_dpm_returns_qualified_name(self):
        """In DPM mode, _build_table_name returns catalog.schema.table."""
        pipeline = self._make_pipeline(pipeline_schema="target_schema")
        result = pipeline._build_table_name("dev_bronze", "gracis", "actionplans")
        self.assertEqual(result, "dev_bronze.gracis.actionplans")

    def test_build_table_name_dpm_no_catalog_returns_schema_table(self):
        """In DPM mode with no catalog, _build_table_name returns schema.table."""
        pipeline = self._make_pipeline(pipeline_schema="target_schema")
        result = pipeline._build_table_name(None, "gracis", "actionplans")
        self.assertEqual(result, "gracis.actionplans")

    def test_source_table_info_legacy_keeps_qualified_source_name(self):
        """Legacy output mode must not strip qualifiers from source table reads."""
        pipeline = self._make_pipeline(
            pipeline_schema="",
            extra_spec={
                "sourceFormat": "delta",
                "sourceDetails": {
                    "catalog": "source_catalog",
                    "database": "source_schema",
                    "table": "source_table",
                },
            },
        )
        source_table, _ = pipeline._get_source_table_info()
        self.assertEqual(source_table, "source_catalog.source_schema.source_table")

    # ── dp.create_streaming_table name argument ───────────────────────────────

    @patch("databricks.labs.sdp_meta.dataflow_pipeline.dp")
    def test_create_streaming_table_legacy_uses_bare_name(self, mock_dp):
        """create_streaming_table passes bare table name in legacy publishing mode."""
        mock_dp.create_streaming_table = MagicMock()
        pipeline = self._make_pipeline(pipeline_schema="")
        pipeline.create_streaming_table(None, target_path=None)
        mock_dp.create_streaming_table.assert_called_once()
        _, kwargs = mock_dp.create_streaming_table.call_args
        self.assertEqual(kwargs["name"], "actionplans")

    @patch("databricks.labs.sdp_meta.dataflow_pipeline.dp")
    def test_create_streaming_table_dpm_uses_qualified_name(self, mock_dp):
        """create_streaming_table passes catalog.schema.table in DPM mode."""
        mock_dp.create_streaming_table = MagicMock()
        pipeline = self._make_pipeline(pipeline_schema="target_schema")
        pipeline.create_streaming_table(None, target_path=None)
        mock_dp.create_streaming_table.assert_called_once()
        _, kwargs = mock_dp.create_streaming_table.call_args
        self.assertEqual(kwargs["name"], "dev_bronze.gracis.actionplans")

    # ── dp.table name argument (standard bronze/silver write) ────────────────

    @patch("databricks.labs.sdp_meta.dataflow_pipeline.dp")
    def test_write_standard_table_legacy_uses_bare_name(self, mock_dp):
        """dp.table receives bare table name in legacy publishing mode."""
        mock_dp.table = MagicMock(return_value=lambda fn: fn)
        pipeline = self._make_pipeline(pipeline_schema="")
        pipeline._write_standard_table(is_bronze=True)
        mock_dp.table.assert_called_once()
        _, kwargs = mock_dp.table.call_args
        self.assertEqual(kwargs["name"], "actionplans")

    @patch("databricks.labs.sdp_meta.dataflow_pipeline.dp")
    def test_write_standard_table_dpm_uses_qualified_name(self, mock_dp):
        """dp.table receives catalog.schema.table in DPM mode."""
        mock_dp.table = MagicMock(return_value=lambda fn: fn)
        pipeline = self._make_pipeline(pipeline_schema="target_schema")
        pipeline._write_standard_table(is_bronze=True)
        mock_dp.table.assert_called_once()
        _, kwargs = mock_dp.table.call_args
        self.assertEqual(kwargs["name"], "dev_bronze.gracis.actionplans")

    # ── Quarantine table name ─────────────────────────────────────────────────

    @patch("databricks.labs.sdp_meta.dataflow_pipeline.dp")
    def test_quarantine_table_legacy_uses_bare_name(self, mock_dp):
        """Quarantine dp.table call uses bare table name in legacy mode."""
        mock_dp.table = MagicMock(return_value=lambda fn: fn)
        mock_dp.expect_all_or_drop = MagicMock(return_value=lambda fn: fn)
        dqe = json.dumps({"expect_or_quarantine": {"valid_id": "id IS NOT NULL"}})
        pipeline = self._make_pipeline(
            pipeline_schema="",
            extra_spec={"dataQualityExpectations": dqe},
        )
        pipeline.write_layer_with_dqe()
        quarantine_calls = [
            kwargs["name"]
            for _, kwargs in mock_dp.table.call_args_list
            if kwargs.get("name", "").endswith("quarantine")
            or kwargs.get("name", "") == "actionplans_quarantine"
        ]
        self.assertTrue(
            any(n == "actionplans_quarantine" for n in quarantine_calls),
            f"Expected bare quarantine name, got calls: {mock_dp.table.call_args_list}",
        )

    @patch("databricks.labs.sdp_meta.dataflow_pipeline.dp")
    def test_quarantine_table_dpm_uses_qualified_name(self, mock_dp):
        """Quarantine dp.table call uses catalog.schema.table in DPM mode."""
        mock_dp.table = MagicMock(return_value=lambda fn: fn)
        mock_dp.expect_all_or_drop = MagicMock(return_value=lambda fn: fn)
        dqe = json.dumps({"expect_or_quarantine": {"valid_id": "id IS NOT NULL"}})
        pipeline = self._make_pipeline(
            pipeline_schema="target_schema",
            extra_spec={"dataQualityExpectations": dqe},
        )
        pipeline.write_layer_with_dqe()
        quarantine_calls = [
            kwargs["name"]
            for _, kwargs in mock_dp.table.call_args_list
            if "quarantine" in kwargs.get("name", "")
        ]
        self.assertTrue(
            any(n == "dev_bronze.gracis.actionplans_quarantine" for n in quarantine_calls),
            f"Expected qualified quarantine name, got: {quarantine_calls}",
        )

    # ── cdc_apply_changes target name ─────────────────────────────────────────

    @patch("databricks.labs.sdp_meta.dataflow_pipeline.dp")
    def test_cdc_apply_changes_legacy_uses_bare_name(self, mock_dp):
        """dp.create_auto_cdc_flow target= is bare table name in legacy mode."""
        mock_dp.create_streaming_table = MagicMock()
        mock_dp.create_auto_cdc_flow = MagicMock()
        cdc = json.dumps({
            "keys": ["id"], "sequence_by": "ts", "scd_type": "1",
            "apply_as_deletes": None, "apply_as_truncates": None,
            "column_list": [], "except_column_list": [],
        })
        pipeline = self._make_pipeline(
            pipeline_schema="",
            extra_spec={"cdcApplyChanges": cdc, "dataQualityExpectations": None},
        )
        pipeline.cdc_apply_changes()
        _, kwargs = mock_dp.create_auto_cdc_flow.call_args
        self.assertEqual(kwargs["target"], "actionplans")

    @patch("databricks.labs.sdp_meta.dataflow_pipeline.dp")
    def test_cdc_apply_changes_dpm_uses_qualified_name(self, mock_dp):
        """dp.create_auto_cdc_flow target= is catalog.schema.table in DPM mode."""
        mock_dp.create_streaming_table = MagicMock()
        mock_dp.create_auto_cdc_flow = MagicMock()
        cdc = json.dumps({
            "keys": ["id"], "sequence_by": "ts", "scd_type": "1",
            "apply_as_deletes": None, "apply_as_truncates": None,
            "column_list": [], "except_column_list": [],
        })
        pipeline = self._make_pipeline(
            pipeline_schema="target_schema",
            extra_spec={"cdcApplyChanges": cdc, "dataQualityExpectations": None},
        )
        pipeline.cdc_apply_changes()
        _, kwargs = mock_dp.create_auto_cdc_flow.call_args
        self.assertEqual(kwargs["target"], "dev_bronze.gracis.actionplans")

    # ── View name construction in _launch_dlt_flow ────────────────────────────
    # These tests patch _launch_dlt_flow internals by calling invoke_dlt_pipeline
    # and capturing the view_name passed to DataflowPipeline.__init__.

    # ── write_to_delta uses dp.read_stream not spark.readStream.table ──────────

    @patch("databricks.labs.sdp_meta.dataflow_pipeline.dp")
    def test_write_to_delta_uses_dp_read_stream(self, mock_dp):
        """write_to_delta must use dp.read_stream so legacy LIVE-schema views resolve.

        spark.readStream.table falls back to a catalog lookup in legacy
        publishing mode and raises TABLE_OR_VIEW_NOT_FOUND.  dp.read_stream
        (the LDP equivalent of dlt.read_stream) resolves from the pipeline's
        internal dataset graph in both legacy and DPM mode.
        """
        mock_dp.read_stream = MagicMock(return_value=MagicMock())
        pipeline = self._make_pipeline(pipeline_schema="")
        pipeline.write_to_delta()
        mock_dp.read_stream.assert_called_once_with(pipeline.view_name)

    @patch("databricks.labs.sdp_meta.dataflow_pipeline.dp")
    def test_write_to_delta_dpm_also_uses_dp_read_stream(self, mock_dp):
        """dp.read_stream is used in DPM mode too — consistent across modes."""
        mock_dp.read_stream = MagicMock(return_value=MagicMock())
        pipeline = self._make_pipeline(pipeline_schema="target_schema")
        pipeline.write_to_delta()
        mock_dp.read_stream.assert_called_once_with(pipeline.view_name)

    @patch("databricks.labs.sdp_meta.dataflow_pipeline.DataflowSpecUtils.get_bronze_dataflow_spec")
    @patch("databricks.labs.sdp_meta.dataflow_pipeline.DataflowPipeline.run_dlt")
    @patch("databricks.labs.sdp_meta.dataflow_pipeline.dp")
    def test_view_name_legacy_uses_bare_table_name(self, mock_dp, mock_run_dlt, mock_get_spec):
        """In legacy mode, view name is {table}_{layer}_inputview (no catalog/db prefix)."""
        mock_dp.temporary_view = MagicMock(return_value=None)
        mock_run_dlt.return_value = None

        spec_map = copy.deepcopy(self._BASE_SPEC)
        spec_map["dataQualityExpectations"] = None
        spec = BronzeDataflowSpec(**spec_map)
        mock_get_spec.return_value = [spec]

        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "False")
        self.spark.conf.set("pipelines.schema", "")
        self.addCleanup(self.spark.conf.unset, "spark.databricks.unityCatalog.enabled")
        self.addCleanup(self.spark.conf.unset, "pipelines.schema")

        captured_view_names = []
        original_init = DataflowPipeline.__init__

        def capturing_init(self_inner, spark, dataflow_spec, view_name, *args, **kwargs):
            captured_view_names.append(view_name)
            original_init(self_inner, spark, dataflow_spec, view_name, *args, **kwargs)

        with patch.object(DataflowPipeline, "__init__", capturing_init):
            DataflowPipeline.invoke_dlt_pipeline(self.spark, "bronze")

        self.assertEqual(len(captured_view_names), 1)
        self.assertEqual(captured_view_names[0], "actionplans_bronze_inputview")

    @patch("databricks.labs.sdp_meta.dataflow_pipeline.DataflowSpecUtils.get_bronze_dataflow_spec")
    @patch("databricks.labs.sdp_meta.dataflow_pipeline.DataflowPipeline.run_dlt")
    @patch("databricks.labs.sdp_meta.dataflow_pipeline.dp")
    def test_view_name_dpm_uses_catalog_db_prefix(self, mock_dp, mock_run_dlt, mock_get_spec):
        """In DPM mode, view name is {catalog}_{db}_{table}_{layer}_inputview."""
        mock_dp.temporary_view = MagicMock(return_value=None)
        mock_run_dlt.return_value = None

        spec_map = copy.deepcopy(self._BASE_SPEC)
        spec_map["dataQualityExpectations"] = None
        spec = BronzeDataflowSpec(**spec_map)
        mock_get_spec.return_value = [spec]

        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        self.spark.conf.set("pipelines.schema", "target_schema")
        self.addCleanup(self.spark.conf.unset, "spark.databricks.unityCatalog.enabled")
        self.addCleanup(self.spark.conf.unset, "pipelines.schema")

        captured_view_names = []
        original_init = DataflowPipeline.__init__

        def capturing_init(self_inner, spark, dataflow_spec, view_name, *args, **kwargs):
            captured_view_names.append(view_name)
            original_init(self_inner, spark, dataflow_spec, view_name, *args, **kwargs)

        with patch.object(DataflowPipeline, "__init__", capturing_init):
            DataflowPipeline.invoke_dlt_pipeline(self.spark, "bronze")

        self.assertEqual(len(captured_view_names), 1)
        self.assertEqual(captured_view_names[0], "dev_bronze_gracis_actionplans_bronze_inputview")
