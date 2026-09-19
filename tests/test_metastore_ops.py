"""Test for MetastoreOps class."""
from datetime import datetime, timedelta

from tests.utils import SDPFrameworkTestCase


class MetastoreOpsTests(SDPFrameworkTestCase):
    """Test for MetastoreOps class."""

    def _assert_additive_spec_merge(
        self, table, layer_field, layer_value, path=None
    ):
        table_name = f"ravi_dlt_demo.{table}"
        original_created = datetime(2026, 1, 1)
        original_updated = datetime(2026, 1, 2)
        source_updated = original_updated + timedelta(days=1)
        target = self.spark.createDataFrame([
            {
                "dataFlowId": "existing",
                "payload": "before",
                "createDate": original_created,
                "createdBy": "original-author",
                "updateDate": original_updated,
                "updatedBy": "original-author",
            }
        ])
        if path is None:
            target.write.format("delta").saveAsTable(table_name)
        else:
            target.write.format("delta").save(path)
            self.deltaPipelinesMetaStoreOps.register_table_in_metastore(
                "ravi_dlt_demo", table, path
            )

        source_rows = []
        for data_flow_id, payload in (
            ("existing", "updated"),
            ("new", "inserted"),
        ):
            row = {
                "dataFlowId": data_flow_id,
                "payload": payload,
                "createDate": source_updated,
                "createdBy": "new-author",
                "updateDate": source_updated,
                "updatedBy": "new-author",
                "clusterByAuto": True,
                "rowFilter": "ROW FILTER main.security.fn ON (tenant_id)",
                layer_field: layer_value,
            }
            source_rows.append(row)
        source = self.spark.createDataFrame(source_rows)

        for _ in range(2):
            self.deltaPipelinesInternalTableOps.merge(
                source,
                table_name,
                ["dataFlowId"],
                target.columns,
            )

        evolved = self.spark.table(table_name)
        self.assertIn("clusterByAuto", evolved.columns)
        self.assertIn("rowFilter", evolved.columns)
        self.assertIn(layer_field, evolved.columns)
        rows = {
            row["dataFlowId"]: row.asDict()
            for row in evolved.collect()
        }
        self.assertEqual(set(rows), {"existing", "new"})
        self.assertEqual(rows["existing"]["payload"], "updated")
        self.assertEqual(
            rows["existing"]["createDate"], original_created
        )
        self.assertEqual(
            rows["existing"]["createdBy"], "original-author"
        )
        self.assertTrue(rows["new"]["clusterByAuto"])
        self.assertEqual(rows["new"][layer_field], layer_value)

    def test_createDatabase(self):
        """Test create database."""
        db_name = "meta_store_test"
        comments = "unit test"
        self.deltaPipelinesMetaStoreOps.create_database(db_name, comments)
        db_list = self.spark.sql("SHOW DATABASES").collect()
        db_created = False
        for db in db_list:
            if db["namespace"] == db_name:
                db_created = True
        self.assertTrue(db_created)

    def test_tryRunningSql(self):
        """Test tryRunningsql."""
        db_name = "meta_store_test"
        self.deltaPipelinesMetaStoreOps.try_run_sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        db_list = self.spark.sql("SHOW DATABASES").collect()
        db_created = False
        for db in db_list:
            if db["namespace"] == db_name:
                db_created = True
        self.assertTrue(db_created)

    def test_dropDatabase(self):
        """Test dropDatabase."""
        db_name = "meta_store_test"
        self.deltaPipelinesMetaStoreOps.create_database(db_name, comments="unit test")
        self.deltaPipelinesMetaStoreOps.drop_database(db_name)
        db_list = self.spark.sql("SHOW DATABASES").collect()
        db_created = False
        for db in db_list:
            if db["namespace"] == db_name:
                db_created = True
        self.assertFalse(db_created)

    def test_resetTableInMetastore(self):
        """Test resetTable in metastore."""
        db_name = "meta_store_test"
        self.deltaPipelinesMetaStoreOps.create_database(db_name, comments="unit test")
        path = self.temp_delta_tables_path + "/temp_delta_table"
        table = "test_temp_delta_table"

        dept = [("Finance", 10), ("Marketing", 20), ("Sales", 30), ("IT", 40)]
        deptColumns = ["dept_name", "dept_id"]
        deptDF = self.spark.createDataFrame(data=dept, schema=deptColumns)
        deptDF.write.format("delta").save(path)

        self.deltaPipelinesMetaStoreOps.register_table_in_metastore(db_name, table, path)
        table_list = self.spark.sql(f"show tables in {db_name}")
        table_name = table_list.filter(table_list.tableName == table).collect()
        self.assertTrue(len(table_name) > 0)
        self.deltaPipelinesMetaStoreOps.reset_table_in_metastore(db_name, table, path)
        table_list = self.spark.sql(f"show tables in {db_name}")
        table_name = table_list.filter(table_list.tableName == table).collect()
        self.assertTrue(len(table_name) > 0)

    def test_registerTableInMetastore(self):
        """Test registerTables in metastore."""
        db_name = "meta_store_test"
        self.deltaPipelinesMetaStoreOps.create_database(db_name, comments="unit test")
        path = self.temp_delta_tables_path + "/temp_delta_table"
        table = "test_temp_delta_table"

        dept = [("Finance", 10), ("Marketing", 20), ("Sales", 30), ("IT", 40)]
        deptColumns = ["dept_name", "dept_id"]
        deptDF = self.spark.createDataFrame(data=dept, schema=deptColumns)
        deptDF.write.format("delta").save(path)

        self.deltaPipelinesMetaStoreOps.register_table_in_metastore(db_name, table, path)
        table_list = self.spark.sql(f"show tables in {db_name}")
        table_name = table_list.filter(table_list.tableName == table).collect()
        self.assertTrue(len(table_name) > 0)

    def test_deregisterTableFromMetastore(self):
        """Test deregistering tables."""
        db_name = "meta_store_test"
        self.deltaPipelinesMetaStoreOps.create_database(db_name, comments="unit test")
        path = self.temp_delta_tables_path + "/temp_delta_table"
        table = "test_temp_delta_table"

        dept = [("Finance", 10), ("Marketing", 20), ("Sales", 30), ("IT", 40)]
        deptColumns = ["dept_name", "dept_id"]
        deptDF = self.spark.createDataFrame(data=dept, schema=deptColumns)
        deptDF.write.format("delta").save(path)

        self.deltaPipelinesMetaStoreOps.register_table_in_metastore(db_name, table, path)
        table_list = self.spark.sql(f"show tables in {db_name}")
        table_name = table_list.filter(table_list.tableName == table).collect()
        self.assertTrue(len(table_name) > 0)
        self.deltaPipelinesMetaStoreOps.deregister_table_from_metastore(db_name, table)
        table_list = self.spark.sql(f"show tables in {db_name}")
        table_name = table_list.filter(table_list.tableName == table).collect()
        self.assertEqual(len(table_name), 0)

    def test_getTableLocation_positive(self):
        """Get Table location test."""
        db_name = "meta_store_test"
        self.deltaPipelinesMetaStoreOps.create_database(db_name, comments="unit test")
        path = self.temp_delta_tables_path + "/temp_delta_table"
        table = "test_temp_delta_table"

        dept = [("Finance", 10), ("Marketing", 20), ("Sales", 30), ("IT", 40)]
        deptColumns = ["dept_name", "dept_id"]
        deptDF = self.spark.createDataFrame(data=dept, schema=deptColumns)
        deptDF.write.format("delta").save(path)

        self.deltaPipelinesMetaStoreOps.register_table_in_metastore(db_name, table, path)
        location = self.deltaPipelinesMetaStoreOps.get_table_location(db_name, table)
        self.assertEqual(location, f"file:{path}")

    def test_merge_evolves_managed_legacy_bronze_spec_schema(self):
        self._assert_additive_spec_merge(
            "legacy_bronze_spec",
            "cdcApplyChangesFlowsSchemas",
            {"events": "id INT"},
        )

    def test_merge_evolves_path_backed_legacy_silver_spec_schema(self):
        self._assert_additive_spec_merge(
            "legacy_silver_spec",
            "cdcApplyChangesFlows",
            '{"keys":["id"],"flows":[]}',
            path=f"{self.temp_delta_tables_path}/legacy_silver_spec",
        )

    def test_merge_rejects_incompatible_existing_column_types(self):
        table_name = "ravi_dlt_demo.incompatible_spec"
        target = self.spark.createDataFrame(
            [{"dataFlowId": "1", "payload": "before"}]
        )
        target.write.format("delta").saveAsTable(table_name)
        source = self.spark.createDataFrame(
            [{"dataFlowId": 1, "payload": "after"}]
        )

        with self.assertRaisesRegex(
            ValueError,
            r"dataFlowId \(target=string, source=bigint\)",
        ):
            self.deltaPipelinesInternalTableOps.merge(
                source,
                table_name,
                ["dataFlowId"],
                target.columns,
            )
