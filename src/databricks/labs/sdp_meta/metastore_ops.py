"""Delta metasore ops."""
from delta.tables import DeltaTable
from pyspark.sql.functions import expr


class DeltaPipelinesMetaStoreOps:
    """This class handles all metastore operations for onboarding tables."""

    def __init__(self, spark):
        """Initialize."""
        self.spark = spark

    def create_database(self, database, comments):
        """Create Database."""
        self.try_run_sql(f"CREATE DATABASE IF NOT EXISTS {database} COMMENT '{comments}'")

    def try_run_sql(self, sql):
        """Try running SQL."""
        self.spark.sql(sql)

    def drop_database(self, database):
        """Drop database."""
        self.try_run_sql(f"DROP DATABASE IF EXISTS {database} CASCADE")

    def reset_table_in_metastore(self, database, table, path):
        """Reset table metadata."""
        self.deregister_table_from_metastore(database, table)
        self.register_table_in_metastore(database, table, path)

    def register_table_in_metastore(self, database, table, path):
        """Register table in metastore."""
        final_table_name = f"{database}.{table}"
        self.spark.sql("CREATE TABLE if not exists " + final_table_name + " USING DELTA LOCATION '" + path + "'")

    def deregister_table_from_metastore(self, database, table):
        """Deregister table."""
        self.try_run_sql(f"DROP TABLE IF EXISTS {database}.{table}")

    def get_table_location(self, database, table):
        """Get table location on blobstorage."""
        return self.spark.sql(f"describe extended {database}.{table}").where("col_name='Location'").collect()[0][1]


class DeltaPipelinesInternalTableOps:
    """Delta internal operations."""

    def __init__(self, spark):
        """Initialize."""
        self.spark = spark

    @staticmethod
    def _incompatible_fields(source_schema, target_schema):
        """Return shared fields whose Delta types cannot be merged safely."""
        source_fields = {field.name: field for field in source_schema.fields}
        target_fields = {field.name: field for field in target_schema.fields}
        return [
            (
                name,
                target_fields[name].dataType.simpleString(),
                source_field.dataType.simpleString(),
            )
            for name, source_field in source_fields.items()
            if name in target_fields
            and source_field.dataType.simpleString()
            != target_fields[name].dataType.simpleString()
        ]

    def _evolve_target_schema(self, upsert_records, target_table):
        """Add source-only columns to a Delta table and return its new columns."""
        target_delta_table = DeltaTable.forName(self.spark, target_table)
        target_schema = target_delta_table.toDF().schema
        incompatible = self._incompatible_fields(
            upsert_records.schema, target_schema
        )
        if incompatible:
            details = ", ".join(
                f"{name} (target={target_type}, source={source_type})"
                for name, target_type, source_type in incompatible
            )
            raise ValueError(
                f"Cannot evolve Delta schema for {target_table}: incompatible "
                f"column types: {details}"
            )

        target_names = {field.name for field in target_schema.fields}
        new_fields = [
            field
            for field in upsert_records.schema.fields
            if field.name not in target_names
        ]
        if new_fields:
            # A zero-row append commits only the additive schema change. Delta's
            # mergeSchema validation remains the source of truth for whether
            # the new field definitions can be represented by the target.
            (
                upsert_records.limit(0)
                .write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(target_table)
            )
            target_delta_table = DeltaTable.forName(self.spark, target_table)

        return target_delta_table, target_delta_table.toDF().columns

    def merge(self, upsert_records, target_table, merge_keys, original_columns):
        """Merge builder using python semantics.

        Code added for selective updates as against update *
        not_to_update_columns has the exclusion list, hard coded for now.

        ``original_columns`` is retained for API compatibility. Merge mappings
        are built from the evolved physical table schema so append onboarding
        can persist fields added after the table was first created.
        """
        upsert_records_df = upsert_records
        target_delta_table, target_columns = self._evolve_target_schema(
            upsert_records_df, target_table
        )
        # forPath(self.spark, target_table_path)

        new_l = []
        merge_condition = None
        for key in merge_keys:
            new_l.append(f"source.{key} = target.{key}")
        if len(new_l) > 1:
            merge_condition = " AND ".join(new_l)
        else:
            merge_condition = " ".join(new_l)

        if merge_condition is None:
            raise Exception("please provide merge keys")

        not_to_update_columns = ["createDate", "createdBy"]
        update_dict = {}
        source_columns = set(upsert_records_df.columns)
        compatible_columns = [
            column for column in target_columns if column in source_columns
        ]
        for column in compatible_columns:
            if column not in not_to_update_columns:
                update_dict[column] = "source." + column
        insert_dict = {}
        for column in compatible_columns:
            insert_dict[column] = "source." + column
        target_delta_table.alias("target").merge(
            upsert_records_df.alias("source"), expr(merge_condition)
        ).whenMatchedUpdate(set=update_dict).whenNotMatchedInsert(values=insert_dict).execute()
