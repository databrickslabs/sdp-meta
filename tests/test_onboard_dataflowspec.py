"""Test OnboardDataflowSpec class."""
import copy
import json
import os
import shutil
import tempfile
import yaml
from tests.utils import SDPFrameworkTestCase
from databricks.labs.sdp_meta.onboard_dataflowspec import OnboardDataflowspec
from databricks.labs.sdp_meta.dataflow_spec import BronzeDataflowSpec, SilverDataflowSpec
from unittest.mock import MagicMock, patch
from pyspark.sql import DataFrame, Row


class OnboardDataflowspecTests(SDPFrameworkTestCase):
    """OnboardDataflowSpec Unit Test ."""

    def _make_onboarder(self):
        """Construct a minimal OnboardDataflowspec for direct-method tests.

        The convert_yml_to_json tests below need an instance because
        convert_yml_to_json delegates to ``self._load_structured_file`` (which
        uses ``self.spark`` so cloud-path inputs work via Spark IO). The
        params map is irrelevant for these tests; it only needs to satisfy
        the constructor.
        """
        params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        return OnboardDataflowspec(self.spark, params)

    @staticmethod
    def _row(values):
        row = MagicMock()
        row.asDict.return_value = values
        return row

    def test_validated_sinks_rejects_missing_required_keys(self):
        onboarder = self._make_onboarder()
        with self.assertRaisesRegex(Exception, "Missing sink details keys"):
            onboarder.get_validated_sinks_details([self._row({"name": "sink"})])

    def test_validated_sinks_rejects_unsupported_format(self):
        onboarder = self._make_onboarder()
        sink = self._row({
            "name": "sink",
            "format": "parquet",
            "options": self._row({"path": "/tmp/out"}),
        })
        with self.assertRaisesRegex(Exception, "not supported"):
            onboarder.get_validated_sinks_details([sink])

    def test_validated_delta_sink_requires_path_or_table(self):
        onboarder = self._make_onboarder()
        sink = self._row({
            "name": "sink",
            "format": "delta",
            "options": self._row({"checkpointLocation": "/tmp/checkpoint"}),
        })
        with self.assertRaisesRegex(Exception, "Missing delta sink options"):
            onboarder.get_validated_sinks_details([sink])

    def test_validated_sinks_normalizes_and_serializes(self):
        onboarder = self._make_onboarder()
        sinks = [
            self._row({
                "name": "DELTA_SINK",
                "format": "DELTA",
                "options": self._row({"path": "/tmp/out", "unused": None}),
                "select_exp": ["id"],
                "where_clause": ["id > 0"],
            }),
            self._row({
                "name": "KAFKA_SINK",
                "format": "kafka",
                "options": self._row({"topic": "events"}),
            }),
        ]

        result = json.loads(onboarder.get_validated_sinks_details(sinks))

        self.assertEqual(result[0]["name"], "delta_sink")
        self.assertEqual(result[0]["format"], "delta")
        self.assertEqual(json.loads(result[0]["options"]), {"path": "/tmp/out"})
        self.assertEqual(result[1]["format"], "kafka")

    def test_bronze_source_details_maps_metadata_and_schema(self):
        onboarder = self._make_onboarder()
        onboarder.bronze_schema_mapper = MagicMock(return_value="mapped-schema")
        source_metadata = self._row({
            "select_metadata_cols": self._row({
                "file_name": "_metadata.file_name",
                "unused": None,
            }),
            "include_autoloader_metadata_column": True,
        })
        onboarding_row = {
            "source_format": "cloudFiles",
            "bronze_reader_options": self._row({"cloudFiles.format": "json"}),
            "source_details": self._row({
                "source_path_dev": "/tmp/input",
                "source_catalog_dev": "main",
                "source_database": "raw",
                "source_table": "events",
                "source_metadata": source_metadata,
                "source_schema_path": "/tmp/schema.ddl",
            }),
        }

        details, options, schema = (
            onboarder.get_bronze_source_details_reader_options_schema(
                onboarding_row, "dev"
            )
        )

        self.assertEqual(details["path"], "/tmp/input")
        self.assertEqual(details["catalog"], "main")
        self.assertEqual(details["source_database"], "raw")
        self.assertEqual(options, {"cloudFiles.format": "json"})
        self.assertEqual(schema, "mapped-schema")
        self.assertEqual(
            json.loads(details["source_metadata"]),
            {
                "select_metadata_cols": {
                    "file_name": "_metadata.file_name"
                },
                "include_autoloader_metadata_column": True,
            },
        )
        onboarder.bronze_schema_mapper.assert_called_once_with(
            "/tmp/schema.ddl", self.spark
        )

    def test_append_flow_source_metadata_is_json_encoded(self):
        onboarder = self._make_onboarder()
        onboarding_row = {
            "bronze_append_flows": [
                Row(
                    name="events_append",
                    create_streaming_table=False,
                    source_format="cloudFiles",
                    source_details=Row(
                        source_path_dev="/tmp/append",
                        source_metadata=Row(
                            select_metadata_cols=Row(
                                file_path="_metadata.file_path"
                            )
                        ),
                    ),
                )
            ]
        }

        encoded, schemas = onboarder.get_append_flows_json(
            onboarding_row, "bronze", "dev"
        )

        append_flow = json.loads(encoded)[0]
        source_metadata = append_flow["source_details"]["source_metadata"]
        self.assertIsInstance(source_metadata, str)
        self.assertEqual(
            json.loads(source_metadata),
            {
                "select_metadata_cols": {
                    "file_path": "_metadata.file_path"
                }
            },
        )
        self.assertEqual(
            append_flow["source_details"]["path"], "/tmp/append"
        )
        self.assertEqual(schemas, {})

    def test_snapshot_source_requires_format(self):
        onboarder = self._make_onboarder()
        onboarding_row = {
            "source_format": "snapshot",
            "bronze_reader_options": {},
            "source_details": self._row({"source_path_dev": "/tmp/input"}),
        }
        with self.assertRaisesRegex(Exception, "snapshot_format is missing"):
            onboarder.get_bronze_source_details_reader_options_schema(
                onboarding_row, "dev"
            )

    def test_non_uc_snapshot_source_requires_path(self):
        onboarder = self._make_onboarder()
        onboarder.uc_enabled = False
        onboarding_row = {
            "source_format": "snapshot",
            "bronze_reader_options": {},
            "source_details": self._row({
                "snapshot_format": "delta",
                "source_database": "raw",
                "source_table": "events",
            }),
        }
        with self.assertRaisesRegex(Exception, "source_path_dev is missing"):
            onboarder.get_bronze_source_details_reader_options_schema(
                onboarding_row, "dev"
            )

    def test_onboard_yml_bronze_dataflow_spec(self):
        """Test onboarding bronze dataflow spec from YAML file."""
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        onboarding_params_map["onboarding_file_path"] = "tests/resources/onboarding.yml"
        onboard_dfs = OnboardDataflowspec(self.spark, onboarding_params_map)
        onboard_dfs.onboard_bronze_dataflow_spec()
        # Verify the onboarded data
        bronze_df = self.read_dataflowspec(
            onboarding_params_map["database"],
            onboarding_params_map["bronze_dataflowspec_table"]
        )
        # Check number of records matches YAML file
        self.assertEqual(bronze_df.count(), 3)  # Two dataflows in YAML

    def test_convert_yml_to_json_does_not_modify_source(self):
        """convert_yml_to_json must not overwrite or touch the original YAML file.

        It is, however, expected to write a sibling ``<basename>_yml_converted.json``
        next to the source on FUSE-writable filesystems — this is required for
        Spark's JSON reader to find the file on serverless / Spark Connect
        compute (where a bare ``/tmp/...`` path is auto-prefixed with
        ``dbfs:`` and fails as ``PATH_NOT_FOUND``).
        """
        tmp_dir = tempfile.mkdtemp()
        try:
            src_path = os.path.join(tmp_dir, "onboarding.yml")
            shutil.copyfile("tests/resources/onboarding.yml", src_path)
            with open(src_path, "r") as fh:
                original_contents = fh.read()
            original_mtime = os.path.getmtime(src_path)

            json_path = self._make_onboarder().convert_yml_to_json(src_path)

            self.assertTrue(os.path.exists(src_path), "source YAML file was deleted")
            with open(src_path, "r") as fh:
                self.assertEqual(fh.read(), original_contents, "source YAML file was modified")
            self.assertEqual(os.path.getmtime(src_path), original_mtime)

            self.assertTrue(json_path.endswith(".json"))
            self.assertEqual(os.path.dirname(json_path), tmp_dir,
                             "converted file must be written next to the source on "
                             "FUSE-writable paths so Spark on serverless can find it")
            self.assertEqual(os.path.basename(json_path), "onboarding_yml_converted.json",
                             "converted sibling must be a non-hidden file (no '.' or '_' "
                             "prefix) so Spark's JSON reader does not skip it")
            with open(json_path, "r") as fh:
                parsed = json.load(fh)
            self.assertIsInstance(parsed, list)
            self.assertEqual(len(parsed), 3)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_convert_yml_to_json_supports_yaml_extension(self):
        """convert_yml_to_json must handle both .yml and .yaml extensions."""
        tmp_dir = tempfile.mkdtemp()
        try:
            src_path = os.path.join(tmp_dir, "onboarding.yaml")
            shutil.copyfile("tests/resources/onboarding.yml", src_path)

            json_path = self._make_onboarder().convert_yml_to_json(src_path)

            self.assertTrue(json_path.endswith(".json"))
            self.assertTrue(os.path.exists(json_path))
            with open(json_path, "r") as fh:
                parsed = json.load(fh)
            self.assertEqual(len(parsed), 3)

            with open(src_path, "r") as fh:
                yaml.safe_load(fh)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_convert_yml_to_json_supports_uppercase_extension(self):
        """convert_yml_to_json must handle uppercase .YML extensions."""
        tmp_dir = tempfile.mkdtemp()
        try:
            src_path = os.path.join(tmp_dir, "ONBOARDING.YML")
            shutil.copyfile("tests/resources/onboarding.yml", src_path)

            json_path = self._make_onboarder().convert_yml_to_json(src_path)

            self.assertTrue(json_path.endswith(".json"))
            with open(json_path, "r") as fh:
                parsed = json.load(fh)
            self.assertEqual(len(parsed), 3)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_convert_yml_to_json_unanchored_replace_does_not_mangle_path(self):
        """A directory containing '.yml' in its name must not be mangled."""
        tmp_dir = tempfile.mkdtemp(suffix=".yml_archive")
        try:
            src_path = os.path.join(tmp_dir, "onboarding.yml")
            shutil.copyfile("tests/resources/onboarding.yml", src_path)

            json_path = self._make_onboarder().convert_yml_to_json(src_path)

            self.assertTrue(os.path.exists(json_path))
            self.assertTrue(json_path.endswith(".json"))
            self.assertEqual(json_path.count(".json"), 1,
                             "directory with '.yml' in name should not be mangled")
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_convert_yml_to_json_invalid_yaml_raises_value_error(self):
        """Malformed YAML should produce a clear ValueError."""
        tmp_dir = tempfile.mkdtemp()
        try:
            src_path = os.path.join(tmp_dir, "bad.yml")
            with open(src_path, "w") as fh:
                fh.write("foo: [unclosed\n  bar: : :\n")

            with self.assertRaises(ValueError) as ctx:
                self._make_onboarder().convert_yml_to_json(src_path)
            self.assertIn(src_path, str(ctx.exception))
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_convert_yml_to_json_missing_file_raises_value_error(self):
        """Missing source file should raise ValueError, not bare OSError."""
        with self.assertRaises(ValueError):
            self._make_onboarder().convert_yml_to_json("/nonexistent/path/to/onboarding.yml")

    def test_onboard_yaml_bronze_dataflow_spec(self):
        """End-to-end onboarding works with .yaml extension."""
        tmp_dir = tempfile.mkdtemp()
        try:
            src_path = os.path.join(tmp_dir, "onboarding.yaml")
            shutil.copyfile("tests/resources/onboarding.yml", src_path)

            onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
            onboarding_params_map["onboarding_file_path"] = src_path
            onboard_dfs = OnboardDataflowspec(self.spark, onboarding_params_map)
            onboard_dfs.onboard_bronze_dataflow_spec()
            bronze_df = self.read_dataflowspec(
                onboarding_params_map["database"],
                onboarding_params_map["bronze_dataflowspec_table"]
            )
            self.assertEqual(bronze_df.count(), 3)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def _stage_silver_yaml_onboarding(self, tmp_dir, silver_yaml_payload):
        """Stage a temp YAML onboarding file whose silver_transformation_json_dev
        points at a temp YAML silver-transformations file containing the given
        payload. Returns the path to the staged onboarding file.
        """
        silver_path = os.path.join(tmp_dir, "silver_transformations.yml")
        with open(silver_path, "w") as fh:
            fh.write(silver_yaml_payload)

        with open("tests/resources/onboarding.yml", "r") as fh:
            spec = yaml.safe_load(fh)
        for entry in spec:
            if "silver_transformation_json_dev" in entry:
                entry["silver_transformation_json_dev"] = silver_path
        onboarding_path = os.path.join(tmp_dir, "onboarding.yml")
        with open(onboarding_path, "w") as fh:
            yaml.safe_dump(spec, fh, sort_keys=False)
        return onboarding_path

    def test_silver_transformation_yaml_with_array_field_as_scalar_raises(self):
        """Regression test: createDataFrame is stricter than spark.read.json.

        Previously, silver transformations were loaded with
        ``spark.read.option('multiline','true').schema(columns).json(path)``,
        which silently coerced/dropped malformed values. The rewrite uses
        ``spark.createDataFrame(rows, schema=columns)`` which raises a clear
        TypeError if a value violates the declared schema. This test pins
        that behavior so future regressions are intentional.

        ``where_clause`` is declared as ``ArrayType(StringType())``. Supplying
        a scalar string must now raise.
        """
        bad_yaml = (
            "- target_table: customers\n"
            "  select_exp:\n"
            "    - id\n"
            "  where_clause: 'id > 0'\n"  # WRONG: must be a list
        )
        tmp_dir = tempfile.mkdtemp()
        try:
            onboarding_path = self._stage_silver_yaml_onboarding(tmp_dir, bad_yaml)
            params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
            del params["bronze_dataflowspec_table"]
            del params["bronze_dataflowspec_path"]
            params["onboarding_file_path"] = onboarding_path
            onboarder = OnboardDataflowspec(self.spark, params)
            with self.assertRaises((TypeError, ValueError, Exception)):
                onboarder.onboard_silver_dataflow_spec()
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_silver_transformation_yaml_with_well_formed_lists_succeeds(self):
        """Companion to the strict-schema regression test: well-formed YAML still loads."""
        good_yaml = (
            "- target_table: customers\n"
            "  select_exp:\n"
            "    - id\n"
            "    - email\n"
            "  where_clause:\n"
            "    - 'id IS NOT NULL'\n"
            "- target_table: transactions\n"
            "  select_exp:\n"
            "    - id\n"
            "  where_clause:\n"
            "    - 'amount IS NOT NULL'\n"
            "- target_table: iot_cdc\n"
            "  select_exp:\n"
            "    - device_id\n"
            "  where_clause:\n"
            "    - 'device_id IS NOT NULL'\n"
        )
        tmp_dir = tempfile.mkdtemp()
        try:
            onboarding_path = self._stage_silver_yaml_onboarding(tmp_dir, good_yaml)
            params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
            del params["bronze_dataflowspec_table"]
            del params["bronze_dataflowspec_path"]
            params["onboarding_file_path"] = onboarding_path
            onboarder = OnboardDataflowspec(self.spark, params)
            onboarder.onboard_silver_dataflow_spec()
            silver_df = self.read_dataflowspec(
                params["database"], params["silver_dataflowspec_table"]
            )
            self.assertEqual(silver_df.count(), 3)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_load_structured_file_json(self):
        """_load_structured_file should parse JSON files."""
        onboard_dfs = OnboardDataflowspec(
            self.spark, copy.deepcopy(self.onboarding_bronze_silver_params_map)
        )
        result = onboard_dfs._load_structured_file("tests/resources/dqe/products.json")
        self.assertIsInstance(result, dict)
        self.assertIn("expect_or_drop", result)
        self.assertEqual(result["expect_or_drop"]["valid_product_id"], "product_id IS NOT NULL")

    def test_load_structured_file_yaml(self):
        """_load_structured_file should parse YAML files identically to JSON."""
        onboard_dfs = OnboardDataflowspec(
            self.spark, copy.deepcopy(self.onboarding_bronze_silver_params_map)
        )
        json_result = onboard_dfs._load_structured_file("tests/resources/dqe/products.json")
        yaml_result = onboard_dfs._load_structured_file("tests/resources/dqe/products.yml")
        self.assertEqual(json_result, yaml_result)

    def test_load_structured_file_silver_transformations_yaml(self):
        """Silver transformations YAML should match the JSON fixture exactly."""
        onboard_dfs = OnboardDataflowspec(
            self.spark, copy.deepcopy(self.onboarding_bronze_silver_params_map)
        )
        json_result = onboard_dfs._load_structured_file("tests/resources/silver_transformations.json")
        yaml_result = onboard_dfs._load_structured_file("tests/resources/silver_transformations.yml")
        self.assertEqual(json_result, yaml_result)
        self.assertEqual(len(yaml_result), 3)

    def test_load_structured_file_unsupported_extension_raises(self):
        """Unsupported extensions must raise instead of silently returning None."""
        onboard_dfs = OnboardDataflowspec(
            self.spark, copy.deepcopy(self.onboarding_bronze_silver_params_map)
        )
        with self.assertRaises(ValueError) as ctx:
            onboard_dfs._load_structured_file("tests/resources/schema.ddl")
        self.assertIn("Unsupported file format", str(ctx.exception))

    def test_load_structured_file_none_returns_none(self):
        """A falsy path should return None (caller-side guard preserved)."""
        onboard_dfs = OnboardDataflowspec(
            self.spark, copy.deepcopy(self.onboarding_bronze_silver_params_map)
        )
        self.assertIsNone(onboard_dfs._load_structured_file(None))
        self.assertIsNone(onboard_dfs._load_structured_file(""))

    def test_get_data_quality_expectations_yaml_round_trip(self):
        """DQ expectations from a YAML file must produce the same JSON string as JSON."""
        onboard_dfs = OnboardDataflowspec(
            self.spark, copy.deepcopy(self.onboarding_bronze_silver_params_map)
        )
        json_dqe = onboard_dfs._OnboardDataflowspec__get_data_quality_expecations(
            "tests/resources/dqe/products.json"
        )
        yaml_dqe = onboard_dfs._OnboardDataflowspec__get_data_quality_expecations(
            "tests/resources/dqe/products.yml"
        )
        self.assertEqual(json.loads(json_dqe), json.loads(yaml_dqe))

    def test_get_data_quality_expectations_unsupported_no_longer_silently_drops(self):
        """The previous bug: non-.json file extensions silently returned None and dropped DQ rules."""
        onboard_dfs = OnboardDataflowspec(
            self.spark, copy.deepcopy(self.onboarding_bronze_silver_params_map)
        )
        with self.assertRaises(ValueError):
            onboard_dfs._OnboardDataflowspec__get_data_quality_expecations(
                "tests/resources/schema.ddl"
            )

    def test_validate_params_for_onboardBronzeDataflowSpec(self):
        """Test for onboardDataflowspec parameters."""
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        del onboarding_params_map["silver_dataflowspec_table"]
        del onboarding_params_map["silver_dataflowspec_path"]
        for key in onboarding_params_map:
            test_onboarding_params_map = copy.deepcopy(onboarding_params_map)
            del test_onboarding_params_map[key]
            with self.assertRaises(ValueError):
                OnboardDataflowspec(self.spark, test_onboarding_params_map).onboard_bronze_dataflow_spec()

    def test_validate_params_for_onboardSilverDataflowSpec_uc(self):
        """Test for onboardDataflowspec parameters."""
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        onboard_dfs = OnboardDataflowspec(self.spark, onboarding_params_map, uc_enabled=True)
        print(onboard_dfs.bronze_dict_obj)
        print(onboard_dfs.silver_dict_obj)
        self.assertNotIn('silver_dataflowspec_path', onboard_dfs.bronze_dict_obj)
        self.assertNotIn('bronze_dataflowspec_path', onboard_dfs.silver_dict_obj)

    def test_validate_params_for_onboardSilverDataflowSpec(self):
        """Test for onboardDataflowspec parameters."""
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        del onboarding_params_map["bronze_dataflowspec_table"]
        del onboarding_params_map["bronze_dataflowspec_path"]

        for key in onboarding_params_map:
            test_onboarding_params_map = copy.deepcopy(onboarding_params_map)
            del test_onboarding_params_map[key]
            with self.assertRaises(ValueError):
                OnboardDataflowspec(self.spark, test_onboarding_params_map).onboard_silver_dataflow_spec()

    def test_validate_params_for_onboardDataFlowSpecs(self):
        """Test for onboardDataflowspec parameters."""
        for key in self.onboarding_bronze_silver_params_map:
            test_onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
            del test_onboarding_params_map[key]
            with self.assertRaises(ValueError):
                OnboardDataflowspec(self.spark, test_onboarding_params_map).onboard_dataflow_specs()

    def test_upgrade_onboardDataFlowSpecs_positive(self):
        """Test for onboardDataflowspec."""
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        bronze_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['bronze_dataflowspec_table'])
        silver_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['silver_dataflowspec_table'])
        self.assertEqual(bronze_dataflowSpec_df.count(), 3)
        self.assertEqual(silver_dataflowSpec_df.count(), 3)

    def test_onboardDataFlowSpecs_positive(self):
        """Test for onboardDataflowspec."""
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        bronze_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['bronze_dataflowspec_table'])
        silver_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['silver_dataflowspec_table'])
        self.assertEqual(bronze_dataflowSpec_df.count(), 3)
        self.assertEqual(silver_dataflowSpec_df.count(), 3)

    def read_dataflowspec(self, database, table):
        return self.spark.read.table(f"{database}.{table}")

    def test_onboardDataFlowSpecs_with_uc_enabled(self):
        """Test for onboardDataflowspec."""
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark,
                                                   self.onboarding_bronze_silver_params_uc_map,
                                                   uc_enabled=True)
        self.assertNotIn('bronze_dataflowspec_path', onboardDataFlowSpecs.bronze_dict_obj)
        self.assertNotIn('silver_dataflowspec_path', onboardDataFlowSpecs.silver_dict_obj)

    @patch.object(OnboardDataflowspec, 'onboard_bronze_dataflow_spec', new_callable=MagicMock())
    @patch.object(OnboardDataflowspec, 'onboard_silver_dataflow_spec', new_callable=MagicMock())
    def test_onboardDataFlowSpecs_validate_with_uc_enabled(self, mock_bronze, mock_silver):
        """Test for onboardDataflowspec."""
        mock_bronze.return_value = None
        mock_silver.return_value = None
        bronze_silver_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_uc_map)
        del bronze_silver_params_map["uc_enabled"]
        OnboardDataflowspec(self.spark,
                            bronze_silver_params_map,
                            uc_enabled=True).onboard_dataflow_specs()
        assert mock_bronze.called
        assert mock_silver.called

    def test_onboardDataFlowSpecs_pre_validates_uc_names_and_aggregates_errors(self):
        """Pre-flight UC validation must surface every bad row at once.

        Issue #261: a hyphenated catalog or schema in the onboarding file
        only blew up mid-onboarding with a confusing Spark error, leaving
        the bronze dataflowspec table half-written. The pre-flight
        validator added to ``onboard_dataflow_specs`` walks the file
        once before any Spark side-effects and aggregates *all* identifier
        errors into a single ``ValueError`` so the user can fix everything
        in one pass.

        This test exercises every category the validator covers:
          - main bronze/silver UC name (``database`` / ``table``)
          - quarantine UC name (database / table / catalog)
          - partition / cluster column lists (single string + comma + list
            literal forms — every shape the parser accepts)

        so a regression in any one of them breaks this test.
        """
        # Every row uses a valid ``source_format`` so this test stays
        # focused on UC-identifier violations. ``source_format`` /
        # ``scd_type`` enum coverage lives in the dedicated enum test
        # below.
        bad_rows = [
            {
                # Main UC name violations.
                "data_flow_id": "100",
                "data_flow_group": "A1",
                "source_format": "cloudFiles",
                "source_details": {"source_path_dev": "/tmp/x"},
                "bronze_database_dev": "bad-db",
                "bronze_table": "ok_table",
                "bronze_reader_options": {},
                "bronze_table_path_dev": "/tmp/bronze/x",
            },
            {
                # Bronze table + silver table violations.
                "data_flow_id": "101",
                "data_flow_group": "A1",
                "source_format": "cloudFiles",
                "source_details": {"source_path_dev": "/tmp/y"},
                "bronze_database_dev": "ok_db",
                "bronze_table": "1bad_table",
                "bronze_reader_options": {},
                "bronze_table_path_dev": "/tmp/bronze/y",
                "silver_database_dev": "ok_db",
                "silver_table": "ok.dotted",
                "silver_table_path_dev": "/tmp/silver/y",
            },
            {
                # Quarantine UC name violations + a bad partition column
                # in comma-separated form. These are the new cases Phase 1
                # added; main UC fields are deliberately valid here so we
                # know the new checks are what surfaced these errors.
                "data_flow_id": "102",
                "data_flow_group": "A1",
                "source_format": "cloudFiles",
                "source_details": {"source_path_dev": "/tmp/z"},
                "bronze_database_dev": "ok_db",
                "bronze_table": "ok_table",
                "bronze_reader_options": {},
                "bronze_table_path_dev": "/tmp/bronze/z",
                "bronze_database_quarantine_dev": "bad-q-db",
                "bronze_quarantine_table": "1bad_q_table",
                "bronze_partition_columns": "ok_col,bad-col",
            },
            {
                # cluster_by violations — list literal (silver) and python
                # list (bronze) — covers the two cluster_by shapes the
                # parser accepts.
                "data_flow_id": "103",
                "data_flow_group": "A1",
                "source_format": "cloudFiles",
                "source_details": {"source_path_dev": "/tmp/w"},
                "bronze_database_dev": "ok_db",
                "bronze_table": "ok_table_b",
                "bronze_reader_options": {},
                "bronze_table_path_dev": "/tmp/bronze/w",
                "bronze_cluster_by": "['ok_col', '1bad_col']",
                "silver_database_dev": "ok_db",
                "silver_table": "ok_table_s",
                "silver_table_path_dev": "/tmp/silver/w",
                "silver_cluster_by": ["ok_col", "bad-col"],
            },
        ]
        tmp_dir = tempfile.mkdtemp(prefix="sdp_meta_prevalidate_")
        try:
            bad_file = os.path.join(tmp_dir, "onboarding_bad.json")
            with open(bad_file, "w") as f:
                json.dump(bad_rows, f)

            params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
            params["onboarding_file_path"] = bad_file

            with self.assertRaises(ValueError) as ctx:
                OnboardDataflowspec(self.spark, params).onboard_dataflow_specs()

            msg = str(ctx.exception)
            # Every distinct violation should be reported in a single error.
            # Pin the field names so a regression in any one validator path
            # (main / quarantine / partition / cluster) is immediately obvious.
            for needle in (
                "flow 100 bronze_database_dev",
                "flow 101 bronze_table",
                "flow 101 silver_table",
                "flow 102 bronze_database_quarantine_dev",
                "flow 102 bronze_quarantine_table",
                "flow 102 bronze_partition_columns",
                "flow 103 bronze_cluster_by",
                "flow 103 silver_cluster_by",
            ):
                self.assertIn(needle, msg)
            self.assertIn("validation error(s)", msg)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_onboardDataFlowSpecs_pre_validates_source_format_and_scd_type(self):
        """Pre-flight enum validation must surface bad source_format / scd_type.

        A typo'd ``source_format`` like ``"cloud_files"`` falls through
        every ``elif source_format == ...`` branch in
        ``DataflowPipeline``, leaving the bronze read with no input. A
        bad ``scd_type`` reaches DLT's apply_changes and fails opaquely.
        Pre-flight catches both at onboarding with the allowed values
        inlined in the error.

        Case variants like ``"cloudfiles"`` are NOT errors: v0.0.10
        onboarding accepted them via ``.lower()``, so pre-flight accepts
        and canonicalizes them (issue #370 class of upgrade break).

        Includes a valid row to confirm the validator only flags the
        bogus values.
        """
        rows = [
            {
                # Bogus top-level source_format + bogus CDC scd_type, both
                # for bronze. UC names valid so they don't add noise.
                "data_flow_id": "200",
                "data_flow_group": "A1",
                "source_format": "cloud_files",  # real typo, not a case variant
                "source_details": {"source_path_dev": "/tmp/a"},
                "bronze_database_dev": "ok_db",
                "bronze_table": "ok_table",
                "bronze_reader_options": {},
                "bronze_table_path_dev": "/tmp/bronze/a",
                "bronze_cdc_apply_changes": {
                    "keys": ["id"],
                    "sequence_by": "ts",
                    "scd_type": "3",  # not "1" or "2"
                },
            },
            {
                # Bogus apply_changes_from_snapshot.scd_type on silver +
                # bogus source_format on a silver append flow. Tests both
                # the ``apply_changes_from_snapshot`` branch and the
                # per-append-flow source_format branch.
                "data_flow_id": "201",
                "data_flow_group": "A1",
                "source_format": "snapshot",
                "source_details": {"source_path_dev": "/tmp/b"},
                "bronze_database_dev": "ok_db",
                "bronze_table": "ok_table_b",
                "bronze_reader_options": {},
                "bronze_table_path_dev": "/tmp/bronze/b",
                "silver_database_dev": "ok_db",
                "silver_table": "ok_table_s",
                "silver_table_path_dev": "/tmp/silver/b",
                "silver_apply_changes_from_snapshot": {
                    "keys": ["id"],
                    "scd_type": "scd_2",  # bad
                },
                "silver_append_flows": [
                    {
                        "name": "af1",
                        "source_format": "csv",  # not in supported set
                        "create_streaming_table": False,
                        "source_details": {"source_path_dev": "/tmp/af"},
                    },
                ],
            },
            {
                # Fully valid row — must NOT contribute to the error list.
                "data_flow_id": "202",
                "data_flow_group": "A1",
                "source_format": "delta",
                "source_details": {
                    "source_database_dev": "ok_src_db",
                    "source_table": "ok_src_tbl",
                },
                "bronze_database_dev": "ok_db",
                "bronze_table": "ok_table_v",
                "bronze_reader_options": {},
                "bronze_table_path_dev": "/tmp/bronze/v",
            },
        ]
        tmp_dir = tempfile.mkdtemp(prefix="sdp_meta_prevalidate_enum_")
        try:
            f_path = os.path.join(tmp_dir, "onboarding_enum_bad.json")
            with open(f_path, "w") as fh:
                json.dump(rows, fh)

            params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
            params["onboarding_file_path"] = f_path

            with self.assertRaises(ValueError) as ctx:
                OnboardDataflowspec(self.spark, params).onboard_dataflow_specs()

            msg = str(ctx.exception)
            for needle in (
                "flow 200 source_format",
                "flow 200 bronze_cdc_apply_changes.scd_type",
                "flow 201 silver_apply_changes_from_snapshot.scd_type",
                "flow 201 silver_append_flows[0].source_format",
            ):
                self.assertIn(needle, msg)
            # Valid row 202 must not appear anywhere in the error list.
            self.assertNotIn("flow 202", msg)
            # Allowed-values list should be inlined so the user can fix
            # without going to docs.
            self.assertIn("cloudFiles", msg)  # in source_format error
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_onboardDataFlowSpecs_pre_validates_cdc_column_names(self):
        """Pre-flight CDC column-name validation must catch bad column refs.

        Column-name fields inside ``*_cdc_apply_changes`` /
        ``*_apply_changes_from_snapshot`` (``keys``, ``sequence_by``,
        ``column_list``, ``track_history_column_list``, etc.) drive
        DLT's apply_changes column projection. A hyphenated or
        leading-digit entry there fails at DLT runtime; pre-flight
        catches it here.

        Critically: ``apply_as_deletes`` and ``apply_as_truncates`` are
        SQL expressions (``expr(...)``), not column names, and must NOT
        be validated as identifiers. This test pins that contract by
        feeding a SQL expression in those fields and asserting they
        don't trip the validator.
        """
        rows = [
            {
                # Bronze cdc_apply_changes with bad ``keys`` (Python list
                # form) and bad ``sequence_by`` (comma-separated string
                # form). All other UC fields valid so the only errors
                # we should see come from the column-list validator.
                "data_flow_id": "300",
                "data_flow_group": "A1",
                "source_format": "cloudFiles",
                "source_details": {"source_path_dev": "/tmp/p"},
                "bronze_database_dev": "ok_db",
                "bronze_table": "ok_table",
                "bronze_reader_options": {},
                "bronze_table_path_dev": "/tmp/bronze/p",
                "bronze_cdc_apply_changes": {
                    "keys": ["id", "bad-col"],
                    "sequence_by": "ts,1bad",
                    "scd_type": "1",
                    # SQL expressions — must NOT be identifier-validated.
                    "apply_as_deletes": "operation = 'DELETE'",
                    "apply_as_truncates": "operation = 'TRUNCATE'",
                    "where": "id IS NOT NULL",
                },
            },
            {
                # Silver apply_changes_from_snapshot with bad
                # ``track_history_column_list``. Use stringified-list
                # shape to exercise that branch of validate_uc_column_list.
                "data_flow_id": "301",
                "data_flow_group": "A1",
                "source_format": "snapshot",
                "source_details": {"source_path_dev": "/tmp/q"},
                "bronze_database_dev": "ok_db",
                "bronze_table": "ok_table_b",
                "bronze_reader_options": {},
                "bronze_table_path_dev": "/tmp/bronze/q",
                "silver_database_dev": "ok_db",
                "silver_table": "ok_table_s",
                "silver_table_path_dev": "/tmp/silver/q",
                "silver_apply_changes_from_snapshot": {
                    "keys": ["id"],
                    "scd_type": "2",
                    "track_history_column_list": ["ok_col", "bad.col"],
                },
            },
            {
                # Fully valid CDC block — must NOT contribute errors.
                # Includes apply_as_deletes / apply_as_truncates with
                # SQL expressions to confirm they aren't identifier-
                # validated even when other column-name fields exist.
                "data_flow_id": "302",
                "data_flow_group": "A1",
                "source_format": "cloudFiles",
                "source_details": {"source_path_dev": "/tmp/r"},
                "bronze_database_dev": "ok_db",
                "bronze_table": "ok_table_v",
                "bronze_reader_options": {},
                "bronze_table_path_dev": "/tmp/bronze/r",
                "bronze_cdc_apply_changes": {
                    "keys": ["id", "tenant_id"],
                    "sequence_by": "ts",
                    "scd_type": "2",
                    "column_list": ["col_a", "col_b"],
                    "track_history_except_column_list": ["audit_ts"],
                    "apply_as_deletes": "op = 'D'",
                },
            },
        ]
        tmp_dir = tempfile.mkdtemp(prefix="sdp_meta_prevalidate_cdc_")
        try:
            f_path = os.path.join(tmp_dir, "onboarding_cdc_bad.json")
            with open(f_path, "w") as fh:
                json.dump(rows, fh)

            params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
            params["onboarding_file_path"] = f_path

            with self.assertRaises(ValueError) as ctx:
                OnboardDataflowspec(self.spark, params).onboard_dataflow_specs()

            msg = str(ctx.exception)
            for needle in (
                "flow 300 bronze_cdc_apply_changes.keys",
                "flow 300 bronze_cdc_apply_changes.sequence_by",
                "flow 301 silver_apply_changes_from_snapshot.track_history_column_list",
            ):
                self.assertIn(needle, msg)
            # Pin the apply_as_deletes / apply_as_truncates / where
            # contract: SQL expressions don't get identifier-validated,
            # so the validator must never emit an error pointed at those
            # fields, even though they contain characters (``=``, ``'``,
            # spaces) that would fail the regular-identifier check.
            # Match on the qualified path (``<block>.<field>``) so we
            # don't false-positive on the words appearing elsewhere in
            # the message (e.g. "where" inside "fix these and rerun").
            self.assertNotIn("bronze_cdc_apply_changes.apply_as_deletes", msg)
            self.assertNotIn("bronze_cdc_apply_changes.apply_as_truncates", msg)
            self.assertNotIn("bronze_cdc_apply_changes.where", msg)
            # Valid row 302 must not appear at all.
            self.assertNotIn("flow 302", msg)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_onboardDataFlowSpecs_pre_validation_reports_every_category_in_one_error(self):
        """End-to-end: one onboarding file, one violation per category.

        The pre-flight contract is "tell the user everything that's
        wrong in a single shot so they can fix the file once". This
        test pins that contract by seeding a single onboarding row
        that intentionally violates one rule from each validation
        category we've added (Phases 1-3):

          - UC catalog/schema/table identifier (with hyphen)
          - Quarantine UC name (with leading digit)
          - Partition-column list (with hyphen)
          - Cluster-by list (with period)
          - Top-level ``source_format`` enum (typo)
          - CDC ``scd_type`` enum (unsupported value; note int 1/2 are
            VALID — v0.0.10 compat, issue #370 — so we use 3 here)
          - CDC column-name field (``keys`` with hyphen)
          - Append-flow ``source_format`` enum (unsupported value)

        We assert that ALL of them surface in the single aggregated
        ``ValueError``. If a future refactor short-circuits on the
        first error, this test fails loudly.
        """
        bad_row = {
            "data_flow_id": "999",
            "data_flow_group": "A1",
            # Top-level source_format typo.
            "source_format": "cloudFile",
            "source_details": {"source_path_dev": "/tmp/p"},
            # Bronze: bad catalog (hyphen) + bad table (period).
            "bronze_catalog_dev": "bad-catalog",
            "bronze_database_dev": "ok_db",
            "bronze_table": "ok.table",
            "bronze_reader_options": {},
            "bronze_table_path_dev": "/tmp/bronze/p",
            # Quarantine name with leading digit.
            "bronze_quarantine_table": "1bad_quarantine",
            # Partition columns + clusterBy with bad entries.
            "bronze_partition_columns": "ok_col,bad-col",
            "bronze_cluster_by": ["ok_col", "bad.col"],
            "bronze_cdc_apply_changes": {
                # ``scd_type`` 3 is outside the supported {1, 2} set.
                # (Int 1/2 would be ACCEPTED and coerced to "1"/"2" for
                # v0.0.10 backward compat — issue #370.)
                # ``keys`` has a hyphen — column-list error.
                "scd_type": 3,
                "keys": ["id", "bad-key"],
                "sequence_by": "ts",
            },
            "bronze_append_flows": [
                {
                    "name": "af1",
                    # Unsupported append-flow source_format.
                    "source_format": "parquet",
                    "source_details": {"source_path_dev": "/tmp/af"},
                }
            ],
        }
        tmp_dir = tempfile.mkdtemp(prefix="sdp_meta_prevalidate_e2e_")
        try:
            f_path = os.path.join(tmp_dir, "onboarding_e2e_bad.json")
            with open(f_path, "w") as fh:
                json.dump([bad_row], fh)

            params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
            params["onboarding_file_path"] = f_path
            # Bronze-only run keeps the test tightly scoped: every
            # error surfaced must come from this one row's bronze
            # fields, so a missing error means the validator is
            # short-circuiting (the bug we're guarding against).
            params["bronze_dataflowspec_table"] = "bronze_dataflowspec_table"
            params["silver_dataflowspec_table"] = "silver_dataflowspec_table"

            with self.assertRaises(ValueError) as ctx:
                OnboardDataflowspec(self.spark, params).onboard_dataflow_specs()

            msg = str(ctx.exception)
            expected_needles = [
                # Identifier categories (Phase 1).
                "flow 999 bronze_catalog_dev",
                "flow 999 bronze_table",
                "flow 999 bronze_quarantine_table",
                "flow 999 bronze_partition_columns",
                "flow 999 bronze_cluster_by",
                # Enum categories (Phase 2).
                "flow 999 source_format",
                "flow 999 bronze_cdc_apply_changes.scd_type",
                # CDC column-list (Phase 3).
                "flow 999 bronze_cdc_apply_changes.keys",
                # Append-flow source_format (Phase 2).
                "flow 999 bronze_append_flows[0].source_format",
            ]
            missing = [n for n in expected_needles if n not in msg]
            self.assertFalse(
                missing,
                f"pre-flight short-circuited; missing categories in error message: "
                f"{missing}\nfull message:\n{msg}",
            )
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    # ── Issue #343: single-layer entrypoints must also pre-flight ────
    # https://github.com/databrickslabs/sdp-meta/issues/343
    #
    # Finding #1: --onboard_layer bronze / silver routes to
    # onboard_bronze_dataflow_spec() / onboard_silver_dataflow_spec()
    # respectively, and NEITHER used to call
    # __pre_validate_onboarding_uc_names(). Column-list, cluster_by,
    # partition_columns, cdc column names, scd_type, and append_flows
    # source_format checks were silently skipped for the majority of
    # CLI usage.
    #
    # Finding #2: the pre-flight walker gated every check with
    # ``if row_dict.get(field):`` which is falsy for "". A required
    # field like ``bronze_database_dev: ""`` used to sail through
    # pre-flight and only surface as a confusing runtime failure.
    #
    # The four tests below pin the fixes.

    def test_issue343_bronze_only_entrypoint_runs_preflight(self):
        """``onboard_bronze_dataflow_spec`` must run pre-flight itself.

        Reproduces the exact --onboard_layer bronze code path from
        ``__main__.py:58-59``: a caller (SDP-META onboarding job in
        production, or a notebook user) reaches the single-layer
        method WITHOUT going through ``onboard_dataflow_specs``. The
        pre-flight walker must fire from inside the single-layer
        method so bad cluster_by / partition / cdc column names get
        surfaced before any Spark side-effect.
        """
        bad_rows = [
            {
                "data_flow_id": "343B1",
                "data_flow_group": "A1",
                "source_format": "cloudFiles",
                "source_details": {"source_path_dev": "/tmp/x"},
                "bronze_database_dev": "ok_db",
                "bronze_table": "ok_table",
                "bronze_reader_options": {},
                "bronze_table_path_dev": "/tmp/bronze/x",
                # Bad column-list \u2014 must be caught by the pre-flight
                # that the single-layer entrypoint now invokes.
                "bronze_cluster_by": ["ok_col", "bad-col"],
                "bronze_partition_columns": "1bad_col",
                "bronze_cdc_apply_changes": {
                    "keys": ["id"],
                    "sequence_by": "ts",
                    "scd_type": "3",  # invalid enum
                },
            },
        ]
        tmp_dir = tempfile.mkdtemp(prefix="sdp_meta_i343_bronze_")
        try:
            bad_file = os.path.join(tmp_dir, "onboarding_bad.json")
            with open(bad_file, "w") as f:
                json.dump(bad_rows, f)

            params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
            params["onboarding_file_path"] = bad_file

            with self.assertRaises(ValueError) as ctx:
                OnboardDataflowspec(
                    self.spark, params
                ).onboard_bronze_dataflow_spec()

            msg = str(ctx.exception)
            for needle in (
                "flow 343B1 bronze_cluster_by",
                "flow 343B1 bronze_partition_columns",
                "flow 343B1 bronze_cdc_apply_changes.scd_type",
            ):
                self.assertIn(needle, msg)
            # Silver-layer categories must NOT appear \u2014 the caller
            # only asked for bronze onboarding, and the row is
            # deliberately bronze-only. This pins the ``layers=`` scope
            # so a future refactor can't silently widen the walk.
            self.assertNotIn("silver_", msg)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_row_filter_injection_rejected_by_preflight(self):
        """Row-filter values must be denylist-validated at onboarding.

        Code-review finding #3: ``*_row_filter`` /
        ``*_quarantine_row_filter`` (issue #303/#306) are read verbatim
        and later spliced into ``dp.table(row_filter=...)`` /
        ``create_streaming_table(row_filter=...)`` DDL that the
        pipelines API cannot parameterise. Pre-flight now runs them
        through ``validate_sql_where_clause`` so a statement-separator /
        comment / DDL-keyword payload is caught before any Spark
        side-effect.
        """
        bad_rows = [
            {
                "data_flow_id": "RF1",
                "data_flow_group": "A1",
                "source_format": "cloudFiles",
                "source_details": {"source_path_dev": "/tmp/rf1"},
                "bronze_database_dev": "ok_db",
                "bronze_table": "ok_table",
                "bronze_reader_options": {},
                "bronze_table_path_dev": "/tmp/bronze/rf1",
                # Injection payload: statement separator + DDL keyword.
                "bronze_row_filter": (
                    "ROW FILTER main.b.fn ON (r); DROP TABLE secrets"
                ),
                # Independent quarantine filter with a comment-escape.
                "bronze_quarantine_row_filter": (
                    "ROW FILTER main.b.qfn ON (r) -- bypass"
                ),
            },
        ]
        tmp_dir = tempfile.mkdtemp(prefix="sdp_meta_rowfilter_")
        try:
            bad_file = os.path.join(tmp_dir, "onboarding_bad.json")
            with open(bad_file, "w") as f:
                json.dump(bad_rows, f)

            params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
            params["onboarding_file_path"] = bad_file

            with self.assertRaises(ValueError) as ctx:
                OnboardDataflowspec(
                    self.spark, params
                ).onboard_bronze_dataflow_spec()

            msg = str(ctx.exception)
            self.assertIn("flow RF1 bronze_row_filter", msg)
            self.assertIn("flow RF1 bronze_quarantine_row_filter", msg)
            # The specific offending tokens are surfaced to the user.
            self.assertIn("';'", msg)
            self.assertIn("'--'", msg)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_legitimate_row_filter_passes_preflight(self):
        """A well-formed ``ROW FILTER ... ON (...)`` clause must NOT be
        rejected by the new pre-flight denylist \u2014 guards against the
        validator being too aggressive and breaking issue #303/#306
        row-level-security specs.
        """
        good_rows = [
            {
                "data_flow_id": "RF2",
                "data_flow_group": "A1",
                "source_format": "cloudFiles",
                "source_details": {"source_path_dev": "/tmp/rf2"},
                "bronze_database_dev": "ok_db",
                "bronze_table": "ok_table",
                "bronze_reader_options": {},
                "bronze_table_path_dev": "/tmp/bronze/rf2",
                "bronze_row_filter": (
                    "ROW FILTER main.bronze.region_filter ON (region)"
                ),
                "bronze_quarantine_row_filter": (
                    "ROW FILTER main.bronze.quarantine_region_filter "
                    "ON (region)"
                ),
            },
        ]
        tmp_dir = tempfile.mkdtemp(prefix="sdp_meta_rowfilter_ok_")
        try:
            good_file = os.path.join(tmp_dir, "onboarding_ok.json")
            with open(good_file, "w") as f:
                json.dump(good_rows, f)

            params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
            params["onboarding_file_path"] = good_file

            onboard = OnboardDataflowspec(self.spark, params)
            # Should not raise from the pre-flight walker. We call the
            # private walker directly so the test doesn't depend on the
            # full Spark onboarding side-effects.
            onboard._OnboardDataflowspec__pre_validate_onboarding_uc_names(
                layers=("bronze",)
            )
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_issue343_silver_only_entrypoint_runs_preflight(self):
        """``onboard_silver_dataflow_spec`` must run pre-flight itself.

        Mirror of the bronze test above for the --onboard_layer silver
        route. Confirms the pre-flight walker now covers this
        entrypoint too, and that bronze-side checks are NOT applied
        to a silver-only row (no false-positives from the wider walk).
        """
        bad_rows = [
            {
                "data_flow_id": "343S1",
                "data_flow_group": "A1",
                # Present so the row is a legitimate silver participant.
                "silver_database_dev": "ok_db",
                "silver_table": "ok_silver_table",
                "silver_table_path_dev": "/tmp/silver/x",
                "silver_transformation_json_dev": "/tmp/xform.json",
                # Bad silver cluster_by \u2014 must be caught by the
                # pre-flight that the silver entrypoint now invokes.
                "silver_cluster_by": ["ok_col", "1bad_col"],
                "silver_apply_changes_from_snapshot": {
                    "keys": ["id"],
                    "scd_type": "scd_1",  # invalid enum
                },
            },
        ]
        tmp_dir = tempfile.mkdtemp(prefix="sdp_meta_i343_silver_")
        try:
            bad_file = os.path.join(tmp_dir, "onboarding_bad.json")
            with open(bad_file, "w") as f:
                json.dump(bad_rows, f)

            params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
            params["onboarding_file_path"] = bad_file

            with self.assertRaises(ValueError) as ctx:
                OnboardDataflowspec(
                    self.spark, params
                ).onboard_silver_dataflow_spec()

            msg = str(ctx.exception)
            for needle in (
                "flow 343S1 silver_cluster_by",
                "flow 343S1 silver_apply_changes_from_snapshot.scd_type",
            ):
                self.assertIn(needle, msg)
            # Bronze-side categories must NOT appear on a silver-only
            # row when the caller only asked for silver onboarding.
            self.assertNotIn("bronze_cluster_by", msg)
            self.assertNotIn("bronze_partition_columns", msg)

        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_combined_preflight_does_not_treat_silver_source_as_bronze_row(self):
        """A silver-only source row remains valid when onboarding both layers."""
        rows = [{
            "data_flow_id": "446S1",
            "data_flow_group": "silver_group",
            "source_format": "delta",
            "source_details": {
                "source_database": "raw",
                "source_table": "orders",
            },
            "silver_database_dev": "main.silver",
            "silver_table": "orders",
            "silver_transformation_json_dev": "/tmp/xform.json",
        }]
        tmp_dir = tempfile.mkdtemp(prefix="sdp_meta_i446_silver_")
        try:
            onboarding_file = os.path.join(tmp_dir, "onboarding.json")
            with open(onboarding_file, "w") as f:
                json.dump(rows, f)
            params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
            params["onboarding_file_path"] = onboarding_file
            onboarder = OnboardDataflowspec(self.spark, params)

            onboarder._OnboardDataflowspec__pre_validate_onboarding_uc_names(
                layers=("bronze", "silver")
            )
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_issue343_empty_required_field_rejected(self):
        """Empty ``<layer>_database_<env>`` / ``<layer>_table`` \u2192 400.

        Finding #2: the pre-flight used to gate every check with
        ``if row_dict.get(field):`` which is falsy for ``""``. A row
        with ``bronze_database_dev: ""`` (empty string) used to pass
        pre-flight silently and blow up mid-onboarding with a
        confusing Spark error, leaving the dataflowspec table
        half-written.

        The two layer-anchor fields ``<layer>_database_<env>`` and
        ``<layer>_table`` are now enforced as required *when the row
        participates in that layer*. Optional fields (quarantine
        tables, cluster_by, partition_columns) keep the previous
        skip-if-falsy semantics.
        """
        bad_rows = [
            {
                # Bronze participant (has source_details) with an EMPTY
                # required bronze_database_dev. Must be rejected.
                "data_flow_id": "343E1",
                "data_flow_group": "A1",
                "source_format": "cloudFiles",
                "source_details": {"source_path_dev": "/tmp/e1"},
                "bronze_database_dev": "",  # empty string!
                "bronze_table": "ok_table",
                "bronze_reader_options": {},
                "bronze_table_path_dev": "/tmp/bronze/e1",
            },
            {
                # Silver participant with an empty silver_table.
                "data_flow_id": "343E2",
                "data_flow_group": "A1",
                "silver_database_dev": "ok_db",
                "silver_table": "",  # empty string!
                "silver_table_path_dev": "/tmp/silver/e2",
                "silver_transformation_json_dev": "/tmp/xform.json",
            },
        ]
        tmp_dir = tempfile.mkdtemp(prefix="sdp_meta_i343_empty_")
        try:
            bad_file = os.path.join(tmp_dir, "onboarding_bad.json")
            with open(bad_file, "w") as f:
                json.dump(bad_rows, f)

            params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
            params["onboarding_file_path"] = bad_file

            with self.assertRaises(ValueError) as ctx:
                OnboardDataflowspec(
                    self.spark, params
                ).onboard_dataflow_specs()

            msg = str(ctx.exception)
            # The verbose required-field message names the flow, the
            # field, and the empty-vs-absent state, and includes a
            # fix-example so users don't have to guess what the field
            # is for. Assert on the anchor prefix + the actionable
            # substrings independently so the test doesn't over-fit
            # to the exact wording.
            self.assertIn("flow 343E1 bronze_database_dev", msg)
            self.assertIn("required key is present but empty", msg)
            self.assertIn("bronze_database_dev: main.my_bronze_schema", msg)
            self.assertIn("flow 343E2 silver_table", msg)
            self.assertIn("required key is present but empty", msg)
            self.assertIn("silver_table: my_silver_table", msg)
            # And the summary header should surface both affected
            # flows so a scanner can jump straight to the offenders.
            self.assertIn("343E1", msg)
            self.assertIn("343E2", msg)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_issue343_absent_required_field_reports_missing_not_empty(self):
        """Distinguish absent key from present-but-empty in the report.

        The two states have different fixes: absent means the user has
        to *add* the key, present-but-empty means they have to *fill
        in* a value. Verbose reporting has to name which state so the
        user doesn't grep the onboarding file for a key that isn't
        there.

        Row A: participates in bronze (source_details present) but
        never mentions bronze_table \u2014 the key is absent. Expect
        "missing from this onboarding row".

        Row B: mentions bronze_database_dev with value "" \u2014 present
        but empty. Expect "present but empty (value='')".
        """
        bad_rows = [
            {
                "data_flow_id": "343A1",
                "data_flow_group": "A1",
                "source_format": "cloudFiles",
                "source_details": {"source_path_dev": "/tmp/a1"},
                "bronze_database_dev": "ok_db",
                # bronze_table intentionally OMITTED here \u2014 absent, not empty.
                "bronze_reader_options": {},
                "bronze_table_path_dev": "/tmp/bronze/a1",
            },
            {
                "data_flow_id": "343A2",
                "data_flow_group": "A1",
                "source_format": "cloudFiles",
                "source_details": {"source_path_dev": "/tmp/a2"},
                "bronze_database_dev": "",  # present but empty
                "bronze_table": "ok_table",
                "bronze_reader_options": {},
                "bronze_table_path_dev": "/tmp/bronze/a2",
            },
        ]
        tmp_dir = tempfile.mkdtemp(prefix="sdp_meta_i343_absent_")
        try:
            bad_file = os.path.join(tmp_dir, "onboarding_bad.json")
            with open(bad_file, "w") as f:
                json.dump(bad_rows, f)

            params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
            params["onboarding_file_path"] = bad_file

            with self.assertRaises(ValueError) as ctx:
                OnboardDataflowspec(
                    self.spark, params
                ).onboard_bronze_dataflow_spec()

            msg = str(ctx.exception)
            # Row A: absent bronze_table \u2014 phrase names "missing".
            self.assertIn("flow 343A1 bronze_table", msg)
            self.assertIn("required key is missing", msg)
            # Row B: present-but-empty bronze_database_dev \u2014 names "empty" + value.
            self.assertIn("flow 343A2 bronze_database_dev", msg)
            self.assertIn("required key is present but empty", msg)
            self.assertIn("value=''", msg)
            # And the fix-example is inlined for the user to copy.
            self.assertIn("bronze_table: my_bronze_table", msg)
            self.assertIn("bronze_database_dev: main.my_bronze_schema", msg)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_issue343_error_report_groups_by_flow_id(self):
        """The aggregation should sort errors by flow_id, not by
        discovery order, so all violations for one row read together.

        Rows are interleaved (100, 200, 100, 200) intentionally \u2014
        without sorting, the bullets would follow that order. With
        sorting, all "flow 100" bullets appear before any "flow 200"
        bullet. This matches how a human fixes a spec file: row by
        row, not violation by violation.
        """
        bad_rows = [
            {
                "data_flow_id": "GRP100",
                "data_flow_group": "G",
                "source_format": "cloudFiles",
                "source_details": {"source_path_dev": "/tmp/g100"},
                "bronze_database_dev": "bad-db",  # hyphen \u2014 identifier error
                "bronze_table": "ok_t",
                "bronze_reader_options": {},
                "bronze_table_path_dev": "/tmp/bronze/g100",
            },
            {
                "data_flow_id": "GRP200",
                "data_flow_group": "G",
                "source_format": "cloudFiles",
                "source_details": {"source_path_dev": "/tmp/g200"},
                "bronze_database_dev": "ok_db",
                "bronze_table": "1bad_t",  # leading digit
                "bronze_reader_options": {},
                "bronze_table_path_dev": "/tmp/bronze/g200",
                "bronze_partition_columns": "bad-col",  # hyphen
            },
        ]
        tmp_dir = tempfile.mkdtemp(prefix="sdp_meta_i343_group_")
        try:
            bad_file = os.path.join(tmp_dir, "onboarding_bad.json")
            with open(bad_file, "w") as f:
                json.dump(bad_rows, f)

            params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
            params["onboarding_file_path"] = bad_file

            with self.assertRaises(ValueError) as ctx:
                OnboardDataflowspec(
                    self.spark, params
                ).onboard_bronze_dataflow_spec()

            msg = str(ctx.exception)
            grp100_pos = msg.find("flow GRP100")
            grp200_pos = msg.find("flow GRP200")
            self.assertGreater(grp100_pos, -1, msg)
            self.assertGreater(grp200_pos, -1, msg)
            self.assertLess(
                grp100_pos,
                grp200_pos,
                msg=(
                    "Errors should be grouped by flow_id \u2014 GRP100 "
                    "bullets should all precede GRP200 bullets so the "
                    "report is scannable row-by-row.\n" + msg
                ),
            )
            # Header lists the affected flows explicitly (issue #343 verbose ask).
            self.assertIn("GRP100", msg)
            self.assertIn("GRP200", msg)
            self.assertIn("pre-flight validation error", msg)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_issue343_bronze_silver_wrapper_still_aggregates_once(self):
        """``onboard_dataflow_specs`` must remain a single aggregation.

        With the single-layer methods now also calling pre-validate on
        their own layer, we need to make sure the wrapper's up-front
        ``__pre_validate_onboarding_uc_names(layers=("bronze","silver"))``
        + the two subsequent single-layer calls don't produce three
        pre-flight runs (and thus three separate error reports) for
        the same file. The idempotency guard collapses them into one.

        This test exercises the happy-path wrapper on a file with
        errors in BOTH layers and asserts a single ValueError carrying
        every violation. If the guard breaks, we'd see the wrapper's
        aggregation report plus a second bronze-only aggregation
        report from the subsequent call, or the second call would
        double-report the same violation.
        """
        bad_rows = [
            {
                "data_flow_id": "343I1",
                "data_flow_group": "A1",
                "source_format": "cloudFiles",
                "source_details": {"source_path_dev": "/tmp/i1"},
                "bronze_database_dev": "ok_db",
                "bronze_table": "1bad_bronze_table",  # bad
                "bronze_reader_options": {},
                "bronze_table_path_dev": "/tmp/bronze/i1",
                "silver_database_dev": "ok_db",
                "silver_table": "bad.silver.table",  # bad
                "silver_table_path_dev": "/tmp/silver/i1",
            },
        ]
        tmp_dir = tempfile.mkdtemp(prefix="sdp_meta_i343_idem_")
        try:
            bad_file = os.path.join(tmp_dir, "onboarding_bad.json")
            with open(bad_file, "w") as f:
                json.dump(bad_rows, f)

            params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
            params["onboarding_file_path"] = bad_file

            with self.assertRaises(ValueError) as ctx:
                OnboardDataflowspec(
                    self.spark, params
                ).onboard_dataflow_specs()

            msg = str(ctx.exception)
            # Both violations reported.
            self.assertIn("flow 343I1 bronze_table", msg)
            self.assertIn("flow 343I1 silver_table", msg)
            # Each violation reported EXACTLY ONCE \u2014 idempotency
            # guard prevented the second wrapper-triggered call to
            # ``onboard_bronze_dataflow_spec`` from re-walking bronze.
            self.assertEqual(
                msg.count("flow 343I1 bronze_table"), 1,
                f"bronze_table violation reported more than once "
                f"(idempotency guard leaking?):\n{msg}",
            )
            self.assertEqual(
                msg.count("flow 343I1 silver_table"), 1,
                f"silver_table violation reported more than once "
                f"(idempotency guard leaking?):\n{msg}",
            )
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_issue343_preflight_rerun_after_fix_actually_reruns(self):
        """The idempotency guard must NOT skip a rerun after a fix.

        Concrete scenario: the user runs onboarding, the pre-flight
        raises, they edit the onboarding file to fix a bad
        identifier, then call the same onboarder instance again. The
        second call must actually revalidate \u2014 if the guard cached
        "already validated" from the FAILED first call, the second
        call would silently skip the walk and the fix would appear
        to have taken effect without any pre-flight coverage.

        Pins the "only set _prevalidated_layers after successful walk"
        contract in the fix.
        """
        good_rows = [
            {
                "data_flow_id": "343R1",
                "data_flow_group": "A1",
                "source_format": "cloudFiles",
                "source_details": {"source_path_dev": "/tmp/r1"},
                "bronze_database_dev": "ok_db",
                "bronze_table": "ok_bronze_table",
                "bronze_reader_options": {},
                "bronze_table_path_dev": "/tmp/bronze/r1",
            },
        ]
        bad_rows = copy.deepcopy(good_rows)
        bad_rows[0]["bronze_table"] = "1bad_bronze_table"

        tmp_dir = tempfile.mkdtemp(prefix="sdp_meta_i343_rerun_")
        try:
            spec_path = os.path.join(tmp_dir, "onboarding.json")
            # First write: bad file. Trigger pre-flight failure.
            with open(spec_path, "w") as f:
                json.dump(bad_rows, f)
            params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
            params["onboarding_file_path"] = spec_path
            onboarder = OnboardDataflowspec(self.spark, params)
            with self.assertRaises(ValueError):
                onboarder.onboard_bronze_dataflow_spec()

            # Second write: fixed file. Rerun MUST re-walk pre-flight
            # (otherwise the "already validated" flag from the failed
            # first call would silently accept a still-bad file). We
            # test this by re-triggering with a DIFFERENT bad row and
            # asserting the new violation surfaces.
            different_bad_rows = copy.deepcopy(good_rows)
            different_bad_rows[0]["data_flow_id"] = "343R2"
            different_bad_rows[0]["bronze_table"] = "bad.table.name"
            with open(spec_path, "w") as f:
                json.dump(different_bad_rows, f)
            with self.assertRaises(ValueError) as ctx:
                onboarder.onboard_bronze_dataflow_spec()
            self.assertIn("flow 343R2 bronze_table", str(ctx.exception))
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_onboardDataFlowSpecs_with_merge(self):
        """Test for onboardDataflowspec with merge scenario."""
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        bronze_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['bronze_dataflowspec_table'])
        silver_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['silver_dataflowspec_table'])
        self.assertEqual(bronze_dataflowSpec_df.count(), 3)
        self.assertEqual(silver_dataflowSpec_df.count(), 3)
        local_params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        local_params["overwrite"] = "False"
        local_params["onboarding_file_path"] = self.onboarding_v2_json_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        bronze_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['bronze_dataflowspec_table'])
        silver_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['silver_dataflowspec_table'])
        self.assertEqual(bronze_dataflowSpec_df.count(), 3)
        self.assertEqual(silver_dataflowSpec_df.count(), 3)
        bronze_df_rows = bronze_dataflowSpec_df.collect()
        for bronze_df_row in bronze_df_rows:
            bronze_row = BronzeDataflowSpec(**bronze_df_row.asDict())
            if bronze_row.dataFlowId in ["100", "101"]:
                self.assertIsNone(bronze_row.readerConfigOptions.get("cloudFiles.rescuedDataColumn"))
            if bronze_row.dataFlowId == "103":
                self.assertEqual(bronze_row.readerConfigOptions.get("maxOffsetsPerTrigger"), "60000")

    def test_onboardDataFlowSpecs_with_merge_uc(self):
        """Test for onboardDataflowspec with merge scenario."""
        local_params = copy.deepcopy(self.onboarding_bronze_silver_params_uc_map)
        local_params["onboarding_file_path"] = self.onboarding_json_file
        del local_params["uc_enabled"]
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params, uc_enabled=True)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        bronze_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['bronze_dataflowspec_table'])
        silver_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['silver_dataflowspec_table'])
        self.assertEqual(bronze_dataflowSpec_df.count(), 3)
        self.assertEqual(silver_dataflowSpec_df.count(), 3)
        local_params["overwrite"] = "False"
        local_params["onboarding_file_path"] = self.onboarding_v2_json_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params, uc_enabled=True)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        bronze_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['bronze_dataflowspec_table'])
        silver_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['silver_dataflowspec_table'])
        self.assertEqual(bronze_dataflowSpec_df.count(), 3)
        self.assertEqual(silver_dataflowSpec_df.count(), 3)
        bronze_df_rows = bronze_dataflowSpec_df.collect()
        for bronze_df_row in bronze_df_rows:
            bronze_row = BronzeDataflowSpec(**bronze_df_row.asDict())
            if bronze_row.dataFlowId in ["101", "102"]:
                self.assertIsNone(bronze_row.readerConfigOptions.get("cloudFiles.rescuedDataColumn"))
            if bronze_row.dataFlowId == "103":
                self.assertEqual(bronze_row.readerConfigOptions.get("maxOffsetsPerTrigger"), "60000")

    def test_onboardDataflowSpec_with_multiple_partitions(self):
        """Test for onboardDataflowspec with multiple partitions for bronze layer."""
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_uc_map)
        del onboarding_params_map["uc_enabled"]
        onboarding_params_map["onboarding_file_path"] = self.onboarding_multiple_partitions_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, onboarding_params_map, uc_enabled=True)
        onboardDataFlowSpecs.onboard_dataflow_specs()

        # Assert Bronze DataflowSpec for multiple partition, and quarantine partition columns.
        bronze_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_uc_map['database'],
            self.onboarding_bronze_silver_params_uc_map['bronze_dataflowspec_table'])
        bronze_df_rows = bronze_dataflowSpec_df.collect()
        for bronze_df_row in bronze_df_rows:
            bronze_row = BronzeDataflowSpec(**bronze_df_row.asDict())
            self.assertEqual(len(bronze_row.partitionColumns), 2)
            quarantine_partitions = [
                col for col in bronze_row.quarantineTargetDetails.get('partition_columns').strip('[]').split(',')
            ]
            self.assertEqual(len(quarantine_partitions), 2)
        # Assert Silver DataflowSpec for multiple partition columns.
        silver_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['silver_dataflowspec_table'])
        silver_df_rows = silver_dataflowSpec_df.collect()
        for silver_df_row in silver_df_rows:
            silver_row = SilverDataflowSpec(**silver_df_row.asDict())
            self.assertEqual(len(silver_row.partitionColumns), 2)

    def test_onboardBronzeDataflowSpec_positive(self):
        """Test for onboardDataflowspec."""
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        del onboarding_params_map["silver_dataflowspec_table"]
        del onboarding_params_map["silver_dataflowspec_path"]
        onboarding_params_map["onboarding_file_path"] = self.onboarding_json_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, onboarding_params_map)
        onboardDataFlowSpecs.onboard_bronze_dataflow_spec()
        bronze_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['bronze_dataflowspec_table'])
        self.assertEqual(bronze_dataflowSpec_df.count(), 3)

    def test_getOnboardingFileDataframe_for_unsupported_file(self):
        """Test onboardingFiles not supported."""
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        del onboarding_params_map["silver_dataflowspec_table"]
        del onboarding_params_map["silver_dataflowspec_path"]
        onboarding_params_map["onboarding_file_path"] = self.onboarding_unsupported_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, onboarding_params_map)
        with self.assertRaises(Exception):
            onboardDataFlowSpecs.onboard_bronze_dataflow_spec()

    def test_onboardSilverDataflowSpec_positive(self):
        """Test Silverdataflowspec positive."""
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        del onboarding_params_map["bronze_dataflowspec_table"]
        del onboarding_params_map["bronze_dataflowspec_path"]
        onboarding_params_map["onboarding_file_path"] = self.onboarding_json_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, onboarding_params_map)
        onboardDataFlowSpecs.onboard_silver_dataflow_spec()
        silver_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['silver_dataflowspec_table'])
        self.assertEqual(silver_dataflowSpec_df.count(), 3)

    def test_dataflow_ids_dup_onboard(self):
        """Test dataflow for duplicate ids."""
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        del onboarding_params_map["silver_dataflowspec_table"]
        del onboarding_params_map["silver_dataflowspec_path"]
        onboarding_params_map["onboarding_file_path"] = self.onboarding_json_dups
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, onboarding_params_map)
        with self.assertRaises(Exception):
            onboardDataFlowSpecs.onboard_bronze_dataflow_spec()

    def test_validate_mandatory_fields_bronze(self):
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        del onboarding_params_map["silver_dataflowspec_table"]
        del onboarding_params_map["silver_dataflowspec_path"]
        onboarding_params_map["onboarding_file_path"] = self.onboarding_missing_keys_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, onboarding_params_map)
        with self.assertRaises(Exception):
            onboardDataFlowSpecs.onboard_bronze_dataflow_spec()

    def test_validate_mandatory_fields_silver(self):
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        del onboarding_params_map["bronze_dataflowspec_table"]
        del onboarding_params_map["bronze_dataflowspec_path"]
        onboarding_params_map["onboarding_file_path"] = self.onboarding_missing_keys_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, onboarding_params_map)
        with self.assertRaises(Exception):
            onboardDataFlowSpecs.onboard_silver_dataflow_spec()

    def test_onboardSilverDataflowSpec_with_merge(self):
        """Test for onboardDataflowspec with merge scenario."""
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)
        onboardDataFlowSpecs.onboard_silver_dataflow_spec()
        silver_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['silver_dataflowspec_table'])
        self.assertEqual(silver_dataflowSpec_df.count(), 3)
        local_params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        local_params["overwrite"] = "False"
        local_params["onboarding_file_path"] = self.onboarding_v2_json_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params)
        onboardDataFlowSpecs.onboard_silver_dataflow_spec()
        silver_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['silver_dataflowspec_table'])
        self.assertEqual(silver_dataflowSpec_df.count(), 3)

    @patch.object(DataFrame, "write", new_callable=MagicMock)
    def test_silver_dataflow_spec_dataframe_withuc(self, mock_write):
        """Test for onboardDataflowspec with merge scenario."""
        mock_write.format.return_value.mode.return_value.option.return_value.saveAsTable.return_value = None

        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_uc_map)
        del onboarding_params_map["uc_enabled"]
        del onboarding_params_map["bronze_dataflowspec_table"]
        del onboarding_params_map["bronze_dataflowspec_path"]
        del onboarding_params_map["silver_dataflowspec_path"]
        print(onboarding_params_map)
        o_dfs = OnboardDataflowspec(self.spark, onboarding_params_map, uc_enabled=True)
        o_dfs.onboard_silver_dataflow_spec()
        # Assert
        database = onboarding_params_map["database"]
        table = onboarding_params_map["silver_dataflowspec_table"]
        mock_write.format.assert_called_once_with("delta")
        mock_write.format.return_value.mode.assert_called_once_with("overwrite")
        mock_write.format.return_value.mode.return_value.option.assert_called_once_with("mergeSchema", "true")
        mock_write.format.return_value.mode.return_value.option.return_value.saveAsTable.assert_called_once_with(
            f"{database}.{table}")

    @patch.object(DataFrame, "write", new_callable=MagicMock)
    def test_bronze_dataflow_spec_dataframe_withuc(self, mock_write):
        """Test for onboardDataflowspec with merge scenario."""
        mock_write.format.return_value.mode.return_value.option.return_value.saveAsTable.return_value = None
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_uc_map)
        del onboarding_params_map["uc_enabled"]
        del onboarding_params_map["silver_dataflowspec_table"]
        del onboarding_params_map["silver_dataflowspec_path"]
        del onboarding_params_map["bronze_dataflowspec_path"]
        print(onboarding_params_map)
        o_dfs = OnboardDataflowspec(self.spark, onboarding_params_map, uc_enabled=True)
        o_dfs.onboard_bronze_dataflow_spec()
        # Assert
        database = onboarding_params_map["database"]
        table = onboarding_params_map["bronze_dataflowspec_table"]
        mock_write.format.assert_called_once_with("delta")
        mock_write.format.return_value.mode.assert_called_once_with("overwrite")
        mock_write.format.return_value.mode.return_value.option.assert_called_once_with("mergeSchema", "true")
        mock_write.format.return_value.mode.return_value.option.return_value.saveAsTable.assert_called_once_with(
            f"{database}.{table}")

    def test_bronze_dataflow_spec_append_flow(self):
        """Test for onboardDataflowspec with appendflow scenario."""
        local_params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        local_params["onboarding_file_path"] = self.onboarding_append_flow_json_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        bronze_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['bronze_dataflowspec_table'])
        bronze_dataflowSpec_df.show(truncate=False)
        silver_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['silver_dataflowspec_table'])
        silver_dataflowSpec_df.show(truncate=False)
        self.assertEqual(bronze_dataflowSpec_df.count(), 3)
        self.assertEqual(silver_dataflowSpec_df.count(), 3)

    # ------------------------------------------------------------------
    # Multi-source AUTO CDC onboarding tests (issue #294)
    # ------------------------------------------------------------------

    def test_bronze_cdc_flows_onboarding(self):
        """Bronze-layer multi-source CDC: cdcApplyChangesFlows column is
        populated with a JSON-encoded group containing both flows; the
        per-flow source schemas are looked up via ``source_schema_path``
        and stored in ``cdcApplyChangesFlowsSchemas`` keyed by
        ``flow.name``."""
        local_params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        local_params["onboarding_file_path"] = (
            self.onboarding_bronze_cdc_flows_json_file
        )
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params)
        onboardDataFlowSpecs.onboard_bronze_dataflow_spec()
        bronze_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map["database"],
            self.onboarding_bronze_silver_params_map["bronze_dataflowspec_table"],
        )
        self.assertEqual(bronze_df.count(), 1)
        row = bronze_df.collect()[0]
        self.assertIsNotNone(row.cdcApplyChangesFlows)
        group = json.loads(row.cdcApplyChangesFlows)
        self.assertEqual(group["keys"], ["customer_id"])
        self.assertEqual(group["scd_type"], "1")
        self.assertEqual(len(group["flows"]), 2)
        names = sorted(f["name"] for f in group["flows"])
        self.assertEqual(names, ["eu_cdc", "us_cdc"])
        # Per-flow source schemas are resolved at onboarding time and
        # stored in cdcApplyChangesFlowsSchemas keyed by flow name. We
        # don't assert the exact schema JSON shape here — that's the
        # schema loader's responsibility — only that both entries exist
        # and are non-empty so the runtime view factory has something
        # to apply.
        self.assertIsNotNone(row.cdcApplyChangesFlowsSchemas)
        schemas = dict(row.cdcApplyChangesFlowsSchemas)
        self.assertEqual(set(schemas.keys()), {"us_cdc", "eu_cdc"})
        for s in schemas.values():
            self.assertTrue(s and len(s) > 0)
        # Mutual exclusion: the row didn't define cdcApplyChanges so
        # that column must be None — confirming the CDC flows path
        # didn't accidentally write into the legacy column too.
        self.assertIsNone(row.cdcApplyChanges)

    def test_silver_cdc_flows_onboarding(self):
        """Silver-layer multi-source CDC: cdcApplyChangesFlows column is
        populated; cdcApplyChangesFlowsSchemas is not present on silver
        (silver flows always read Delta upstream, no per-flow schema
        needed)."""
        local_params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        local_params["onboarding_file_path"] = (
            self.onboarding_silver_cdc_flows_json_file
        )
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params)
        onboardDataFlowSpecs.onboard_silver_dataflow_spec()
        silver_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map["database"],
            self.onboarding_bronze_silver_params_map["silver_dataflowspec_table"],
        )
        self.assertEqual(silver_df.count(), 1)
        row = silver_df.collect()[0]
        self.assertIsNotNone(row.cdcApplyChangesFlows)
        group = json.loads(row.cdcApplyChangesFlows)
        self.assertEqual(group["keys"], ["customer_id"])
        names = sorted(f["name"] for f in group["flows"])
        self.assertEqual(names, ["customers_eu", "customers_us"])
        # Silver flows have no per-flow schema map column.
        self.assertFalse(hasattr(row, "cdcApplyChangesFlowsSchemas"))
        self.assertIsNone(row.cdcApplyChanges)

    def test_mixed_bronze_only_plus_silver_cdc_flows_onboarding(self):
        """Single onboarding file with N bronze-only rows + 1 silver-only
        row that uses multi-source AUTO CDC. This is the natural shape
        for the multi-source CDC silver demo (issue #294): each region
        gets its own bronze CDC table (no silver fields) and a separate
        silver row defines the unified target plus the per-flow merge.

        Asserts three core onboarding contract changes:

        * The bronze loop processes only the bronze-shaped rows
          (silver-only rows are not added to the bronze dataflowspec
          table).
        * The silver loop SKIPS bronze-only rows (no silver_database)
          instead of raising "Missing field=silver_database_..." on
          them — the failure that originally surfaced when running
          ``demo/launch_multi_source_cdc_demo.py``.
        * The silver loop does NOT require
          ``silver_transformation_json_<env>`` when the row uses
          ``silver_cdc_apply_changes_flows``, since the per-flow
          ``select_exp`` provides the transformation logic.
        """
        local_params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        local_params["onboarding_file_path"] = (
            self.onboarding_mixed_bronze_silver_cdc_flows_json_file
        )
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params)
        onboardDataFlowSpecs.onboard_dataflow_specs()

        # Bronze: exactly the 2 bronze-only rows (US + EU). The silver-
        # only row carries no bronze fields and so should NOT land in
        # the bronze dataflowspec table.
        bronze_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map["database"],
            self.onboarding_bronze_silver_params_map["bronze_dataflowspec_table"],
        )
        self.assertEqual(bronze_df.count(), 2)
        bronze_tables = sorted(
            r.targetDetails["table"] for r in bronze_df.collect()
        )
        self.assertEqual(bronze_tables, ["customers_eu_cdc", "customers_us_cdc"])

        # Silver: exactly the 1 silver-only row, with the multi-source
        # CDC group populated and the legacy single-source CDC column
        # left None. The runtime ignores selectExp/whereClause for
        # multi-source CDC silver rows so it's fine for them to be
        # NULL after the LEFT join against silver_transformations.
        silver_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map["database"],
            self.onboarding_bronze_silver_params_map["silver_dataflowspec_table"],
        )
        self.assertEqual(silver_df.count(), 1)
        srow = silver_df.collect()[0]
        self.assertEqual(srow.targetDetails["table"], "customers")
        self.assertIsNotNone(srow.cdcApplyChangesFlows)
        group = json.loads(srow.cdcApplyChangesFlows)
        self.assertEqual(group["keys"], ["customer_id"])
        self.assertEqual(
            sorted(f["name"] for f in group["flows"]),
            ["customers_eu", "customers_us"],
        )
        self.assertIsNone(srow.cdcApplyChanges)

    def test_cdc_flows_pre_validate_mutual_exclusion(self):
        """Pre-flight validation rejects a row that sets BOTH
        ``<layer>_cdc_apply_changes`` AND
        ``<layer>_cdc_apply_changes_flows`` on the same layer. We build
        the broken file on the fly so the fixture stays narrowly
        scoped to the happy path."""
        broken_fixture = os.path.join(
            self.onboarding_spec_paths,
            "onboarding_cdc_flows_mutually_exclusive.json",
        )
        with open(self.onboarding_silver_cdc_flows_json_file) as f:
            data = json.load(f)
        # Inject a competing single-flow CDC block on silver alongside
        # the flows-group block already in the fixture.
        data[0]["silver_cdc_apply_changes"] = {
            "keys": ["id"],
            "sequence_by": "operation_date",
            "scd_type": "1",
        }
        with open(broken_fixture, "w") as f:
            json.dump(data, f)
        local_params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        local_params["onboarding_file_path"] = broken_fixture
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params)
        with self.assertRaises(ValueError) as ctx:
            onboardDataFlowSpecs.onboard_dataflow_specs()
        self.assertIn("silver_cdc_apply_changes_flows", str(ctx.exception))
        self.assertIn("use one or the other", str(ctx.exception))

    def test_cdc_flows_pre_validate_duplicate_flow_names(self):
        """Pre-flight validation rejects duplicate ``flow.name`` within a
        group — the runtime would silently let one flow's view name
        collide with another's otherwise."""
        broken_fixture = os.path.join(
            self.onboarding_spec_paths,
            "onboarding_cdc_flows_dupe_names.json",
        )
        with open(self.onboarding_silver_cdc_flows_json_file) as f:
            data = json.load(f)
        # Set both flows to the same name to provoke the dupe check.
        for f_block in data[0]["silver_cdc_apply_changes_flows"]["flows"]:
            f_block["name"] = "dupe"
        with open(broken_fixture, "w") as f:
            json.dump(data, f)
        local_params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        local_params["onboarding_file_path"] = broken_fixture
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params)
        with self.assertRaises(ValueError) as ctx:
            onboardDataFlowSpecs.onboard_dataflow_specs()
        self.assertIn("duplicate flow name", str(ctx.exception))

    def test_cdc_flows_pre_validate_unsupported_source_format(self):
        """Pre-flight validation rejects per-flow ``source_format``
        that the runtime doesn't support for the given layer. Silver
        only accepts ``delta``; ``snapshot`` and streaming formats
        must fail here so the user fixes it before any Spark
        side-effect."""
        broken_fixture = os.path.join(
            self.onboarding_spec_paths,
            "onboarding_cdc_flows_bad_format.json",
        )
        with open(self.onboarding_silver_cdc_flows_json_file) as f:
            data = json.load(f)
        data[0]["silver_cdc_apply_changes_flows"]["flows"][0]["source_format"] = (
            "kafka"
        )
        with open(broken_fixture, "w") as f:
            json.dump(data, f)
        local_params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        local_params["onboarding_file_path"] = broken_fixture
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params)
        with self.assertRaises(ValueError) as ctx:
            onboardDataFlowSpecs.onboard_dataflow_specs()
        self.assertIn("is not supported for silver", str(ctx.exception))

    def test_silver_fanout_dataflow_spec_dataframe(self):
        """Test for onboardDataflowspec with fanout scenario."""
        local_params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        local_params["onboarding_file_path"] = self.onboarding_silver_fanout_json_file
        del local_params["bronze_dataflowspec_table"]
        del local_params["bronze_dataflowspec_path"]
        local_params["overwrite"] = "False"
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params)
        onboardDataFlowSpecs.onboard_silver_dataflow_spec()
        silver_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['silver_dataflowspec_table'])
        silver_dataflowSpec_df.show(truncate=False)
        self.assertEqual(silver_dataflowSpec_df.count(), 4)

    def test_silver_fanout_single_file_onboarding(self):
        """Single-file fanout: one onboarding spec contains a base
        bronze+silver row plus N silver-only consumer rows that
        reference the same bronze. Onboarding with ``bronze_silver``
        layer must produce 1 bronze + 4 silvers in ONE pass \u2014 no
        secondary ``onboard_layer=silver, overwrite=False`` task
        required.

        Before the fix this would have raised
        ``Missing field=source_details in onboarding_row`` on the
        first fanout consumer row, because rows 2-4 set
        ``bronze_database_<env>`` (pointing at the bronze schema where
        the producer row writes ``customers``) but lack
        ``source_details`` (they're silver-only consumers). The
        ``__get_bronze_dataflow_spec_dataframe`` skip now treats
        rows lacking ``source_details`` as silver-only \u2014 the silver
        pass picks them up from the same file.
        """
        local_params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        local_params["onboarding_file_path"] = (
            self.onboarding_silver_fanout_single_file
        )
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params)
        onboardDataFlowSpecs.onboard_dataflow_specs()

        # Bronze: exactly 1 producer row. The 3 fanout consumer rows
        # must NOT land in the bronze dataflowspec table.
        bronze_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['bronze_dataflowspec_table'])
        self.assertEqual(
            bronze_df.count(), 1,
            "Bronze pass should produce exactly 1 row (the producer); "
            "fanout consumers must be skipped",
        )
        bronze_tables = [r.targetDetails["table"] for r in bronze_df.collect()]
        self.assertEqual(bronze_tables, ["customers"])

        # Silver: all 4 silver targets (1 producer + 3 fanout consumers).
        silver_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['silver_dataflowspec_table'])
        self.assertEqual(
            silver_df.count(), 4,
            "Silver pass should produce 4 rows (the producer's silver "
            "target + 3 fanout consumers)",
        )
        silver_tables = sorted(
            r.targetDetails["table"] for r in silver_df.collect()
        )
        self.assertEqual(
            silver_tables,
            ["customers_clean", "customers_germany", "customers_japan", "customers_uk"],
        )

    def test_onboard_bronze_silver_with_v7(self):
        local_params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        local_params["onboarding_file_path"] = self.onboarding_json_v7_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        bronze_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['bronze_dataflowspec_table'])
        bronze_dataflowSpec_df.show(truncate=False)
        silver_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['silver_dataflowspec_table'])
        silver_dataflowSpec_df.show(truncate=False)
        self.assertEqual(bronze_dataflowSpec_df.count(), 3)
        self.assertEqual(silver_dataflowSpec_df.count(), 3)

    def test_onboard_bronze_create_sink(self):
        local_params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        local_params["onboarding_file_path"] = self.onboarding_sink_json_file
        local_params["bronze_dataflowspec_table"] = "bronze_dataflowspec_sink"
        del local_params["silver_dataflowspec_table"]
        del local_params["silver_dataflowspec_path"]
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params)
        onboardDataFlowSpecs.onboard_bronze_dataflow_spec()
        bronze_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            "bronze_dataflowspec_sink")
        bronze_dataflowSpec_df.show(truncate=False)
        self.assertEqual(bronze_dataflowSpec_df.count(), 1)

    def test_silver_bronze_create_sink(self):
        local_params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        local_params["onboarding_file_path"] = self.onboarding_sink_json_file
        local_params["silver_dataflowspec_table"] = "silver_dataflowspec_sink"
        del local_params["bronze_dataflowspec_table"]
        del local_params["bronze_dataflowspec_path"]
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params)
        onboardDataFlowSpecs.onboard_silver_dataflow_spec()
        silver_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            "silver_dataflowspec_sink")
        silver_dataflowSpec_df.show(truncate=False)
        self.assertEqual(silver_dataflowSpec_df.count(), 1)

    def test_onboard_bronze_silver_with_v8(self):
        local_params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        local_params["onboarding_file_path"] = self.onboarding_json_v8_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        bronze_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['bronze_dataflowspec_table'])
        bronze_dataflowSpec_df.show(truncate=False)
        silver_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['silver_dataflowspec_table'])
        silver_dataflowSpec_df.show(truncate=False)
        self.assertEqual(bronze_dataflowSpec_df.count(), 3)
        self.assertEqual(silver_dataflowSpec_df.count(), 3)

    def test_onboard_bronze_silver_with_v9(self):
        local_params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        local_params["onboarding_file_path"] = self.onboarding_json_v9_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        bronze_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['bronze_dataflowspec_table'])
        bronze_dataflowSpec_df.show(truncate=False)
        silver_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['silver_dataflowspec_table'])
        silver_dataflowSpec_df.show(truncate=False)
        self.assertEqual(bronze_dataflowSpec_df.count(), 3)
        self.assertEqual(silver_dataflowSpec_df.count(), 3)

    def test_onboard_bronze_silver_with_v10(self):
        local_params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        local_params["onboarding_file_path"] = self.onboarding_json_v10_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        bronze_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['bronze_dataflowspec_table'])
        bronze_dataflowSpec_df.show(truncate=False)
        silver_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['silver_dataflowspec_table'])
        silver_dataflowSpec_df.show(truncate=False)
        self.assertEqual(bronze_dataflowSpec_df.count(), 5)
        self.assertEqual(silver_dataflowSpec_df.count(), 5)

    def test_onboard_apply_changes_from_snapshot_positive(self):
        """Test for onboardDataflowspec."""
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        onboarding_params_map['env'] = 'it'
        del onboarding_params_map["silver_dataflowspec_table"]
        del onboarding_params_map["silver_dataflowspec_path"]
        onboarding_params_map["onboarding_file_path"] = self.onboarding_apply_changes_from_snapshot_json_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, onboarding_params_map, uc_enabled=True)
        onboardDataFlowSpecs.onboard_bronze_dataflow_spec()
        bronze_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['bronze_dataflowspec_table'])
        self.assertEqual(bronze_dataflowSpec_df.count(), 2)

    def test_onboard_apply_changes_from_snapshot_negative(self):
        """Test for onboardDataflowspec."""
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        onboarding_params_map['env'] = 'it'
        del onboarding_params_map["silver_dataflowspec_table"]
        del onboarding_params_map["silver_dataflowspec_path"]
        onboarding_params_map["onboarding_file_path"] = self.onboarding_apply_changes_from_snapshot_json__error_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, onboarding_params_map, uc_enabled=True)
        with self.assertRaises(Exception):
            onboardDataFlowSpecs.onboard_bronze_dataflow_spec()

    def test_onboard_silver_apply_changes_from_snapshot_positive(self):
        """Test for onboardDataflowspec."""
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        onboarding_params_map['env'] = 'dev'
        onboarding_params_map["onboarding_file_path"] = self.onboarding_silver_apply_changes_from_snapshot_json_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, onboarding_params_map, uc_enabled=True)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        bronze_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['bronze_dataflowspec_table'])
        self.assertEqual(bronze_dataflowSpec_df.count(), 3)
        bronze_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['silver_dataflowspec_table'])
        self.assertEqual(bronze_dataflowSpec_df.count(), 3)

    def test_get_quarantine_details_with_partitions_and_properties(self):
        """Test get_quarantine_details with partitions and properties."""
        onboarding_row = {
            "bronze_quarantine_table_partitions": "partition_col",
            "bronze_database_quarantine_it": "quarantine_db",
            "bronze_quarantine_table": "quarantine_table",
            "bronze_quarantine_table_path_it": "quarantine_path",
            "bronze_quarantine_table_properties": MagicMock(
                asDict=MagicMock(return_value={"property_key": "property_value"})
            )
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)
        quarantine_target_details, quarantine_table_properties = (
            onboardDataFlowSpecs._OnboardDataflowspec__get_quarantine_details(
                "it", "bronze", onboarding_row)
        )
        self.assertEqual(quarantine_target_details["database"], "quarantine_db")
        self.assertEqual(quarantine_target_details["table"], "quarantine_table")
        self.assertEqual(quarantine_target_details["partition_columns"], "partition_col")
        self.assertEqual(quarantine_target_details["path"], "quarantine_path")
        self.assertEqual(quarantine_table_properties, {"property_key": "property_value"})

    def test_get_quarantine_details_without_partitions_and_properties(self):
        """Test get_quarantine_details without partitions and properties."""
        onboarding_row = {
            "bronze_database_quarantine_it": "quarantine_db",
            "bronze_quarantine_table": "quarantine_table",
            "bronze_quarantine_table_path_it": "quarantine_path"
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)
        quarantine_target_details, quarantine_table_properties = (
            onboardDataFlowSpecs._OnboardDataflowspec__get_quarantine_details(
                "it", "bronze", onboarding_row
            )
        )
        self.assertEqual(quarantine_target_details["path"], "quarantine_path")
        self.assertEqual(quarantine_table_properties, {})

    def test_get_quarantine_details_with_uc_enabled(self):
        """Test get_quarantine_details with UC enabled."""
        onboarding_row = {
            "bronze_database_quarantine_it": "quarantine_db",
            "bronze_quarantine_table": "quarantine_table",
            "bronze_quarantine_table_properties": MagicMock(
                asDict=MagicMock(return_value={"property_key": "property_value"})
            )
        }
        onboardDataFlowSpecs = OnboardDataflowspec(
            self.spark, self.onboarding_bronze_silver_params_map, uc_enabled=True
        )
        quarantine_target_details, quarantine_table_properties = (
            onboardDataFlowSpecs._OnboardDataflowspec__get_quarantine_details(
                "it", "bronze", onboarding_row
            )
        )
        self.assertEqual(quarantine_target_details["database"], "quarantine_db")
        self.assertEqual(quarantine_target_details["table"], "quarantine_table")
        self.assertNotIn("path", quarantine_target_details)
        self.assertEqual(quarantine_table_properties, {"property_key": "property_value"})

    def test_get_quarantine_details_passes_through_table_comment(self):
        """Quarantine-table comment must flow into target details.

        Regression for the dead ``if "{layer}_quarantine_table_comment"``
        branch: the key test was a plain string literal (missing the
        ``f`` prefix), so ``bronze_quarantine_table_comment`` never
        matched and any comment defined in the onboarding file was
        silently dropped. This asserts the comment now propagates.
        """
        onboarding_row = {
            "bronze_database_quarantine_it": "quarantine_db",
            "bronze_quarantine_table": "quarantine_table",
            "bronze_quarantine_table_comment": "bronze customers quarantine table",
        }
        onboardDataFlowSpecs = OnboardDataflowspec(
            self.spark, self.onboarding_bronze_silver_params_map, uc_enabled=True
        )
        quarantine_target_details, _ = (
            onboardDataFlowSpecs._OnboardDataflowspec__get_quarantine_details(
                "it", "bronze", onboarding_row
            )
        )
        self.assertEqual(
            quarantine_target_details["comment"],
            "bronze customers quarantine table",
        )

    def test_get_quarantine_details_with_cluster_by_and_properties(self):
        """Test get_quarantine_details with partitions and properties."""
        onboarding_row = {
            "bronze_quarantine_table_cluster_by": ['col1', 'col2'],
            "bronze_database_quarantine_it": "quarantine_db",
            "bronze_quarantine_table": "quarantine_table",
            "bronze_quarantine_table_path_it": "quarantine_path",
            "bronze_quarantine_table_properties": MagicMock(
                asDict=MagicMock(return_value={"key": "value"})
            )
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)
        quarantine_target_details, quarantine_table_properties = (
            onboardDataFlowSpecs._OnboardDataflowspec__get_quarantine_details(
                "it", "bronze", onboarding_row)
        )
        self.assertEqual(quarantine_target_details["database"], "quarantine_db")
        self.assertEqual(quarantine_target_details["table"], "quarantine_table")
        self.assertEqual(quarantine_target_details["cluster_by"], ['col1', 'col2'])
        self.assertEqual(quarantine_target_details["path"], "quarantine_path")

    def test_set_quarantine_details_with_cluster_by_and_zOrder_properties(self):
        """Test get_quarantine_details with partitions and properties."""
        onboarding_row = {
            "bronze_quarantine_table_cluster_by": ['col1', 'col2'],
            "bronze_database_quarantine_it": "quarantine_db",
            "bronze_quarantine_table": "quarantine_table",
            "bronze_quarantine_table_path_it": "quarantine_path",
            "bronze_quarantine_table_properties": MagicMock(
                asDict=MagicMock(return_value={"pipelines.autoOptimize.zOrderCols": "col1,col2"})
            )
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)

        with self.assertRaises(Exception) as context:
            onboardDataFlowSpecs._OnboardDataflowspec__get_quarantine_details(
                "it", "bronze", onboarding_row)
        print(str(context.exception))
        self.assertTrue(
            "Cannot support zOrder and cluster_by together at bronze_quarantine_table_cluster_by"
            in str(context.exception))

    def test_set_bronze_table_cluster_by_properties(self):
        """Test get_quarantine_details with partitions and properties."""
        onboarding_row = {
            "bronze_cluster_by": ['col1', 'col2'],
            "bronze_table_properties": {"pipelines.autoOptimize.managed": "true"}
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)
        cluster_by = onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_properties(
            onboarding_row, onboarding_row['bronze_table_properties'], "bronze_cluster_by")
        self.assertEqual(cluster_by, ['col1', 'col2'])

    def test_set_bronze_table_cluster_by_and_zOrder_properties(self):
        """Test get_quarantine_details with partitions and properties."""
        onboarding_row = {
            "bronze_cluster_by": ['col1', 'col2'],
            "bronze_table_properties": MagicMock(
                asDict=MagicMock(return_value={"pipelines.autoOptimize.zOrderCols": "col1,col2"})
            )
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)

        with self.assertRaises(Exception) as context:
            onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_properties(
                onboarding_row, onboarding_row['bronze_table_properties'], "bronze_cluster_by")
        self.assertTrue(
            "Cannot support zOrder and cluster_by together at bronze_cluster_by" in str(context.exception))

    def test_set_silver_table_cluster_by_properties(self):
        """Test get_quarantine_details with partitions and properties."""
        onboarding_row = {
            "silver_cluster_by": ['col1', 'col2'],
            "silver_table_properties": {"pipelines.autoOptimize.managed": "true"}
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)
        cluster_by = onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_properties(
            onboarding_row, onboarding_row['silver_table_properties'], "silver_cluster_by")
        self.assertEqual(cluster_by, ['col1', 'col2'])

    def test_set_silver_table_cluster_by_and_zOrder_properties(self):
        """Test get_quarantine_details with partitions and properties."""
        onboarding_row = {
            "silver_cluster_by": ['col1', 'col2'],
            "silver_table_properties": MagicMock(
                asDict=MagicMock(return_value={"pipelines.autoOptimize.zOrderCols": "col1,col2"})
            )
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)

        with self.assertRaises(Exception) as context:
            onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_properties(
                onboarding_row, onboarding_row['silver_table_properties'], "silver_cluster_by")
        self.assertTrue(
            "Cannot support zOrder and cluster_by together at silver_cluster_by" in str(context.exception))

    def test_cluster_by_validation_non_list(self):
        """Test cluster_by validation with non-list value that cannot be parsed."""
        onboarding_row = {
            "bronze_cluster_by": "col1,col2",  # String that cannot be parsed as list
            "bronze_table_properties": {"pipelines.autoOptimize.managed": "true"}
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)

        with self.assertRaises(Exception) as context:
            onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_properties(
                onboarding_row, onboarding_row['bronze_table_properties'], "bronze_cluster_by")
        self.assertIn("Cannot parse string as list", str(context.exception))

    def test_cluster_by_validation_non_string_element(self):
        """Test cluster_by validation with non-string element in list."""
        onboarding_row = {
            "bronze_cluster_by": ["col1", 123, "col3"],  # Integer in list
            "bronze_table_properties": {"pipelines.autoOptimize.managed": "true"}
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)

        with self.assertRaises(Exception) as context:
            onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_properties(
                onboarding_row, onboarding_row['bronze_table_properties'], "bronze_cluster_by")
        self.assertIn("Element at index 1 must be a string but got int", str(context.exception))

    def test_cluster_by_validation_whitespace_element(self):
        """Test cluster_by validation with whitespace in element."""
        onboarding_row = {
            "bronze_cluster_by": ["col1", " col2 ", "col3"],  # Whitespace around col2
            "bronze_table_properties": {"pipelines.autoOptimize.managed": "true"}
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)

        with self.assertRaises(Exception) as context:
            onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_properties(
                onboarding_row, onboarding_row['bronze_table_properties'], "bronze_cluster_by")
        self.assertIn("contains leading/trailing whitespace", str(context.exception))

    def test_cluster_by_validation_empty_element(self):
        """Test cluster_by validation with empty element."""
        onboarding_row = {
            "bronze_cluster_by": ["col1", "", "col3"],  # Empty string
            "bronze_table_properties": {"pipelines.autoOptimize.managed": "true"}
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)

        with self.assertRaises(Exception) as context:
            onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_properties(
                onboarding_row, onboarding_row['bronze_table_properties'], "bronze_cluster_by")
        self.assertIn("is empty or contains only whitespace", str(context.exception))

    def test_cluster_by_validation_unbalanced_quotes(self):
        """Test cluster_by validation with unbalanced quotes."""
        onboarding_row = {
            "bronze_cluster_by": ["col1", "col2\"", "col3"],  # Unbalanced quote
            "bronze_table_properties": {"pipelines.autoOptimize.managed": "true"}
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)

        with self.assertRaises(Exception) as context:
            onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_properties(
                onboarding_row, onboarding_row['bronze_table_properties'], "bronze_cluster_by")
        self.assertIn("contains unbalanced quotes", str(context.exception))

    def test_cluster_by_validation_valid_list(self):
        """Test cluster_by validation with valid list."""
        onboarding_row = {
            "silver_cluster_by": ["id", "customer_id"],  # Valid list
            "silver_table_properties": {"pipelines.autoOptimize.managed": "true"}
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)

        cluster_by = onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_properties(
            onboarding_row, onboarding_row['silver_table_properties'], "silver_cluster_by")
        self.assertEqual(cluster_by, ["id", "customer_id"])

    def test_cluster_by_string_parsing_valid(self):
        """Test cluster_by parsing from valid string representation."""
        onboarding_row = {
            "bronze_cluster_by": "['id', 'email']",  # String representation of list
            "bronze_table_properties": {"pipelines.autoOptimize.managed": "true"}
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)

        cluster_by = onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_properties(
            onboarding_row, onboarding_row['bronze_table_properties'], "bronze_cluster_by")
        self.assertEqual(cluster_by, ["id", "email"])

    def test_cluster_by_string_parsing_invalid_syntax(self):
        """Test cluster_by parsing with invalid string syntax."""
        onboarding_row = {
            "bronze_cluster_by": "['id', 'email'",  # Missing closing bracket
            "bronze_table_properties": {"pipelines.autoOptimize.managed": "true"}
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)

        with self.assertRaises(Exception) as context:
            onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_properties(
                onboarding_row, onboarding_row['bronze_table_properties'], "bronze_cluster_by")
        self.assertIn("Cannot parse string as list", str(context.exception))

    def test_cluster_by_string_parsing_not_list(self):
        """Test cluster_by parsing when string represents non-list."""
        onboarding_row = {
            "bronze_cluster_by": "'id,email'",  # String that evaluates to string, not list
            "bronze_table_properties": {"pipelines.autoOptimize.managed": "true"}
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)

        with self.assertRaises(Exception) as context:
            onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_properties(
                onboarding_row, onboarding_row['bronze_table_properties'], "bronze_cluster_by")
        self.assertIn("Parsed value is not a list", str(context.exception))

    def test_cluster_by_string_parsing_double_quotes(self):
        """Test cluster_by parsing with double quotes."""
        onboarding_row = {
            "bronze_cluster_by": '["id", "customer_id"]',  # Double quotes
            "bronze_table_properties": {"pipelines.autoOptimize.managed": "true"}
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)

        cluster_by = onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_properties(
            onboarding_row, onboarding_row['bronze_table_properties'], "bronze_cluster_by")
        self.assertEqual(cluster_by, ["id", "customer_id"])

    def test_bronze_cluster_by_auto_true(self):
        """Test cluster_by_auto property with True value."""
        onboarding_row = {
            "bronze_cluster_by_auto": True
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)
        cluster_by_auto = onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_auto(
            onboarding_row, "bronze_cluster_by_auto")
        self.assertEqual(cluster_by_auto, True)

    def test_bronze_cluster_by_auto_false(self):
        """Test cluster_by_auto property with False value."""
        onboarding_row = {
            "bronze_cluster_by_auto": False
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)
        cluster_by_auto = onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_auto(
            onboarding_row, "bronze_cluster_by_auto")
        self.assertEqual(cluster_by_auto, False)

    def test_bronze_cluster_by_auto_string_true(self):
        """Test cluster_by_auto property with string 'true'."""
        onboarding_row = {
            "bronze_cluster_by_auto": "true"
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)
        cluster_by_auto = onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_auto(
            onboarding_row, "bronze_cluster_by_auto")
        self.assertEqual(cluster_by_auto, True)

    def test_bronze_cluster_by_auto_string_false(self):
        """Test cluster_by_auto property with string 'false'."""
        onboarding_row = {
            "bronze_cluster_by_auto": "false"
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)
        cluster_by_auto = onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_auto(
            onboarding_row, "bronze_cluster_by_auto")
        self.assertEqual(cluster_by_auto, False)

    def test_bronze_cluster_by_auto_none(self):
        """Test cluster_by_auto property with None value."""
        onboarding_row = {
            "bronze_cluster_by_auto": None
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)
        cluster_by_auto = onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_auto(
            onboarding_row, "bronze_cluster_by_auto")
        self.assertEqual(cluster_by_auto, None)

    def test_bronze_cluster_by_auto_invalid_string(self):
        """Test cluster_by_auto property with invalid string value."""
        onboarding_row = {
            "bronze_cluster_by_auto": "invalid"
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)
        with self.assertRaises(Exception) as context:
            onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_auto(
                onboarding_row, "bronze_cluster_by_auto")
        self.assertIn("Expected boolean or string representation of boolean", str(context.exception))

    def test_silver_cluster_by_auto_true(self):
        """Test silver cluster_by_auto property with True value."""
        onboarding_row = {
            "silver_cluster_by_auto": True
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)
        cluster_by_auto = onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_auto(
            onboarding_row, "silver_cluster_by_auto")
        self.assertEqual(cluster_by_auto, True)

    def test_bronze_cluster_by_auto_string_case_insensitive(self):
        """Test cluster_by_auto property with case-insensitive string values."""
        test_values = ["true", "True", "TRUE", "TrUe"]
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)

        for value in test_values:
            onboarding_row = {"bronze_cluster_by_auto": value}
            cluster_by_auto = onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_auto(
                onboarding_row, "bronze_cluster_by_auto")
            self.assertEqual(cluster_by_auto, True, f"Failed for value: {value}")

        test_values_false = ["false", "False", "FALSE", "FaLsE"]
        for value in test_values_false:
            onboarding_row = {"bronze_cluster_by_auto": value}
            cluster_by_auto = onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_auto(
                onboarding_row, "bronze_cluster_by_auto")
            self.assertEqual(cluster_by_auto, False, f"Failed for value: {value}")

    def test_bronze_cluster_by_auto_string_with_whitespace(self):
        """Test cluster_by_auto property with string values containing whitespace."""
        onboarding_row = {"bronze_cluster_by_auto": "  true  "}
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)
        cluster_by_auto = onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_auto(
            onboarding_row, "bronze_cluster_by_auto")
        self.assertEqual(cluster_by_auto, True)

        onboarding_row = {"bronze_cluster_by_auto": "  false  "}
        cluster_by_auto = onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_auto(
            onboarding_row, "bronze_cluster_by_auto")
        self.assertEqual(cluster_by_auto, False)

    def test_bronze_cluster_by_auto_invalid_type(self):
        """Test cluster_by_auto property with invalid type."""
        onboarding_row = {"bronze_cluster_by_auto": 123}
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)
        with self.assertRaises(Exception) as context:
            onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_auto(
                onboarding_row, "bronze_cluster_by_auto")
        self.assertIn("Expected boolean or string", str(context.exception))

    def test_bronze_cluster_by_auto_missing_key(self):
        """Test cluster_by_auto property when key is missing."""
        onboarding_row = {}
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)
        cluster_by_auto = onboardDataFlowSpecs._OnboardDataflowspec__get_cluster_by_auto(
            onboarding_row, "bronze_cluster_by_auto")
        self.assertEqual(cluster_by_auto, False)

    def test_quarantine_cluster_by_auto_true(self):
        """Test quarantine table cluster_by_auto with True value."""
        onboarding_row = {
            "bronze_database_quarantine_it": "quarantine_db",
            "bronze_quarantine_table": "quarantine_table",
            "bronze_quarantine_table_cluster_by_auto": True
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)
        quarantine_target_details, _ = (
            onboardDataFlowSpecs._OnboardDataflowspec__get_quarantine_details(
                "it", "bronze", onboarding_row
            )
        )
        self.assertEqual(quarantine_target_details.get("cluster_by_auto"), True)

    def test_quarantine_cluster_by_auto_false(self):
        """Test quarantine table cluster_by_auto with False value."""
        onboarding_row = {
            "bronze_database_quarantine_it": "quarantine_db",
            "bronze_quarantine_table": "quarantine_table",
            "bronze_quarantine_table_cluster_by_auto": False
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)
        quarantine_target_details, _ = (
            onboardDataFlowSpecs._OnboardDataflowspec__get_quarantine_details(
                "it", "bronze", onboarding_row
            )
        )
        self.assertEqual(quarantine_target_details.get("cluster_by_auto"), False)

    def test_quarantine_cluster_by_auto_string(self):
        """Test quarantine table cluster_by_auto with string value."""
        onboarding_row = {
            "bronze_database_quarantine_it": "quarantine_db",
            "bronze_quarantine_table": "quarantine_table",
            "bronze_quarantine_table_cluster_by_auto": "true"
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)
        quarantine_target_details, _ = (
            onboardDataFlowSpecs._OnboardDataflowspec__get_quarantine_details(
                "it", "bronze", onboarding_row
            )
        )
        self.assertEqual(quarantine_target_details.get("cluster_by_auto"), True)

    def test_quarantine_cluster_by_auto_default(self):
        """Test quarantine table cluster_by_auto defaults when not provided."""
        onboarding_row = {
            "bronze_database_quarantine_it": "quarantine_db",
            "bronze_quarantine_table": "quarantine_table"
        }
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)
        quarantine_target_details, _ = (
            onboardDataFlowSpecs._OnboardDataflowspec__get_quarantine_details(
                "it", "bronze", onboarding_row
            )
        )
        self.assertEqual(quarantine_target_details.get("cluster_by_auto"), False)

    def test_unsupported_source_format_raises_exception(self):
        """Test that unsupported source_format raises an Exception during bronze onboarding."""
        onboard_dfs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)
        onboarding_row = {
            "data_flow_id": "test_flow",
            "data_flow_group": "A1",
            "source_format": "parquet",
            "source_system": "TEST",
            "bronze_table": "test_table",
            "bronze_database_dev": "bronze_db",
            "source_details": {"path": "/tmp/test"},
        }
        onboarding_df = self.spark.createDataFrame([onboarding_row])
        with self.assertRaises(Exception) as context:
            onboard_dfs._OnboardDataflowspec__get_bronze_dataflow_spec_dataframe(
                onboarding_df, "dev"
            )
        self.assertIn("not supported in SDP-META", str(context.exception))
