"""Tests for Mockhaus data ingestion functionality."""

import tempfile
import unittest
from pathlib import Path

from mockhaus import MockhausExecutor


class TestDataIngestion(unittest.TestCase):
    """Test data ingestion features including stages, file formats, and COPY INTO."""

    def setUp(self) -> None:
        """Set up test environment."""
        # Use in-memory history database for tests to avoid locking issues
        self.executor = MockhausExecutor(enable_history=True, history_db_path=":memory:")
        self.executor.connect()

        # Create test directories
        self.test_data_dir = Path(tempfile.mkdtemp())

        # Create sample CSV data
        self.csv_data = (
            "id,name,email,created_date\n1,Alice,alice@test.com,2023-01-15\n2,Bob,bob@test.com,2023-02-20\n3,Charlie,charlie@test.com,2023-03-10"
        )
        self.csv_file = self.test_data_dir / "test.csv"
        self.csv_file.write_text(self.csv_data)

        # Create sample JSON data
        self.json_data = '{"id": 1, "name": "Alice", "email": "alice@test.com"}\n{"id": 2, "name": "Bob", "email": "bob@test.com"}'
        self.json_file = self.test_data_dir / "test.json"
        self.json_file.write_text(self.json_data)

    def tearDown(self) -> None:
        """Clean up after tests."""
        self.executor.disconnect()
        # Clean up test files
        import shutil

        shutil.rmtree(self.test_data_dir, ignore_errors=True)

    def test_create_user_stage(self) -> None:
        """Test creating a USER stage."""
        result = self.executor.execute_snowflake_sql("CREATE STAGE test_user_stage")

        assert result.success
        assert result.translated_sql == "-- Created stage test_user_stage"

        # Verify stage was stored
        stage = self.executor._ingestion_handler.stage_manager.get_stage("test_user_stage")
        assert stage is not None
        assert stage.name == "test_user_stage"
        assert stage.stage_type == "USER"

    def test_create_external_stage_with_url(self) -> None:
        """Test creating an EXTERNAL stage with URL."""
        file_url = f"file://{self.test_data_dir}"
        sql = f"CREATE STAGE test_external_stage URL = '{file_url}'"

        result = self.executor.execute_snowflake_sql(sql)

        assert result.success

        # Verify stage was stored with correct path
        stage = self.executor._ingestion_handler.stage_manager.get_stage("test_external_stage")
        assert stage is not None
        assert stage.stage_type == "EXTERNAL"
        assert stage.url == file_url
        assert stage.local_path == str(self.test_data_dir)

    def test_create_s3_external_stage(self) -> None:
        """Test creating an EXTERNAL stage with S3 URL."""
        s3_url = "s3://my-bucket/data/"
        sql = f"CREATE STAGE test_s3_stage URL = '{s3_url}'"

        result = self.executor.execute_snowflake_sql(sql)

        assert result.success

        # Verify stage was stored
        stage = self.executor._ingestion_handler.stage_manager.get_stage("test_s3_stage")
        assert stage is not None
        assert stage.stage_type == "EXTERNAL"
        assert stage.url == s3_url
        # Should create local path under external/s3/
        assert "external/s3/my-bucket/data" in stage.local_path

    def test_drop_stage(self) -> None:
        """Test dropping a stage."""
        # Create a stage first
        self.executor.execute_snowflake_sql("CREATE STAGE test_drop_stage")

        # Verify it exists
        stage = self.executor._ingestion_handler.stage_manager.get_stage("test_drop_stage")
        assert stage is not None

        # Drop it
        result = self.executor.execute_snowflake_sql("DROP STAGE test_drop_stage")
        assert result.success

        # Verify it's gone
        stage = self.executor._ingestion_handler.stage_manager.get_stage("test_drop_stage")
        assert stage is None

    def test_create_csv_file_format(self) -> None:
        """Test creating a CSV file format."""
        sql = "CREATE FILE FORMAT test_csv_format TYPE = 'CSV' FIELD_DELIMITER = '|' SKIP_HEADER = 1"

        result = self.executor.execute_snowflake_sql(sql)

        assert result.success
        assert result.translated_sql == "-- Created file format test_csv_format"

        # Verify format was stored
        format_obj = self.executor._ingestion_handler.format_manager.get_format("test_csv_format")
        assert format_obj is not None
        assert format_obj.name == "test_csv_format"
        assert format_obj.format_type == "CSV"
        assert format_obj.properties["field_delimiter"] == "|"
        assert format_obj.properties["skip_header"] == 1

    def test_create_json_file_format(self) -> None:
        """Test creating a JSON file format."""
        sql = "CREATE FILE FORMAT test_json_format TYPE = 'JSON'"

        result = self.executor.execute_snowflake_sql(sql)

        assert result.success

        # Verify format was stored
        format_obj = self.executor._ingestion_handler.format_manager.get_format("test_json_format")
        assert format_obj is not None
        assert format_obj.format_type == "JSON"

    def test_drop_file_format(self) -> None:
        """Test dropping a file format."""
        # Create a format first
        self.executor.execute_snowflake_sql("CREATE FILE FORMAT test_drop_format TYPE = 'CSV'")

        # Verify it exists
        format_obj = self.executor._ingestion_handler.format_manager.get_format("test_drop_format")
        assert format_obj is not None

        # Drop it
        result = self.executor.execute_snowflake_sql("DROP FILE FORMAT test_drop_format")
        assert result.success

        # Verify it's gone
        format_obj = self.executor._ingestion_handler.format_manager.get_format("test_drop_format")
        assert format_obj is None

    def test_copy_into_with_named_format(self) -> None:
        """Test COPY INTO using a named file format."""
        # Create table
        create_table_sql = """
        CREATE TABLE test_customers (
            id INTEGER,
            name VARCHAR(100),
            email VARCHAR(255),
            created_date DATE
        )
        """
        result = self.executor.execute_snowflake_sql(create_table_sql)
        assert result.success

        # Create stage pointing to test data
        file_url = f"file://{self.test_data_dir}"
        stage_sql = f"CREATE STAGE test_data_stage URL = '{file_url}'"
        result = self.executor.execute_snowflake_sql(stage_sql)
        assert result.success

        # Create file format
        format_sql = "CREATE FILE FORMAT csv_with_header TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1"
        result = self.executor.execute_snowflake_sql(format_sql)
        assert result.success

        # Copy data
        copy_sql = "COPY INTO test_customers FROM '@test_data_stage/test.csv' FILE_FORMAT = 'csv_with_header'"
        result = self.executor.execute_snowflake_sql(copy_sql)

        assert result.success
        assert result.data is not None
        assert result.data[0]["rows_loaded"] == 3

        # Verify data was loaded
        select_result = self.executor.execute_snowflake_sql("SELECT * FROM test_customers ORDER BY id")
        assert select_result.success
        assert select_result.data is not None
        assert len(select_result.data) == 3
        assert select_result.data[0]["name"] == "Alice"
        assert select_result.data[1]["name"] == "Bob"
        assert select_result.data[2]["name"] == "Charlie"

    def test_copy_into_with_inline_format(self) -> None:
        """Test COPY INTO using inline file format specification."""
        # Create table
        create_table_sql = """
        CREATE TABLE test_customers2 (
            id INTEGER,
            name VARCHAR(100),
            email VARCHAR(255),
            created_date DATE
        )
        """
        result = self.executor.execute_snowflake_sql(create_table_sql)
        assert result.success

        # Create stage pointing to test data
        file_url = f"file://{self.test_data_dir}"
        stage_sql = f"CREATE STAGE test_data_stage2 URL = '{file_url}'"
        result = self.executor.execute_snowflake_sql(stage_sql)
        assert result.success

        # Copy data with inline format
        copy_sql = "COPY INTO test_customers2 FROM '@test_data_stage2/test.csv' FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)"
        result = self.executor.execute_snowflake_sql(copy_sql)

        assert result.success
        assert result.data is not None
        assert result.data[0]["rows_loaded"] == 3

    def test_copy_into_with_user_stage(self) -> None:
        """Test COPY INTO using user stage reference (@~/)."""
        # Copy test file to user stage directory
        user_stage_dir = Path.home() / ".mockhaus" / "user"
        user_stage_dir.mkdir(parents=True, exist_ok=True)
        user_test_file = user_stage_dir / "user_test.csv"
        user_test_file.write_text(self.csv_data)

        try:
            # Create table
            create_table_sql = """
            CREATE TABLE test_user_customers (
                id INTEGER,
                name VARCHAR(100),
                email VARCHAR(255),
                created_date DATE
            )
            """
            result = self.executor.execute_snowflake_sql(create_table_sql)
            assert result.success

            # Copy data from user stage
            copy_sql = "COPY INTO test_user_customers FROM '@~/user_test.csv' FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)"
            result = self.executor.execute_snowflake_sql(copy_sql)

            assert result.success
            assert result.data is not None
            assert result.data[0]["rows_loaded"] == 3

        finally:
            # Clean up
            user_test_file.unlink(missing_ok=True)

    def test_copy_into_file_not_found(self) -> None:
        """Test COPY INTO with non-existent file."""
        # Create table
        create_table_sql = """
        CREATE TABLE test_error_table (
            id INTEGER,
            name VARCHAR(100)
        )
        """
        result = self.executor.execute_snowflake_sql(create_table_sql)
        assert result.success

        # Create stage
        file_url = f"file://{self.test_data_dir}"
        stage_sql = f"CREATE STAGE test_error_stage URL = '{file_url}'"
        result = self.executor.execute_snowflake_sql(stage_sql)
        assert result.success

        # Try to copy from non-existent file
        copy_sql = "COPY INTO test_error_table FROM '@test_error_stage/nonexistent.csv' FILE_FORMAT = (TYPE = 'CSV')"
        result = self.executor.execute_snowflake_sql(copy_sql)

        assert not result.success
        assert result.error is not None
        assert "File not found" in result.error

    def test_copy_into_invalid_format_reference(self) -> None:
        """Test COPY INTO with non-existent file format."""
        # Create table
        create_table_sql = """
        CREATE TABLE test_error_table2 (
            id INTEGER,
            name VARCHAR(100)
        )
        """
        result = self.executor.execute_snowflake_sql(create_table_sql)
        assert result.success

        # Create stage
        file_url = f"file://{self.test_data_dir}"
        stage_sql = f"CREATE STAGE test_error_stage2 URL = '{file_url}'"
        result = self.executor.execute_snowflake_sql(stage_sql)
        assert result.success

        # Try to copy with non-existent format
        copy_sql = "COPY INTO test_error_table2 FROM '@test_error_stage2/test.csv' FILE_FORMAT = 'nonexistent_format'"
        result = self.executor.execute_snowflake_sql(copy_sql)

        assert not result.success
        assert result.error is not None
        assert "File format 'nonexistent_format' not found" in result.error

    def test_stage_path_resolution(self) -> None:
        """Test various stage path resolution patterns."""
        stage_manager = self.executor._ingestion_handler.stage_manager

        # Test user stage resolution
        user_path = stage_manager.resolve_stage_path("@~/test.csv")
        expected_user_path = str(Path.home() / ".mockhaus" / "user" / "test.csv")
        assert user_path == expected_user_path

        # Test table stage resolution
        table_path = stage_manager.resolve_stage_path("@%my_table/data.csv")
        expected_table_path = str(Path.home() / ".mockhaus" / "tables" / "my_table" / "data.csv")
        assert table_path == expected_table_path

        # Test named stage resolution (without creating the stage)
        named_path = stage_manager.resolve_stage_path("@my_stage/file.csv")
        expected_named_path = str(Path.home() / ".mockhaus" / "stages" / "my_stage" / "file.csv")
        assert named_path == expected_named_path

    def test_file_format_property_mapping(self) -> None:
        """Test mapping of file format properties to DuckDB options."""
        format_manager = self.executor._ingestion_handler.format_manager

        # Create a CSV format with various properties
        csv_format = format_manager.create_format(
            "test_mapping",
            "CSV",
            {"field_delimiter": "|", "skip_header": 1, "field_optionally_enclosed_by": '"', "null_if": ["NULL", "N/A"], "date_format": "YYYY-MM-DD"},
        )

        # Test mapping to DuckDB options
        options = format_manager.map_to_duckdb_options(csv_format)

        assert options["FORMAT"] == "CSV"
        assert options["DELIMITER"] == "|"
        assert options["HEADER"] is True
        assert options["QUOTE"] == '"'
        assert options["NULL"] == "NULL"  # First null value
        assert options["DATEFORMAT"] == "YYYY-MM-DD"

    def test_inline_format_parsing(self) -> None:
        """Test parsing of inline format specifications."""
        format_manager = self.executor._ingestion_handler.format_manager

        inline_spec = "TYPE = 'CSV' FIELD_DELIMITER = '|' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '\"'"
        options = format_manager.parse_inline_format(inline_spec)

        assert options["TYPE"] == "CSV"
        assert options["field_delimiter"] == "|"
        assert options["skip_header"] == 1
        assert options["field_optionally_enclosed_by"] == '"'

    def test_default_file_formats(self) -> None:
        """Test that default file formats are created."""
        format_manager = self.executor._ingestion_handler.format_manager

        # Check that default formats exist
        csv_default = format_manager.get_format("CSV_DEFAULT")
        assert csv_default is not None
        assert csv_default.format_type == "CSV"

        json_default = format_manager.get_format("JSON_DEFAULT")
        assert json_default is not None
        assert json_default.format_type == "JSON"

        parquet_default = format_manager.get_format("PARQUET_DEFAULT")
        assert parquet_default is not None
        assert parquet_default.format_type == "PARQUET"

    def test_list_stages(self) -> None:
        """Test listing all stages."""
        stage_manager = self.executor._ingestion_handler.stage_manager

        # Create some stages
        stage_manager.create_stage("stage1", "USER")
        stage_manager.create_stage("stage2", "EXTERNAL", "s3://bucket/path")

        # List stages
        stages = stage_manager.list_stages()

        # Should have at least the stages we created
        stage_names = [stage.name for stage in stages]
        assert "stage1" in stage_names
        assert "stage2" in stage_names

    def test_list_file_formats(self) -> None:
        """Test listing all file formats."""
        format_manager = self.executor._ingestion_handler.format_manager

        # Create a custom format
        format_manager.create_format("custom_csv", "CSV", {"field_delimiter": ";"})

        # List formats
        formats = format_manager.list_formats()

        # Should have defaults plus our custom format
        format_names = [fmt.name for fmt in formats]
        assert "CSV_DEFAULT" in format_names
        assert "JSON_DEFAULT" in format_names
        assert "PARQUET_DEFAULT" in format_names
        assert "custom_csv" in format_names

    def test_stage_validation(self) -> None:
        """Test stage access validation."""
        stage_manager = self.executor._ingestion_handler.stage_manager

        # Test valid stage reference (directory exists)
        file_url = f"file://{self.test_data_dir}"
        stage_manager.create_stage("valid_stage", "EXTERNAL", file_url)
        assert stage_manager.validate_stage_access("@valid_stage/")

        # Test invalid stage reference
        assert not stage_manager.validate_stage_access("@nonexistent_stage/file.csv")

    def test_complex_copy_scenario(self) -> None:
        """Test a complex COPY INTO scenario with multiple files and formats."""
        # Create multiple CSV files with different formats
        pipe_delimited_data = "id|name|email\n1|Alice|alice@test.com\n2|Bob|bob@test.com"
        pipe_file = self.test_data_dir / "pipe_delimited.csv"
        pipe_file.write_text(pipe_delimited_data)

        # Create table
        create_table_sql = """
        CREATE TABLE complex_test (
            id INTEGER,
            name VARCHAR(100),
            email VARCHAR(255)
        )
        """
        result = self.executor.execute_snowflake_sql(create_table_sql)
        assert result.success

        # Create stage
        file_url = f"file://{self.test_data_dir}"
        stage_sql = f"CREATE STAGE complex_stage URL = '{file_url}'"
        result = self.executor.execute_snowflake_sql(stage_sql)
        assert result.success

        # Create custom format for pipe-delimited data
        format_sql = "CREATE FILE FORMAT pipe_format TYPE = 'CSV' FIELD_DELIMITER = '|' SKIP_HEADER = 1"
        result = self.executor.execute_snowflake_sql(format_sql)
        assert result.success

        # Copy pipe-delimited data
        copy_sql = "COPY INTO complex_test FROM '@complex_stage/pipe_delimited.csv' FILE_FORMAT = 'pipe_format'"
        result = self.executor.execute_snowflake_sql(copy_sql)

        assert result.success
        assert result.data is not None
        assert result.data[0]["rows_loaded"] == 2

        # Verify data
        select_result = self.executor.execute_snowflake_sql("SELECT COUNT(*) as count FROM complex_test")
        assert select_result.success
        assert select_result.data is not None
        assert select_result.data[0]["count"] == 2


if __name__ == "__main__":
    unittest.main()
