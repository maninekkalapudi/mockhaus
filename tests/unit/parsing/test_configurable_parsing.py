"""Tests for configurable parsing (regex vs AST)."""

import unittest

from mockhaus import MockhausExecutor


class TestConfigurableParsing(unittest.TestCase):
    """Test that both regex and AST parsing work correctly."""

    def setUp(self) -> None:
        """Set up test environment."""
        # Create two executors: one with regex, one with AST
        self.regex_executor = MockhausExecutor(use_ast_parser=False)
        self.regex_executor.connect()

        self.ast_executor = MockhausExecutor(use_ast_parser=True)
        self.ast_executor.connect()

    def tearDown(self) -> None:
        """Clean up after tests."""
        self.regex_executor.disconnect()
        self.ast_executor.disconnect()

    def test_create_stage_simple_both_parsers(self) -> None:
        """Test simple CREATE STAGE with both parsers."""
        sql = "CREATE STAGE test_stage"

        # Test with regex parser
        result_regex = self.regex_executor.execute_snowflake_sql(sql)
        assert result_regex.success
        assert result_regex.translated_sql == "-- Created stage test_stage"

        # Test with AST parser
        result_ast = self.ast_executor.execute_snowflake_sql(sql)
        assert result_ast.success
        assert result_ast.translated_sql == "-- Created stage test_stage"

    def test_create_stage_with_url_both_parsers(self) -> None:
        """Test CREATE STAGE with URL with both parsers."""
        sql = "CREATE STAGE external_stage URL = 's3://my-bucket/path'"

        # Test with regex parser
        result_regex = self.regex_executor.execute_snowflake_sql(sql)
        assert result_regex.success
        assert result_regex.translated_sql == "-- Created stage external_stage"

        # Verify stage was created with correct properties
        stage = self.regex_executor._ingestion_handler.stage_manager.get_stage("external_stage")
        assert stage is not None
        assert stage.stage_type == "EXTERNAL"
        assert stage.url == "s3://my-bucket/path"

        # Test with AST parser
        result_ast = self.ast_executor.execute_snowflake_sql(sql)
        assert result_ast.success
        assert result_ast.translated_sql == "-- Created stage external_stage"

        # Verify stage was created with correct properties
        stage = self.ast_executor._ingestion_handler.stage_manager.get_stage("external_stage")
        assert stage is not None
        assert stage.stage_type == "EXTERNAL"
        assert stage.url == "s3://my-bucket/path"

    def test_drop_stage_both_parsers(self) -> None:
        """Test DROP STAGE with both parsers."""
        # Create stages first
        self.regex_executor.execute_snowflake_sql("CREATE STAGE stage_to_drop_regex")
        self.ast_executor.execute_snowflake_sql("CREATE STAGE stage_to_drop_ast")

        # Drop with regex parser
        result_regex = self.regex_executor.execute_snowflake_sql("DROP STAGE stage_to_drop_regex")
        assert result_regex.success
        assert result_regex.translated_sql == "-- Dropped stage stage_to_drop_regex"

        # Drop with AST parser
        result_ast = self.ast_executor.execute_snowflake_sql("DROP STAGE stage_to_drop_ast")
        assert result_ast.success
        assert result_ast.translated_sql == "-- Dropped stage stage_to_drop_ast"

    def test_quoted_identifier_ast_only(self) -> None:
        """Test quoted identifiers work with AST parser (regex won't handle this)."""
        sql = "CREATE STAGE \"My Special Stage\" URL = 'file:///tmp/data'"

        # This should fail with regex parser (can't handle quoted identifiers)
        result_regex = self.regex_executor.execute_snowflake_sql(sql)
        assert not result_regex.success
        assert "Invalid CREATE STAGE syntax" in result_regex.error

        # This should work with AST parser
        result_ast = self.ast_executor.execute_snowflake_sql(sql)
        assert result_ast.success
        assert result_ast.translated_sql == "-- Created stage My Special Stage"

        # Verify stage was created
        stage = self.ast_executor._ingestion_handler.stage_manager.get_stage("My Special Stage")
        assert stage is not None
        assert stage.stage_type == "EXTERNAL"
        assert stage.url == "file:///tmp/data"

    def test_complex_url_both_parsers(self) -> None:
        """Test complex URLs with special characters."""
        sql = "CREATE STAGE azure_stage URL = 'azure://container.blob.core.windows.net/path/to/data'"

        # Both parsers should handle this
        result_regex = self.regex_executor.execute_snowflake_sql(sql)
        assert result_regex.success

        result_ast = self.ast_executor.execute_snowflake_sql(sql)
        assert result_ast.success

        # Verify URLs match
        stage_regex = self.regex_executor._ingestion_handler.stage_manager.get_stage("azure_stage")
        stage_ast = self.ast_executor._ingestion_handler.stage_manager.get_stage("azure_stage")

        assert stage_regex.url == stage_ast.url
        assert stage_regex.url == "azure://container.blob.core.windows.net/path/to/data"

    def test_create_file_format_simple_both_parsers(self) -> None:
        """Test simple CREATE FILE FORMAT with both parsers."""
        sql = "CREATE FILE FORMAT test_format TYPE = 'CSV'"

        # Test with regex parser
        result_regex = self.regex_executor.execute_snowflake_sql(sql)
        assert result_regex.success
        assert result_regex.translated_sql == "-- Created file format test_format"

        # Test with AST parser
        result_ast = self.ast_executor.execute_snowflake_sql(sql)
        assert result_ast.success
        assert result_ast.translated_sql == "-- Created file format test_format"

    def test_create_file_format_with_options_both_parsers(self) -> None:
        """Test CREATE FILE FORMAT with options with both parsers."""
        sql = "CREATE FILE FORMAT csv_pipe TYPE = 'CSV' FIELD_DELIMITER = '|' SKIP_HEADER = 1"

        # Test with regex parser
        result_regex = self.regex_executor.execute_snowflake_sql(sql)
        assert result_regex.success
        assert result_regex.translated_sql == "-- Created file format csv_pipe"

        # Verify format was created with correct properties
        format_obj = self.regex_executor._ingestion_handler.format_manager.get_format("csv_pipe")
        assert format_obj is not None
        assert format_obj.format_type == "CSV"
        assert format_obj.properties["field_delimiter"] == "|"
        assert format_obj.properties["skip_header"] == 1

        # Test with AST parser
        result_ast = self.ast_executor.execute_snowflake_sql(sql)
        assert result_ast.success
        assert result_ast.translated_sql == "-- Created file format csv_pipe"

        # Verify format was created with correct properties
        format_obj = self.ast_executor._ingestion_handler.format_manager.get_format("csv_pipe")
        assert format_obj is not None
        assert format_obj.format_type == "CSV"
        assert format_obj.properties["field_delimiter"] == "|"
        assert format_obj.properties["skip_header"] == 1

    def test_create_file_format_json_both_parsers(self) -> None:
        """Test CREATE FILE FORMAT for JSON with both parsers."""
        sql = "CREATE FILE FORMAT json_format TYPE = 'JSON'"

        # Test with regex parser
        result_regex = self.regex_executor.execute_snowflake_sql(sql)
        assert result_regex.success

        # Test with AST parser
        result_ast = self.ast_executor.execute_snowflake_sql(sql)
        assert result_ast.success

        # Verify both created the same format type
        format_regex = self.regex_executor._ingestion_handler.format_manager.get_format("json_format")
        format_ast = self.ast_executor._ingestion_handler.format_manager.get_format("json_format")

        assert format_regex.format_type == format_ast.format_type
        assert format_regex.format_type == "JSON"

    def test_drop_file_format_both_parsers(self) -> None:
        """Test DROP FILE FORMAT with both parsers."""
        # Create formats first
        self.regex_executor.execute_snowflake_sql("CREATE FILE FORMAT format_to_drop_regex TYPE = 'CSV'")
        self.ast_executor.execute_snowflake_sql("CREATE FILE FORMAT format_to_drop_ast TYPE = 'CSV'")

        # Drop with regex parser
        result_regex = self.regex_executor.execute_snowflake_sql("DROP FILE FORMAT format_to_drop_regex")
        assert result_regex.success
        assert result_regex.translated_sql == "-- Dropped file format format_to_drop_regex"

        # Drop with AST parser
        result_ast = self.ast_executor.execute_snowflake_sql("DROP FILE FORMAT format_to_drop_ast")
        assert result_ast.success
        assert result_ast.translated_sql == "-- Dropped file format format_to_drop_ast"

    def test_quoted_file_format_identifier_ast_only(self) -> None:
        """Test quoted identifiers work with AST parser for file formats (regex won't handle this)."""
        sql = "CREATE FILE FORMAT \"My Special Format\" TYPE = 'CSV'"

        # This should fail with regex parser (can't handle quoted identifiers)
        result_regex = self.regex_executor.execute_snowflake_sql(sql)
        assert not result_regex.success
        assert "Invalid CREATE FILE FORMAT syntax" in result_regex.error

        # This should work with AST parser
        result_ast = self.ast_executor.execute_snowflake_sql(sql)
        assert result_ast.success
        assert result_ast.translated_sql == "-- Created file format My Special Format"

        # Verify format was created
        format_obj = self.ast_executor._ingestion_handler.format_manager.get_format("My Special Format")
        assert format_obj is not None
        assert format_obj.format_type == "CSV"

    def test_file_format_complex_properties_both_parsers(self) -> None:
        """Test file format with complex properties."""
        sql = "CREATE FILE FORMAT complex_csv TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1"

        # Both parsers should handle this
        result_regex = self.regex_executor.execute_snowflake_sql(sql)
        assert result_regex.success

        result_ast = self.ast_executor.execute_snowflake_sql(sql)
        assert result_ast.success

        # Verify properties match
        format_regex = self.regex_executor._ingestion_handler.format_manager.get_format("complex_csv")
        format_ast = self.ast_executor._ingestion_handler.format_manager.get_format("complex_csv")

        assert format_regex.format_type == format_ast.format_type
        assert format_regex.properties["field_delimiter"] == format_ast.properties["field_delimiter"]
        assert format_regex.properties["skip_header"] == format_ast.properties["skip_header"]


if __name__ == "__main__":
    unittest.main()
