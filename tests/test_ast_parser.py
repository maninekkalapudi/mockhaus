"""Tests for AST-based Snowflake SQL parsing."""

import unittest

from mockhaus.snowflake.ast_parser import SnowflakeASTParser


class TestASTParser(unittest.TestCase):
    """Test AST-based parsing of Snowflake statements."""

    def setUp(self) -> None:
        """Set up test environment."""
        self.parser = SnowflakeASTParser()

    def test_parse_create_stage_simple(self) -> None:
        """Test parsing simple CREATE STAGE statement."""
        sql = "CREATE STAGE my_stage"
        result = self.parser.parse_create_stage(sql)

        assert result["error"] is None
        assert result["stage_name"] == "my_stage"
        assert result["stage_type"] == "USER"
        assert result["url"] is None
        assert result["properties"] == {}

    def test_parse_create_stage_with_url(self) -> None:
        """Test parsing CREATE STAGE with URL."""
        sql = "CREATE STAGE my_external_stage URL = 's3://my-bucket/path'"
        result = self.parser.parse_create_stage(sql)

        assert result["error"] is None
        assert result["stage_name"] == "my_external_stage"
        assert result["stage_type"] == "EXTERNAL"
        assert result["url"] == "s3://my-bucket/path"
        assert result["properties"]["URL"] == "s3://my-bucket/path"

    def test_parse_create_stage_with_file_url(self) -> None:
        """Test parsing CREATE STAGE with file URL."""
        sql = "CREATE STAGE local_stage URL = 'file:///tmp/data'"
        result = self.parser.parse_create_stage(sql)

        assert result["error"] is None
        assert result["stage_name"] == "local_stage"
        assert result["stage_type"] == "EXTERNAL"
        assert result["url"] == "file:///tmp/data"

    def test_parse_create_stage_quoted_identifier(self) -> None:
        """Test parsing CREATE STAGE with quoted identifier."""
        sql = "CREATE STAGE \"My Stage\" URL = 's3://bucket/'"
        result = self.parser.parse_create_stage(sql)

        assert result["error"] is None
        assert result["stage_name"] == "My Stage"
        assert result["stage_type"] == "EXTERNAL"
        assert result["url"] == "s3://bucket/"

    def test_parse_create_stage_invalid_syntax(self) -> None:
        """Test parsing invalid CREATE STAGE syntax."""
        sql = "CREATE STAGE"
        result = self.parser.parse_create_stage(sql)

        assert result["error"] is not None
        assert "Failed to parse" in result["error"]

    def test_parse_create_stage_not_stage_statement(self) -> None:
        """Test parsing non-STAGE CREATE statement."""
        sql = "CREATE TABLE my_table (id INT)"
        result = self.parser.parse_create_stage(sql)

        assert result["error"] == "Not a CREATE STAGE statement"

    def test_parse_drop_stage_simple(self) -> None:
        """Test parsing simple DROP STAGE statement."""
        sql = "DROP STAGE my_stage"
        result = self.parser.parse_drop_stage(sql)

        assert result["error"] is None
        assert result["stage_name"] == "my_stage"
        assert result["if_exists"] is False

    def test_parse_drop_stage_if_exists(self) -> None:
        """Test parsing DROP STAGE IF EXISTS statement."""
        sql = "DROP STAGE IF EXISTS my_stage"
        result = self.parser.parse_drop_stage(sql)

        assert result["error"] is None
        assert result["stage_name"] == "my_stage"
        assert result["if_exists"] is True

    def test_parse_drop_stage_quoted_identifier(self) -> None:
        """Test parsing DROP STAGE with quoted identifier."""
        sql = 'DROP STAGE "My Stage"'
        result = self.parser.parse_drop_stage(sql)

        assert result["error"] is None
        assert result["stage_name"] == "My Stage"
        assert result["if_exists"] is False

    def test_parse_drop_stage_not_stage_statement(self) -> None:
        """Test parsing non-STAGE DROP statement."""
        sql = "DROP TABLE my_table"
        result = self.parser.parse_drop_stage(sql)

        assert result["error"] == "Not a DROP STAGE statement"

    def test_parse_create_file_format_simple_csv(self) -> None:
        """Test parsing simple CREATE FILE FORMAT for CSV."""
        sql = "CREATE FILE FORMAT my_csv_format TYPE = 'CSV'"
        result = self.parser.parse_create_file_format(sql)

        assert result["error"] is None
        assert result["format_name"] == "my_csv_format"
        assert result["format_type"] == "CSV"
        assert result["properties"] == {}

    def test_parse_create_file_format_csv_with_options(self) -> None:
        """Test parsing CREATE FILE FORMAT with CSV options."""
        sql = "CREATE FILE FORMAT my_csv_format TYPE = 'CSV' FIELD_DELIMITER = '|' SKIP_HEADER = 1"
        result = self.parser.parse_create_file_format(sql)

        assert result["error"] is None
        assert result["format_name"] == "my_csv_format"
        assert result["format_type"] == "CSV"
        assert result["properties"]["field_delimiter"] == "|"
        assert result["properties"]["skip_header"] == 1

    def test_parse_create_file_format_csv_with_quotes(self) -> None:
        """Test parsing CREATE FILE FORMAT with quote character."""
        sql = "CREATE FILE FORMAT my_csv_format TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '\"'"
        result = self.parser.parse_create_file_format(sql)

        assert result["error"] is None
        assert result["format_name"] == "my_csv_format"
        assert result["format_type"] == "CSV"
        assert result["properties"]["field_optionally_enclosed_by"] == '"'

    def test_parse_create_file_format_json(self) -> None:
        """Test parsing CREATE FILE FORMAT for JSON."""
        sql = "CREATE FILE FORMAT my_json_format TYPE = 'JSON'"
        result = self.parser.parse_create_file_format(sql)

        assert result["error"] is None
        assert result["format_name"] == "my_json_format"
        assert result["format_type"] == "JSON"
        assert result["properties"] == {}

    def test_parse_create_file_format_parquet(self) -> None:
        """Test parsing CREATE FILE FORMAT for Parquet."""
        sql = "CREATE FILE FORMAT my_parquet_format TYPE = 'PARQUET' COMPRESSION = 'SNAPPY'"
        result = self.parser.parse_create_file_format(sql)

        assert result["error"] is None
        assert result["format_name"] == "my_parquet_format"
        assert result["format_type"] == "PARQUET"
        assert result["properties"]["compression"] == "SNAPPY"

    def test_parse_create_file_format_with_record_delimiter(self) -> None:
        """Test parsing CREATE FILE FORMAT with record delimiter."""
        sql = "CREATE FILE FORMAT my_csv_format TYPE = 'CSV' RECORD_DELIMITER = '\\r\\n'"
        result = self.parser.parse_create_file_format(sql)

        assert result["error"] is None
        assert result["format_name"] == "my_csv_format"
        assert result["format_type"] == "CSV"
        assert result["properties"]["record_delimiter"] == "\r\n"

    def test_parse_create_file_format_with_date_formats(self) -> None:
        """Test parsing CREATE FILE FORMAT with date/time formats."""
        sql = "CREATE FILE FORMAT my_csv_format TYPE = 'CSV' DATE_FORMAT = 'YYYY-MM-DD' TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
        result = self.parser.parse_create_file_format(sql)

        assert result["error"] is None
        assert result["format_name"] == "my_csv_format"
        assert result["format_type"] == "CSV"
        assert result["properties"]["date_format"] == "YYYY-MM-DD"
        assert result["properties"]["timestamp_format"] == "YYYY-MM-DD HH24:MI:SS"

    def test_parse_create_file_format_quoted_identifier(self) -> None:
        """Test parsing CREATE FILE FORMAT with quoted identifier."""
        sql = "CREATE FILE FORMAT \"My CSV Format\" TYPE = 'CSV'"
        result = self.parser.parse_create_file_format(sql)

        assert result["error"] is None
        assert result["format_name"] == "My CSV Format"
        assert result["format_type"] == "CSV"

    def test_parse_create_file_format_default_type(self) -> None:
        """Test parsing CREATE FILE FORMAT without explicit TYPE (defaults to CSV)."""
        sql = "CREATE FILE FORMAT my_format"
        result = self.parser.parse_create_file_format(sql)

        assert result["error"] is None
        assert result["format_name"] == "my_format"
        assert result["format_type"] == "CSV"  # Default type

    def test_parse_create_file_format_invalid_syntax(self) -> None:
        """Test parsing invalid CREATE FILE FORMAT syntax."""
        sql = "CREATE FILE FORMAT"
        result = self.parser.parse_create_file_format(sql)

        assert result["error"] is not None
        assert "Failed to parse" in result["error"]

    def test_parse_create_file_format_not_file_format_statement(self) -> None:
        """Test parsing non-FILE FORMAT CREATE statement."""
        sql = "CREATE STAGE my_stage"
        result = self.parser.parse_create_file_format(sql)

        assert result["error"] == "Not a CREATE FILE FORMAT statement"

    def test_parse_drop_file_format_simple(self) -> None:
        """Test parsing simple DROP FILE FORMAT statement."""
        sql = "DROP FILE FORMAT my_format"
        result = self.parser.parse_drop_file_format(sql)

        assert result["error"] is None
        assert result["format_name"] == "my_format"
        assert result["if_exists"] is False

    def test_parse_drop_file_format_if_exists(self) -> None:
        """Test parsing DROP FILE FORMAT IF EXISTS statement."""
        sql = "DROP FILE FORMAT IF EXISTS my_format"
        result = self.parser.parse_drop_file_format(sql)

        assert result["error"] is None
        assert result["format_name"] == "my_format"
        assert result["if_exists"] is True

    def test_parse_drop_file_format_quoted_identifier(self) -> None:
        """Test parsing DROP FILE FORMAT with quoted identifier."""
        sql = 'DROP FILE FORMAT "My Format"'
        result = self.parser.parse_drop_file_format(sql)

        assert result["error"] is None
        assert result["format_name"] == "My Format"
        assert result["if_exists"] is False

    def test_parse_drop_file_format_not_file_format_statement(self) -> None:
        """Test parsing non-FILE FORMAT DROP statement."""
        sql = "DROP STAGE my_stage"
        result = self.parser.parse_drop_file_format(sql)

        assert result["error"] == "Not a DROP FILE FORMAT statement"

    def test_parse_copy_into_with_named_format(self) -> None:
        """Test parsing COPY INTO with named file format."""
        sql = "COPY INTO test_customers FROM '@test_data_stage/test.csv' FILE_FORMAT = 'csv_with_header'"
        result = self.parser.parse_copy_into(sql)

        assert result["error"] is None
        assert result["table_name"] == "test_customers"
        assert result["stage_reference"] == "@test_data_stage/test.csv"
        assert result["file_format_name"] == "csv_with_header"
        assert result["inline_format"] is None

    def test_parse_copy_into_with_inline_format(self) -> None:
        """Test parsing COPY INTO with inline format specification."""
        sql = "COPY INTO test_customers2 FROM '@test_data_stage2/test.csv' FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)"
        result = self.parser.parse_copy_into(sql)

        assert result["error"] is None
        assert result["table_name"] == "test_customers2"
        assert result["stage_reference"] == "@test_data_stage2/test.csv"
        assert result["file_format_name"] is None
        assert result["inline_format"] == "TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1"
        assert result["inline_format_options"]["TYPE"] == "CSV"
        assert result["inline_format_options"]["field_delimiter"] == ","
        assert result["inline_format_options"]["skip_header"] == 1

    def test_parse_copy_into_with_user_stage(self) -> None:
        """Test parsing COPY INTO with user stage reference."""
        sql = "COPY INTO test_user_customers FROM '@~/user_test.csv' FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)"
        result = self.parser.parse_copy_into(sql)

        assert result["error"] is None
        assert result["table_name"] == "test_user_customers"
        assert result["stage_reference"] == "@~/user_test.csv"
        assert result["inline_format"] is not None

    def test_parse_copy_into_with_options(self) -> None:
        """Test parsing COPY INTO with additional options."""
        sql = "COPY INTO test_table FROM '@stage/file.csv' FILE_FORMAT = 'csv_format' ON_ERROR = 'CONTINUE' FORCE = TRUE"
        result = self.parser.parse_copy_into(sql)

        assert result["error"] is None
        assert result["table_name"] == "test_table"
        assert result["stage_reference"] == "@stage/file.csv"
        assert result["file_format_name"] == "csv_format"
        assert result["options"]["on_error"] == "CONTINUE"
        assert result["options"]["force"] is True

    def test_parse_copy_into_with_pattern(self) -> None:
        """Test parsing COPY INTO with PATTERN option."""
        sql = "COPY INTO test_table FROM '@stage/' FILE_FORMAT = 'csv_format' PATTERN = '*.csv'"
        result = self.parser.parse_copy_into(sql)

        assert result["error"] is None
        assert result["table_name"] == "test_table"
        assert result["stage_reference"] == "@stage/"
        assert result["options"]["pattern"] == "*.csv"

    def test_parse_copy_into_complex_inline_format(self) -> None:
        """Test parsing COPY INTO with complex inline format."""
        sql = ("COPY INTO table FROM '@stage/file.csv' FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '|' "
               "FIELD_OPTIONALLY_ENCLOSED_BY = '\"' RECORD_DELIMITER = '\\r\\n')")
        result = self.parser.parse_copy_into(sql)

        assert result["error"] is None
        assert result["table_name"] == "table"
        assert result["inline_format_options"]["TYPE"] == "CSV"
        assert result["inline_format_options"]["field_delimiter"] == "|"
        assert result["inline_format_options"]["field_optionally_enclosed_by"] == '"'
        assert result["inline_format_options"]["record_delimiter"] == "\\r\\n"

    def test_parse_copy_into_quoted_stage_reference(self) -> None:
        """Test parsing COPY INTO with quoted stage reference."""
        sql = "COPY INTO test_table FROM '@\"my stage\"/test.csv' FILE_FORMAT = 'csv_format'"
        result = self.parser.parse_copy_into(sql)

        assert result["error"] is None
        assert result["table_name"] == "test_table"
        assert result["stage_reference"] == '@"my stage"/test.csv'
        assert result["file_format_name"] == "csv_format"

    def test_parse_copy_into_invalid_syntax(self) -> None:
        """Test parsing invalid COPY INTO syntax."""
        sql = "COPY INTO"
        result = self.parser.parse_copy_into(sql)

        assert result["error"] is not None
        assert "Invalid COPY INTO syntax" in result["error"]

    def test_parse_copy_into_not_copy_statement(self) -> None:
        """Test parsing non-COPY INTO statement."""
        sql = "SELECT * FROM table"
        result = self.parser.parse_copy_into(sql)

        assert result["error"] == "Not a COPY INTO statement"


if __name__ == "__main__":
    unittest.main()
