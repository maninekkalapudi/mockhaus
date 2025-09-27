"""Integration tests for enhanced CSV COPY INTO functionality."""

import csv
import os
import tempfile
from pathlib import Path

import duckdb

from mockhaus.snowflake.copy_into import CopyIntoTranslator
from mockhaus.snowflake.file_formats.manager import MockFileFormatManager
from mockhaus.snowflake.stages import MockStageManager


class TestEnhancedCSVCopyInto:
    """Test enhanced CSV functionality with COPY INTO operations."""

    def setup_method(self):
        """Set up test fixtures."""
        self.conn = duckdb.connect(":memory:")
        self.stage_manager = MockStageManager(self.conn)
        self.format_manager = MockFileFormatManager(self.conn)
        self.translator = CopyIntoTranslator(self.stage_manager, self.format_manager)

        # Create a temporary directory for test files
        self.temp_dir = Path(tempfile.mkdtemp())

    def teardown_method(self):
        """Clean up test fixtures."""
        self.conn.close()
        # Clean up temp directory
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _create_test_csv(self, filename: str, data: list, encoding: str = "utf-8") -> str:
        """Create a test CSV file with given data."""
        file_path = os.path.join(self.temp_dir, filename)

        with open(file_path, "w", newline="", encoding=encoding) as f:
            writer = csv.writer(f)
            writer.writerows(data)

        return file_path

    def test_enhanced_field_delimiter_copy_into(self):
        """Test COPY INTO with enhanced field delimiter support."""
        # Create test CSV with pipe delimiter
        test_data = [
            ["id", "name", "email"],
            ["1", "Alice", "alice@test.com"],
            ["2", "Bob", "bob@test.com"],
        ]
        csv_path = self._create_test_csv("pipe_delim.csv", test_data)

        # Update CSV content to use pipe delimiter
        with open(csv_path, "w", encoding="utf-8") as f:
            f.write("id|name|email\n")
            f.write("1|Alice|alice@test.com\n")
            f.write("2|Bob|bob@test.com\n")

        # Create stage pointing to temp directory
        self.stage_manager.create_stage("test_stage", stage_type="EXTERNAL", url=f"file://{self.temp_dir.as_posix()}")

        # Create table
        self.conn.execute("CREATE TABLE test_table (id INT, name VARCHAR, email VARCHAR)")

        # Test COPY INTO with pipe delimiter
        sql = """
        COPY INTO test_table FROM '@test_stage/pipe_delim.csv'
        FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '|' SKIP_HEADER = 1)
        """

        result = self.translator.execute_copy_operation(sql, self.conn)

        assert result["success"] is True
        assert result["rows_loaded"] == 2

        # Verify data
        rows = self.conn.execute("SELECT * FROM test_table ORDER BY id").fetchall()
        assert len(rows) == 2
        assert rows[0] == (1, "Alice", "alice@test.com")
        assert rows[1] == (2, "Bob", "bob@test.com")

    def test_record_delimiter_copy_into(self):
        """Test COPY INTO with different record delimiters."""
        # Create CSV with CRLF line endings
        csv_path = os.path.join(self.temp_dir, "crlf_delim.csv")
        with open(csv_path, "wb") as f:
            f.write(b"id,name\r\n1,Alice\r\n2,Bob\r\n")

        # Create stage
        self.stage_manager.create_stage("test_stage", stage_type="EXTERNAL", url=f"file://{self.temp_dir.as_posix()}")

        # Create table
        self.conn.execute("CREATE TABLE test_table (id INT, name VARCHAR)")

        # Test COPY INTO with CRLF record delimiter
        sql = """
        COPY INTO test_table FROM '@test_stage/crlf_delim.csv'
        FILE_FORMAT = (TYPE = 'CSV' RECORD_DELIMITER = '\\r\\n' SKIP_HEADER = 1)
        """

        result = self.translator.execute_copy_operation(sql, self.conn)

        assert result["success"] is True
        assert result["rows_loaded"] == 2

    def test_compression_fallback_copy_into(self):
        """Test COPY INTO with unsupported compression fallback."""
        # Create test CSV
        test_data = [
            ["id", "name"],
            ["1", "Alice"],
            ["2", "Bob"],
        ]
        self._create_test_csv("test.csv", test_data)

        # Create stage
        self.stage_manager.create_stage("test_stage", stage_type="EXTERNAL", url=f"file://{self.temp_dir.as_posix()}")

        # Create table
        self.conn.execute("CREATE TABLE test_table (id INT, name VARCHAR)")

        # Test COPY INTO with unsupported compression (should fallback to auto)
        sql = """
        COPY INTO test_table FROM '@test_stage/test.csv'
        FILE_FORMAT = (TYPE = 'CSV' COMPRESSION = 'BROTLI' SKIP_HEADER = 1)
        """

        result = self.translator.execute_copy_operation(sql, self.conn)

        # Should succeed with fallback
        assert result["success"] is True
        assert result["rows_loaded"] == 2

    def test_encoding_mapping_copy_into(self):
        """Test COPY INTO with encoding mapping."""
        # Create test CSV with UTF-8 encoding
        test_data = [
            ["id", "name"],
            ["1", "Cärlös"],  # Special characters
            ["2", "Bøb"],
        ]
        self._create_test_csv("utf8.csv", test_data, encoding="utf-8")

        # Create stage
        self.stage_manager.create_stage("test_stage", stage_type="EXTERNAL", url=f"file://{self.temp_dir.as_posix()}")

        # Create table
        self.conn.execute("CREATE TABLE test_table (id INT, name VARCHAR)")

        # Test COPY INTO with UTF-8 encoding
        sql = """
        COPY INTO test_table FROM '@test_stage/utf8.csv'
        FILE_FORMAT = (TYPE = 'CSV' ENCODING = 'UTF-8' SKIP_HEADER = 1)
        """

        result = self.translator.execute_copy_operation(sql, self.conn)

        assert result["success"] is True
        assert result["rows_loaded"] == 2

        # Verify special characters are preserved
        rows = self.conn.execute("SELECT name FROM test_table ORDER BY id").fetchall()
        assert rows[0][0] == "Cärlös"
        assert rows[1][0] == "Bøb"

    def test_null_if_first_value_copy_into(self):
        """Test COPY INTO with NULL_IF using first value only."""
        # Create CSV with various NULL representations
        csv_path = os.path.join(self.temp_dir, "nulls.csv")
        with open(csv_path, "w", encoding="utf-8") as f:
            f.write("id,name,email\n")
            f.write("1,Alice,alice@test.com\n")
            f.write("2,,bob@test.com\n")  # Empty field (first NULL_IF)
            f.write("3,NULL,charlie@test.com\n")  # NULL string (not first)
            f.write("4,N/A,david@test.com\n")  # N/A string (not first)

        # Create stage
        self.stage_manager.create_stage("test_stage", stage_type="EXTERNAL", url=f"file://{self.temp_dir.as_posix()}")

        # Create table
        self.conn.execute("CREATE TABLE test_table (id INT, name VARCHAR, email VARCHAR)")

        # Test COPY INTO with multiple NULL_IF values (should use first)
        sql = """
        COPY INTO test_table FROM '@test_stage/nulls.csv'
        FILE_FORMAT = (TYPE = 'CSV' NULL_IF = ('', 'NULL', 'N/A') SKIP_HEADER = 1)
        """

        result = self.translator.execute_copy_operation(sql, self.conn)

        assert result["success"] is True
        assert result["rows_loaded"] == 4

        # Verify NULL handling (only empty string should be treated as NULL)
        rows = self.conn.execute("SELECT id, name FROM test_table ORDER BY id").fetchall()
        assert rows[0] == (1, "Alice")
        assert rows[1] == (2, None)  # Empty string -> NULL
        assert rows[2] == (3, "NULL")  # 'NULL' string preserved (not first in list)
        assert rows[3] == (4, "N/A")  # 'N/A' string preserved (not first in list)

    def test_error_handling_mapping_copy_into(self):
        """Test ERROR_ON_COLUMN_COUNT_MISMATCH mapping."""
        # Create CSV with mismatched columns
        csv_path = os.path.join(self.temp_dir, "mismatch.csv")
        with open(csv_path, "w", encoding="utf-8") as f:
            f.write("id,name\n")
            f.write("1,Alice\n")
            f.write("2,Bob,extra_column\n")  # Extra column
            f.write("3\n")  # Missing column

        # Create stage
        self.stage_manager.create_stage("test_stage", stage_type="EXTERNAL", url=f"file://{self.temp_dir.as_posix()}")

        # Create table
        self.conn.execute("CREATE TABLE test_table (id INT, name VARCHAR)")

        # Test COPY INTO with ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE (ignore errors)
        sql = """
        COPY INTO test_table FROM '@test_stage/mismatch.csv'
        FILE_FORMAT = (TYPE = 'CSV' ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE SKIP_HEADER = 1)
        """

        result = self.translator.execute_copy_operation(sql, self.conn)

        # Should succeed by ignoring problematic rows
        assert result["success"] is True

    def test_complex_csv_format_copy_into(self):
        """Test COPY INTO with complex CSV format combining multiple features."""
        # Create complex CSV with multiple features
        csv_path = os.path.join(self.temp_dir, "complex.csv")
        with open(csv_path, "wb") as f:
            # Use CRLF and pipe delimiter with quoted fields
            f.write(b'id|"name"|"email"\r\n')
            f.write(b'1|"Alice Smith"|"alice@test.com"\r\n')
            f.write(b'2|""|"bob@test.com"\r\n')  # Empty quoted field
            f.write(b'3|"Charlie"|"charlie@test.com"\r\n')

        # Create stage
        self.stage_manager.create_stage("test_stage", stage_type="EXTERNAL", url=f"file://{self.temp_dir.as_posix()}")

        # Create table
        self.conn.execute("CREATE TABLE test_table (id INT, name VARCHAR, email VARCHAR)")

        # Test COPY INTO with complex format
        sql = """
        COPY INTO test_table FROM '@test_stage/complex.csv'
        FILE_FORMAT = (
            TYPE = 'CSV'
            FIELD_DELIMITER = '|'
            RECORD_DELIMITER = '\\r\\n'
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1
            NULL_IF = ('', 'NULL')
            COMPRESSION = 'NONE'
            ENCODING = 'UTF-8'
        )
        """

        result = self.translator.execute_copy_operation(sql, self.conn)

        assert result["success"] is True
        assert result["rows_loaded"] == 3

        # Verify data with proper handling of quoted fields and NULLs
        rows = self.conn.execute("SELECT * FROM test_table ORDER BY id").fetchall()
        assert len(rows) == 3
        assert rows[0] == (1, "Alice Smith", "alice@test.com")
        assert rows[1] == (2, None, "bob@test.com")  # Empty quoted field -> NULL
        assert rows[2] == (3, "Charlie", "charlie@test.com")

    def test_unsupported_options_with_warnings(self):
        """Test that unsupported options generate warnings but don't fail."""
        # Create simple test CSV
        test_data = [
            ["id", "name"],
            ["1", "Alice"],
        ]
        self._create_test_csv("simple.csv", test_data)

        # Create stage
        self.stage_manager.create_stage("test_stage", stage_type="EXTERNAL", url=f"file://{self.temp_dir.as_posix()}")

        # Create table
        self.conn.execute("CREATE TABLE test_table (id INT, name VARCHAR)")

        # Test COPY INTO with unsupported options
        sql = """
        COPY INTO test_table FROM '@test_stage/simple.csv'
        FILE_FORMAT = (
            TYPE = 'CSV'
            SKIP_HEADER = 1
            BINARY_FORMAT = 'HEX'
            SKIP_BLANK_LINES = TRUE
            VALIDATE_UTF8 = FALSE
        )
        """

        result = self.translator.execute_copy_operation(sql, self.conn)

        # Should succeed despite unsupported options
        assert result["success"] is True
        assert result["rows_loaded"] == 1
