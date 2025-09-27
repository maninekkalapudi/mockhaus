"""Real integration tests for PARQUET COPY INTO operations using actual PARQUET files."""

import shutil
import tempfile
from pathlib import Path

import duckdb
import pytest

from mockhaus.snowflake.copy_into import CopyIntoTranslator
from mockhaus.snowflake.file_formats import MockFileFormatManager
from mockhaus.snowflake.stages import MockStageManager


class TestParquetCopyIntoReal:
    """Integration tests using real PARQUET files with different compression formats."""

    fixtures_dir: Path

    @classmethod
    def setup_class(cls) -> None:
        """Set up test fixtures path."""
        cls.fixtures_dir = Path(__file__).parent.parent / "fixtures" / "parquet"
        if not cls.fixtures_dir.exists():
            pytest.skip("PARQUET test fixtures not found. Run generate_test_files.py first.")

    def setup_method(self) -> None:
        """Set up test environment for each test."""
        self.conn = duckdb.connect(":memory:")
        self.stage_manager = MockStageManager(self.conn)
        self.format_manager = MockFileFormatManager(self.conn)
        self.translator = CopyIntoTranslator(self.stage_manager, self.format_manager)

    def test_basic_parquet_snappy_compression(self) -> None:
        """Test loading basic PARQUET file with Snappy compression."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Copy test file to temp directory
            source_file = self.fixtures_dir / "basic_snappy.parquet"
            dest_file = Path(tmpdir) / "data.parquet"
            shutil.copy2(source_file, dest_file)

            # Create stage pointing to temp directory using EXTERNAL type for file:// URLs
            self.stage_manager.create_stage("test_stage", stage_type="EXTERNAL", url=f"file://{Path(tmpdir).as_posix()}")

            # Create target table with correct schema
            self.conn.execute("""
                CREATE TABLE employees (
                    id INTEGER,
                    name VARCHAR,
                    age INTEGER,
                    salary DOUBLE,
                    is_active BOOLEAN,
                    department VARCHAR
                )
            """)

            # Create PARQUET format with Snappy compression
            self.format_manager.create_format("SNAPPY_PARQUET", "PARQUET", {"COMPRESSION": "SNAPPY"})

            # Execute COPY INTO operation
            sql = "COPY INTO employees FROM '@test_stage/data.parquet' FILE_FORMAT = SNAPPY_PARQUET"
            result = self.translator.execute_copy_operation(sql, self.conn)

            # Verify operation succeeded
            assert result["success"] is True, f"Copy operation failed: {result.get('errors', [])}"
            assert result["rows_loaded"] == 5

            # Verify data was loaded correctly
            rows = self.conn.execute("SELECT * FROM employees ORDER BY id").fetchall()
            assert len(rows) == 5

            # Verify specific data
            first_row = rows[0]
            assert first_row[0] == 1  # id
            assert first_row[1] == "Alice"  # name
            assert first_row[2] == 25  # age
            assert first_row[3] == 50000.0  # salary
            assert first_row[4] is True  # is_active
            assert first_row[5] == "Engineering"  # department

    def test_all_compression_formats(self) -> None:
        """Test loading PARQUET files with all supported compression formats."""
        compression_formats = ["none", "snappy", "gzip", "brotli", "lz4", "zstd"]

        for compression in compression_formats:
            with tempfile.TemporaryDirectory() as tmpdir:
                # Copy test file to temp directory
                source_file = self.fixtures_dir / f"basic_{compression}.parquet"
                dest_file = Path(tmpdir) / "data.parquet"
                shutil.copy2(source_file, dest_file)

                # Create stage
                self.stage_manager.create_stage(f"stage_{compression}", stage_type="EXTERNAL", url=f"file://{Path(tmpdir).as_posix()}")

                # Create target table (drop if exists from previous iteration)
                self.conn.execute(f"DROP TABLE IF EXISTS employees_{compression}")
                self.conn.execute(f"""
                    CREATE TABLE employees_{compression} (
                        id INTEGER,
                        name VARCHAR,
                        age INTEGER,
                        salary DOUBLE,
                        is_active BOOLEAN,
                        department VARCHAR
                    )
                """)

                # Map compression names to DuckDB format
                compression_mapping = {"none": "NONE", "snappy": "SNAPPY", "gzip": "GZIP", "brotli": "BROTLI", "lz4": "LZ4", "zstd": "ZSTD"}

                # Create format with appropriate compression
                format_name = f"PARQUET_{compression.upper()}"
                self.format_manager.create_format(format_name, "PARQUET", {"COMPRESSION": compression_mapping[compression]})

                # Execute COPY INTO
                sql = f"COPY INTO employees_{compression} FROM '@stage_{compression}/data.parquet' FILE_FORMAT = {format_name}"
                result = self.translator.execute_copy_operation(sql, self.conn)

                # Verify operation succeeded
                assert result["success"] is True, f"Copy operation failed for {compression}: {result.get('errors', [])}"
                assert result["rows_loaded"] == 5, f"Expected 5 rows for {compression}, got {result['rows_loaded']}"

                # Verify data integrity
                count_result = self.conn.execute(f"SELECT COUNT(*) FROM employees_{compression}").fetchone()
                assert count_result is not None
                count = count_result[0]
                assert count == 5, f"Expected 5 rows in table for {compression}, got {count}"

    def test_parquet_with_nulls(self) -> None:
        """Test loading PARQUET file containing NULL values."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Copy test file with NULLs
            source_file = self.fixtures_dir / "with_nulls_snappy.parquet"
            dest_file = Path(tmpdir) / "data.parquet"
            shutil.copy2(source_file, dest_file)

            # Create stage
            self.stage_manager.create_stage("null_stage", stage_type="EXTERNAL", url=f"file://{Path(tmpdir).as_posix()}")

            # Create target table
            self.conn.execute("""
                CREATE TABLE employees_with_nulls (
                    id INTEGER,
                    name VARCHAR,
                    age INTEGER,
                    salary DOUBLE,
                    is_active BOOLEAN
                )
            """)

            # Execute COPY INTO with default PARQUET format
            sql = "COPY INTO employees_with_nulls FROM '@null_stage/data.parquet' FILE_FORMAT = PARQUET_DEFAULT"
            result = self.translator.execute_copy_operation(sql, self.conn)

            # Verify operation succeeded
            assert result["success"] is True, f"Copy operation failed: {result.get('errors', [])}"
            assert result["rows_loaded"] == 5

            # Verify NULL handling
            rows = self.conn.execute("SELECT * FROM employees_with_nulls ORDER BY id NULLS LAST").fetchall()

            # Check that NULLs are properly loaded
            null_id_row = [row for row in rows if row[0] is None][0]
            assert null_id_row[0] is None  # NULL id
            assert null_id_row[1] == "Charlie"  # name should be preserved

            # Check other NULL values
            null_name_row = [row for row in rows if row[1] is None][0]
            assert null_name_row[1] is None  # NULL name

    def test_parquet_binary_as_text(self) -> None:
        """Test BINARY_AS_TEXT option with PARQUET files containing binary data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Copy test file with binary data
            source_file = self.fixtures_dir / "with_binary_snappy.parquet"
            dest_file = Path(tmpdir) / "data.parquet"
            shutil.copy2(source_file, dest_file)

            # Create stage
            self.stage_manager.create_stage("binary_stage", stage_type="EXTERNAL", url=f"file://{Path(tmpdir).as_posix()}")

            # Create target table
            self.conn.execute("""
                CREATE TABLE binary_test (
                    id INTEGER,
                    name VARCHAR,
                    binary_data VARCHAR,
                    text_data VARCHAR
                )
            """)

            # Create PARQUET format with BINARY_AS_TEXT enabled
            self.format_manager.create_format("BINARY_AS_TEXT_FORMAT", "PARQUET", {"BINARY_AS_TEXT": True})

            # Execute COPY INTO
            sql = "COPY INTO binary_test FROM '@binary_stage/data.parquet' FILE_FORMAT = BINARY_AS_TEXT_FORMAT"
            result = self.translator.execute_copy_operation(sql, self.conn)

            # Verify operation succeeded
            assert result["success"] is True, f"Copy operation failed: {result.get('errors', [])}"
            assert result["rows_loaded"] == 3

            # Verify data was loaded
            rows = self.conn.execute("SELECT * FROM binary_test ORDER BY id").fetchall()
            assert len(rows) == 3

            # Verify binary data is handled (content depends on DuckDB's binary handling)
            first_row = rows[0]
            assert first_row[0] == 1
            assert first_row[1] == "Alice"
            # Binary data should be converted to string representation
            assert first_row[2] is not None  # binary_data column
            assert first_row[3] == "text1"  # text_data column

    def test_inline_parquet_format_specification(self) -> None:
        """Test COPY INTO with inline PARQUET format specification."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Copy test file
            source_file = self.fixtures_dir / "basic_zstd.parquet"
            dest_file = Path(tmpdir) / "data.parquet"
            shutil.copy2(source_file, dest_file)

            # Create stage
            self.stage_manager.create_stage("inline_stage", stage_type="EXTERNAL", url=f"file://{Path(tmpdir).as_posix()}")

            # Create target table
            self.conn.execute("""
                CREATE TABLE inline_test (
                    id INTEGER,
                    name VARCHAR,
                    age INTEGER,
                    salary DOUBLE,
                    is_active BOOLEAN,
                    department VARCHAR
                )
            """)

            # Execute COPY INTO with inline format
            sql = """
                COPY INTO inline_test
                FROM '@inline_stage/data.parquet'
                FILE_FORMAT = (TYPE = 'PARQUET' COMPRESSION = 'ZSTD' BINARY_AS_TEXT = TRUE)
            """
            result = self.translator.execute_copy_operation(sql, self.conn)

            # Verify operation succeeded
            assert result["success"] is True, f"Copy operation failed: {result.get('errors', [])}"
            assert result["rows_loaded"] == 5

            # Verify data integrity
            count_result = self.conn.execute("SELECT COUNT(*) FROM inline_test").fetchone()
            assert count_result is not None
            count = count_result[0]
            assert count == 5

    def test_unsupported_parquet_options_graceful_handling(self) -> None:
        """Test that unsupported PARQUET options are handled gracefully but don't prevent data loading."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Copy test file
            source_file = self.fixtures_dir / "basic_snappy.parquet"
            dest_file = Path(tmpdir) / "data.parquet"
            shutil.copy2(source_file, dest_file)

            # Create stage
            self.stage_manager.create_stage("unsupported_stage", stage_type="EXTERNAL", url=f"file://{Path(tmpdir).as_posix()}")

            # Create target table
            self.conn.execute("""
                CREATE TABLE unsupported_test (
                    id INTEGER,
                    name VARCHAR,
                    age INTEGER,
                    salary DOUBLE,
                    is_active BOOLEAN,
                    department VARCHAR
                )
            """)

            # Create format with unsupported options
            self.format_manager.create_format(
                "UNSUPPORTED_OPTIONS_FORMAT",
                "PARQUET",
                {
                    "COMPRESSION": "SNAPPY",  # Supported
                    "BINARY_AS_TEXT": True,  # Supported
                    "NULL_IF": ["", "NULL"],  # Unsupported but should be ignored
                    "TRIM_SPACE": False,  # Unsupported but should be ignored
                },
            )

            # Execute COPY INTO - should succeed despite unsupported options
            sql = "COPY INTO unsupported_test FROM '@unsupported_stage/data.parquet' FILE_FORMAT = UNSUPPORTED_OPTIONS_FORMAT"
            result = self.translator.execute_copy_operation(sql, self.conn)

            # Verify operation succeeded despite unsupported options
            assert result["success"] is True, f"Copy operation should succeed despite unsupported options: {result.get('errors', [])}"
            assert result["rows_loaded"] == 5

            # Verify data was loaded correctly
            count_result = self.conn.execute("SELECT COUNT(*) FROM unsupported_test").fetchone()
            assert count_result is not None
            count = count_result[0]
            assert count == 5

    def test_lzo_compression_fallback(self) -> None:
        """Test that LZO compression falls back to Snappy gracefully."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Use Snappy file but specify LZO compression (should fallback)
            source_file = self.fixtures_dir / "basic_snappy.parquet"
            dest_file = Path(tmpdir) / "data.parquet"
            shutil.copy2(source_file, dest_file)

            # Create stage
            self.stage_manager.create_stage("lzo_stage", stage_type="EXTERNAL", url=f"file://{Path(tmpdir).as_posix()}")

            # Create target table
            self.conn.execute("""
                CREATE TABLE lzo_test (
                    id INTEGER,
                    name VARCHAR,
                    age INTEGER,
                    salary DOUBLE,
                    is_active BOOLEAN,
                    department VARCHAR
                )
            """)

            # Create format with LZO compression (unsupported)
            self.format_manager.create_format(
                "LZO_FORMAT",
                "PARQUET",
                {"COMPRESSION": "LZO"},  # Should fallback to Snappy
            )

            # Execute COPY INTO - should work with fallback
            sql = "COPY INTO lzo_test FROM '@lzo_stage/data.parquet' FILE_FORMAT = LZO_FORMAT"
            result = self.translator.execute_copy_operation(sql, self.conn)

            # Verify operation succeeded with fallback
            assert result["success"] is True, f"Copy operation should succeed with LZO fallback: {result.get('errors', [])}"
            assert result["rows_loaded"] == 5

            # Verify the format mapping used snappy fallback
            lzo_format = self.format_manager.get_format("LZO_FORMAT")
            assert lzo_format is not None
            options = self.format_manager.map_to_duckdb_options(lzo_format)
            assert options["COMPRESSION"] == "snappy", "LZO should fallback to snappy"

    def test_parquet_format_auto_detection(self) -> None:
        """Test that PARQUET format with AUTO compression works correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Copy test file (any compression should work with AUTO)
            source_file = self.fixtures_dir / "basic_gzip.parquet"
            dest_file = Path(tmpdir) / "data.parquet"
            shutil.copy2(source_file, dest_file)

            # Create stage
            self.stage_manager.create_stage("auto_stage", stage_type="EXTERNAL", url=f"file://{Path(tmpdir).as_posix()}")

            # Create target table
            self.conn.execute("""
                CREATE TABLE auto_test (
                    id INTEGER,
                    name VARCHAR,
                    age INTEGER,
                    salary DOUBLE,
                    is_active BOOLEAN,
                    department VARCHAR
                )
            """)

            # Use default PARQUET format (AUTO compression)
            sql = "COPY INTO auto_test FROM '@auto_stage/data.parquet' FILE_FORMAT = PARQUET_DEFAULT"
            result = self.translator.execute_copy_operation(sql, self.conn)

            # Verify operation succeeded
            assert result["success"] is True, f"Copy operation failed with AUTO compression: {result.get('errors', [])}"
            assert result["rows_loaded"] == 5

            # Verify data integrity
            rows = self.conn.execute("SELECT COUNT(*) FROM auto_test").fetchone()
            assert rows is not None
            assert rows[0] == 5
