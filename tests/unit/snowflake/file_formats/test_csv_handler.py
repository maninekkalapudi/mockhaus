"""Unit tests for enhanced CSV format handler."""

from src.mockhaus.snowflake.file_formats.csv import CSVFormatHandler


class TestCSVFormatHandler:
    """Test enhanced CSV format handler functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.handler = CSVFormatHandler()

    def test_format_type(self):
        """Test format type property."""
        assert self.handler.format_type == "CSV"

    def test_default_properties(self):
        """Test default properties."""
        defaults = self.handler.get_default_properties()

        assert defaults["field_delimiter"] == ","
        assert defaults["record_delimiter"] == "\\n"
        assert defaults["skip_header"] == 0
        assert defaults["compression"] == "AUTO"
        assert defaults["encoding"] == "UTF-8"

    def test_basic_field_delimiter_mapping(self):
        """Test basic field delimiter mapping."""
        props = {"FIELD_DELIMITER": "|"}
        result = self.handler.map_to_duckdb_options(props)

        assert result.options["delimiter"] == "|"
        assert len(result.warnings) == 0

    def test_record_delimiter_mapping(self):
        """Test record delimiter mapping - standard delimiters use auto-detection."""
        standard_delimiters = ["\\n", "\\r\\n", "\\r", "\n", "\r\n", "\r"]

        for delim in standard_delimiters:
            props = {"RECORD_DELIMITER": delim}
            result = self.handler.map_to_duckdb_options(props)
            # Standard delimiters should not be mapped (let DuckDB auto-detect)
            assert "new_line" not in result.options
            assert len(result.warnings) == 0

    def test_unusual_record_delimiter_warning(self):
        """Test warning for unusual record delimiters."""
        props = {"RECORD_DELIMITER": "|||"}
        result = self.handler.map_to_duckdb_options(props)

        assert "new_line" not in result.options
        assert len(result.warnings) == 1
        assert "Unusual RECORD_DELIMITER" in result.warnings[0]
        assert "auto-detection" in result.warnings[0]

    def test_skip_header_mapping(self):
        """Test SKIP_HEADER to skip_rows and header mapping."""
        # Test with SKIP_HEADER = 1 (common case - just skip header row)
        props = {"SKIP_HEADER": 1}
        result = self.handler.map_to_duckdb_options(props)

        assert "skip" not in result.options  # No skip needed, header=true handles it
        assert result.options["header"] is True

        # Test with SKIP_HEADER = 2 (skip header + 1 additional row)
        props = {"SKIP_HEADER": 2}
        result = self.handler.map_to_duckdb_options(props)

        assert result.options["skip"] == 1  # Skip 1 additional row beyond header
        assert result.options["header"] is True

        # Test with zero skip count
        props = {"SKIP_HEADER": 0}
        result = self.handler.map_to_duckdb_options(props)

        assert "skip" not in result.options
        assert result.options["header"] is False

    def test_quote_character_mapping(self):
        """Test quote character mapping."""
        # Test valid quote characters
        for quote_char in ['"', "'"]:
            props = {"FIELD_OPTIONALLY_ENCLOSED_BY": quote_char}
            result = self.handler.map_to_duckdb_options(props)

            assert result.options["quote"] == quote_char

        # Test invalid quote character (should be ignored)
        props = {"FIELD_OPTIONALLY_ENCLOSED_BY": "x"}
        result = self.handler.map_to_duckdb_options(props)

        assert "quote" not in result.options

    def test_escape_character_mapping(self):
        """Test escape character mapping."""
        props = {"ESCAPE": "\\"}
        result = self.handler.map_to_duckdb_options(props)

        assert result.options["escape"] == "\\"

    def test_null_if_single_value(self):
        """Test NULL_IF with single value."""
        props = {"NULL_IF": "NULL"}
        result = self.handler.map_to_duckdb_options(props)

        assert result.options["nullstr"] == "NULL"
        assert len(result.warnings) == 0

    def test_null_if_list_first_value(self):
        """Test NULL_IF with list - should use first value."""
        props = {"NULL_IF": ["", "NULL", "N/A", "\\N"]}
        result = self.handler.map_to_duckdb_options(props)

        assert result.options["nullstr"] == ""
        assert len(result.warnings) == 1
        assert "only using first" in result.warnings[0]

    def test_datetime_format_mapping(self):
        """Test date and timestamp format mapping."""
        props = {
            "DATE_FORMAT": "YYYY-MM-DD",
            "TIMESTAMP_FORMAT": "YYYY-MM-DD HH24:MI:SS"
        }
        result = self.handler.map_to_duckdb_options(props)

        assert result.options["dateformat"] == "YYYY-MM-DD"
        assert result.options["timestampformat"] == "YYYY-MM-DD HH24:MI:SS"

    def test_datetime_format_auto_ignored(self):
        """Test that AUTO date/timestamp formats are ignored."""
        props = {
            "DATE_FORMAT": "AUTO",
            "TIMESTAMP_FORMAT": "AUTO"
        }
        result = self.handler.map_to_duckdb_options(props)

        assert "dateformat" not in result.options
        assert "timestampformat" not in result.options

    def test_compression_direct_mapping(self):
        """Test direct compression mapping."""
        test_cases = [
            ("AUTO", "auto"),
            ("NONE", "none"),
            ("GZIP", "gzip"),
        ]

        for snowflake_comp, expected_duckdb in test_cases:
            props = {"COMPRESSION": snowflake_comp}
            result = self.handler.map_to_duckdb_options(props)

            assert result.options["compression"] == expected_duckdb
            assert len(result.warnings) == 0

    def test_compression_fallback_with_warning(self):
        """Test compression fallback for unsupported types."""
        unsupported_types = ["BZ2", "BROTLI", "ZSTD", "DEFLATE", "RAW_DEFLATE"]

        for compression_type in unsupported_types:
            props = {"COMPRESSION": compression_type}
            result = self.handler.map_to_duckdb_options(props)

            assert result.options["compression"] == "auto"
            assert len(result.warnings) == 1
            assert compression_type in result.warnings[0]
            assert "not supported" in result.warnings[0]

    def test_compression_unknown_fallback(self):
        """Test unknown compression type fallback."""
        props = {"COMPRESSION": "UNKNOWN_TYPE"}
        result = self.handler.map_to_duckdb_options(props)

        assert result.options["compression"] == "auto"
        assert len(result.warnings) == 1
        assert "Unknown compression type" in result.warnings[0]

    def test_encoding_native_mapping(self):
        """Test native encoding mapping."""
        test_cases = [
            ("UTF-8", "UTF-8"),
            ("UTF-16", "UTF-16"),
            ("UTF-16BE", "UTF-16"),
            ("UTF-16LE", "UTF-16"),
            ("ISO-8859-1", "Latin-1"),
        ]

        for snowflake_encoding, expected_duckdb in test_cases:
            props = {"ENCODING": snowflake_encoding}
            result = self.handler.map_to_duckdb_options(props)

            assert result.options["encoding"] == expected_duckdb
            assert len(result.warnings) == 0

    def test_encoding_extension_fallback(self):
        """Test encoding fallback for extended encodings."""
        extended_encodings = ["ISO-8859-2", "WINDOWS-1252", "BIG5", "EUC-JP"]

        for encoding in extended_encodings:
            props = {"ENCODING": encoding}
            result = self.handler.map_to_duckdb_options(props)

            assert result.options["encoding"] == "UTF-8"
            assert len(result.warnings) == 1
            assert "encodings extension" in result.warnings[0]

    def test_error_handling_mapping(self):
        """Test ERROR_ON_COLUMN_COUNT_MISMATCH to ignore_errors mapping."""
        # Snowflake TRUE = error on mismatch, DuckDB ignore_errors = False
        props = {"ERROR_ON_COLUMN_COUNT_MISMATCH": True}
        result = self.handler.map_to_duckdb_options(props)

        assert result.options["ignore_errors"] is False

        # Snowflake FALSE = ignore mismatch, DuckDB ignore_errors = True
        props = {"ERROR_ON_COLUMN_COUNT_MISMATCH": False}
        result = self.handler.map_to_duckdb_options(props)

        assert result.options["ignore_errors"] is True

    def test_unsupported_options_handling(self):
        """Test graceful handling of unsupported options."""
        props = {
            "TYPE": "CSV",
            "FIELD_DELIMITER": ",",
            "BINARY_FORMAT": "HEX",              # Unsupported
            "SKIP_BLANK_LINES": True,            # Unsupported
            "VALIDATE_UTF8": False,              # Unsupported
            "ESCAPE_UNENCLOSED_FIELD": "\\",     # Unsupported
            "MULTI_LINE": True,                  # Unsupported (future feature)
        }
        result = self.handler.map_to_duckdb_options(props)

        # Core options should be mapped
        assert result.options["delimiter"] == ","

        # Unsupported options should generate warnings
        warning_text = " ".join(result.warnings)
        assert "BINARY_FORMAT" in warning_text
        assert "SKIP_BLANK_LINES" in warning_text
        assert "VALIDATE_UTF8" in warning_text
        assert "ESCAPE_UNENCLOSED_FIELD" in warning_text
        assert "MULTI_LINE" in warning_text

        # Should track ignored options
        assert "BINARY_FORMAT" in result.ignored_options
        assert "SKIP_BLANK_LINES" in result.ignored_options
        assert "VALIDATE_UTF8" in result.ignored_options
        assert "ESCAPE_UNENCLOSED_FIELD" in result.ignored_options
        assert "MULTI_LINE" in result.ignored_options

    def test_case_insensitive_properties(self):
        """Test that both uppercase and lowercase properties work."""
        # Test uppercase (Snowflake style)
        props_upper = {
            "FIELD_DELIMITER": "|",
            "SKIP_HEADER": 1,
            "COMPRESSION": "GZIP"
        }
        result_upper = self.handler.map_to_duckdb_options(props_upper)

        # Test lowercase (alternative style)
        props_lower = {
            "field_delimiter": "|",
            "skip_header": 1,
            "compression": "GZIP"
        }
        result_lower = self.handler.map_to_duckdb_options(props_lower)

        # Results should be identical
        assert result_upper.options["delimiter"] == result_lower.options["delimiter"]
        assert result_upper.options["header"] == result_lower.options["header"]  # Both should have header=true
        assert result_upper.options["compression"] == result_lower.options["compression"]

    def test_complex_csv_format_scenario(self):
        """Test a complex CSV format with multiple options."""
        props = {
            "TYPE": "CSV",
            "FIELD_DELIMITER": "|",
            "RECORD_DELIMITER": "\\r\\n",
            "SKIP_HEADER": 1,
            "FIELD_OPTIONALLY_ENCLOSED_BY": '"',
            "ESCAPE": "\\",
            "NULL_IF": ["", "NULL", "N/A"],
            "DATE_FORMAT": "YYYY-MM-DD",
            "TIMESTAMP_FORMAT": "YYYY-MM-DD HH24:MI:SS",
            "COMPRESSION": "GZIP",
            "ENCODING": "UTF-8",
            "ERROR_ON_COLUMN_COUNT_MISMATCH": False,
        }

        result = self.handler.map_to_duckdb_options(props)

        # Verify all mappings
        assert result.options["FORMAT"] == "CSV"
        assert result.options["delimiter"] == "|"
        assert "new_line" not in result.options  # Standard delimiters use auto-detection
        assert "skip" not in result.options  # SKIP_HEADER=1 just uses header=true
        assert result.options["header"] is True
        assert result.options["quote"] == '"'
        assert result.options["escape"] == "\\"
        assert result.options["nullstr"] == ""  # First value from NULL_IF
        assert result.options["dateformat"] == "YYYY-MM-DD"
        assert result.options["timestampformat"] == "YYYY-MM-DD HH24:MI:SS"
        assert result.options["compression"] == "gzip"
        assert result.options["encoding"] == "UTF-8"
        assert result.options["ignore_errors"] is True

        # Should have warning about multiple NULL_IF values
        assert len(result.warnings) == 1
        assert "only using first" in result.warnings[0]

    def test_empty_properties(self):
        """Test handling of empty properties dictionary."""
        props = {}
        result = self.handler.map_to_duckdb_options(props)

        # Should have FORMAT and default values applied
        assert result.options["FORMAT"] == "CSV"
        assert result.options["compression"] == "auto"  # Default AUTO -> auto
        assert result.options["encoding"] == "UTF-8"    # Default encoding
        assert result.options["header"] is False        # Default skip_header = 0
        assert len(result.warnings) == 0
        assert len(result.ignored_options) == 0
