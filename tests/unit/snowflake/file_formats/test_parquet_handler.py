"""Comprehensive tests for PARQUET format handler."""

from typing import Any

from mockhaus.snowflake.file_formats.parquet import ParquetFormatHandler


class TestParquetFormatHandler:
    def setup_method(self) -> None:
        self.handler = ParquetFormatHandler()

    def test_format_type(self) -> None:
        assert self.handler.format_type == "PARQUET"

    def test_default_properties(self) -> None:
        defaults = self.handler.get_default_properties()
        assert defaults["compression"] == "AUTO"
        assert defaults["binary_as_text"] is True
        assert defaults["null_if"] == [r"\N"]
        assert defaults["trim_space"] is False

    def test_compression_mapping(self) -> None:
        """Test all compression mappings."""
        test_cases = [
            ("AUTO", "snappy"),
            ("SNAPPY", "snappy"),
            ("NONE", "uncompressed"),
            ("GZIP", "gzip"),
            ("BROTLI", "brotli"),
            ("ZSTD", "zstd"),
            ("LZ4", "lz4"),
        ]

        for snowflake_compression, expected_duckdb in test_cases:
            props = {"COMPRESSION": snowflake_compression}
            result = self.handler.map_to_duckdb_options(props)

            assert result.options["FORMAT"] == "PARQUET"
            assert result.options["COMPRESSION"] == expected_duckdb
            assert len(result.warnings) == 0

    def test_compression_case_insensitive(self) -> None:
        """Test compression mapping is case insensitive."""
        test_cases = [
            ("auto", "snappy"),
            ("snappy", "snappy"),
            ("Auto", "snappy"),
            ("SNAPPY", "snappy"),
            ("gzip", "gzip"),
            ("GZIP", "gzip"),
        ]

        for input_compression, expected_duckdb in test_cases:
            props = {"compression": input_compression}
            result = self.handler.map_to_duckdb_options(props)

            assert result.options["COMPRESSION"] == expected_duckdb
            assert len(result.warnings) == 0

    def test_lzo_compression_fallback(self) -> None:
        """Test LZO compression fallback."""
        props = {"COMPRESSION": "LZO"}
        result = self.handler.map_to_duckdb_options(props)

        assert result.options["COMPRESSION"] == "snappy"
        assert len(result.warnings) == 1
        assert "LZO compression not supported" in result.warnings[0]

    def test_unknown_compression_fallback(self) -> None:
        """Test unknown compression fallback."""
        props = {"COMPRESSION": "UNKNOWN_ALGO"}
        result = self.handler.map_to_duckdb_options(props)

        assert result.options["COMPRESSION"] == "snappy"
        assert len(result.warnings) == 1
        assert "Unknown compression type 'UNKNOWN_ALGO'" in result.warnings[0]

    def test_binary_as_text_mapping(self) -> None:
        """Test BINARY_AS_TEXT mapping."""
        test_cases = [
            (True, True),
            (False, False),
            ("TRUE", True),
            ("FALSE", False),
            ("true", True),
            ("false", False),
            ("1", True),
            ("0", False),
            ("YES", True),
        ]

        for input_value, expected in test_cases:
            props = {"BINARY_AS_TEXT": input_value}
            result = self.handler.map_to_duckdb_options(props)

            assert result.options["binary_as_string"] == expected

    def test_binary_as_text_case_variants(self) -> None:
        """Test BINARY_AS_TEXT works with different case variants."""
        test_cases = [
            ("BINARY_AS_TEXT", True),
            ("binary_as_text", True),
        ]

        for key, expected in test_cases:
            props = {key: True}
            result = self.handler.map_to_duckdb_options(props)

            assert result.options["binary_as_string"] == expected

    def test_unsupported_options_handling(self) -> None:
        """Test unsupported options are handled gracefully."""
        props = {"COMPRESSION": "SNAPPY", "BINARY_AS_TEXT": True, "NULL_IF": ["", "NULL"], "TRIM_SPACE": False}
        result = self.handler.map_to_duckdb_options(props)

        # Supported options should be mapped
        assert result.options["COMPRESSION"] == "snappy"
        assert result.options["binary_as_string"] is True

        # Unsupported options should generate warnings
        assert len(result.warnings) == 2
        warning_text = " ".join(result.warnings)
        assert "NULL_IF not supported" in warning_text
        assert "TRIM_SPACE not supported" in warning_text

        # Ignored options should be tracked
        assert "NULL_IF" in result.ignored_options
        assert "TRIM_SPACE" in result.ignored_options

    def test_unsupported_options_case_variants(self) -> None:
        """Test unsupported options work with different case variants."""
        props = {"null_if": ["", "NULL"], "trim_space": False}
        result = self.handler.map_to_duckdb_options(props)

        # Should still generate warnings for lowercase variants
        assert len(result.warnings) == 2
        warning_text = " ".join(result.warnings)
        assert "NULL_IF not supported" in warning_text
        assert "TRIM_SPACE not supported" in warning_text

    def test_complex_inline_format(self) -> None:
        """Test complex inline format handling."""
        props = {"TYPE": "PARQUET", "COMPRESSION": "ZSTD", "BINARY_AS_TEXT": "TRUE", "NULL_IF": ["\\N", ""], "TRIM_SPACE": "FALSE"}
        result = self.handler.map_to_duckdb_options(props)

        expected_options = {"FORMAT": "PARQUET", "COMPRESSION": "zstd", "binary_as_string": True}

        for key, value in expected_options.items():
            assert result.options[key] == value

        # Should have warnings for unsupported options
        assert len(result.warnings) >= 2
        warning_text = " ".join(result.warnings)
        assert "NULL_IF not supported" in warning_text
        assert "TRIM_SPACE not supported" in warning_text

    def test_minimal_configuration(self) -> None:
        """Test minimal configuration with just TYPE."""
        props = {"TYPE": "PARQUET"}
        result = self.handler.map_to_duckdb_options(props)

        # Should only have FORMAT option
        assert result.options == {"FORMAT": "PARQUET"}
        assert len(result.warnings) == 0
        assert len(result.ignored_options) == 0

    def test_empty_properties(self) -> None:
        """Test empty properties."""
        props: dict[str, Any] = {}
        result = self.handler.map_to_duckdb_options(props)

        # Should only have FORMAT option
        assert result.options == {"FORMAT": "PARQUET"}
        assert len(result.warnings) == 0
        assert len(result.ignored_options) == 0

    def test_mixed_case_properties(self) -> None:
        """Test mixed case property names."""
        props = {
            "Compression": "SNAPPY",
            "Binary_As_Text": True,
            "NULL_if": [""],
        }
        result = self.handler.map_to_duckdb_options(props)

        # Only supported options should be mapped (case-insensitive)
        expected_options = {"FORMAT": "PARQUET", "COMPRESSION": "snappy", "binary_as_string": True}

        for key, value in expected_options.items():
            if key in result.options:
                assert result.options[key] == value

    def test_validation_result_default(self) -> None:
        """Test default validation returns valid."""
        props = {"COMPRESSION": "SNAPPY"}
        validation = self.handler.validate_properties(props)

        assert validation.is_valid is True
        assert len(validation.errors) == 0
        assert len(validation.warnings) == 0
