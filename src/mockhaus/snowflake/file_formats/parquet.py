"""PARQUET format handler with comprehensive Snowflake option mapping."""

from typing import Any

from .base import BaseFormatHandler, FormatMappingResult


class ParquetFormatHandler(BaseFormatHandler):
    """Handler for PARQUET format mappings."""

    @property
    def format_type(self) -> str:
        return "PARQUET"

    def get_default_properties(self) -> dict[str, Any]:
        """Get default PARQUET properties."""
        return {
            "compression": "AUTO",
            "binary_as_text": True,
            "null_if": [r"\N"],
            "trim_space": False,
        }

    def map_to_duckdb_options(self, properties: dict[str, Any]) -> FormatMappingResult:
        """Map PARQUET properties to DuckDB options."""
        options: dict[str, Any] = {"FORMAT": "PARQUET"}
        warnings: list[str] = []
        ignored_options: list[str] = []

        # Handle compression mapping
        self._map_compression(properties, options, warnings)

        # Handle binary_as_text mapping
        self._map_binary_as_text(properties, options)

        # Handle unsupported options gracefully
        self._handle_unsupported_options(properties, warnings, ignored_options)

        # Log warnings
        if warnings:
            self._log_warnings(warnings)

        return FormatMappingResult(options=options, warnings=warnings, ignored_options=ignored_options)

    def _map_compression(self, props: dict[str, Any], options: dict[str, Any], warnings: list[str]) -> None:
        """Map compression settings."""
        # Check both uppercase and lowercase variants
        compression = props.get("compression") or props.get("COMPRESSION")
        if not compression:
            return

        compression_mapping = {
            "AUTO": "snappy",  # Snowflake AUTO → DuckDB default
            "SNAPPY": "snappy",  # Direct mapping
            "NONE": "uncompressed",  # Snowflake NONE → DuckDB uncompressed
            "GZIP": "gzip",  # Direct mapping
            "BROTLI": "brotli",  # Direct mapping
            "ZSTD": "zstd",  # Direct mapping
            "LZ4": "lz4",  # Direct mapping
        }

        compression_upper = str(compression).upper()
        if compression_upper == "LZO":
            # LZO not supported in DuckDB
            warnings.append("LZO compression not supported in DuckDB, using snappy instead")
            options["COMPRESSION"] = "snappy"
        elif compression_upper in compression_mapping:
            options["COMPRESSION"] = compression_mapping[compression_upper]
        else:
            warnings.append(f"Unknown compression type '{compression}', using snappy")
            options["COMPRESSION"] = "snappy"

    def _map_binary_as_text(self, props: dict[str, Any], options: dict[str, Any]) -> None:
        """Map BINARY_AS_TEXT to binary_as_string."""
        # Check both uppercase and lowercase variants
        binary_as_text = props.get("BINARY_AS_TEXT")
        if binary_as_text is None:
            binary_as_text = props.get("binary_as_text")

        if binary_as_text is not None:
            # Convert to boolean if string
            if isinstance(binary_as_text, str):
                binary_as_text = binary_as_text.upper() in ("TRUE", "1", "YES")
            options["binary_as_string"] = bool(binary_as_text)

    def _handle_unsupported_options(self, props: dict[str, Any], warnings: list[str], ignored_options: list[str]) -> None:
        """Handle unsupported options gracefully."""
        unsupported_options = {"NULL_IF": "null_if", "TRIM_SPACE": "trim_space"}

        for snowflake_opt, snake_case_opt in unsupported_options.items():
            if snowflake_opt in props or snake_case_opt in props:
                warnings.append(f"{snowflake_opt} not supported in DuckDB PARQUET format, ignoring option")
                ignored_options.append(snowflake_opt)
