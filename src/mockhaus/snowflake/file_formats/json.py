"""JSON format handler extracted from existing implementation."""

from typing import Any

from .base import BaseFormatHandler, FormatMappingResult


class JSONFormatHandler(BaseFormatHandler):
    """Handler for JSON format mappings."""

    @property
    def format_type(self) -> str:
        return "JSON"

    def get_default_properties(self) -> dict[str, Any]:
        """Get default JSON properties."""
        return {"compression": "AUTO", "date_format": "AUTO", "time_format": "AUTO", "timestamp_format": "AUTO"}

    def map_to_duckdb_options(self, properties: dict[str, Any]) -> FormatMappingResult:
        """Map JSON properties to DuckDB options."""
        # DuckDB JSON format is relatively simple
        # Most Snowflake JSON options don't have direct equivalents
        options: dict[str, Any] = {"FORMAT": "JSON"}
        warnings: list[str] = []
        ignored_options: list[str] = []

        # JSON format in DuckDB doesn't support many of the Snowflake options
        # We'll accept them but warn that they're not supported
        unsupported_json_options = [
            "compression",
            "COMPRESSION",
            "date_format",
            "DATE_FORMAT",
            "time_format",
            "TIME_FORMAT",
            "timestamp_format",
            "TIMESTAMP_FORMAT",
        ]

        for option in unsupported_json_options:
            if option in properties:
                warnings.append(f"{option.upper()} not fully supported in DuckDB JSON format, using defaults")
                ignored_options.append(option.upper())

        # Log any warnings
        if warnings:
            self._log_warnings(warnings)

        return FormatMappingResult(options=options, warnings=warnings, ignored_options=ignored_options)
