"""
This module provides a specialized handler for JSON file formats.

It defines the `JSONFormatHandler` class, which is responsible for mapping
Snowflake's JSON file format properties to their equivalents in DuckDB. As DuckDB's
JSON loading is less configurable than Snowflake's, this handler primarily
identifies and warns about unsupported options.
"""

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
        """
        Maps Snowflake JSON properties to DuckDB `COPY` options.

        Since DuckDB's JSON loader has fewer options than Snowflake's, this method
        mainly identifies unsupported properties and generates warnings.

        Args:
            properties: A dictionary of Snowflake JSON format properties.

        Returns:
            A `FormatMappingResult` with the mapped DuckDB options and any warnings.
        """
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
