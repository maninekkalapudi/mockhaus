"""CSV format handler extracted from existing implementation."""

from typing import Any

from .base import BaseFormatHandler, FormatMappingResult


class CSVFormatHandler(BaseFormatHandler):
    """Handler for CSV format mappings."""

    @property
    def format_type(self) -> str:
        return "CSV"

    def get_default_properties(self) -> dict[str, Any]:
        """Get default CSV properties."""
        return {
            "field_delimiter": ",",
            "record_delimiter": "\\n",
            "skip_header": 0,
            "field_optionally_enclosed_by": None,
            "null_if": [],
            "compression": "AUTO",
            "date_format": "AUTO",
            "time_format": "AUTO",
            "timestamp_format": "AUTO",
        }

    def map_to_duckdb_options(self, properties: dict[str, Any]) -> FormatMappingResult:
        """Map CSV properties to DuckDB options."""
        options: dict[str, Any] = {"FORMAT": "CSV"}
        warnings: list[str] = []
        ignored_options: list[str] = []

        # Field delimiter
        field_delimiter = properties.get("field_delimiter") or properties.get("FIELD_DELIMITER")
        if field_delimiter:
            options["DELIMITER"] = field_delimiter

        # Header
        skip_header = properties.get("skip_header") or properties.get("SKIP_HEADER", 0)
        if isinstance(skip_header, int | str):
            try:
                options["HEADER"] = int(skip_header) > 0
            except (ValueError, TypeError):
                options["HEADER"] = False

        # Quote character
        quote_char = properties.get("field_optionally_enclosed_by") or properties.get("FIELD_OPTIONALLY_ENCLOSED_BY")
        if quote_char and quote_char in ['"', "'"]:
            options["QUOTE"] = quote_char

        # Null values
        null_if = properties.get("null_if") or properties.get("NULL_IF", [])
        if null_if and isinstance(null_if, list) and len(null_if) > 0:
            # DuckDB accepts a single null string, use the first one
            options["NULL"] = null_if[0]

        # Date formats (DuckDB has limited support)
        date_format = properties.get("date_format") or properties.get("DATE_FORMAT")
        if date_format and date_format != "AUTO":
            options["DATEFORMAT"] = date_format

        timestamp_format = properties.get("timestamp_format") or properties.get("TIMESTAMP_FORMAT")
        if timestamp_format and timestamp_format != "AUTO":
            options["TIMESTAMPFORMAT"] = timestamp_format

        # Log any warnings
        if warnings:
            self._log_warnings(warnings)

        return FormatMappingResult(options=options, warnings=warnings, ignored_options=ignored_options)
