"""
This module provides a specialized handler for CSV file formats.

It defines the `CSVFormatHandler` class, which is responsible for mapping
Snowflake's rich set of CSV file format properties to their corresponding
equivalents in DuckDB's `COPY` command options. It handles various options
like delimiters, headers, quoting, and compression.
"""

from typing import Any

from .base import BaseFormatHandler, FormatMappingResult


class CSVFormatHandler(BaseFormatHandler):
    """Handler for CSV format mappings with comprehensive Snowflake compatibility."""

    @property
    def format_type(self) -> str:
        """Returns the format type, which is 'CSV'."""
        return "CSV"

    def get_default_properties(self) -> dict[str, Any]:
        """Get default CSV properties."""
        return {
            "field_delimiter": ",",
            "record_delimiter": "\\n",
            "skip_header": 0,
            "field_optionally_enclosed_by": None,
            "escape": None,
            "null_if": [],
            "compression": "AUTO",
            "encoding": "UTF-8",
            "date_format": "AUTO",
            "time_format": "AUTO",
            "timestamp_format": "AUTO",
            "error_on_column_count_mismatch": True,
        }

    def map_to_duckdb_options(self, properties: dict[str, Any]) -> FormatMappingResult:
        """
        Maps Snowflake CSV properties to DuckDB `COPY` options.

        Args:
            properties: A dictionary of Snowflake CSV format properties.

        Returns:
            A `FormatMappingResult` with the mapped DuckDB options and any warnings.
        """
        options: dict[str, Any] = {"FORMAT": "CSV"}
        warnings: list[str] = []
        ignored_options: list[str] = []

        # Core mappings
        self._map_delimiters(properties, options, warnings)
        self._map_header_options(properties, options)
        self._map_quote_escape(properties, options)
        self._map_null_handling(properties, options, warnings)
        self._map_datetime_formats(properties, options)
        self._map_compression(properties, options, warnings)
        self._map_encoding(properties, options, warnings)
        self._map_error_handling(properties, options)
        self._handle_unsupported_options(properties, warnings, ignored_options)

        # Log any warnings
        if warnings:
            self._log_warnings(warnings)

        return FormatMappingResult(options=options, warnings=warnings, ignored_options=ignored_options)

    def _map_delimiters(self, props: dict, options: dict, warnings: list) -> None:
        """Map field and record delimiters."""
        # Field delimiter (direct mapping)
        field_delimiter = props.get("FIELD_DELIMITER") or props.get("field_delimiter")
        if field_delimiter:
            options["delimiter"] = field_delimiter

        # Record delimiter - DuckDB auto-detects line endings, so we typically don't need to specify
        # Only map if it's an unusual delimiter that DuckDB might not handle automatically
        record_delim = props.get("RECORD_DELIMITER") or props.get("record_delimiter")
        if record_delim:
            # Standard line endings - let DuckDB auto-detect (works better than specifying)
            standard_delimiters = {
                "\\n",
                "\\r\\n",
                "\\r",  # Escaped versions
                "\n",
                "\r\n",
                "\r",  # Literal versions
            }

            if record_delim not in standard_delimiters:
                # Only warn about truly unusual delimiters, but don't try to map them
                warnings.append(f"Unusual RECORD_DELIMITER '{record_delim}', DuckDB will use auto-detection")

    def _map_header_options(self, props: dict, options: dict) -> None:
        """Map header-related options."""
        skip_header = props.get("SKIP_HEADER") or props.get("skip_header", 0)

        if isinstance(skip_header, int | str):
            try:
                skip_count = int(skip_header)
                if skip_count == 1:
                    # Snowflake SKIP_HEADER = 1 means "treat first row as header and skip it"
                    # In DuckDB, this is just header = true (DuckDB automatically skips header)
                    options["header"] = True
                elif skip_count > 1:
                    # If skipping more than 1 row, we need both skip and header
                    options["skip"] = skip_count - 1  # Skip additional rows beyond header
                    options["header"] = True
                else:
                    options["header"] = False
            except (ValueError, TypeError):
                options["header"] = False

    def _map_quote_escape(self, props: dict, options: dict) -> None:
        """Map quote and escape characters."""
        # Quote character (direct mapping)
        quote_char = props.get("FIELD_OPTIONALLY_ENCLOSED_BY") or props.get("field_optionally_enclosed_by")
        if quote_char and quote_char in ['"', "'"]:
            options["quote"] = quote_char

        # Escape character (direct mapping)
        escape_char = props.get("ESCAPE") or props.get("escape")
        if escape_char:
            options["escape"] = escape_char

    def _map_null_handling(self, props: dict, options: dict, warnings: list) -> None:
        """Map NULL handling options."""
        null_if = props.get("NULL_IF") or props.get("null_if", [])

        if null_if:
            if isinstance(null_if, list) and len(null_if) > 0:
                # DuckDB accepts a single null string, use the first one
                options["nullstr"] = null_if[0]
                if len(null_if) > 1:
                    warnings.append(f"NULL_IF has {len(null_if)} values, only using first: '{null_if[0]}'")
            elif isinstance(null_if, str):
                options["nullstr"] = null_if

    def _map_datetime_formats(self, props: dict, options: dict) -> None:
        """Map date and time format options."""
        date_format = props.get("DATE_FORMAT") or props.get("date_format")
        if date_format and date_format != "AUTO":
            options["dateformat"] = date_format

        timestamp_format = props.get("TIMESTAMP_FORMAT") or props.get("timestamp_format")
        if timestamp_format and timestamp_format != "AUTO":
            options["timestampformat"] = timestamp_format

    def _map_compression(self, props: dict, options: dict, warnings: list) -> None:
        """Map compression options with fallback handling."""
        compression = props.get("COMPRESSION") or props.get("compression", "AUTO")
        if not compression:
            return

        compression_mapping = {
            "AUTO": "auto",
            "NONE": "none",
            "GZIP": "gzip",
            # Unsupported types map to auto with warning
            "BZ2": ("auto", "BZ2 compression not supported, using auto detection"),
            "BROTLI": ("auto", "BROTLI compression not supported, using auto detection"),
            "ZSTD": ("auto", "ZSTD compression not supported, using auto detection"),
            "DEFLATE": ("auto", "DEFLATE compression not supported, using auto detection"),
            "RAW_DEFLATE": ("auto", "RAW_DEFLATE compression not supported, using auto detection"),
        }

        compression_upper = compression.upper()
        mapping = compression_mapping.get(compression_upper)

        if isinstance(mapping, tuple):
            # Unsupported with fallback
            options["compression"] = mapping[0]
            warnings.append(mapping[1])
        elif mapping:
            # Direct mapping
            options["compression"] = mapping
        else:
            # Unknown compression
            warnings.append(f"Unknown compression type '{compression}', using auto detection")
            options["compression"] = "auto"

    def _map_encoding(self, props: dict, options: dict, warnings: list) -> None:
        """Map encoding options with native support detection."""
        encoding = props.get("ENCODING") or props.get("encoding", "UTF-8")
        if not encoding:
            return

        # Native DuckDB encodings (no extension required)
        native_encodings = {"UTF-8": "UTF-8", "UTF-16": "UTF-16", "UTF-16BE": "UTF-16", "UTF-16LE": "UTF-16", "ISO-8859-1": "Latin-1"}

        encoding_upper = encoding.upper()
        if encoding_upper in native_encodings:
            # Direct mapping for native encodings
            options["encoding"] = native_encodings[encoding_upper]
        else:
            # Extended encodings require DuckDB encodings extension
            # For now, fallback to UTF-8 with warning
            warnings.append(f"Encoding '{encoding}' requires DuckDB encodings extension, using UTF-8 fallback")
            options["encoding"] = "UTF-8"

    def _map_error_handling(self, props: dict, options: dict) -> None:
        """Map error handling options."""
        # ERROR_ON_COLUMN_COUNT_MISMATCH maps to ignore_errors (inverse logic)
        error_on_mismatch = props.get("ERROR_ON_COLUMN_COUNT_MISMATCH")
        if error_on_mismatch is not None:
            # Snowflake: True = error on mismatch, DuckDB: ignore_errors = True means ignore errors
            options["ignore_errors"] = not error_on_mismatch

    def _handle_unsupported_options(self, props: dict, warnings: list, ignored_options: list) -> None:
        """Handle options that are not supported in DuckDB."""
        unsupported_options = {
            "ESCAPE_UNENCLOSED_FIELD": "No DuckDB equivalent, using default escape behavior",
            "BINARY_FORMAT": "No DuckDB equivalent, DuckDB handles binary data automatically",
            "SKIP_BLANK_LINES": "No DuckDB equivalent, may add in future",
            "VALIDATE_UTF8": "No DuckDB equivalent, handled automatically by DuckDB",
            "FILE_EXTENSION": "Export-only feature, not relevant for COPY INTO",
            "MULTI_LINE": "Complex feature requiring preprocessing, not yet implemented",
            "TRIM_SPACE": "Requires preprocessing, not yet implemented",
            "EMPTY_FIELD_AS_NULL": "Requires preprocessing, not yet implemented",
            "PARSE_HEADER": "Requires preprocessing, not yet implemented",
        }

        for prop_name, reason in unsupported_options.items():
            if prop_name in props or prop_name.lower() in props:
                warnings.append(f"{prop_name}: {reason}")
                ignored_options.append(prop_name)
