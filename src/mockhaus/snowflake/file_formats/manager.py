"""Refactored file format manager using modular handlers."""

import hashlib
import json
import re
from typing import Any

import duckdb

from .base import FileFormat
from .csv import CSVFormatHandler
from .json import JSONFormatHandler
from .parquet import ParquetFormatHandler
from .registry import format_registry

# Register format handlers
format_registry.register("CSV", CSVFormatHandler)
format_registry.register("JSON", JSONFormatHandler)
format_registry.register("PARQUET", ParquetFormatHandler)


class MockFileFormatManager:
    """Refactored file format manager using modular handlers."""

    def __init__(self, connection: duckdb.DuckDBPyConnection) -> None:
        """Initialize file format manager with DuckDB connection."""
        self.connection = connection
        self._create_system_tables()
        self._create_default_formats()

    def _create_system_tables(self) -> None:
        """Create system tables for file format metadata."""
        create_formats_table = """
        CREATE TABLE IF NOT EXISTS mockhaus_file_formats (
            name VARCHAR PRIMARY KEY,
            format_type VARCHAR NOT NULL,
            properties JSON,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        self.connection.execute(create_formats_table)

    def _create_default_formats(self) -> None:
        """Create default file formats using format handlers."""
        default_formats: list[dict[str, Any]] = [
            {
                "name": "CSV_DEFAULT",
                "format_type": "CSV",
                "properties": format_registry.get_handler("CSV").get_default_properties(),
            },
            {
                "name": "JSON_DEFAULT",
                "format_type": "JSON",
                "properties": format_registry.get_handler("JSON").get_default_properties(),
            },
            {
                "name": "PARQUET_DEFAULT",
                "format_type": "PARQUET",
                "properties": format_registry.get_handler("PARQUET").get_default_properties(),
            },
        ]

        for fmt in default_formats:
            name: str = fmt["name"]  # type: ignore[assignment]
            format_type: str = fmt["format_type"]  # type: ignore[assignment]
            properties: dict[str, Any] = fmt["properties"]  # type: ignore[assignment]
            if not self.get_format(name):
                self.create_format(name, format_type, properties)

    def map_to_duckdb_options(self, file_format: FileFormat) -> dict[str, Any]:
        """Map Snowflake file format properties to DuckDB COPY options."""
        try:
            handler = format_registry.get_handler(file_format.format_type)
            result = handler.map_to_duckdb_options(file_format.properties)

            # Store mapping result metadata for debugging
            if result.warnings or result.ignored_options:
                try:
                    from mockhaus.my_logging import debug_log

                    debug_log(f"Format mapping for {file_format.name}: warnings={result.warnings}, ignored={result.ignored_options}")
                except ImportError:
                    # Fallback if logging not available
                    pass

            return result.options

        except ValueError:
            # Fallback for unsupported formats
            return {"FORMAT": file_format.format_type}

    def create_format(self, name: str, format_type: str, properties: dict[str, Any] | None = None) -> FileFormat:
        """Create a new file format using format handlers."""
        if properties is None:
            properties = {}

        # Validate format type using registry
        try:
            handler = format_registry.get_handler(format_type)
        except ValueError as e:
            supported = format_registry.get_supported_formats()
            raise ValueError(f"Unsupported format type: {format_type}. Supported: {supported}") from e

        format_type = format_type.upper()

        # Get default properties from handler
        final_properties = handler.get_default_properties()
        final_properties.update(properties)

        # Validate properties
        validation_result = handler.validate_properties(final_properties)
        if not validation_result.is_valid:
            raise ValueError(f"Invalid properties: {validation_result.errors}")

        # Create format object
        file_format = FileFormat(name=name, format_type=format_type, properties=final_properties)

        # Store in system table
        self._store_format_metadata(file_format)

        return file_format

    def _store_format_metadata(self, file_format: FileFormat) -> None:
        """Store file format metadata in system table."""
        insert_sql = """
        INSERT OR REPLACE INTO mockhaus_file_formats
        (name, format_type, properties)
        VALUES (?, ?, ?)
        """
        self.connection.execute(insert_sql, [file_format.name, file_format.format_type, json.dumps(file_format.properties)])

    def get_format(self, name: str) -> FileFormat | None:
        """Get file format by name."""
        result = self.connection.execute("SELECT * FROM mockhaus_file_formats WHERE name = ?", [name]).fetchone()

        if not result:
            return None

        return FileFormat(name=result[0], format_type=result[1], properties=json.loads(result[2]) if result[2] else {}, created_at=result[3])

    def list_formats(self) -> list[FileFormat]:
        """List all file formats."""
        results = self.connection.execute("SELECT * FROM mockhaus_file_formats").fetchall()
        formats = []

        for result in results:
            formats.append(
                FileFormat(name=result[0], format_type=result[1], properties=json.loads(result[2]) if result[2] else {}, created_at=result[3])
            )

        return formats

    def drop_format(self, name: str) -> bool:
        """Drop a file format."""
        if not self.get_format(name):
            return False

        self.connection.execute("DELETE FROM mockhaus_file_formats WHERE name = ?", [name])
        return True

    def parse_inline_format(self, format_spec: str) -> dict[str, Any]:
        """
        Parse inline format specification from COPY INTO statement.

        Example: "TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1"
        """
        # This is a simplified parser for common format specifications
        # In a full implementation, you'd want a proper parser

        options = {}

        # Extract TYPE
        type_match = re.search(r"TYPE\s*=\s*['\"](\w+)['\"]", format_spec, re.IGNORECASE)
        if type_match:
            format_type = type_match.group(1).upper()
            options["TYPE"] = format_type

        # Extract common CSV options
        delimiter_match = re.search(r"FIELD_DELIMITER\s*=\s*['\"](.)['\"]", format_spec, re.IGNORECASE)
        if delimiter_match:
            options["field_delimiter"] = delimiter_match.group(1)

        header_match = re.search(r"SKIP_HEADER\s*=\s*(\d+)", format_spec, re.IGNORECASE)
        if header_match:
            options["skip_header"] = int(header_match.group(1))

        quote_match = re.search(r"FIELD_OPTIONALLY_ENCLOSED_BY\s*=\s*['\"](.)['\"]", format_spec, re.IGNORECASE)
        if quote_match:
            options["field_optionally_enclosed_by"] = quote_match.group(1)

        # Extract PARQUET options
        compression_match = re.search(r"COMPRESSION\s*=\s*['\"](\w+)['\"]", format_spec, re.IGNORECASE)
        if compression_match:
            options["COMPRESSION"] = compression_match.group(1)

        binary_as_text_match = re.search(r"BINARY_AS_TEXT\s*=\s*(\w+)", format_spec, re.IGNORECASE)
        if binary_as_text_match:
            options["BINARY_AS_TEXT"] = binary_as_text_match.group(1)

        return options

    def create_temp_format_from_inline(self, inline_spec: str | dict[str, Any]) -> FileFormat:
        """Create a temporary file format from inline specification."""
        options = inline_spec.copy() if isinstance(inline_spec, dict) else self.parse_inline_format(inline_spec)

        format_type = options.pop("TYPE", options.pop("type", "CSV"))

        # Create temporary format name
        spec_str = str(inline_spec)
        temp_name = f"TEMP_{hashlib.md5(spec_str.encode()).hexdigest()[:8]}"

        return FileFormat(name=temp_name, format_type=format_type, properties=options)
