"""
This module manages Snowflake file formats for data ingestion in Mockhaus.

It provides functionality to create, manage, and drop file formats, storing their
metadata in a DuckDB system table. It also handles the mapping of Snowflake
file format properties to their equivalents in DuckDB's `COPY` command.
"""

import json
from dataclasses import dataclass, field
from typing import Any

import duckdb


@dataclass
class FileFormat:
    """Represents a Snowflake file format and its properties.

    Attributes:
        name: The name of the file format.
        format_type: The type of the file format (e.g., 'CSV', 'JSON').
        properties: A dictionary of properties for the file format.
        created_at: The timestamp when the file format was created.
    """

    name: str
    format_type: str  # e.g., 'CSV', 'JSON', 'PARQUET'
    properties: dict[str, Any] = field(default_factory=dict)
    created_at: str | None = None


class MockFileFormatManager:
    """Manages Snowflake file formats using a DuckDB table for metadata storage.
    """

    def __init__(self, connection: duckdb.DuckDBPyConnection) -> None:
        """Initializes the file format manager.

        Args:
            connection: An active DuckDB connection.
        """
        self.connection = connection
        self._create_system_tables()
        self._create_default_formats()

    def _create_system_tables(self) -> None:
        """Creates the `mockhaus_file_formats` table if it doesn't exist."""
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
        """Creates default file formats like `CSV_DEFAULT` if they don't exist."""
        default_formats = [
            {
                "name": "CSV_DEFAULT",
                "format_type": "CSV",
                "properties": {
                    "field_delimiter": ",",
                    "record_delimiter": "\n",
                    "skip_header": 0,
                    "field_optionally_enclosed_by": None,
                    "null_if": [],
                    "compression": "AUTO",
                    "date_format": "AUTO",
                    "time_format": "AUTO",
                    "timestamp_format": "AUTO",
                },
            },
            {
                "name": "JSON_DEFAULT",
                "format_type": "JSON",
                "properties": {"compression": "AUTO", "date_format": "AUTO", "time_format": "AUTO", "timestamp_format": "AUTO"},
            },
            {"name": "PARQUET_DEFAULT", "format_type": "PARQUET", "properties": {"compression": "AUTO"}},
        ]

        for fmt in default_formats:
            if not self.get_format(fmt["name"]):  # type: ignore[arg-type]
                self.create_format(fmt["name"], fmt["format_type"], fmt["properties"])  # type: ignore[arg-type]

    def create_format(self, name: str, format_type: str, properties: dict[str, Any] | None = None) -> FileFormat:
        """
        Creates a new file format and stores it in the database.

        Args:
            name: The name of the file format.
            format_type: The type of the file format (e.g., 'CSV').
            properties: A dictionary of properties for the format.

        Returns:
            A `FileFormat` object representing the new format.
        """
        if properties is None:
            properties = {}

        # Validate format type
        valid_types = ["CSV", "JSON", "PARQUET", "AVRO", "ORC", "XML"]
        if format_type.upper() not in valid_types:
            raise ValueError(f"Unsupported format type: {format_type}. Supported: {valid_types}")

        format_type = format_type.upper()

        # Apply default properties based on type
        final_properties = self._get_default_properties(format_type)
        final_properties.update(properties)

        # Create format object
        file_format = FileFormat(name=name, format_type=format_type, properties=final_properties)

        # Store in system table
        self._store_format_metadata(file_format)

        return file_format

    def _get_default_properties(self, format_type: str) -> dict[str, Any]:
        """Returns the default properties for a given format type."""
        defaults = {
            "CSV": {
                "field_delimiter": ",",
                "record_delimiter": "\\n",
                "skip_header": 0,
                "field_optionally_enclosed_by": None,
                "null_if": [],
                "compression": "AUTO",
                "date_format": "AUTO",
                "time_format": "AUTO",
                "timestamp_format": "AUTO",
            },
            "JSON": {"compression": "AUTO", "date_format": "AUTO", "time_format": "AUTO", "timestamp_format": "AUTO"},
            "PARQUET": {"compression": "AUTO"},
        }
        return defaults.get(format_type, {}).copy()  # type: ignore[no-any-return, attr-defined]

    def _store_format_metadata(self, file_format: FileFormat) -> None:
        """Stores file format metadata in the `mockhaus_file_formats` table."""
        insert_sql = """
        INSERT OR REPLACE INTO mockhaus_file_formats
        (name, format_type, properties)
        VALUES (?, ?, ?)
        """
        self.connection.execute(insert_sql, [file_format.name, file_format.format_type, json.dumps(file_format.properties)])

    def get_format(self, name: str) -> FileFormat | None:
        """
        Retrieves a file format by its name.

        Args:
            name: The name of the file format.

        Returns:
            A `FileFormat` object if found, otherwise None.
        """
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
        """
        Drops a file format by its name.

        Args:
            name: The name of the file format to drop.

        Returns:
            True if the format was dropped, False if it was not found.
        """
        if not self.get_format(name):
            return False

        self.connection.execute("DELETE FROM mockhaus_file_formats WHERE name = ?", [name])
        return True

    def map_to_duckdb_options(self, file_format: FileFormat) -> dict[str, Any]:
        """
        Maps Snowflake file format properties to DuckDB `COPY` options.

        Args:
            file_format: The `FileFormat` object to map.

        Returns:
            A dictionary of DuckDB-compatible options.
        """
        props = file_format.properties
        format_type = file_format.format_type

        if format_type == "CSV":
            return self._map_csv_options(props)
        if format_type == "JSON":
            return self._map_json_options(props)
        if format_type == "PARQUET":
            return self._map_parquet_options(props)
        # For unsupported formats, return basic options
        return {"FORMAT": format_type}

    def _map_csv_options(self, props: dict[str, Any]) -> dict[str, Any]:
        """Maps CSV-specific properties to DuckDB options."""
        options = {"FORMAT": "CSV"}

        # Field delimiter
        if "field_delimiter" in props and props["field_delimiter"]:
            options["DELIMITER"] = props["field_delimiter"]

        # Header
        skip_header = props.get("skip_header", 0)
        if isinstance(skip_header, int | str):
            try:
                options["HEADER"] = int(skip_header) > 0  # type: ignore[assignment]
            except (ValueError, TypeError):
                options["HEADER"] = False  # type: ignore[assignment]

        # Quote character
        if "field_optionally_enclosed_by" in props and props["field_optionally_enclosed_by"]:
            quote_char = props["field_optionally_enclosed_by"]
            if quote_char in ['"', "'"]:
                options["QUOTE"] = quote_char

        # Null values
        null_if = props.get("null_if", [])
        if null_if and isinstance(null_if, list) and len(null_if) > 0:
            # DuckDB accepts a single null string, use the first one
            options["NULL"] = null_if[0]

        # Date formats (DuckDB has limited support)
        if "date_format" in props and props["date_format"] != "AUTO":
            options["DATEFORMAT"] = props["date_format"]

        if "timestamp_format" in props and props["timestamp_format"] != "AUTO":
            options["TIMESTAMPFORMAT"] = props["timestamp_format"]

        return options

    def _map_json_options(self, props: dict[str, Any]) -> dict[str, Any]:
        """Map JSON format properties to DuckDB options."""
        del props  # Unused parameter
        # DuckDB JSON format is relatively simple
        # Most Snowflake JSON options don't have direct equivalents
        return {"FORMAT": "JSON"}

    def _map_parquet_options(self, props: dict[str, Any]) -> dict[str, Any]:
        """Map Parquet format properties to DuckDB options."""
        del props  # Unused parameter
        # Parquet options are mostly handled automatically by DuckDB
        return {"FORMAT": "PARQUET"}

    def parse_inline_format(self, format_spec: str) -> dict[str, Any]:
        """
        Parses an inline file format specification from a `COPY INTO` statement.

        Args:
            format_spec: The inline format string (e.g., "TYPE = 'CSV' ...").

        Returns:
            A dictionary of parsed properties.
        """
        # This is a simplified parser for common format specifications
        # In a full implementation, you'd want a proper parser

        options = {}

        # Extract TYPE
        import re

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

        return options

    def create_temp_format_from_inline(self, inline_spec: str) -> FileFormat:
        """
        Creates a temporary `FileFormat` object from an inline specification.

        Args:
            inline_spec: The inline format string.

        Returns:
            A temporary `FileFormat` object.
        """
        options = self.parse_inline_format(inline_spec)
        format_type = options.pop("TYPE", "CSV")

        # Create temporary format name
        import hashlib

        temp_name = f"TEMP_{hashlib.md5(inline_spec.encode()).hexdigest()[:8]}"

        return FileFormat(name=temp_name, format_type=format_type, properties=options)
