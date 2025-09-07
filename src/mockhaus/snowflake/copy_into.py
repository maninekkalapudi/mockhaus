"""
This module handles the translation of Snowflake's `COPY INTO` statements.

It is responsible for parsing the `COPY INTO` command, resolving the stage and
file format, and generating a DuckDB-compatible `COPY` statement that can be
executed to load data into a table.
"""

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .ast_parser import SnowflakeASTParser
from .file_formats import FileFormat, MockFileFormatManager
from .stages import MockStageManager


@dataclass
class CopyIntoContext:
    """
    A data class to hold the parsed components of a `COPY INTO` statement.

    Attributes:
        table_name: The name of the target table for the COPY operation.
        stage_reference: The full stage reference (e.g., '@my_stage/data.csv').
        file_path: The resolved local file path for the data file.
        file_format: The `FileFormat` object to use for parsing the data.
        inline_format: The raw string of an inline file format specification.
        on_error: The error handling behavior (e.g., 'ABORT', 'CONTINUE').
        force: Whether to force loading of data, ignoring certain errors.
        purge: Whether to purge the data file after loading.
        pattern: A regex pattern to filter files in the stage.
        validation_mode: The validation mode for the COPY operation.
    """

    table_name: str
    stage_reference: str
    file_path: str
    file_format: FileFormat | None = None
    inline_format: str | None = None
    on_error: str = "ABORT"
    force: bool = False
    purge: bool = False
    pattern: str | None = None
    validation_mode: str | None = None


class CopyIntoTranslator:
    """Translates Snowflake `COPY INTO` statements to DuckDB `COPY` statements."""

    def __init__(self, stage_manager: MockStageManager, format_manager: MockFileFormatManager, use_ast_parser: bool = True) -> None:
        """
        Initializes the `COPY INTO` translator.

        Args:
            stage_manager: An instance of `MockStageManager`.
            format_manager: An instance of `MockFileFormatManager`.
            use_ast_parser: Whether to use the AST-based parser.
        """
        self.stage_manager = stage_manager
        self.format_manager = format_manager
        self.use_ast_parser = use_ast_parser
        self.ast_parser = SnowflakeASTParser() if use_ast_parser else None

    def parse_copy_into_statement(self, sql: str) -> CopyIntoContext:
        """
        Parses a `COPY INTO` statement and returns a context object.

        This method will use the AST parser if enabled, otherwise it falls back
        to a regex-based parser.

        Args:
            sql: The `COPY INTO` SQL statement.

        Returns:
            A `CopyIntoContext` object with the parsed components.
        """
        if self.use_ast_parser and self.ast_parser:
            return self._parse_copy_into_with_ast(sql)
        return self._parse_copy_into_with_regex(sql)

    def _parse_copy_into_with_ast(self, sql: str) -> CopyIntoContext:
        """Parses a `COPY INTO` statement using the AST parser."""
        if not self.ast_parser:
            raise ValueError("AST parser is not available")
        parsed = self.ast_parser.parse_copy_into(sql)

        if parsed.get("error"):
            raise ValueError(f"Failed to parse COPY INTO statement: {parsed['error']}")

        context = CopyIntoContext(
            table_name=parsed["table_name"],
            stage_reference=parsed["stage_reference"],
            file_path="",  # This will be resolved later
        )

        # Resolve the file format, whether it's a named format or inline
        if parsed.get("file_format_name"):
            context.file_format = self.format_manager.get_format(parsed["file_format_name"])
            if not context.file_format:
                raise ValueError(f"File format '{parsed['file_format_name']}' not found")
        elif parsed.get("inline_format"):
            context.inline_format = parsed["inline_format"]
            context.file_format = self.format_manager.create_temp_format_from_inline(parsed["inline_format"])

        # Set other options from the parsed statement
        options = parsed.get("options", {})
        context.on_error = options.get("ON_ERROR", "ABORT").upper()
        context.force = options.get("FORCE", False)
        context.purge = options.get("PURGE", False)
        context.pattern = options.get("PATTERN")
        context.validation_mode = options.get("VALIDATION_MODE")

        return context

    def _parse_copy_into_with_regex(self, sql: str) -> CopyIntoContext:
        """Parses a `COPY INTO` statement using regex (legacy method)."""
        sql = re.sub(r"\s+", " ", sql.strip())

        # Basic pattern for COPY INTO
        # COPY INTO table_name FROM stage_reference [FILE_FORMAT = ...] [OPTIONS]
        copy_pattern = r'COPY\s+INTO\s+(\w+)\s+FROM\s+([\'"]?@[^\'"\s]+[\'"]?)'

        match = re.search(copy_pattern, sql, re.IGNORECASE)
        if not match:
            raise ValueError(f"Invalid COPY INTO statement: {sql}")

        table_name = match.group(1)
        stage_reference = match.group(2).strip("'\"")

        context = CopyIntoContext(
            table_name=table_name,
            stage_reference=stage_reference,
            file_path="",  # Will be resolved later
        )

        # Extract file format specification
        self._parse_file_format(sql, context)

        # Extract other options
        self._parse_copy_options(sql, context)

        return context

    def _parse_file_format(self, sql: str, context: CopyIntoContext) -> None:
        """Parse file format specification from COPY INTO statement."""
        # Look for FILE_FORMAT = (FORMAT_NAME = 'name') or FILE_FORMAT = 'name'
        format_name_pattern = r'FILE_FORMAT\s*=\s*\(\s*FORMAT_NAME\s*=\s*[\'"](\w+)[\'"]\s*\)'
        format_direct_pattern = r'FILE_FORMAT\s*=\s*[\'"](\w+)[\'"]'

        # Look for inline format specification
        inline_pattern = r"FILE_FORMAT\s*=\s*\(([^)]+)\)"

        format_name_match = re.search(format_name_pattern, sql, re.IGNORECASE)
        format_direct_match = re.search(format_direct_pattern, sql, re.IGNORECASE)
        inline_match = re.search(inline_pattern, sql, re.IGNORECASE)

        if format_name_match:
            format_name = format_name_match.group(1)
            context.file_format = self.format_manager.get_format(format_name)
            if not context.file_format:
                raise ValueError(f"File format '{format_name}' not found")
        elif format_direct_match:
            format_name = format_direct_match.group(1)
            context.file_format = self.format_manager.get_format(format_name)
            if not context.file_format:
                raise ValueError(f"File format '{format_name}' not found")
        elif inline_match:
            inline_spec = inline_match.group(1)
            context.inline_format = inline_spec
            context.file_format = self.format_manager.create_temp_format_from_inline(inline_spec)

    def _parse_copy_options(self, sql: str, context: CopyIntoContext) -> None:
        """Parse COPY INTO options like ON_ERROR, FORCE, etc."""
        # ON_ERROR option
        on_error_pattern = r'ON_ERROR\s*=\s*[\'"](\w+)[\'"]'
        on_error_match = re.search(on_error_pattern, sql, re.IGNORECASE)
        if on_error_match:
            context.on_error = on_error_match.group(1).upper()

        # FORCE option
        if re.search(r"\bFORCE\s*=\s*TRUE\b", sql, re.IGNORECASE):
            context.force = True

        # PURGE option
        if re.search(r"\bPURGE\s*=\s*TRUE\b", sql, re.IGNORECASE):
            context.purge = True

        # PATTERN option
        pattern_match = re.search(r'PATTERN\s*=\s*[\'"]([^\'\"]+)[\'"]', sql, re.IGNORECASE)
        if pattern_match:
            context.pattern = pattern_match.group(1)

        # VALIDATION_MODE option
        validation_match = re.search(r'VALIDATION_MODE\s*=\s*[\'"](\w+)[\'"]', sql, re.IGNORECASE)
        if validation_match:
            context.validation_mode = validation_match.group(1).upper()

    def translate_copy_into(self, sql: str) -> str:
        """
        Translates a Snowflake `COPY INTO` statement to a DuckDB `COPY` statement.

        Args:
            sql: The `COPY INTO` SQL statement.

        Returns:
            A DuckDB-compatible `COPY` statement.
        """
        context = self.parse_copy_into_statement(sql)

        # Resolve the stage reference to a local file path
        resolved_path = self.stage_manager.resolve_stage_path(context.stage_reference)
        if not resolved_path:
            raise ValueError(f"Cannot resolve stage reference: {context.stage_reference}")

        context.file_path = resolved_path

        # Check if file exists
        file_path = Path(context.file_path)
        if not file_path.exists():
            if context.pattern:
                # Handle pattern matching
                files = self._find_files_by_pattern(file_path.parent, context.pattern)
                if not files:
                    raise FileNotFoundError(f"No files found matching pattern '{context.pattern}' in {file_path.parent}")
                # For simplicity, use the first matching file
                context.file_path = str(files[0])
            else:
                raise FileNotFoundError(f"File not found: {context.file_path}")

        # Generate DuckDB COPY statement
        return self._generate_duckdb_copy(context)

    def _find_files_by_pattern(self, directory: Path, pattern: str) -> list[Path]:
        """Find files matching a pattern in directory."""
        if not directory.exists():
            return []

        # Convert SQL pattern to glob pattern (simplified)
        # In Snowflake, patterns use regex-like syntax
        # For now, we'll handle simple glob patterns
        glob_pattern = pattern.replace("%", "*")

        return list(directory.glob(glob_pattern))

    def _generate_duckdb_copy(self, context: CopyIntoContext) -> str:
        """
        Generates the final DuckDB `COPY` statement from the context.

        Args:
            context: The `CopyIntoContext` object.

        Returns:
            The final DuckDB `COPY` statement as a string.
        """
        copy_sql = f"COPY {context.table_name} FROM '{context.file_path}'"

        # Add format options
        if context.file_format:
            options = self.format_manager.map_to_duckdb_options(context.file_format)
            if options:
                option_parts = []
                for key, value in options.items():
                    if isinstance(value, bool):
                        option_parts.append(f"{key} {str(value).lower()}")
                    elif isinstance(value, str):
                        option_parts.append(f"{key} '{value}'")
                    else:
                        option_parts.append(f"{key} {value}")

                if option_parts:
                    copy_sql += f" ({', '.join(option_parts)})"

        return copy_sql

    def validate_copy_operation(self, context: CopyIntoContext) -> list[str]:
        """
        Validates a `COPY` operation and returns a list of warnings.

        Args:
            context: The `CopyIntoContext` for the operation.

        Returns:
            A list of warning messages.
        """
        warnings = []

        # Check if file exists
        if not Path(context.file_path).exists():
            warnings.append(f"File does not exist: {context.file_path}")

        # Check file format compatibility
        if context.file_format:
            file_ext = Path(context.file_path).suffix.lower()
            format_type = context.file_format.format_type.lower()

            expected_extensions = {
                "csv": [".csv", ".txt"],
                "json": [".json", ".jsonl"],
                "parquet": [".parquet", ".pqt"],
                "avro": [".avro"],
                "orc": [".orc"],
            }

            if format_type in expected_extensions and file_ext not in expected_extensions[format_type]:
                warnings.append(f"File extension '{file_ext}' may not match format type '{format_type}'")

        return warnings

    def execute_copy_operation(self, sql: str, connection: Any) -> dict[str, Any]:
        """
        Executes a `COPY INTO` operation and returns the result.

        Args:
            sql: The `COPY INTO` SQL statement.
            connection: An active DuckDB connection.

        Returns:
            A dictionary containing the result of the operation.
        """
        try:
            duckdb_sql = self.translate_copy_into(sql)

            # Execute the translated statement
            result = connection.execute(duckdb_sql)

            # Get row count (DuckDB COPY returns count)
            row_count = 0
            if result.description and len(result.description) > 0:
                rows = result.fetchall()
                if rows and len(rows) > 0:
                    row_count = rows[0][0] if isinstance(rows[0][0], int) else 0

            return {"success": True, "rows_loaded": row_count, "original_sql": sql, "translated_sql": duckdb_sql, "errors": []}

        except Exception as e:
            return {"success": False, "rows_loaded": 0, "original_sql": sql, "translated_sql": "", "errors": [str(e)]}
