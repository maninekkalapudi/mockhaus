"""
This module handles the translation of Snowflake's `COPY INTO` statements.

It is responsible for parsing the `COPY INTO` command, resolving the stage and
file format, and generating a DuckDB-compatible `COPY` statement that can be
executed to load data into a table.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..my_logging import debug_log
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

    def __init__(self, stage_manager: MockStageManager, format_manager: MockFileFormatManager) -> None:
        """Initialize COPY INTO translator."""
        self.stage_manager = stage_manager
        self.format_manager = format_manager
        self.ast_parser = SnowflakeASTParser()

    def parse_copy_into_statement(self, sql: str) -> CopyIntoContext:
        """Parse a COPY INTO statement and extract components."""
        return self._parse_copy_into_with_ast(sql)

    def _parse_copy_into_with_ast(self, sql: str) -> CopyIntoContext:
        """Parse COPY INTO statement using AST parser."""
        debug_log(f"Parsing COPY INTO statement with AST parser: {sql}")
        parsed = self.ast_parser.parse_copy_into(sql)

        debug_log(f"Parsed COPY INTO statement: {parsed}")

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
        elif parsed.get("inline_format_options"):
            context.inline_format = parsed["inline_format"]  # String representation for backward compatibility
            context.file_format = self.format_manager.create_temp_format_from_inline(parsed["inline_format_options"])

        # Set other options from the parsed statement
        options = parsed.get("options", {})
        context.on_error = options.get("on_error", options.get("ON_ERROR", "ABORT")).upper()
        context.force = options.get("force", options.get("FORCE", False))
        context.purge = options.get("purge", options.get("PURGE", False))
        context.pattern = options.get("pattern", options.get("PATTERN"))
        context.validation_mode = options.get("validation_mode", options.get("VALIDATION_MODE"))

        return context

    def translate_copy_into(self, sql: str) -> str:
        """Translate a COPY INTO statement to DuckDB COPY statement."""
        # Parse the COPY INTO statement
        debug_log(f"Translating COPY INTO statement: {sql}")
        context = self.parse_copy_into_statement(sql)
        debug_log(f"Parsed COPY INTO context: {context}")

        # Resolve the stage reference to a local file path
        resolved_path = self.stage_manager.resolve_stage_path(context.stage_reference)
        if not resolved_path:
            raise ValueError(f"Cannot resolve stage reference: {context.stage_reference}")

        context.file_path = resolved_path

        # Check if file exists or if we need pattern matching
        file_path = Path(context.file_path)

        if context.pattern:
            # When pattern is specified, treat file_path as directory
            search_dir = file_path if file_path.is_dir() else file_path.parent
            files = self._find_files_by_pattern(search_dir, context.pattern)
            if not files:
                raise FileNotFoundError(f"No files found matching pattern '{context.pattern}' in {search_dir}")
            # For simplicity, use the first matching file
            context.file_path = str(files[0])
        elif not file_path.exists():
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
        """Execute COPY INTO operation and return results."""
        try:
            # Translate to DuckDB COPY
            debug_log("Starting COPY operation translation")
            duckdb_sql = self.translate_copy_into(sql)
            debug_log(f"duckdb sql  {duckdb_sql}")

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
            debug_log(f"Error executing COPY INTO operation: {e}")
            return {"success": False, "rows_loaded": 0, "original_sql": sql, "translated_sql": "", "errors": [str(e)]}
