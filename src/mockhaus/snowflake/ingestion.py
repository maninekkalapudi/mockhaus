"""High-level Snowflake data ingestion operations."""

import re
from typing import Any

import duckdb

from .ast_parser import SnowflakeASTParser
from .copy_into import CopyIntoTranslator
from .file_formats import MockFileFormatManager
from .stages import MockStageManager


class SnowflakeIngestionHandler:
    """Handles all Snowflake data ingestion operations."""

    def __init__(self, connection: duckdb.DuckDBPyConnection, use_ast_parser: bool = True) -> None:
        """Initialize the ingestion handler with a DuckDB connection.

        Args:
            connection: DuckDB connection
            use_ast_parser: Whether to use AST parser instead of regex (default: True)
        """
        self.connection = connection
        self.use_ast_parser = use_ast_parser
        self.stage_manager = MockStageManager(connection)
        self.format_manager = MockFileFormatManager(connection)
        self.copy_translator = CopyIntoTranslator(self.stage_manager, self.format_manager, self.use_ast_parser)
        if use_ast_parser:
            self.ast_parser = SnowflakeASTParser()

    def is_data_ingestion_statement(self, sql: str) -> bool:
        """Check if SQL statement is a data ingestion statement."""
        sql_upper = sql.strip().upper()
        return (
            sql_upper.startswith("CREATE STAGE")
            or sql_upper.startswith("CREATE FILE FORMAT")
            or sql_upper.startswith("COPY INTO")
            or sql_upper.startswith("DROP STAGE")
            or sql_upper.startswith("DROP FILE FORMAT")
        )

    def execute_ingestion_statement(self, sql: str) -> dict[str, Any]:
        """Execute data ingestion statements."""
        sql_upper = sql.strip().upper()

        try:
            if sql_upper.startswith("CREATE STAGE"):
                return self._execute_create_stage(sql)
            if sql_upper.startswith("CREATE FILE FORMAT"):
                return self._execute_create_file_format(sql)
            if sql_upper.startswith("COPY INTO"):
                return self.copy_translator.execute_copy_operation(sql, self.connection)
            if sql_upper.startswith("DROP STAGE"):
                return self._execute_drop_stage(sql)
            if sql_upper.startswith("DROP FILE FORMAT"):
                return self._execute_drop_file_format(sql)
            return {"success": False, "rows_loaded": 0, "errors": [f"Unsupported data ingestion statement: {sql}"]}
        except Exception as e:
            return {"success": False, "rows_loaded": 0, "errors": [str(e)]}

    def _execute_create_stage(self, sql: str) -> dict[str, Any]:
        """Execute CREATE STAGE statement."""
        if self.use_ast_parser:
            # Use AST parser
            parsed = self.ast_parser.parse_create_stage(sql)

            if parsed["error"]:
                return {"success": False, "rows_loaded": 0, "errors": [parsed["error"]]}

            stage_name = parsed["stage_name"]
            stage_type = parsed["stage_type"]
            url = parsed["url"]
        else:
            # Use regex parser (original implementation)
            match = re.search(r"CREATE\s+STAGE\s+(\w+)", sql, re.IGNORECASE)
            if not match:
                return {"success": False, "rows_loaded": 0, "errors": ["Invalid CREATE STAGE syntax"]}

            stage_name = match.group(1)

            # Extract URL if present
            url_match = re.search(r"URL\s*=\s*['\"]([^'\"]+)['\"]", sql, re.IGNORECASE)
            url = url_match.group(1) if url_match else None

            stage_type = "EXTERNAL" if url else "USER"

        # Create the stage
        self.stage_manager.create_stage(stage_name, stage_type, url)

        return {"success": True, "rows_loaded": 0, "translated_sql": f"-- Created stage {stage_name}", "errors": []}

    def _execute_create_file_format(self, sql: str) -> dict[str, Any]:
        """Execute CREATE FILE FORMAT statement."""
        if self.use_ast_parser:
            # Use AST parser
            parsed = self.ast_parser.parse_create_file_format(sql)

            if parsed["error"]:
                return {"success": False, "rows_loaded": 0, "errors": [parsed["error"]]}

            format_name = parsed["format_name"]
            format_type = parsed["format_type"]
            properties = parsed["properties"]
        else:
            # Use regex parser (original implementation)
            # Simple parser for CREATE FILE FORMAT
            # CREATE FILE FORMAT format_name TYPE = 'CSV' [options]
            match = re.search(r"CREATE\s+FILE\s+FORMAT\s+(\w+)", sql, re.IGNORECASE)
            if not match:
                return {"success": False, "rows_loaded": 0, "errors": ["Invalid CREATE FILE FORMAT syntax"]}

            format_name = match.group(1)

            # Extract TYPE
            type_match = re.search(r"TYPE\s*=\s*['\"](\w+)['\"]", sql, re.IGNORECASE)
            format_type = type_match.group(1) if type_match else "CSV"

            # Parse other properties (simplified)
            properties = {}

            # Field delimiter
            delimiter_match = re.search(r"FIELD_DELIMITER\s*=\s*['\"](.)['\"]", sql, re.IGNORECASE)
            if delimiter_match:
                properties["field_delimiter"] = delimiter_match.group(1)

            # Skip header
            header_match = re.search(r"SKIP_HEADER\s*=\s*(\d+)", sql, re.IGNORECASE)
            if header_match:
                properties["skip_header"] = int(header_match.group(1))

        # Create the file format
        self.format_manager.create_format(format_name, format_type, properties)

        return {"success": True, "rows_loaded": 0, "translated_sql": f"-- Created file format {format_name}", "errors": []}

    def _execute_drop_stage(self, sql: str) -> dict[str, Any]:
        """Execute DROP STAGE statement."""
        if self.use_ast_parser:
            # Use AST parser
            parsed = self.ast_parser.parse_drop_stage(sql)

            if parsed["error"]:
                return {"success": False, "rows_loaded": 0, "errors": [parsed["error"]]}

            stage_name = parsed["stage_name"]
        else:
            # Use regex parser (original implementation)
            match = re.search(r"DROP\s+STAGE\s+(\w+)", sql, re.IGNORECASE)
            if not match:
                return {"success": False, "rows_loaded": 0, "errors": ["Invalid DROP STAGE syntax"]}

            stage_name = match.group(1)

        # Drop the stage
        success = self.stage_manager.drop_stage(stage_name)

        return {
            "success": success,
            "rows_loaded": 0,
            "translated_sql": f"-- Dropped stage {stage_name}",
            "errors": [] if success else [f"Stage {stage_name} not found"],
        }

    def _execute_drop_file_format(self, sql: str) -> dict[str, Any]:
        """Execute DROP FILE FORMAT statement."""
        if self.use_ast_parser:
            # Use AST parser
            parsed = self.ast_parser.parse_drop_file_format(sql)

            if parsed["error"]:
                return {"success": False, "rows_loaded": 0, "errors": [parsed["error"]]}

            format_name = parsed["format_name"]
        else:
            # Use regex parser (original implementation)
            match = re.search(r"DROP\s+FILE\s+FORMAT\s+(\w+)", sql, re.IGNORECASE)
            if not match:
                return {"success": False, "rows_loaded": 0, "errors": ["Invalid DROP FILE FORMAT syntax"]}

            format_name = match.group(1)

        # Drop the file format
        success = self.format_manager.drop_format(format_name)

        return {
            "success": success,
            "rows_loaded": 0,
            "translated_sql": f"-- Dropped file format {format_name}",
            "errors": [] if success else [f"File format {format_name} not found"],
        }
