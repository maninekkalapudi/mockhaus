"""High-level Snowflake data ingestion operations."""

from typing import Any

import duckdb

from ..logging import debug_log
from .ast_parser import SnowflakeASTParser
from .copy_into import CopyIntoTranslator
from .file_formats import MockFileFormatManager
from .stages import MockStageManager


class SnowflakeIngestionHandler:
    """Handles all Snowflake data ingestion operations."""

    def __init__(self, connection: duckdb.DuckDBPyConnection) -> None:
        """Initialize the ingestion handler with a DuckDB connection.

        Args:
            connection: DuckDB connection
        """
        self.connection = connection
        self.stage_manager = MockStageManager(connection)
        self.format_manager = MockFileFormatManager(connection)
        self.copy_translator = CopyIntoTranslator(self.stage_manager, self.format_manager)
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
        debug_log("we are here")

        try:
            if sql_upper.startswith("CREATE STAGE"):
                return self._execute_create_stage(sql)
            if sql_upper.startswith("CREATE FILE FORMAT"):
                return self._execute_create_file_format(sql)
            if sql_upper.startswith("COPY INTO"):
                debug_log("we are here 234")
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
        # Use AST parser
        parsed = self.ast_parser.parse_create_stage(sql)

        if parsed["error"]:
            return {"success": False, "rows_loaded": 0, "errors": [parsed["error"]]}

        stage_name = parsed["stage_name"]
        stage_type = parsed["stage_type"]
        url = parsed["url"]

        # Create the stage
        self.stage_manager.create_stage(stage_name, stage_type, url)

        return {"success": True, "rows_loaded": 0, "translated_sql": f"-- Created stage {stage_name}", "errors": []}

    def _execute_create_file_format(self, sql: str) -> dict[str, Any]:
        """Execute CREATE FILE FORMAT statement."""
        # Use AST parser
        parsed = self.ast_parser.parse_create_file_format(sql)

        if parsed["error"]:
            return {"success": False, "rows_loaded": 0, "errors": [parsed["error"]]}

        format_name = parsed["format_name"]
        format_type = parsed["format_type"]
        properties = parsed["properties"]

        # Create the file format
        self.format_manager.create_format(format_name, format_type, properties)

        return {"success": True, "rows_loaded": 0, "translated_sql": f"-- Created file format {format_name}", "errors": []}

    def _execute_drop_stage(self, sql: str) -> dict[str, Any]:
        """Execute DROP STAGE statement."""
        # Use AST parser
        parsed = self.ast_parser.parse_drop_stage(sql)

        if parsed["error"]:
            return {"success": False, "rows_loaded": 0, "errors": [parsed["error"]]}

        stage_name = parsed["stage_name"]

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
        # Use AST parser
        parsed = self.ast_parser.parse_drop_file_format(sql)

        if parsed["error"]:
            return {"success": False, "rows_loaded": 0, "errors": [parsed["error"]]}

        format_name = parsed["format_name"]

        # Drop the file format
        success = self.format_manager.drop_format(format_name)

        return {
            "success": success,
            "rows_loaded": 0,
            "translated_sql": f"-- Dropped file format {format_name}",
            "errors": [] if success else [f"File format {format_name} not found"],
        }
