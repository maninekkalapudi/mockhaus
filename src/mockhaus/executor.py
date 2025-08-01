"""Query execution engine using DuckDB."""

import re
from dataclasses import dataclass
from typing import Any

import duckdb

from .copy_into import CopyIntoTranslator
from .file_formats import MockFileFormatManager
from .stages import MockStageManager
from .translator import SnowflakeToDuckDBTranslator


@dataclass
class QueryResult:
    """Result of a query execution."""

    success: bool
    data: list[dict[str, Any]] | None = None
    columns: list[str] | None = None
    row_count: int = 0
    execution_time_ms: float = 0.0
    error: str | None = None
    original_sql: str = ""
    translated_sql: str = ""


class MockhausExecutor:
    """Executes translated SQL queries using DuckDB."""

    def __init__(self, database_path: str | None = None) -> None:
        """
        Initialize the executor.

        Args:
            database_path: Path to DuckDB database file. If None, uses in-memory database.
        """
        self.database_path = database_path
        self.translator = SnowflakeToDuckDBTranslator()
        self._connection: duckdb.DuckDBPyConnection | None = None
        self._stage_manager: MockStageManager | None = None
        self._format_manager: MockFileFormatManager | None = None
        self._copy_translator: CopyIntoTranslator | None = None

    def connect(self) -> None:
        """Establish connection to DuckDB."""
        if self._connection is None:
            # Use ":memory:" for in-memory database when path is None
            db_path = (
                self.database_path if self.database_path is not None else ":memory:"
            )
            self._connection = duckdb.connect(db_path)
            self._setup_database()
            self._setup_data_ingestion()

    def disconnect(self) -> None:
        """Close the DuckDB connection."""
        if self._connection:
            self._connection.close()
            self._connection = None

    def _setup_database(self) -> None:
        """Set up the database with initial configuration."""
        if not self._connection:
            return

        # Set up some basic configuration that might help with Snowflake compatibility
        try:
            # Enable case-insensitive string comparisons (closer to Snowflake behavior)
            # Note: This is a simplified approach, full case-insensitivity would need more work
            pass
        except Exception:
            # Ignore setup errors for now
            pass

    def _setup_data_ingestion(self) -> None:
        """Set up data ingestion components."""
        if not self._connection:
            return

        # Initialize stage and format managers
        self._stage_manager = MockStageManager(self._connection)
        self._format_manager = MockFileFormatManager(self._connection)
        self._copy_translator = CopyIntoTranslator(self._stage_manager, self._format_manager)

    def execute_snowflake_sql(self, snowflake_sql: str) -> QueryResult:
        """
        Execute a Snowflake SQL query by translating it to DuckDB SQL first.

        Args:
            snowflake_sql: The Snowflake SQL query to execute

        Returns:
            QueryResult containing the execution results
        """
        import time

        start_time = time.time()

        try:
            # Ensure we're connected
            self.connect()

            # Check if this is a data ingestion statement
            if self._is_data_ingestion_statement(snowflake_sql):
                result = self._execute_data_ingestion_statement(snowflake_sql)
                execution_time = (time.time() - start_time) * 1000
                
                return QueryResult(
                    success=result["success"],
                    data=[{"rows_loaded": result["rows_loaded"]}] if result["success"] else None,
                    columns=["rows_loaded"] if result["success"] else None,
                    row_count=1 if result["success"] else 0,
                    execution_time_ms=execution_time,
                    original_sql=snowflake_sql,
                    translated_sql=result.get("translated_sql", ""),
                    error=result["errors"][0] if result["errors"] else None,
                )

            # Translate the SQL
            translated_sql = self.translator.translate(snowflake_sql)

            # Execute the translated query
            result = self._execute_duckdb_sql(translated_sql)

            execution_time = (
                time.time() - start_time
            ) * 1000  # Convert to milliseconds

            return QueryResult(
                success=True,
                data=result["data"],
                columns=result["columns"],
                row_count=result["row_count"],
                execution_time_ms=execution_time,
                original_sql=snowflake_sql,
                translated_sql=translated_sql,
            )

        except Exception as e:
            execution_time = (time.time() - start_time) * 1000

            return QueryResult(
                success=False,
                error=str(e),
                execution_time_ms=execution_time,
                original_sql=snowflake_sql,
                translated_sql="",
            )

    def _execute_duckdb_sql(self, duckdb_sql: str) -> dict[str, Any]:
        """
        Execute a DuckDB SQL query and return results.

        Args:
            duckdb_sql: The DuckDB SQL query to execute

        Returns:
            Dictionary containing query results
        """
        if not self._connection:
            raise RuntimeError("Not connected to database")

        # Execute the query
        result = self._connection.execute(duckdb_sql)

        # Fetch results
        rows = result.fetchall()
        columns = [desc[0] for desc in result.description] if result.description else []

        # Convert to list of dictionaries
        data = []
        for row in rows:
            row_dict = {}
            for i, value in enumerate(row):
                if i < len(columns):
                    row_dict[columns[i]] = value
            data.append(row_dict)

        return {"data": data, "columns": columns, "row_count": len(rows)}

    def _is_data_ingestion_statement(self, sql: str) -> bool:
        """Check if SQL statement is a data ingestion statement."""
        sql_upper = sql.strip().upper()
        return (
            sql_upper.startswith("CREATE STAGE") or
            sql_upper.startswith("CREATE FILE FORMAT") or
            sql_upper.startswith("COPY INTO") or
            sql_upper.startswith("DROP STAGE") or
            sql_upper.startswith("DROP FILE FORMAT")
        )

    def _execute_data_ingestion_statement(self, sql: str) -> dict[str, Any]:
        """Execute data ingestion statements."""
        sql_upper = sql.strip().upper()
        
        try:
            if sql_upper.startswith("CREATE STAGE"):
                return self._execute_create_stage(sql)
            elif sql_upper.startswith("CREATE FILE FORMAT"):
                return self._execute_create_file_format(sql)
            elif sql_upper.startswith("COPY INTO"):
                return self._copy_translator.execute_copy_operation(sql, self._connection)
            elif sql_upper.startswith("DROP STAGE"):
                return self._execute_drop_stage(sql)
            elif sql_upper.startswith("DROP FILE FORMAT"):
                return self._execute_drop_file_format(sql)
            else:
                return {
                    "success": False,
                    "rows_loaded": 0,
                    "errors": [f"Unsupported data ingestion statement: {sql}"]
                }
        except Exception as e:
            return {
                "success": False,
                "rows_loaded": 0,
                "errors": [str(e)]
            }

    def _execute_create_stage(self, sql: str) -> dict[str, Any]:
        """Execute CREATE STAGE statement."""
        # Simple parser for CREATE STAGE
        # CREATE STAGE stage_name [URL = 'url'] [other options]
        match = re.search(r'CREATE\s+STAGE\s+(\w+)', sql, re.IGNORECASE)
        if not match:
            return {"success": False, "rows_loaded": 0, "errors": ["Invalid CREATE STAGE syntax"]}
        
        stage_name = match.group(1)
        
        # Extract URL if present
        url_match = re.search(r"URL\s*=\s*['\"]([^'\"]+)['\"]", sql, re.IGNORECASE)
        url = url_match.group(1) if url_match else None
        
        stage_type = "EXTERNAL" if url else "USER"
        
        self._stage_manager.create_stage(stage_name, stage_type, url)
        
        return {
            "success": True,
            "rows_loaded": 0,
            "translated_sql": f"-- Created stage {stage_name}",
            "errors": []
        }

    def _execute_create_file_format(self, sql: str) -> dict[str, Any]:
        """Execute CREATE FILE FORMAT statement."""
        # Simple parser for CREATE FILE FORMAT
        # CREATE FILE FORMAT format_name TYPE = 'CSV' [options]
        match = re.search(r'CREATE\s+FILE\s+FORMAT\s+(\w+)', sql, re.IGNORECASE)
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
        
        self._format_manager.create_format(format_name, format_type, properties)
        
        return {
            "success": True,
            "rows_loaded": 0,
            "translated_sql": f"-- Created file format {format_name}",
            "errors": []
        }

    def _execute_drop_stage(self, sql: str) -> dict[str, Any]:
        """Execute DROP STAGE statement."""
        match = re.search(r'DROP\s+STAGE\s+(\w+)', sql, re.IGNORECASE)
        if not match:
            return {"success": False, "rows_loaded": 0, "errors": ["Invalid DROP STAGE syntax"]}
        
        stage_name = match.group(1)
        success = self._stage_manager.drop_stage(stage_name)
        
        return {
            "success": success,
            "rows_loaded": 0,
            "translated_sql": f"-- Dropped stage {stage_name}",
            "errors": [] if success else [f"Stage {stage_name} not found"]
        }

    def _execute_drop_file_format(self, sql: str) -> dict[str, Any]:
        """Execute DROP FILE FORMAT statement."""
        match = re.search(r'DROP\s+FILE\s+FORMAT\s+(\w+)', sql, re.IGNORECASE)
        if not match:
            return {"success": False, "rows_loaded": 0, "errors": ["Invalid DROP FILE FORMAT syntax"]}
        
        format_name = match.group(1)
        success = self._format_manager.drop_format(format_name)
        
        return {
            "success": success,
            "rows_loaded": 0,
            "translated_sql": f"-- Dropped file format {format_name}",
            "errors": [] if success else [f"File format {format_name} not found"]
        }

    def create_sample_data(self) -> None:
        """Create some sample data for testing."""
        self.connect()

        if not self._connection:
            return

        # Create a sample table with various data types
        sample_ddl = """
        CREATE TABLE IF NOT EXISTS sample_customers (
            customer_id INTEGER,
            customer_name VARCHAR(100),
            email VARCHAR(255),
            signup_date DATE,
            last_login TIMESTAMP,
            is_active BOOLEAN,
            account_balance DECIMAL(10,2)
        )
        """

        sample_data = """
        INSERT INTO sample_customers VALUES
        (1, 'Alice Johnson', 'alice@example.com', '2023-01-15', '2024-01-15 14:30:00', true, 1250.75),
        (2, 'Bob Smith', 'bob@example.com', '2023-02-20', '2024-01-14 09:15:00', true, 0.00),
        (3, 'Charlie Brown', 'charlie@example.com', '2023-03-10', '2024-01-10 16:45:00', false, -50.25),
        (4, 'Diana Prince', 'diana@example.com', '2023-04-05', '2024-01-16 11:20:00', true, 3750.00),
        (5, 'Eve Davis', 'eve@example.com', '2023-05-12', '2024-01-13 13:10:00', true, 892.50)
        """

        try:
            self._connection.execute(sample_ddl)
            # Clear existing data first
            self._connection.execute("DELETE FROM sample_customers")
            self._connection.execute(sample_data)
        except Exception as e:
            print(f"Warning: Could not create sample data: {e}")

    def __enter__(self) -> "MockhausExecutor":
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.disconnect()
