"""Query execution engine using DuckDB."""

import contextlib
from dataclasses import dataclass
from typing import Any

import duckdb

from .snowflake import SnowflakeIngestionHandler, SnowflakeToDuckDBTranslator
from .snowflake.database_manager import SnowflakeDatabaseManager


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

    def __init__(self, database_path: str | None = None, use_ast_parser: bool = True) -> None:
        """
        Initialize the executor.

        Args:
            database_path: Path to DuckDB database file. If None, uses in-memory database.
            use_ast_parser: Whether to use AST parser for ingestion statements (default: True).
        """
        self.database_path = database_path
        self.use_ast_parser = use_ast_parser
        self.translator = SnowflakeToDuckDBTranslator()
        self._connection: duckdb.DuckDBPyConnection | None = None
        self._ingestion_handler: SnowflakeIngestionHandler | None = None
        self._database_manager = SnowflakeDatabaseManager()

    def connect(self) -> None:
        """Establish connection to DuckDB."""
        if self._connection is None:
            # Use ":memory:" for in-memory database when path is None
            db_path = self.database_path if self.database_path is not None else ":memory:"
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
        with contextlib.suppress(Exception):
            # Enable case-insensitive string comparisons (closer to Snowflake behavior)
            # Note: This is a simplified approach, full case-insensitivity would need more work
            pass

    def _setup_data_ingestion(self) -> None:
        """Set up data ingestion components."""
        if not self._connection:
            return

        # Initialize ingestion handler with parser preference
        self._ingestion_handler = SnowflakeIngestionHandler(self._connection, self.use_ast_parser)

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
            # Check if this is a database DDL statement first
            if self._database_manager.is_database_ddl(snowflake_sql):
                result = self._database_manager.execute_database_ddl(snowflake_sql)
                execution_time = (time.time() - start_time) * 1000

                if result["success"]:
                    # For USE DATABASE commands, switch to the new database
                    if "database_path" in result:
                        self.database_path = result["database_path"]
                        # Reconnect to the new database
                        self.disconnect()
                        self.connect()

                    # Format response based on command type
                    data = None
                    columns = None
                    if "databases" in result:
                        # SHOW DATABASES
                        data = result["databases"]
                        columns = ["name", "current", "size_mb", "path"] if data else []
                    else:
                        # CREATE/DROP/USE DATABASE
                        data = [{"message": result["message"]}]
                        columns = ["message"]

                    return QueryResult(
                        success=True,
                        data=data,
                        columns=columns,
                        row_count=len(data) if data else 0,
                        execution_time_ms=execution_time,
                        original_sql=snowflake_sql,
                        translated_sql="-- Database DDL (no translation needed)",
                    )
                return QueryResult(
                    success=False,
                    error=result["error"],
                    execution_time_ms=execution_time,
                    original_sql=snowflake_sql,
                    translated_sql="",
                )

            # Ensure we're connected
            self.connect()

            # Check if this is a data ingestion statement
            if self._ingestion_handler and self._ingestion_handler.is_data_ingestion_statement(snowflake_sql):
                result = self._ingestion_handler.execute_ingestion_statement(snowflake_sql)
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

            execution_time = (time.time() - start_time) * 1000  # Convert to milliseconds

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
        except Exception:
            # Log warning instead of printing to stdout
            pass  # Could not create sample data

    def __enter__(self) -> "MockhausExecutor":
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.disconnect()
