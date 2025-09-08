"""
This module provides query history tracking and analysis for Mockhaus.

It defines the `QueryHistory` class, which manages the storage and retrieval of
query execution records using dedicated tables within a DuckDB database. This
allows for auditing, debugging, and performance analysis of executed queries.

The module also defines several dataclasses to structure the query information,
including `QueryRecord`, `QueryContext`, and `QueryMetrics`.
"""

import json
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

import duckdb


@dataclass
class QueryContext:
    """
    Represents the context in which a query is executed.

    This information is used to tag query records in the history for better
    filtering and analysis, especially in a multi-session or multi-user environment.

    Attributes:
        session_id: The unique identifier for the user session.
        connection_id: A unique identifier for the specific connection.
        database_name: The name of the database the query was executed against.
        schema_name: The name of the schema in use.
        user: The user who executed the query.
        warehouse: The virtual warehouse used (for Snowflake compatibility).
        client_info: A dictionary of client-provided metadata.
        query_tags: A dictionary of user-defined tags for the query.
    """

    session_id: str | None = None
    connection_id: str | None = None
    database_name: str | None = None
    schema_name: str | None = None
    user: str | None = None
    warehouse: str | None = None
    client_info: dict[str, Any] | None = None
    query_tags: dict[str, Any] | None = None


@dataclass
class QueryRecord:
    """
    Represents a single, comprehensive record of a query execution.

    This dataclass structures all the information about a query that is stored
    in the `query_history` table.
    """

    id: int
    query_id: str
    timestamp: datetime
    original_sql: str
    translated_sql: str | None
    query_type: str | None
    status: str
    error_message: str | None = None
    error_code: str | None = None
    execution_time_ms: int | None = None
    rows_affected: int | None = None
    session_id: str | None = None
    connection_id: str | None = None
    database_name: str | None = None
    schema_name: str | None = None
    user: str | None = None
    warehouse: str | None = None
    client_info: dict[str, Any] | None = None
    query_tags: dict[str, Any] | None = None


@dataclass
class QueryMetrics:
    """
    Represents performance metrics associated with a query execution.

    This data is stored in the `query_metrics` table and linked to a `QueryRecord`
    by the `query_id`.
    """

    query_id: str
    parse_time_ms: int | None = None
    translation_time_ms: int | None = None
    execution_time_ms: int | None = None
    total_time_ms: int | None = None
    memory_usage_bytes: int | None = None
    cpu_usage_percent: float | None = None


@dataclass
class QueryStatistics:
    """
    Represents aggregated statistics for a set of queries over a time period.

    This is used to provide an overview of system performance and usage.
    """

    total_queries: int
    successful_queries: int
    failed_queries: int
    avg_execution_time_ms: float
    p95_execution_time_ms: float
    queries_by_type: dict[str, int]
    errors_by_code: dict[str, int]
    queries_by_hour: dict[str, int]


class QueryHistory:
    """
    Manages query history storage and retrieval using DuckDB tables.

    This class is responsible for creating the necessary schema and tables for
    storing query history, recording new queries, and providing methods to
    search, retrieve, and analyze the historical data.
    """

    SCHEMA_NAME = "__mockhaus__"

    def __init__(self, connection: duckdb.DuckDBPyConnection | None = None):
        """
        Initializes the QueryHistory manager.

        Args:
            connection: An optional DuckDB connection. If not provided, it must be
                        set later using the `connect` method.
        """
        self._connection = connection
        self._initialized = False
        self._is_memory_db = False

    def connect(self, connection: duckdb.DuckDBPyConnection) -> None:
        """
        Sets the DuckDB connection and initializes the history schema if needed.

        This method must be called before any other operations if a connection
        was not provided during initialization.

        Args:
            connection: The DuckDB connection to use for history storage.
        """
        if self._connection is None:
            self._connection = connection
            # Check if this is an in-memory database by checking available catalogs
            try:
                catalogs = connection.execute("SELECT catalog_name FROM information_schema.schemata").fetchall()
                catalog_names = {row[0] for row in catalogs}
                self._is_memory_db = "memory" in catalog_names
            except Exception:
                self._is_memory_db = False
        if not self._initialized:
            self._init_schema()
            self._initialized = True

    def _get_schema_name(self) -> str:
        """Returns the full schema name, accounting for in-memory databases."""
        if self._is_memory_db:
            return f"memory.{self.SCHEMA_NAME}"
        return self.SCHEMA_NAME

    def _init_schema(self) -> None:
        """
        Initializes the database schema and tables required for query history.

        Creates the `__mockhaus__` schema, `query_history` and `query_metrics`
        tables, and associated views if they do not already exist.
        """
        if not self._connection:
            raise RuntimeError("Not connected to history database")

        schema_name = self._get_schema_name()

        # Create schema
        self._connection.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

        # Create sequence for query_history ID
        self._connection.execute(f"""
            CREATE SEQUENCE IF NOT EXISTS {schema_name}.query_history_id_seq
        """)

        # Create query_history table
        self._connection.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.query_history (
                id BIGINT PRIMARY KEY,
                query_id VARCHAR NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                session_id VARCHAR,
                connection_id VARCHAR,

                -- Query details
                original_sql TEXT NOT NULL,
                translated_sql TEXT,
                query_type VARCHAR(50),

                -- Context
                database_name VARCHAR(255),
                schema_name VARCHAR(255),
                user VARCHAR(255),
                warehouse VARCHAR(255),

                -- Execution details
                status VARCHAR(50) NOT NULL,
                error_message TEXT,
                error_code VARCHAR(50),
                execution_time_ms INTEGER,
                rows_affected INTEGER,

                -- Metadata
                client_info JSON,
                query_tags JSON
            )
        """)

        # Create query_metrics table
        self._connection.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.query_metrics (
                query_id VARCHAR PRIMARY KEY,
                parse_time_ms INTEGER,
                translation_time_ms INTEGER,
                execution_time_ms INTEGER,
                total_time_ms INTEGER,
                memory_usage_bytes BIGINT,
                cpu_usage_percent DOUBLE
            )
        """)

        # Create useful views
        self._connection.execute(f"""
            CREATE OR REPLACE VIEW {schema_name}.recent_queries AS
            SELECT * FROM {schema_name}.query_history
            ORDER BY timestamp DESC
            LIMIT 1000
        """)

        self._connection.execute(f"""
            CREATE OR REPLACE VIEW {schema_name}.query_performance AS
            SELECT
                h.*,
                m.parse_time_ms,
                m.translation_time_ms,
                m.total_time_ms
            FROM {schema_name}.query_history h
            LEFT JOIN {schema_name}.query_metrics m ON h.query_id = m.query_id
        """)

    def record_query(
        self,
        original_sql: str,
        translated_sql: str | None,
        context: QueryContext,
        execution_time_ms: float,
        status: str = "SUCCESS",
        rows_affected: int | None = None,
        error: Exception | None = None,
    ) -> str:
        """
        Records a single query execution in the history table.

        Args:
            original_sql: The original SQL query string.
            translated_sql: The translated SQL query string, if applicable.
            context: The context in which the query was executed.
            execution_time_ms: The time taken for the query to execute.
            status: The final status of the query (e.g., 'SUCCESS', 'ERROR').
            rows_affected: The number of rows affected or returned by the query.
            error: The exception object if the query failed.

        Returns:
            The unique `query_id` assigned to this query record.
        """
        if not self._connection:
            raise RuntimeError("QueryHistory connection not set. Call connect() first.")

        query_id = str(uuid.uuid4())
        query_type = self._extract_query_type(original_sql)

        # Prepare the record
        record = {
            "query_id": query_id,
            "original_sql": original_sql,
            "translated_sql": translated_sql,
            "query_type": query_type,
            "status": status,
            "execution_time_ms": int(execution_time_ms),
            "rows_affected": rows_affected,
            "session_id": context.session_id,
            "connection_id": context.connection_id,
            "database_name": context.database_name,
            "schema_name": context.schema_name,
            "user": context.user,
            "warehouse": context.warehouse,
            "client_info": json.dumps(context.client_info) if context.client_info else None,
            "query_tags": json.dumps(context.query_tags) if context.query_tags else None,
        }

        if error:
            record["error_message"] = str(error)
            record["error_code"] = type(error).__name__

        # Insert the record
        self._insert_history_record(record)

        return query_id

    def record_metrics(self, metrics: QueryMetrics) -> None:
        """
        Records performance metrics for a specific query.

        Args:
            metrics: A `QueryMetrics` object containing the performance data.
        """
        if not self._connection:
            raise RuntimeError("QueryHistory connection not set. Call connect() first.")

        assert self._connection is not None  # For mypy
        schema_name = self._get_schema_name()
        self._connection.execute(
            f"""
            INSERT INTO {schema_name}.query_metrics
            (query_id, parse_time_ms, translation_time_ms, execution_time_ms,
             total_time_ms, memory_usage_bytes, cpu_usage_percent)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
            [
                metrics.query_id,
                metrics.parse_time_ms,
                metrics.translation_time_ms,
                metrics.execution_time_ms,
                metrics.total_time_ms,
                metrics.memory_usage_bytes,
                metrics.cpu_usage_percent,
            ],
        )

    def get_recent(self, limit: int = 100) -> list[QueryRecord]:
        """
        Retrieves the most recent query records.

        Args:
            limit: The maximum number of records to retrieve.

        Returns:
            A list of `QueryRecord` objects.
        """
        if not self._connection:
            raise RuntimeError("QueryHistory connection not set. Call connect() first.")

        assert self._connection is not None  # For mypy
        schema_name = self._get_schema_name()
        result = self._connection.execute(
            f"""
            SELECT * FROM {schema_name}.recent_queries
            LIMIT ?
        """,
            [limit],
        ).fetchall()

        return [record for row in result if (record := self._row_to_record(row)) is not None]

    def search(
        self,
        text: str | None = None,
        status: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        database: str | None = None,
        query_type: str | None = None,
        limit: int = 100,
    ) -> list[QueryRecord]:
        """
        Searches the query history with various filters.

        Args:
            text: A string to search for in the original or translated SQL.
            status: Filter by query status (e.g., 'SUCCESS', 'ERROR').
            start_time: The minimum timestamp for records to include.
            end_time: The maximum timestamp for records to include.
            database: Filter by the database name.
            query_type: Filter by the type of query (e.g., 'SELECT', 'CREATE').
            limit: The maximum number of records to return.

        Returns:
            A list of matching `QueryRecord` objects.
        """
        if not self._connection:
            raise RuntimeError("QueryHistory connection not set. Call connect() first.")

        assert self._connection is not None  # For mypy
        schema_name = self._get_schema_name()
        conditions = []
        params: list[Any] = []

        if text:
            conditions.append("(original_sql ILIKE ? OR translated_sql ILIKE ?)")
            params.extend([f"%{text}%", f"%{text}%"])

        if status:
            conditions.append("status = ?")
            params.append(status)

        if start_time:
            conditions.append("timestamp >= ?")
            params.append(start_time)

        if end_time:
            conditions.append("timestamp <= ?")
            params.append(end_time)

        if database:
            conditions.append("database_name = ?")
            params.append(database)

        if query_type:
            conditions.append("query_type = ?")
            params.append(query_type)

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        result = self._connection.execute(
            f"""
            SELECT * FROM {schema_name}.query_history
            WHERE {where_clause}
            ORDER BY timestamp DESC
            LIMIT ?
        """,
            params + [limit],
        ).fetchall()

        return [record for row in result if (record := self._row_to_record(row)) is not None]

    def get_by_id(self, query_id: str) -> QueryRecord | None:
        """
        Retrieves a specific query record by its unique `query_id`.

        Args:
            query_id: The ID of the query to retrieve.

        Returns:
            A `QueryRecord` object if found, otherwise None.
        """
        if not self._connection:
            raise RuntimeError("QueryHistory connection not set. Call connect() first.")

        assert self._connection is not None  # For mypy
        schema_name = self._get_schema_name()
        result = self._connection.execute(
            f"""
            SELECT * FROM {schema_name}.query_history
            WHERE query_id = ?
        """,
            [query_id],
        ).fetchone()

        return self._row_to_record(result) if result else None

    def get_statistics(self, start_time: datetime, end_time: datetime) -> QueryStatistics:
        """
        Calculates and returns aggregated statistics for a given time period.

        Args:
            start_time: The beginning of the time period.
            end_time: The end of the time period.

        Returns:
            A `QueryStatistics` object with the aggregated data.
        """
        if not self._connection:
            raise RuntimeError("QueryHistory connection not set. Call connect() first.")

        assert self._connection is not None  # For mypy
        schema_name = self._get_schema_name()
        # Get basic stats
        stats = self._connection.execute(
            f"""
            SELECT
                COUNT(*) as total_queries,
                COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) as successful_queries,
                COUNT(CASE WHEN status != 'SUCCESS' THEN 1 END) as failed_queries,
                AVG(execution_time_ms) as avg_execution_time_ms,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY execution_time_ms) as p95_execution_time_ms
            FROM {schema_name}.query_history
            WHERE timestamp BETWEEN ? AND ?
        """,
            [start_time, end_time],
        ).fetchone()

        # Get queries by type
        by_type = self._connection.execute(
            f"""
            SELECT query_type, COUNT(*) as count
            FROM {schema_name}.query_history
            WHERE timestamp BETWEEN ? AND ?
            GROUP BY query_type
        """,
            [start_time, end_time],
        ).fetchall()

        queries_by_type = {row[0]: row[1] for row in by_type if row[0]}

        # Get errors by code
        by_error = self._connection.execute(
            f"""
            SELECT error_code, COUNT(*) as count
            FROM {schema_name}.query_history
            WHERE timestamp BETWEEN ? AND ? AND error_code IS NOT NULL
            GROUP BY error_code
        """,
            [start_time, end_time],
        ).fetchall()

        errors_by_code = {row[0]: row[1] for row in by_error}

        # Get queries by hour
        by_hour = self._connection.execute(
            f"""
            SELECT
                DATE_TRUNC('hour', timestamp) as hour,
                COUNT(*) as count
            FROM {schema_name}.query_history
            WHERE timestamp BETWEEN ? AND ?
            GROUP BY hour
            ORDER BY hour
        """,
            [start_time, end_time],
        ).fetchall()

        queries_by_hour = {str(row[0]): row[1] for row in by_hour}

        if not stats:
            return QueryStatistics(
                total_queries=0,
                successful_queries=0,
                failed_queries=0,
                avg_execution_time_ms=0.0,
                p95_execution_time_ms=0.0,
                queries_by_type={},
                errors_by_code={},
                queries_by_hour={},
            )

        return QueryStatistics(
            total_queries=int(stats[0] or 0),
            successful_queries=int(stats[1] or 0),
            failed_queries=int(stats[2] or 0),
            avg_execution_time_ms=float(stats[3] or 0),
            p95_execution_time_ms=float(stats[4] or 0),
            queries_by_type=queries_by_type,
            errors_by_code=errors_by_code,
            queries_by_hour=queries_by_hour,
        )

    def clear_history(self, before_date: datetime | None = None) -> int:
        """
        Clears query history records from the database.

        Args:
            before_date: If provided, only records older than this date will be
                         deleted. If None, all history is cleared.

        Returns:
            The number of records deleted.
        """
        if not self._connection:
            raise RuntimeError("QueryHistory connection not set. Call connect() first.")

        assert self._connection is not None  # For mypy
        schema_name = self._get_schema_name()
        # First count how many records will be deleted
        if before_date:
            count_result = self._connection.execute(
                f"""
                SELECT COUNT(*) FROM {schema_name}.query_history
                WHERE timestamp < ?
            """,
                [before_date.isoformat()],
            ).fetchone()

            self._connection.execute(
                f"""
                DELETE FROM {schema_name}.query_history
                WHERE timestamp < ?
            """,
                [before_date.isoformat()],
            )
        else:
            count_result = self._connection.execute(f"""
                SELECT COUNT(*) FROM {schema_name}.query_history
            """).fetchone()

            self._connection.execute(f"""
                DELETE FROM {schema_name}.query_history
            """)

        # Also clean up orphaned metrics
        self._connection.execute(f"""
            DELETE FROM {schema_name}.query_metrics
            WHERE query_id NOT IN (
                SELECT query_id FROM {schema_name}.query_history
            )
        """)

        return count_result[0] if count_result else 0

    def export_json(self, output_path: str, filters: dict[str, Any] | None = None) -> None:
        """
        Exports query history to a JSON file.

        Args:
            output_path: The path to the output JSON file.
            filters: Optional dictionary of filters to apply, passed to the `search` method.
        """
        queries = self.search(**(filters or {}), limit=10000)

        with open(output_path, "w") as f:
            json.dump([asdict(q) for q in queries], f, indent=2, default=str)

    def export_csv(self, output_path: str, columns: list[str] | None = None) -> None:
        """
        Exports query history to a CSV file using DuckDB's `COPY` command.

        Args:
            output_path: The path to the output CSV file.
            columns: An optional list of column names to include in the export.
        """
        if not self._connection:
            raise RuntimeError("QueryHistory connection not set. Call connect() first.")

        assert self._connection is not None  # For mypy
        schema_name = self._get_schema_name()
        columns_str = ", ".join(columns) if columns else "*"

        self._connection.execute(f"""
            COPY (
                SELECT {columns_str} FROM {schema_name}.query_history
                ORDER BY timestamp DESC
            ) TO '{output_path}' (FORMAT CSV, HEADER)
        """)

    def close(self) -> None:
        """
        Resets the history manager's state. The connection itself is managed externally.
        """
        self._connection = None
        self._initialized = False

    def _extract_query_type(self, sql: str) -> str | None:
        """A simple method to extract the query type from a SQL string."""
        sql_upper = sql.strip().upper()
        for query_type in ["SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "TRUNCATE"]:
            if sql_upper.startswith(query_type):
                return query_type
        return None

    def _insert_history_record(self, record: dict[str, Any]) -> None:
        """Inserts a new record into the `query_history` table."""
        assert self._connection is not None  # For mypy
        schema_name = self._get_schema_name()

        # Remove 'id' from record since it's generated by sequence
        record = {k: v for k, v in record.items() if k != "id"}

        columns = ["id"] + list(record.keys())
        placeholders = [f"nextval('{schema_name}.query_history_id_seq')"] + ["?" for _ in record]
        values = [record[col] for col in record]

        self._connection.execute(
            f"""
            INSERT INTO {schema_name}.query_history ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
        """,
            values,
        )

    def _row_to_record(self, row: Any) -> QueryRecord | None:
        """
        Converts a raw database row into a structured `QueryRecord` object.

        Args:
            row: The database row to convert.

        Returns:
            A `QueryRecord` object, or None if the input row is empty.
        """
        if not row:
            return None

        assert self._connection is not None  # For mypy
        # Get column names from the cursor description
        description = self._connection.description
        if description is None:
            raise RuntimeError("No cursor description available")
        columns = [desc[0] for desc in description]
        row_dict = dict(zip(columns, row, strict=False))

        # Parse JSON fields
        if row_dict.get("client_info"):
            row_dict["client_info"] = json.loads(row_dict["client_info"])
        if row_dict.get("query_tags"):
            row_dict["query_tags"] = json.loads(row_dict["query_tags"])

        return QueryRecord(**row_dict)