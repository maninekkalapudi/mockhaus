"""Tests for query history functionality."""

import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from mockhaus.executor import MockhausExecutor
from mockhaus.query_history import QueryContext, QueryHistory, QueryMetrics


class TestQueryHistory:
    """Test cases for QueryHistory class."""

    @pytest.fixture
    def temp_history_db(self):
        """Create a temporary database path for testing."""
        import os

        # Create a temporary directory and generate a db path within it
        temp_dir = tempfile.mkdtemp()
        db_path = os.path.join(temp_dir, "test_history.duckdb")
        yield db_path

        # Cleanup
        import shutil

        shutil.rmtree(temp_dir, ignore_errors=True)

    @pytest.fixture
    def history(self, temp_history_db):
        """Create a QueryHistory instance with temporary database."""
        history = QueryHistory(temp_history_db)
        history.connect()
        yield history
        history.close()

    def test_init_schema(self, history):
        """Test that schema and tables are created correctly."""
        # Check schema exists
        result = history._connection.execute(
            "SELECT schema_name FROM information_schema.schemata WHERE schema_name = ?", [QueryHistory.SCHEMA_NAME]
        ).fetchone()
        assert result is not None

        # Check tables exist
        tables = ["query_history", "query_metrics"]
        for table in tables:
            result = history._connection.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_name = ?", [QueryHistory.SCHEMA_NAME, table]
            ).fetchone()
            assert result is not None

    def test_record_query_success(self, history):
        """Test recording a successful query."""
        context = QueryContext(session_id="test-session", database_name="test_db", user="test_user")

        query_id = history.record_query(
            original_sql="SELECT * FROM users",
            translated_sql="SELECT * FROM users",
            context=context,
            execution_time_ms=100.5,
            status="SUCCESS",
            rows_affected=10,
        )

        # Verify the query was recorded
        assert query_id is not None

        record = history.get_by_id(query_id)
        assert record is not None
        assert record.original_sql == "SELECT * FROM users"
        assert record.status == "SUCCESS"
        assert record.execution_time_ms == 100
        assert record.rows_affected == 10
        assert record.session_id == "test-session"
        assert record.database_name == "test_db"
        assert record.user == "test_user"

    def test_record_query_error(self, history):
        """Test recording a failed query."""
        context = QueryContext()
        error = ValueError("Table not found")

        query_id = history.record_query(
            original_sql="SELECT * FROM nonexistent",
            translated_sql="SELECT * FROM nonexistent",
            context=context,
            execution_time_ms=50.0,
            status="ERROR",
            error=error,
        )

        record = history.get_by_id(query_id)
        assert record is not None
        assert record.status == "ERROR"
        assert record.error_message == "Table not found"
        assert record.error_code == "ValueError"

    def test_record_metrics(self, history):
        """Test recording performance metrics."""
        # First record a query
        query_id = history.record_query(original_sql="SELECT 1", translated_sql="SELECT 1", context=QueryContext(), execution_time_ms=10.0)

        # Record metrics
        metrics = QueryMetrics(query_id=query_id, parse_time_ms=5, translation_time_ms=3, execution_time_ms=10, total_time_ms=18)
        history.record_metrics(metrics)

        # Verify metrics were recorded
        result = history._connection.execute(f"SELECT * FROM {QueryHistory.SCHEMA_NAME}.query_metrics WHERE query_id = ?", [query_id]).fetchone()

        assert result is not None
        assert result[1] == 5  # parse_time_ms
        assert result[2] == 3  # translation_time_ms
        assert result[3] == 10  # execution_time_ms
        assert result[4] == 18  # total_time_ms

    def test_get_recent(self, history):
        """Test getting recent queries."""
        # Record multiple queries
        for i in range(5):
            history.record_query(original_sql=f"SELECT {i}", translated_sql=f"SELECT {i}", context=QueryContext(), execution_time_ms=i * 10)

        # Get recent queries
        records = history.get_recent(limit=3)
        assert len(records) == 3

        # Verify we got the correct number and all queries are represented
        query_sqls = {record.original_sql for record in records}
        assert len(query_sqls) == 3

        # All queries should contain "SELECT"
        for record in records:
            assert "SELECT" in record.original_sql

    def test_search(self, history):
        """Test searching queries."""
        # Record test data
        context = QueryContext(database_name="test_db")

        history.record_query(
            original_sql="SELECT * FROM users WHERE active = true",
            translated_sql="SELECT * FROM users WHERE active = true",
            context=context,
            execution_time_ms=100,
            status="SUCCESS",
        )

        history.record_query(
            original_sql="INSERT INTO users VALUES (1, 'test')",
            translated_sql="INSERT INTO users VALUES (1, 'test')",
            context=context,
            execution_time_ms=50,
            status="SUCCESS",
        )

        history.record_query(
            original_sql="SELECT * FROM products",
            translated_sql="SELECT * FROM products",
            context=context,
            execution_time_ms=75,
            status="ERROR",
            error=Exception("Table not found"),
        )

        # Search by text
        results = history.search(text="users")
        assert len(results) == 2

        # Search by status
        results = history.search(status="ERROR")
        assert len(results) == 1
        assert "products" in results[0].original_sql

        # Search by query type
        results = history.search(query_type="INSERT")
        assert len(results) == 1
        assert results[0].query_type == "INSERT"

        # Search by database
        results = history.search(database="test_db")
        assert len(results) == 3

    def test_get_statistics(self, history):
        """Test getting query statistics."""
        now = datetime.now(UTC)

        # Record test queries
        for i in range(10):
            history.record_query(
                original_sql=f"SELECT {i}",
                translated_sql=f"SELECT {i}",
                context=QueryContext(),
                execution_time_ms=i * 10,
                status="SUCCESS" if i < 8 else "ERROR",
                error=Exception("Test error") if i >= 8 else None,
            )

        stats = history.get_statistics(start_time=now - timedelta(hours=1), end_time=now + timedelta(hours=1))

        assert stats.total_queries == 10
        assert stats.successful_queries == 8
        assert stats.failed_queries == 2
        assert stats.avg_execution_time_ms == 45.0  # (0+10+20+...+90)/10
        assert stats.queries_by_type["SELECT"] == 10
        assert "Exception" in stats.errors_by_code
        assert stats.errors_by_code["Exception"] == 2

    def test_clear_history(self, history):
        """Test clearing query history."""
        # Record some queries
        for i in range(5):
            history.record_query(original_sql=f"SELECT {i}", translated_sql=f"SELECT {i}", context=QueryContext(), execution_time_ms=10)

        # Clear all history
        count = history.clear_history()
        assert count == 5

        # Verify history is empty
        records = history.get_recent()
        assert len(records) == 0

    def test_clear_history_before_date(self, history):
        """Test clearing history before a specific date."""
        # Record queries with different timestamps
        now = datetime.now(UTC)

        # Manually insert with specific timestamps
        for i in range(5):
            days_ago = 5 - i
            timestamp = now - timedelta(days=days_ago)

            history._connection.execute(
                f"""
                INSERT INTO {QueryHistory.SCHEMA_NAME}.query_history
                (id, query_id, timestamp, original_sql, status, execution_time_ms)
                VALUES (nextval('{QueryHistory.SCHEMA_NAME}.query_history_id_seq'), ?, ?, ?, ?, ?)
            """,
                [f"query-{i}", timestamp, f"SELECT {i}", "SUCCESS", 10],
            )

        # Clear queries older than 2 days (this will remove queries 0, 1, 2)
        cutoff = now - timedelta(days=2)
        count = history.clear_history(before_date=cutoff)

        assert count == 3  # Queries 0, 1, 2 should be deleted

        records = history.get_recent()
        assert len(records) == 2  # Queries 3, 4 should remain

    def test_export_json(self, history, tmp_path):
        """Test exporting history to JSON."""
        # Record test data
        for i in range(3):
            history.record_query(original_sql=f"SELECT {i}", translated_sql=f"SELECT {i}", context=QueryContext(), execution_time_ms=10)

        # Export to JSON
        output_file = tmp_path / "history.json"
        history.export_json(str(output_file))

        # Verify file was created and contains data
        assert output_file.exists()

        import json

        with open(output_file) as f:
            data = json.load(f)

        assert len(data) == 3
        assert all("query_id" in record for record in data)

    def test_export_csv(self, history, tmp_path):
        """Test exporting history to CSV."""
        # Record test data
        for i in range(3):
            history.record_query(original_sql=f"SELECT {i}", translated_sql=f"SELECT {i}", context=QueryContext(), execution_time_ms=10)

        # Export to CSV
        output_file = tmp_path / "history.csv"
        history.export_csv(str(output_file))

        # Verify file was created
        assert output_file.exists()

        # Read and verify CSV content
        import csv

        with open(output_file) as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 3

    def test_extract_query_type(self, history):
        """Test query type extraction."""
        test_cases = [
            ("SELECT * FROM users", "SELECT"),
            ("  INSERT INTO users VALUES (1)", "INSERT"),
            ("UPDATE users SET name = 'test'", "UPDATE"),
            ("DELETE FROM users WHERE id = 1", "DELETE"),
            ("CREATE TABLE test (id INT)", "CREATE"),
            ("DROP TABLE test", "DROP"),
            ("ALTER TABLE users ADD COLUMN age INT", "ALTER"),
            ("TRUNCATE TABLE users", "TRUNCATE"),
            ("EXPLAIN SELECT * FROM users", None),
        ]

        for sql, expected_type in test_cases:
            result = history._extract_query_type(sql)
            assert result == expected_type


class TestExecutorWithHistory:
    """Test query history integration with MockhausExecutor."""

    @pytest.fixture
    def temp_db(self):
        """Create temporary databases for testing."""
        import os
        import shutil

        # Create a temporary directory and generate db paths within it
        temp_dir = tempfile.mkdtemp()
        main_db = os.path.join(temp_dir, "main.duckdb")
        history_db = os.path.join(temp_dir, "history.duckdb")

        yield main_db, history_db

        # Cleanup
        shutil.rmtree(temp_dir, ignore_errors=True)

    def test_executor_records_history(self, temp_db):
        """Test that executor records queries in history."""
        main_db, history_db = temp_db

        executor = MockhausExecutor(database_path=main_db, enable_history=True, history_db_path=history_db)

        # Execute a query
        result = executor.execute_snowflake_sql("SELECT 1 as test")
        assert result.success

        # Check history was recorded
        history = QueryHistory(history_db)
        records = history.get_recent(limit=1)

        assert len(records) == 1
        assert records[0].original_sql == "SELECT 1 as test"
        assert records[0].status == "SUCCESS"
        assert records[0].rows_affected == 1

        history.close()
        executor.disconnect()

    def test_executor_records_errors(self, temp_db):
        """Test that executor records failed queries."""
        main_db, history_db = temp_db

        executor = MockhausExecutor(database_path=main_db, enable_history=True, history_db_path=history_db)

        # Execute a failing query
        result = executor.execute_snowflake_sql("SELECT * FROM nonexistent_table")
        assert not result.success

        # Check error was recorded
        history = QueryHistory(history_db)
        records = history.get_recent(limit=1)

        assert len(records) == 1
        assert records[0].status == "ERROR"
        assert records[0].error_message is not None

        history.close()
        executor.disconnect()

    def test_executor_with_context(self, temp_db):
        """Test executor with custom query context."""
        main_db, history_db = temp_db

        context = QueryContext(session_id="test-session", user="test_user", database_name="test_db")

        executor = MockhausExecutor(database_path=main_db, enable_history=True, history_db_path=history_db, query_context=context)

        # Execute a query
        executor.execute_snowflake_sql("SELECT 1")

        # Check context was recorded
        history = QueryHistory(history_db)
        records = history.get_recent(limit=1)

        assert len(records) == 1
        assert records[0].session_id == "test-session"
        assert records[0].user == "test_user"
        assert records[0].database_name == "test_db"

        history.close()
        executor.disconnect()

    def test_executor_history_disabled(self, temp_db):
        """Test executor with history disabled."""
        main_db, history_db = temp_db

        executor = MockhausExecutor(database_path=main_db, enable_history=False)

        # Execute a query
        result = executor.execute_snowflake_sql("SELECT 1")
        assert result.success

        # Verify no history was created
        assert not Path(history_db).exists()

        executor.disconnect()
