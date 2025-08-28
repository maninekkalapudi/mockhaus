"""Unit tests for MockhausExecutor with query history integration."""

from mockhaus.executor import MockhausExecutor
from mockhaus.query_history import QueryContext


class TestExecutorWithHistory:
    """Unit tests for MockhausExecutor query history integration."""

    def test_executor_records_history(self):
        """Test that executor records queries in history."""
        context = QueryContext(session_id="test-session", database_name="test_db", user="test_user")
        executor = MockhausExecutor(query_context=context)
        executor.connect()

        # Execute a query
        result = executor.execute_snowflake_sql("SELECT 1 as test")
        assert result.success

        # Check history was recorded via the executor's history
        records = executor._history.get_recent(limit=1)

        assert len(records) == 1
        assert records[0].original_sql == "SELECT 1 as test"
        assert records[0].status == "SUCCESS"
        assert records[0].rows_affected == 1

        executor.disconnect()

    def test_executor_records_errors(self):
        """Test that executor records failed queries."""
        context = QueryContext(session_id="test-session", database_name="test_db", user="test_user")
        executor = MockhausExecutor(query_context=context)
        executor.connect()

        # Execute a failing query
        result = executor.execute_snowflake_sql("SELECT * FROM nonexistent_table")
        assert not result.success

        # Check error was recorded
        records = executor._history.get_recent(limit=1)

        assert len(records) == 1
        assert records[0].status == "ERROR"
        assert records[0].error_message is not None

        executor.disconnect()

    def test_executor_with_context(self):
        """Test executor with custom query context."""
        context = QueryContext(session_id="test-session", user="test_user", database_name="test_db")

        executor = MockhausExecutor(query_context=context)
        executor.connect()

        # Execute a query
        executor.execute_snowflake_sql("SELECT 1")

        # Check context was recorded
        records = executor._history.get_recent(limit=1)
        assert len(records) == 1
        assert records[0].session_id == "test-session"
        assert records[0].user == "test_user"
        assert records[0].database_name == "test_db"

        executor.disconnect()
