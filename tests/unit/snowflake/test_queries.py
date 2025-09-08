"""Test suite with real Snowflake SQL queries for milestone 0."""

import pytest

from mockhaus import MockhausExecutor


class TestSnowflakeQueries:
    """Test real Snowflake SQL queries against Mockhaus translator."""

    @pytest.fixture
    def executor(self):
        """Create an executor with sample data."""
        # Use in-memory history database for tests to avoid locking issues
        executor = MockhausExecutor()
        executor.connect()
        executor.create_sample_data()
        return executor

    def test_basic_select(self, executor):
        """Test 1: Basic SELECT with column selection."""
        sql = "SELECT customer_id, customer_name FROM sample_customers"
        result = executor.execute_snowflake_sql(sql)

        assert result.success
        assert result.data is not None
        assert len(result.data) == 5
        assert result.columns == ["customer_id", "customer_name"]
        assert result.data[0]["customer_name"] == "Alice Johnson"

    def test_select_with_where_numeric(self, executor):
        """Test 2: SELECT with WHERE clause on numeric column."""
        sql = "SELECT customer_name, account_balance FROM sample_customers WHERE account_balance > 1000"
        result = executor.execute_snowflake_sql(sql)

        assert result.success
        assert result.data is not None
        assert len(result.data) == 2  # Alice and Diana
        assert all(row["account_balance"] > 1000 for row in result.data)

    def test_select_with_where_string(self, executor):
        """Test 3: SELECT with WHERE clause on string column."""
        sql = "SELECT customer_id, customer_name FROM sample_customers WHERE customer_name = 'Bob Smith'"
        result = executor.execute_snowflake_sql(sql)

        assert result.success
        assert result.data is not None
        assert len(result.data) == 1
        assert result.data[0]["customer_name"] == "Bob Smith"
        assert result.data[0]["customer_id"] == 2

    def test_select_with_order_by(self, executor):
        """Test 4: SELECT with ORDER BY clause."""
        sql = "SELECT customer_name, signup_date FROM sample_customers ORDER BY signup_date DESC"
        result = executor.execute_snowflake_sql(sql)

        assert result.success
        assert result.data is not None
        assert len(result.data) == 5
        # First row should be the latest signup (Eve Davis - 2023-05-12)
        assert result.data[0]["customer_name"] == "Eve Davis"

    def test_aggregate_count(self, executor):
        """Test 5: Aggregate function - COUNT."""
        sql = "SELECT COUNT(*) as total_customers FROM sample_customers"
        result = executor.execute_snowflake_sql(sql)

        assert result.success
        assert result.data is not None
        assert len(result.data) == 1
        assert result.data[0]["total_customers"] == 5

    def test_aggregate_multiple(self, executor):
        """Test 6: Multiple aggregate functions."""
        sql = """
        SELECT
            COUNT(*) as total_customers,
            AVG(account_balance) as avg_balance,
            MAX(account_balance) as max_balance,
            MIN(account_balance) as min_balance
        FROM sample_customers
        """
        result = executor.execute_snowflake_sql(sql)

        assert result.success
        assert result.data is not None
        assert len(result.data) == 1
        assert result.data[0]["total_customers"] == 5
        assert result.data[0]["max_balance"] == 3750.00
        assert result.data[0]["min_balance"] == -50.25

    def test_select_with_boolean_filter(self, executor):
        """Test 7: SELECT with boolean column filter."""
        sql = "SELECT customer_name, is_active FROM sample_customers WHERE is_active = true"
        result = executor.execute_snowflake_sql(sql)

        assert result.success
        assert result.data is not None
        assert len(result.data) == 4  # All except Charlie Brown
        assert all(row["is_active"] for row in result.data)

    def test_date_functions(self, executor):
        """Test 8: Date functions."""
        sql = "SELECT customer_name, signup_date, CURRENT_DATE as today FROM sample_customers LIMIT 1"
        result = executor.execute_snowflake_sql(sql)

        assert result.success
        assert result.data is not None
        assert len(result.data) == 1
        assert "today" in result.data[0]
        # DuckDB returns date objects, so convert to string for comparison
        signup_date = result.data[0]["signup_date"]
        assert str(signup_date) == "2023-01-15"  # Alice's signup date

    def test_select_with_limit(self, executor):
        """Test 9: SELECT with LIMIT clause."""
        sql = "SELECT customer_name FROM sample_customers ORDER BY customer_id LIMIT 3"
        result = executor.execute_snowflake_sql(sql)

        assert result.success
        assert result.data is not None
        assert len(result.data) == 3
        expected_names = ["Alice Johnson", "Bob Smith", "Charlie Brown"]
        actual_names = [row["customer_name"] for row in result.data]
        assert actual_names == expected_names

    def test_complex_where_clause(self, executor):
        """Test 10: Complex WHERE clause with multiple conditions."""
        sql = """
        SELECT customer_name, account_balance, is_active
        FROM sample_customers
        WHERE account_balance > 0 AND is_active = true
        ORDER BY account_balance DESC
        """
        result = executor.execute_snowflake_sql(sql)

        assert result.success
        assert result.data is not None
        assert len(result.data) == 3  # Diana, Alice, Eve (Bob has 0 balance)
        # Check that results are ordered by balance descending
        balances = [row["account_balance"] for row in result.data]
        assert balances == sorted(balances, reverse=True)
        # Ensure all are active and have positive balance
        for row in result.data:
            assert row["is_active"] is True
            assert row["account_balance"] > 0


class TestTranslationDetails:
    """Test translation details and edge cases."""

    def test_case_insensitive_keywords(self):
        """Test that case-insensitive SQL keywords work."""
        executor = MockhausExecutor()
        executor.connect()
        executor.create_sample_data()

        # Test mixed case SQL
        sql = "select Customer_Name, Account_Balance from Sample_Customers where Account_Balance > 1000"
        result = executor.execute_snowflake_sql(sql)

        assert result.success
        assert result.data is not None
        assert len(result.data) == 2

    def test_translation_info(self):
        """Test that translation information is captured."""
        executor = MockhausExecutor()
        executor.connect()
        executor.create_sample_data()

        sql = "SELECT customer_id FROM sample_customers LIMIT 1"
        result = executor.execute_snowflake_sql(sql)

        assert result.success
        assert result.original_sql == sql
        assert result.translated_sql is not None
        assert "SELECT" in result.translated_sql
        assert result.execution_time_ms > 0

    def test_error_handling(self):
        """Test error handling for invalid SQL."""
        executor = MockhausExecutor()
        executor.connect()

        # Invalid SQL
        sql = "SELECT * FROM nonexistent_table"
        result = executor.execute_snowflake_sql(sql)

        assert not result.success
        assert result.error is not None
        assert result.original_sql == sql
