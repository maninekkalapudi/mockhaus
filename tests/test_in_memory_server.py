"""Test in-memory server functionality."""

from mockhaus.executor import MockhausExecutor


class TestInMemoryServer:
    """Test in-memory server mode functionality."""

    def test_server_mode_detection(self):
        """Test that executor is always in-memory mode."""
        # Executor is now always in-memory
        executor = MockhausExecutor()
        assert executor.database_path is None  # Always in-memory

    def test_server_mode_forces_in_memory(self):
        """Test that executor is always in-memory regardless of parameters."""
        # Executor is always in-memory now, constructor doesn't accept database_path
        executor = MockhausExecutor()
        assert executor.database_path is None

    def test_in_memory_database_operations(self):
        """Test basic database operations in in-memory mode."""
        with MockhausExecutor() as executor:
            # Test CREATE DATABASE
            result = executor.execute_snowflake_sql("CREATE DATABASE test_db")
            assert result.success
            assert "created (in-memory)" in result.data[0]["message"]

            # Test USE DATABASE
            result = executor.execute_snowflake_sql("USE test_db")
            assert result.success
            assert "Using database" in result.data[0]["message"]

            # Test SHOW DATABASES
            result = executor.execute_snowflake_sql("SHOW DATABASES")
            assert result.success
            db_names = [db["name"] for db in result.data]
            assert "main" in db_names
            assert "test_db" in db_names

            # Test creating table in attached database
            result = executor.execute_snowflake_sql("CREATE TABLE users (id INT, name VARCHAR)")
            assert result.success

            # Test inserting data
            result = executor.execute_snowflake_sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
            assert result.success

            # Test querying data
            result = executor.execute_snowflake_sql("SELECT * FROM users ORDER BY id")
            assert result.success
            assert len(result.data) == 2
            assert result.data[0]["name"] == "Alice"
            assert result.data[1]["name"] == "Bob"

            # Test DROP DATABASE
            result = executor.execute_snowflake_sql("DROP DATABASE test_db")
            assert result.success

            # Verify database is gone
            result = executor.execute_snowflake_sql("SHOW DATABASES")
            assert result.success
            db_names = [db["name"] for db in result.data]
            assert "test_db" not in db_names

    def test_cross_database_queries(self):
        """Test queries across multiple in-memory databases."""
        with MockhausExecutor() as executor:
            # Create two databases
            executor.execute_snowflake_sql("CREATE DATABASE sales")
            executor.execute_snowflake_sql("CREATE DATABASE analytics")

            # Create tables in each database
            executor.execute_snowflake_sql("USE sales")
            executor.execute_snowflake_sql("CREATE TABLE customers (id INT, name VARCHAR)")
            executor.execute_snowflake_sql("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')")

            executor.execute_snowflake_sql("USE analytics")
            executor.execute_snowflake_sql("CREATE TABLE metrics (customer_id INT, value DECIMAL(10,2))")
            executor.execute_snowflake_sql("INSERT INTO metrics VALUES (1, 100.50), (2, 200.00)")

            # Test cross-database join from main
            executor.execute_snowflake_sql("USE main")
            result = executor.execute_snowflake_sql("""
                SELECT c.name, m.value
                FROM sales.customers c
                JOIN analytics.metrics m ON c.id = m.customer_id
                ORDER BY c.id
            """)
            assert result.success
            assert len(result.data) == 2
            assert result.data[0]["name"] == "Alice"
            assert float(result.data[0]["value"]) == 100.5

    def test_database_ddl_error_handling(self):
        """Test error handling for database DDL operations."""
        with MockhausExecutor() as executor:
            # Test creating duplicate database
            executor.execute_snowflake_sql("CREATE DATABASE test_db")
            result = executor.execute_snowflake_sql("CREATE DATABASE test_db")
            assert not result.success
            assert "already exists" in result.error

            # Test IF NOT EXISTS
            result = executor.execute_snowflake_sql("CREATE DATABASE IF NOT EXISTS test_db")
            assert result.success

            # Test dropping non-existent database
            result = executor.execute_snowflake_sql("DROP DATABASE non_existent")
            assert not result.success
            assert "does not exist" in result.error

            # Test IF EXISTS
            result = executor.execute_snowflake_sql("DROP DATABASE IF EXISTS non_existent")
            assert result.success

            # Test dropping main database
            result = executor.execute_snowflake_sql("DROP DATABASE main")
            assert not result.success
            assert "Cannot drop main database" in result.error

            # Test using non-existent database
            result = executor.execute_snowflake_sql("USE non_existent")
            assert not result.success
            assert "does not exist" in result.error
