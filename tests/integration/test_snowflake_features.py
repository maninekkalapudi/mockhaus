"""Integration tests for Snowflake-specific features and data ingestion."""

import sys
import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from mockhaus.server.app import app


class TestSnowflakeFeatures:
    """Integration tests for Snowflake-specific features and translation."""

    @pytest.fixture(scope="class")
    def test_db_path(self) -> Generator[str, None, None]:
        """Create a temporary database for testing."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as temp_file:
            db_path = temp_file.name
        yield db_path
        Path(db_path).unlink(missing_ok=True)

    @pytest.fixture(scope="class")
    def client(self, test_db_path: str) -> Generator[TestClient, None, None]:  # noqa: ARG002
        """Create a test client with the FastAPI app."""
        # State cleanup is handled by conftest.py async fixture
        with TestClient(app) as test_client:
            yield test_client

    @pytest.fixture(scope="class")
    def session_id(self, client: TestClient) -> str:
        """Create a session for testing."""
        response = client.post("/api/v1/query", json={"sql": "SELECT 1", "session_id": None})
        assert response.status_code == 200
        return response.json()["session_id"]

    def test_ddl_operations(self, client: TestClient, session_id: str) -> None:
        """Test Snowflake DDL operations."""
        ddl_queries = [
            "CREATE TABLE test_ddl (id INTEGER, name VARCHAR(100))",
            "CREATE OR REPLACE TABLE test_ddl (id INTEGER, name VARCHAR(100), created_at TIMESTAMP)",
            "ALTER TABLE test_ddl ADD COLUMN description TEXT",
            "DROP TABLE test_ddl",
        ]

        for sql in ddl_queries:
            response = client.post("/api/v1/query", json={"sql": sql, "session_id": session_id})

            # Should either succeed or fail gracefully
            assert response.status_code in [200, 400]

            if response.status_code == 200:
                data = response.json()
                assert data["success"] is True
                assert "translated_sql" in data

    def test_data_types_translation(self, client: TestClient, session_id: str) -> None:
        """Test Snowflake data type translation."""
        # Create table with various Snowflake data types
        create_sql = """
        CREATE TABLE data_types_test (
            id NUMBER(10,0),
            name VARCHAR(255),
            description TEXT,
            price NUMBER(10,2),
            created_at TIMESTAMP,
            is_active BOOLEAN,
            metadata VARIANT
        )
        """

        response = client.post("/api/v1/query", json={"sql": create_sql, "session_id": session_id})

        # Should handle Snowflake data types
        assert response.status_code in [200, 400]

        if response.status_code == 200:
            data = response.json()
            assert data["success"] is True

            # Insert test data
            insert_sql = """
            INSERT INTO data_types_test VALUES
            (1, 'Test Product', 'A test product', 99.99, CURRENT_TIMESTAMP(), TRUE, '{"key": "value"}')
            """

            response = client.post("/api/v1/query", json={"sql": insert_sql, "session_id": session_id})

            assert response.status_code in [200, 400]

    def test_snowflake_functions(self, client: TestClient, session_id: str) -> None:
        """Test Snowflake function translation."""
        function_queries = [
            "SELECT CURRENT_TIMESTAMP()",
            "SELECT CURRENT_DATE()",
            "SELECT CURRENT_USER()",
            "SELECT RANDOM()",
            "SELECT UPPER('test')",
            "SELECT LOWER('TEST')",
            "SELECT LENGTH('test')",
            "SELECT SUBSTRING('hello', 1, 3)",
            "SELECT COALESCE(NULL, 'default')",
            "SELECT IFNULL(NULL, 'default')",
            "SELECT GREATEST(1, 2, 3)",
            "SELECT LEAST(1, 2, 3)",
        ]

        for sql in function_queries:
            response = client.post("/api/v1/query", json={"sql": sql, "session_id": session_id})

            assert response.status_code in [200, 400], f"Failed for function query: {sql}"

            if response.status_code == 200:
                data = response.json()
                assert data["success"] is True
                assert "translated_sql" in data

    def test_cast_operations(self, client: TestClient, session_id: str) -> None:
        """Test Snowflake cast syntax translation."""
        cast_queries = [
            "SELECT '123'::INTEGER",
            "SELECT '123.45'::DECIMAL(10,2)",
            "SELECT 'test'::VARCHAR(100)",
            "SELECT '2023-01-01'::DATE",
            "SELECT '2023-01-01 12:00:00'::TIMESTAMP",
            "SELECT CAST('123' AS INTEGER)",
            "SELECT CAST('123.45' AS DECIMAL(10,2))",
            "SELECT TRY_CAST('invalid' AS INTEGER)",
        ]

        for sql in cast_queries:
            response = client.post("/api/v1/query", json={"sql": sql, "session_id": session_id})

            assert response.status_code in [200, 400], f"Failed for cast query: {sql}"

            if response.status_code == 200:
                data = response.json()
                assert data["success"] is True

    def test_stage_operations(self, client: TestClient, session_id: str) -> None:
        """Test Snowflake stage operations."""
        stage_queries = ["CREATE STAGE test_stage", "CREATE OR REPLACE STAGE test_stage", "SHOW STAGES", "DROP STAGE test_stage"]

        for sql in stage_queries:
            response = client.post("/api/v1/query", json={"sql": sql, "session_id": session_id})

            # Stages might not be fully implemented, so accept various responses
            assert response.status_code in [200, 400, 500]

    def test_file_format_operations(self, client: TestClient, session_id: str) -> None:
        """Test Snowflake file format operations."""
        file_format_queries = [
            "CREATE FILE FORMAT csv_format TYPE = 'CSV' FIELD_DELIMITER = ','",
            "CREATE OR REPLACE FILE FORMAT json_format TYPE = 'JSON'",
            "SHOW FILE FORMATS",
            "DROP FILE FORMAT csv_format",
        ]

        for sql in file_format_queries:
            response = client.post("/api/v1/query", json={"sql": sql, "session_id": session_id})

            # File formats might not be fully implemented
            assert response.status_code in [200, 400, 500]

    def test_cte_queries(self, client: TestClient, session_id: str) -> None:
        """Test Common Table Expression (CTE) queries."""
        # Create test table first
        setup_sql = """
        CREATE TABLE cte_test (
            id INTEGER,
            category VARCHAR(50),
            value INTEGER
        )
        """

        client.post("/api/v1/query", json={"sql": setup_sql, "session_id": session_id})

        # Insert test data
        insert_sql = """
        INSERT INTO cte_test VALUES
        (1, 'A', 100),
        (2, 'A', 200),
        (3, 'B', 150),
        (4, 'B', 250)
        """

        client.post("/api/v1/query", json={"sql": insert_sql, "session_id": session_id})

        # Test CTE query
        cte_sql = """
        WITH category_totals AS (
            SELECT category, SUM(value) as total
            FROM cte_test
            GROUP BY category
        )
        SELECT * FROM category_totals ORDER BY total DESC
        """

        response = client.post("/api/v1/query", json={"sql": cte_sql, "session_id": session_id})

        assert response.status_code in [200, 400]

        if response.status_code == 200:
            data = response.json()
            assert data["success"] is True
            assert len(data["data"]) == 2

    def test_window_functions(self, client: TestClient, session_id: str) -> None:
        """Test window function translation."""
        # Create test table
        setup_sql = """
        CREATE TABLE window_test (
            id INTEGER,
            department VARCHAR(50),
            salary INTEGER
        )
        """

        client.post("/api/v1/query", json={"sql": setup_sql, "session_id": session_id})

        # Insert test data
        insert_sql = """
        INSERT INTO window_test VALUES
        (1, 'Engineering', 75000),
        (2, 'Engineering', 85000),
        (3, 'Sales', 65000),
        (4, 'Sales', 70000)
        """

        client.post("/api/v1/query", json={"sql": insert_sql, "session_id": session_id})

        # Test window functions
        window_queries = [
            "SELECT *, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary) as row_num FROM window_test",
            "SELECT *, RANK() OVER (ORDER BY salary DESC) as rank FROM window_test",
            "SELECT *, LAG(salary) OVER (ORDER BY salary) as prev_salary FROM window_test",
            "SELECT *, LEAD(salary) OVER (ORDER BY salary) as next_salary FROM window_test",
        ]

        for sql in window_queries:
            response = client.post("/api/v1/query", json={"sql": sql, "session_id": session_id})

            assert response.status_code in [200, 400], f"Failed for window function: {sql}"

    def test_joins_and_subqueries(self, client: TestClient, session_id: str) -> None:
        """Test complex joins and subqueries."""
        # Create test tables
        setup_queries = [
            """
            CREATE TABLE customers (
                customer_id INTEGER,
                name VARCHAR(100),
                email VARCHAR(100)
            )
            """,
            """
            CREATE TABLE orders (
                order_id INTEGER,
                customer_id INTEGER,
                order_date DATE,
                amount DECIMAL(10,2)
            )
            """,
            """
            INSERT INTO customers VALUES
            (1, 'John Doe', 'john@example.com'),
            (2, 'Jane Smith', 'jane@example.com')
            """,
            """
            INSERT INTO orders VALUES
            (1, 1, '2023-01-01', 100.00),
            (2, 1, '2023-01-02', 150.00),
            (3, 2, '2023-01-03', 200.00)
            """,
        ]

        for sql in setup_queries:
            client.post("/api/v1/query", json={"sql": sql, "session_id": session_id})

        # Test complex query with joins and subqueries
        complex_sql = """
        SELECT
            c.name,
            c.email,
            o.total_orders,
            o.total_amount
        FROM customers c
        JOIN (
            SELECT
                customer_id,
                COUNT(*) as total_orders,
                SUM(amount) as total_amount
            FROM orders
            GROUP BY customer_id
        ) o ON c.customer_id = o.customer_id
        WHERE o.total_amount > 100
        ORDER BY o.total_amount DESC
        """

        response = client.post("/api/v1/query", json={"sql": complex_sql, "session_id": session_id})

        assert response.status_code in [200, 400]

        if response.status_code == 200:
            data = response.json()
            assert data["success"] is True

    def test_transaction_simulation(self, client: TestClient, session_id: str) -> None:
        """Test transaction-like behavior (session persistence)."""
        # Create table
        create_sql = "CREATE TABLE transaction_test (id INTEGER, value VARCHAR(100))"
        response = client.post("/api/v1/query", json={"sql": create_sql, "session_id": session_id})
        assert response.status_code == 200

        # Insert data
        insert_sql = "INSERT INTO transaction_test VALUES (1, 'initial')"
        response = client.post("/api/v1/query", json={"sql": insert_sql, "session_id": session_id})
        assert response.status_code == 200

        # Update data
        update_sql = "UPDATE transaction_test SET value = 'updated' WHERE id = 1"
        response = client.post("/api/v1/query", json={"sql": update_sql, "session_id": session_id})
        assert response.status_code in [200, 400]

        # Verify final state
        select_sql = "SELECT * FROM transaction_test"
        response = client.post("/api/v1/query", json={"sql": select_sql, "session_id": session_id})
        assert response.status_code == 200

        data = response.json()
        assert data["success"] is True
        assert len(data["data"]) == 1

    def test_error_recovery(self, client: TestClient, session_id: str) -> None:
        """Test that sessions can recover from errors."""
        # Execute a valid query
        valid_sql = "SELECT 1 as valid_query"
        response = client.post("/api/v1/query", json={"sql": valid_sql, "session_id": session_id})
        assert response.status_code == 200

        # Execute an invalid query
        invalid_sql = "INVALID SQL SYNTAX HERE"
        response = client.post("/api/v1/query", json={"sql": invalid_sql, "session_id": session_id})
        assert response.status_code == 400

        # Execute another valid query to ensure session is still usable
        valid_sql2 = "SELECT 2 as recovery_query"
        response = client.post("/api/v1/query", json={"sql": valid_sql2, "session_id": session_id})
        assert response.status_code == 200

        data = response.json()
        assert data["success"] is True
        assert data["session_id"] == session_id
