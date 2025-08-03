"""Tests for server state persistence across HTTP requests."""

import pytest
from fastapi.testclient import TestClient

from mockhaus.server.app import app


@pytest.fixture
def client():
    """Create test client for FastAPI app."""
    return TestClient(app)


class TestServerPersistence:
    """Test database persistence across HTTP requests in server mode."""

    def test_create_database_persists_across_requests(self, client):
        """Test that CREATE DATABASE persists for subsequent requests."""
        # Create a database
        response1 = client.post("/api/v1/query", json={"sql": "CREATE DATABASE test_persistence"})
        assert response1.status_code == 200
        result1 = response1.json()
        assert result1["success"] is True
        assert "test_persistence" in result1["data"][0]["message"]

        # Verify it shows up in SHOW DATABASES (separate request)
        response2 = client.post("/api/v1/query", json={"sql": "SHOW DATABASES"})
        assert response2.status_code == 200
        result2 = response2.json()
        assert result2["success"] is True

        db_names = [db["name"] for db in result2["data"]]
        assert "main" in db_names
        assert "test_persistence" in db_names

    def test_use_database_persists_across_requests(self, client):
        """Test that USE DATABASE state persists for subsequent requests."""
        # Create and switch to a database
        client.post("/api/v1/query", json={"sql": "CREATE DATABASE test_switch"})

        response1 = client.post("/api/v1/query", json={"sql": "USE DATABASE test_switch"})
        assert response1.status_code == 200
        assert response1.json()["current_database"] == "test_switch"

        # Verify current database persists (separate request)
        response2 = client.post("/api/v1/query", json={"sql": "SELECT 1 as test"})
        assert response2.status_code == 200
        assert response2.json()["current_database"] == "test_switch"

    def test_table_data_persists_across_databases(self, client):
        """Test that tables and data persist in separate databases."""
        # Create two databases
        client.post("/api/v1/query", json={"sql": "CREATE DATABASE db1"})
        client.post("/api/v1/query", json={"sql": "CREATE DATABASE db2"})

        # Create table in db1
        client.post("/api/v1/query", json={"sql": "USE DATABASE db1"})
        client.post("/api/v1/query", json={"sql": "CREATE TABLE users (id INT, name STRING)"})
        client.post("/api/v1/query", json={"sql": "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')"})

        # Create different table in db2
        client.post("/api/v1/query", json={"sql": "USE DATABASE db2"})
        client.post("/api/v1/query", json={"sql": "CREATE TABLE products (id INT, title STRING)"})
        client.post("/api/v1/query", json={"sql": "INSERT INTO products VALUES (100, 'Widget'), (101, 'Gadget')"})

        # Verify data isolation - users table only in db1
        client.post("/api/v1/query", json={"sql": "USE DATABASE db1"})
        response1 = client.post("/api/v1/query", json={"sql": "SELECT * FROM users"})
        assert response1.status_code == 200
        users_data = response1.json()["data"]
        assert len(users_data) == 2
        assert users_data[0]["name"] == "Alice"

        # Verify products table not accessible from db1
        response2 = client.post("/api/v1/query", json={"sql": "SELECT * FROM products"})
        assert response2.status_code == 400
        assert "does not exist" in response2.json()["detail"]["detail"]

        # Verify cross-database query works
        response3 = client.post("/api/v1/query", json={"sql": "SELECT * FROM db2.products"})
        assert response3.status_code == 200
        products_data = response3.json()["data"]
        assert len(products_data) == 2
        assert products_data[0]["title"] == "Widget"

    def test_drop_database_removes_from_subsequent_requests(self, client):
        """Test that DROP DATABASE removes database from subsequent requests."""
        # Create and verify database exists
        client.post("/api/v1/query", json={"sql": "CREATE DATABASE temp_db"})

        response1 = client.post("/api/v1/query", json={"sql": "SHOW DATABASES"})
        db_names = [db["name"] for db in response1.json()["data"]]
        assert "temp_db" in db_names

        # Drop the database
        response2 = client.post("/api/v1/query", json={"sql": "DROP DATABASE temp_db"})
        assert response2.status_code == 200
        assert response2.json()["success"] is True

        # Verify it's gone (separate request)
        response3 = client.post("/api/v1/query", json={"sql": "SHOW DATABASES"})
        db_names = [db["name"] for db in response3.json()["data"]]
        assert "temp_db" not in db_names

    def test_server_mode_environment_variable(self, client):
        """Test that server mode is properly detected via environment variable."""
        # Any query should work in server mode (in-memory)
        response = client.post("/api/v1/query", json={"sql": "CREATE DATABASE env_test"})
        assert response.status_code == 200

        # Verify it's in-memory by checking the message
        result = response.json()
        assert "in-memory" in result["data"][0]["message"]

    def test_multiple_sessions_share_databases(self, client):
        """Test that different session IDs share the same database state."""
        # Create database with one session
        response1 = client.post("/api/v1/query", json={"sql": "CREATE DATABASE shared_db", "session_id": "session1"})
        assert response1.status_code == 200
        session1_id = response1.json()["session_id"]

        # Query with different session should see the same database
        response2 = client.post("/api/v1/query", json={"sql": "SHOW DATABASES", "session_id": "session2"})
        assert response2.status_code == 200
        session2_id = response2.json()["session_id"]

        # Sessions should be different but see same databases
        assert session1_id != session2_id
        db_names = [db["name"] for db in response2.json()["data"]]
        assert "shared_db" in db_names

    def test_complex_workflow_persistence(self, client):
        """Test a complex workflow with multiple operations persisting."""
        # Step 1: Create and populate database
        client.post("/api/v1/query", json={"sql": "CREATE DATABASE workflow_test"})
        client.post("/api/v1/query", json={"sql": "USE DATABASE workflow_test"})
        client.post("/api/v1/query", json={"sql": "CREATE TABLE orders (id INT, customer STRING, amount DECIMAL(10,2))"})
        client.post("/api/v1/query", json={"sql": "INSERT INTO orders VALUES (1, 'John', 99.50), (2, 'Jane', 150.75)"})

        # Step 2: Switch to main, create lookup table
        client.post("/api/v1/query", json={"sql": "USE DATABASE main"})
        client.post("/api/v1/query", json={"sql": "CREATE TABLE customers (name STRING, email STRING)"})
        client.post("/api/v1/query", json={"sql": "INSERT INTO customers VALUES ('John', 'john@example.com'), ('Jane', 'jane@example.com')"})

        # Step 3: Perform cross-database join (separate request)
        response = client.post(
            "/api/v1/query",
            json={
                "sql": """
                SELECT o.id, o.customer, o.amount, c.email
                FROM workflow_test.orders o
                JOIN customers c ON o.customer = c.name
            """
            },
        )
        assert response.status_code == 200
        result = response.json()["data"]
        assert len(result) == 2
        assert result[0]["email"] in ["john@example.com", "jane@example.com"]

        # Step 4: Verify everything still exists (separate request)
        response2 = client.post("/api/v1/query", json={"sql": "SHOW DATABASES"})
        db_names = [db["name"] for db in response2.json()["data"]]
        assert "main" in db_names
        assert "workflow_test" in db_names
