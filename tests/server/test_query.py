"""Tests for query execution endpoints."""

from fastapi.testclient import TestClient

from mockhaus.server.app import app

client = TestClient(app)


def test_query_endpoint_select():
    """Test the query endpoint with a basic SELECT query."""
    response = client.post("/api/v1/query", json={"sql": "SELECT 1 as test_column"})

    assert response.status_code == 200

    data = response.json()
    assert data["success"]
    assert "data" in data
    assert "execution_time" in data
    assert isinstance(data["execution_time"], int | float)
    assert data["execution_time"] >= 0


def test_query_endpoint_with_sample_data():
    """Test the query endpoint with sample data."""
    # First create a session and sample data
    create_session_response = client.post("/api/v1/sessions", json={"type": "memory"})
    assert create_session_response.status_code == 200
    session_data = create_session_response.json()
    assert session_data["success"]
    session_id = session_data["session"]["session_id"]

    # Create sample table within the session
    create_table_response = client.post(
        "/api/v1/query",
        json={
            "sql": """CREATE TABLE sample_customers (
            customer_id INTEGER,
            customer_name VARCHAR(100),
            email VARCHAR(255)
        )""",
            "session_id": session_id,
        },
    )
    assert create_table_response.status_code == 200

    # Insert sample data
    insert_response = client.post(
        "/api/v1/query",
        json={
            "sql": """INSERT INTO sample_customers VALUES
        (1, 'Alice Johnson', 'alice@example.com'),
        (2, 'Bob Smith', 'bob@example.com'),
        (3, 'Charlie Brown', 'charlie@example.com')""",
            "session_id": session_id,
        },
    )
    assert insert_response.status_code == 200

    # Now query the sample data
    response = client.post("/api/v1/query", json={"sql": "SELECT COUNT(*) as customer_count FROM sample_customers", "session_id": session_id})

    assert response.status_code == 200

    data = response.json()
    assert data["success"]
    assert data["data"] is not None
    assert len(data["data"]) > 0
    assert data["data"][0]["customer_count"] == 3


def test_query_endpoint_ddl():
    """Test the query endpoint with DDL (CREATE STAGE)."""
    response = client.post("/api/v1/query", json={"sql": "CREATE STAGE test_stage URL = 's3://test-bucket/data/'"})

    assert response.status_code == 200

    data = response.json()
    assert data["success"]


def test_query_endpoint_invalid_sql():
    """Test the query endpoint with invalid SQL."""
    response = client.post("/api/v1/query", json={"sql": "INVALID SQL STATEMENT"})

    assert response.status_code == 400

    data = response.json()
    assert "detail" in data
    assert not data["detail"]["success"]
    assert data["detail"]["error"] == "SQL_EXECUTION_ERROR"


def test_query_endpoint_empty_sql():
    """Test the query endpoint with empty SQL."""
    response = client.post("/api/v1/query", json={"sql": ""})

    assert response.status_code == 422  # Validation error


def test_query_endpoint_missing_sql():
    """Test the query endpoint with missing SQL field."""
    response = client.post("/api/v1/query", json={})

    assert response.status_code == 422  # Validation error


def test_query_endpoint_with_database():
    """Test the query endpoint with explicit database parameter."""
    response = client.post(
        "/api/v1/query",
        json={
            "sql": "SELECT 1 as test",
            "database": None,  # In-memory database
        },
    )

    assert response.status_code == 200

    data = response.json()
    assert data["success"]
