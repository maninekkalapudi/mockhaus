"""Tests for query execution endpoints."""

import pytest
from fastapi.testclient import TestClient

from mockhaus.server.app import app

client = TestClient(app)


def test_query_endpoint_select():
    """Test the query endpoint with a basic SELECT query."""
    response = client.post("/api/v1/query", json={"sql": "SELECT 1 as test_column"})

    assert response.status_code == 200

    data = response.json()
    assert data["success"] == True
    assert "data" in data
    assert "execution_time" in data
    assert isinstance(data["execution_time"], (int, float))
    assert data["execution_time"] >= 0


def test_query_endpoint_with_sample_data():
    """Test the query endpoint with sample data."""
    response = client.post("/api/v1/query", json={"sql": "SELECT COUNT(*) as customer_count FROM sample_customers"})

    assert response.status_code == 200

    data = response.json()
    assert data["success"] == True
    assert data["data"] is not None
    assert len(data["data"]) > 0


def test_query_endpoint_ddl():
    """Test the query endpoint with DDL (CREATE STAGE)."""
    response = client.post("/api/v1/query", json={"sql": "CREATE STAGE test_stage URL = 's3://test-bucket/data/'"})

    assert response.status_code == 200

    data = response.json()
    assert data["success"] == True


def test_query_endpoint_invalid_sql():
    """Test the query endpoint with invalid SQL."""
    response = client.post("/api/v1/query", json={"sql": "INVALID SQL STATEMENT"})

    assert response.status_code == 400

    data = response.json()
    assert "detail" in data
    assert data["detail"]["success"] == False
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
    assert data["success"] == True
