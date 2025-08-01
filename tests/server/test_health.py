"""Tests for health check endpoints."""

from fastapi.testclient import TestClient

from mockhaus.server.app import app

client = TestClient(app)


def test_health_endpoint():
    """Test the health check endpoint returns proper status."""
    response = client.get("/api/v1/health")
    
    assert response.status_code == 200
    
    data = response.json()
    assert data["status"] == "healthy"
    assert data["version"] == "0.3.0"
    assert "uptime" in data
    assert isinstance(data["uptime"], (int, float))
    assert data["uptime"] >= 0


def test_root_endpoint():
    """Test the root endpoint returns server information."""
    response = client.get("/")
    
    assert response.status_code == 200
    
    data = response.json()
    assert data["name"] == "Mockhaus Server"
    assert data["version"] == "0.3.0"
    assert data["description"] == "Snowflake proxy with DuckDB backend"
    assert data["docs_url"] == "/docs"
    assert data["health_url"] == "/api/v1/health"