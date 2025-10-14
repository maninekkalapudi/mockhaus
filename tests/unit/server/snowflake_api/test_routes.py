"""
Unit tests for the skeleton Snowflake API routes.
"""
import uuid
from fastapi.testclient import TestClient

from mockhaus.server.app import app

client = TestClient(app)


def test_submit_statement_skeleton():
    """Tests that the POST /statements endpoint returns a successful and complete response."""
    response = client.post(
        "/api/v2/statements",
        json={
            "statement": "SELECT 1;",
        },
    )
    assert response.status_code == 200
    data = response.json()
    
    assert "statementHandle" in data
    assert uuid.UUID(data["statementHandle"]) # Check that it's a valid UUID
    assert data["status"] == "SUCCEEDED"
    assert data["sqlState"] == "00000"
    assert data["message"] == "Statement executed successfully."


def test_get_statement_status_skeleton():
    """Tests that the GET /statements/{handle} endpoint returns a successful and complete response."""
    handle = str(uuid.uuid4())
    response = client.get(f"/api/v2/statements/{handle}")
    assert response.status_code == 200
    data = response.json()

    assert data["statementHandle"] == handle
    assert data["status"] == "SUCCEEDED"
    assert data["sqlState"] == "00000"


def test_cancel_statement_skeleton():
    """Tests that the POST /statements/{handle}/cancel endpoint returns a successful and complete response."""
    handle = str(uuid.uuid4())
    response = client.post(f"/api/v2/statements/{handle}/cancel")
    assert response.status_code == 200
    data = response.json()

    assert data["status"] == "SUCCESS"
    assert data["message"] == "Statement cancellation completed."