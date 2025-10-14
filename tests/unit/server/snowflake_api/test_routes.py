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
    assert data["status"] == "SUBMITTED"
    assert data["message"].startswith("Statement submitted:")


def test_get_statement_status_skeleton():
    """Tests that the GET /statements/{handle} endpoint returns a successful and complete response."""
    # Submit a statement first to get a valid handle
    submit_response = client.post(
        "/api/v2/statements",
        json={
            "statement": "SELECT 1;",
        },
    )
    assert submit_response.status_code == 200
    submitted_data = submit_response.json()
    handle = submitted_data["statementHandle"]

    # Immediately check status, expecting SUBMITTED
    response = client.get(f"/api/v2/statements/{handle}")
    assert response.status_code == 200
    data = response.json()

    assert data["statementHandle"] == handle
    assert data["status"] == "RUNNING" # Should be RUNNING shortly after submission
    assert data["sqlState"] == "00000"


def test_get_statement_status_not_found():
    """Tests that GET /statements/{handle} returns 404 for a non-existent handle."""
    non_existent_handle = str(uuid.uuid4())
    response = client.get(f"/api/v2/statements/{non_existent_handle}")
    assert response.status_code == 404
    assert "detail" in response.json()
    assert response.json()["detail"] == "Statement handle not found."


def test_cancel_statement_skeleton():
    """Tests that the POST /statements/{handle}/cancel endpoint returns a successful and complete response."""
    # Submit a statement first to get a valid handle
    submit_response = client.post(
        "/api/v2/statements",
        json={
            "statement": "SELECT 1;",
        },
    )
    assert submit_response.status_code == 200
    submitted_data = submit_response.json()
    handle = submitted_data["statementHandle"]

    response = client.post(f"/api/v2/statements/{handle}/cancel")
    assert response.status_code == 200
    data = response.json()

    assert data["status"] == "SUCCESS"
    assert data["message"] == f"Cancellation request for {handle} received."


def test_cancel_statement_not_found():
    """Tests that POST /statements/{handle}/cancel returns 404 for a non-existent handle."""
    non_existent_handle = str(uuid.uuid4())
    response = client.post(f"/api/v2/statements/{non_existent_handle}/cancel")
    assert response.status_code == 404
    assert "detail" in response.json()
    assert response.json()["detail"] == "Statement handle not found."
