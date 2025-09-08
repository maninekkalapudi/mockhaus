"""Tests for session management endpoints."""

from fastapi.testclient import TestClient

from mockhaus.server.app import app


class TestSessionEndpoints:
    """Test session management API endpoints."""

    def test_create_memory_session(self):
        """Test creating a memory session."""
        with TestClient(app) as client:
            response = client.post("/api/v1/sessions", json={"type": "memory"})
            assert response.status_code == 200

            data = response.json()
            assert data["success"] is True
            assert "session" in data
            assert "session_id" in data["session"]
            assert data["session"]["type"] == "memory"
            assert data["session"]["is_active"] is True
            assert "created_at" in data["session"]
            assert "last_accessed" in data["session"]
            assert data["session"]["ttl_seconds"] == 3600  # Default TTL

    def test_create_memory_session_with_custom_id(self):
        """Test creating a memory session with custom ID."""
        custom_id = "test-session-123"
        with TestClient(app) as client:
            response = client.post("/api/v1/sessions", json={"session_id": custom_id, "type": "memory"})
            assert response.status_code == 200

            data = response.json()
            assert data["success"] is True
            assert data["session"]["session_id"] == custom_id
            assert data["session"]["type"] == "memory"

    def test_create_memory_session_with_ttl(self):
        """Test creating a memory session with custom TTL."""
        with TestClient(app) as client:
            response = client.post(
                "/api/v1/sessions",
                json={
                    "type": "memory",
                    "ttl_seconds": 300,  # 5 minutes
                },
            )
            assert response.status_code == 200

            data = response.json()
            assert data["session"]["ttl_seconds"] == 300

    def test_create_persistent_session(self):
        """Test creating a persistent session with local storage."""
        with TestClient(app) as client:
            response = client.post("/api/v1/sessions", json={"type": "persistent", "storage": {"type": "local", "path": "/tmp/test_session.db"}})
            assert response.status_code == 200

            data = response.json()
            assert data["success"] is True
            assert data["session"]["type"] == "persistent"

    def test_create_persistent_session_without_storage_fails(self):
        """Test that creating persistent session without storage config fails."""
        with TestClient(app) as client:
            response = client.post("/api/v1/sessions", json={"type": "persistent"})
            assert response.status_code == 400
            assert "storage configuration" in response.json()["detail"]["detail"]

    def test_list_sessions_empty_initially(self):
        """Test listing sessions shows proper structure."""
        with TestClient(app) as client:
            response = client.get("/api/v1/sessions")
            assert response.status_code == 200

            data = response.json()
            assert data["success"] is True
            assert "sessions" in data
            assert "session_details" in data
            assert "statistics" in data
            assert "limits" in data

            # Check statistics structure
            stats = data["statistics"]
            assert "active_sessions" in stats
            assert "max_sessions" in stats
            assert "usage_percentage" in stats

    def test_list_sessions_with_active_sessions(self):
        """Test listing sessions with active sessions."""
        with TestClient(app) as client:
            # Create a few sessions
            session1 = client.post("/api/v1/sessions", json={"type": "memory"})
            session2 = client.post("/api/v1/sessions", json={"type": "memory"})

            session1_id = session1.json()["session"]["session_id"]
            session2_id = session2.json()["session"]["session_id"]

            # List sessions
            response = client.get("/api/v1/sessions")
            assert response.status_code == 200

            data = response.json()
            session_details = data["session_details"]
            assert len(session_details) >= 2

            # Check that our sessions are in the list
            session_ids = [s["session_id"] for s in session_details]
            assert session1_id in session_ids
            assert session2_id in session_ids

    def test_get_session_info(self):
        """Test getting session information."""
        with TestClient(app) as client:
            # Create a session
            create_response = client.post("/api/v1/sessions", json={"type": "memory"})
            session_id = create_response.json()["session"]["session_id"]

            # Get session info
            response = client.get(f"/api/v1/sessions/{session_id}")
            assert response.status_code == 200

            data = response.json()
            assert data["success"] is True
            assert "session" in data
            assert data["session"]["session_id"] == session_id
            assert data["session"]["type"] == "memory"
            assert data["session"]["is_active"] is True

    def test_get_nonexistent_session_info(self):
        """Test getting info for nonexistent session."""
        with TestClient(app) as client:
            response = client.get("/api/v1/sessions/nonexistent-session")
            assert response.status_code == 404
            assert "not found" in response.json()["detail"]["detail"].lower()

    def test_terminate_session(self):
        """Test terminating a session."""
        with TestClient(app) as client:
            # Create a session
            create_response = client.post("/api/v1/sessions", json={"type": "memory"})
            session_id = create_response.json()["session"]["session_id"]

            # Terminate the session
            response = client.delete(f"/api/v1/sessions/{session_id}")
            assert response.status_code == 200

            data = response.json()
            assert data["success"] is True
            assert session_id in data["message"]

            # Verify session is gone
            get_response = client.get(f"/api/v1/sessions/{session_id}")
            assert get_response.status_code == 404

    def test_terminate_nonexistent_session(self):
        """Test terminating nonexistent session."""
        with TestClient(app) as client:
            response = client.delete("/api/v1/sessions/nonexistent-session")
            assert response.status_code == 404
            assert "not found" in response.json()["detail"]["detail"].lower()

    def test_cleanup_expired_sessions(self):
        """Test manual cleanup of expired sessions."""
        with TestClient(app) as client:
            response = client.post("/api/v1/sessions/cleanup")
            assert response.status_code == 200

            data = response.json()
            assert data["success"] is True
            assert "sessions_cleaned" in data
            assert isinstance(data["sessions_cleaned"], int)
            assert "message" in data

    def test_session_query_integration(self):
        """Test that sessions work properly with query execution."""
        with TestClient(app) as client:
            # Create a session
            session_response = client.post("/api/v1/sessions", json={"type": "memory"})
            session_id = session_response.json()["session"]["session_id"]

            # Execute a query in the session
            query_response = client.post("/api/v1/query", json={"sql": "SELECT 1 as test_value", "session_id": session_id})
            assert query_response.status_code == 200

            query_data = query_response.json()
            assert query_data["session_id"] == session_id
            assert query_data["success"] is True
            assert len(query_data["data"]) == 1
            assert query_data["data"][0]["test_value"] == 1

    def test_session_isolation(self):
        """Test that different sessions have isolated data."""
        with TestClient(app) as client:
            # Create two sessions
            session1_response = client.post("/api/v1/sessions", json={"type": "memory"})
            session2_response = client.post("/api/v1/sessions", json={"type": "memory"})

            session1_id = session1_response.json()["session"]["session_id"]
            session2_id = session2_response.json()["session"]["session_id"]

            # Create a table in session 1
            client.post("/api/v1/query", json={"sql": "CREATE TABLE test_isolation (id INT, value VARCHAR)", "session_id": session1_id})

            # Insert data in session 1
            client.post("/api/v1/query", json={"sql": "INSERT INTO test_isolation VALUES (1, 'session1')", "session_id": session1_id})

            # Try to query the table from session 2 (should fail)
            query2_response = client.post("/api/v1/query", json={"sql": "SELECT * FROM test_isolation", "session_id": session2_id})
            assert query2_response.status_code == 400  # Table doesn't exist in session 2

            # Verify session 1 still has the data
            query1_response = client.post("/api/v1/query", json={"sql": "SELECT * FROM test_isolation", "session_id": session1_id})
            assert query1_response.status_code == 200

            data = query1_response.json()["data"]
            assert len(data) == 1
            assert data[0]["value"] == "session1"

    def test_invalid_session_type(self):
        """Test creating session with invalid type."""
        with TestClient(app) as client:
            response = client.post("/api/v1/sessions", json={"type": "invalid"})
            assert response.status_code == 400

    def test_malformed_create_session_request(self):
        """Test creating session with malformed request."""
        with TestClient(app) as client:
            # Missing type defaults to memory
            response = client.post("/api/v1/sessions", json={})
            assert response.status_code == 200  # Default type is "memory"
            assert response.json()["session"]["type"] == "memory"

            # Invalid JSON
            response = client.post("/api/v1/sessions", content="invalid json", headers={"Content-Type": "application/json"})
            assert response.status_code == 422

    def test_session_statistics_and_limits(self):
        """Test that session statistics and limits are properly reported."""
        with TestClient(app) as client:
            # Create a session
            client.post("/api/v1/sessions", json={"type": "memory"})

            # Get session list with statistics
            response = client.get("/api/v1/sessions")
            assert response.status_code == 200

            data = response.json()
            stats = data["statistics"]
            limits = data["limits"]

            # Verify statistics structure
            assert stats["active_sessions"] >= 1
            assert stats["max_sessions"] > 0
            assert 0 <= stats["usage_percentage"] <= 100
            assert "eviction_policy" in stats

            # Verify limits structure
            assert limits["max_sessions"] > 0
            assert limits["active_sessions"] >= 1
            assert limits["available_slots"] >= 0
            assert 0 <= limits["usage_percentage"] <= 100

    def test_session_metadata(self):
        """Test that session metadata is properly handled."""
        with TestClient(app) as client:
            # Create a session
            response = client.post("/api/v1/sessions", json={"type": "memory"})
            assert response.status_code == 200

            session_data = response.json()["session"]
            assert "metadata" in session_data
            assert isinstance(session_data["metadata"], dict)

    def test_session_expiry_status(self):
        """Test that session expiry status is properly reported."""
        with TestClient(app) as client:
            # Create a session
            response = client.post("/api/v1/sessions", json={"type": "memory"})
            assert response.status_code == 200

            session_data = response.json()["session"]
            assert "is_expired" in session_data
            assert session_data["is_expired"] is False  # Should not be expired immediately
            assert session_data["is_active"] is True
