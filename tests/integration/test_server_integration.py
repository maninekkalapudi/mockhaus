"""Integration tests for the server functionality."""

import sys
import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from mockhaus.server.app import app
from mockhaus.server.state import server_state


class TestServerIntegration:
    """Integration tests for the complete server functionality."""

    @pytest.fixture(scope="class")
    def test_db_path(self) -> Generator[str, None, None]:
        """Create a temporary database for testing."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as temp_file:
            db_path = temp_file.name
        yield db_path
        # Cleanup after tests
        Path(db_path).unlink(missing_ok=True)

    @pytest.fixture(scope="class")
    def client(self, test_db_path: str) -> Generator[TestClient, None, None]:  # noqa: ARG002
        """Create a test client with the FastAPI app."""
        # Reset server state for clean testing
        server_state.shutdown()

        with TestClient(app) as test_client:
            yield test_client

        # Cleanup after tests
        server_state.shutdown()

    def test_root_endpoint(self, client: TestClient) -> None:
        """Test the root endpoint returns basic server information."""
        response = client.get("/")

        assert response.status_code == 200
        data = response.json()

        assert data["name"] == "Mockhaus Server"
        assert data["version"] == "0.3.0"
        assert data["description"] == "Snowflake proxy with DuckDB backend"
        assert data["docs_url"] == "/docs"
        assert data["health_url"] == "/api/v1/health"

    def test_health_endpoint_integration(self, client: TestClient) -> None:
        """Test the health endpoint integration."""
        response = client.get("/api/v1/health")

        assert response.status_code == 200
        data = response.json()

        assert data["status"] == "healthy"
        assert data["version"] == "0.3.0"
        assert "uptime" in data
        assert isinstance(data["uptime"], int | float)
        assert data["uptime"] >= 0

    def test_query_endpoint_basic_select(self, client: TestClient) -> None:
        """Test basic SELECT query execution."""
        query_data = {"sql": "SELECT 1 as test_column", "session_id": None}

        response = client.post("/api/v1/query", json=query_data)

        assert response.status_code == 200
        data = response.json()

        assert data["success"] is True
        assert "data" in data
        assert "execution_time" in data
        assert "translated_sql" in data
        assert "session_id" in data
        assert isinstance(data["execution_time"], int | float)

    def test_query_endpoint_with_session(self, client: TestClient) -> None:
        """Test query execution with session persistence."""
        # First query to create session
        query_data = {"sql": "CREATE TABLE test_table (id INTEGER, name VARCHAR)", "session_id": None}

        response1 = client.post("/api/v1/query", json=query_data)
        assert response1.status_code == 200
        session_id = response1.json()["session_id"]

        # Second query using the same session
        query_data2 = {"sql": "INSERT INTO test_table VALUES (1, 'test')", "session_id": session_id}

        response2 = client.post("/api/v1/query", json=query_data2)
        assert response2.status_code == 200
        assert response2.json()["session_id"] == session_id

        # Third query to verify data persistence
        query_data3 = {"sql": "SELECT * FROM test_table", "session_id": session_id}

        response3 = client.post("/api/v1/query", json=query_data3)
        assert response3.status_code == 200
        data = response3.json()
        assert data["success"] is True
        assert len(data["data"]) == 1

    def test_query_endpoint_invalid_sql(self, client: TestClient) -> None:
        """Test query endpoint with invalid SQL."""
        query_data = {"sql": "INVALID SQL STATEMENT", "session_id": None}

        response = client.post("/api/v1/query", json=query_data)

        assert response.status_code == 400
        data = response.json()

        assert "detail" in data
        assert data["detail"]["success"] is False
        assert data["detail"]["error"] == "SQL_EXECUTION_ERROR"

    def test_query_endpoint_malformed_request(self, client: TestClient) -> None:
        """Test query endpoint with malformed request."""
        # Missing required 'sql' field
        response = client.post("/api/v1/query", json={})

        assert response.status_code == 422  # Pydantic validation error

    def test_snowflake_translation_features(self, client: TestClient) -> None:
        """Test Snowflake-specific SQL translation features."""
        test_cases = [
            {"sql": "SELECT CURRENT_TIMESTAMP()", "description": "Snowflake timestamp function"},
            {"sql": "SELECT 'test'::VARCHAR(100)", "description": "Snowflake cast syntax"},
            {"sql": "CREATE OR REPLACE TABLE test AS SELECT 1", "description": "Snowflake CREATE OR REPLACE"},
        ]

        for case in test_cases:
            query_data = {"sql": case["sql"], "session_id": None}

            response = client.post("/api/v1/query", json=query_data)

            # Should either succeed or fail gracefully
            assert response.status_code in [200, 400], f"Failed for {case['description']}: {case['sql']}"

            if response.status_code == 200:
                data = response.json()
                assert "translated_sql" in data
                assert data["translated_sql"] is not None

    def test_cors_middleware(self, client: TestClient) -> None:
        """Test CORS middleware is properly configured."""
        # Test preflight request
        response = client.options(
            "/api/v1/health",
            headers={"Origin": "http://localhost:3000", "Access-Control-Request-Method": "GET", "Access-Control-Request-Headers": "Content-Type"},
        )

        # CORS middleware should handle preflight
        assert response.status_code == 200

    def test_api_documentation_endpoints(self, client: TestClient) -> None:
        """Test that API documentation endpoints are accessible."""
        # Test OpenAPI spec
        response = client.get("/docs")
        assert response.status_code == 200

        # Test ReDoc
        response = client.get("/redoc")
        assert response.status_code == 200

        # Test OpenAPI JSON
        response = client.get("/openapi.json")
        assert response.status_code == 200
        openapi_spec = response.json()
        assert "openapi" in openapi_spec
        assert "info" in openapi_spec

    def test_concurrent_queries(self, client: TestClient) -> None:
        """Test handling of multiple simultaneous queries."""
        import queue
        import threading

        results = queue.Queue()

        def execute_query(sql: str, query_id: int):
            """Execute a query and store the result."""
            try:
                response = client.post("/api/v1/query", json={"sql": sql, "session_id": None})
                results.put((query_id, response.status_code, response.json()))
            except Exception as e:
                results.put((query_id, -1, str(e)))

        # Create and start threads for concurrent queries
        queries = [
            ("SELECT 1 as query1", 1),
            ("SELECT 2 as query2", 2),
            ("SELECT 3 as query3", 3),
        ]

        threads = []
        for sql, query_id in queries:
            thread = threading.Thread(target=execute_query, args=(sql, query_id))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Collect and verify results
        query_results = {}
        while not results.empty():
            query_id, status_code, data = results.get()
            query_results[query_id] = (status_code, data)

        # All queries should succeed
        assert len(query_results) == 3
        for query_id, (status_code, data) in query_results.items():
            assert status_code == 200, f"Query {query_id} failed with status {status_code}"
            assert data["success"] is True, f"Query {query_id} reported failure"

    def test_large_query_result(self, client: TestClient) -> None:
        """Test handling of queries that return large result sets."""
        # Create a query that generates multiple rows
        query_data = {"sql": "SELECT number FROM generate_series(1, 1000) AS t(number)", "session_id": None}

        response = client.post("/api/v1/query", json=query_data)

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert len(data["data"]) == 1000

    def test_server_error_handling(self, client: TestClient) -> None:
        """Test server handles various error conditions gracefully."""
        # Test with very long SQL
        long_sql = "SELECT " + ", ".join([f"{i} as col{i}" for i in range(1000)])
        query_data = {"sql": long_sql, "session_id": None}

        response = client.post("/api/v1/query", json=query_data)

        # Should either succeed or fail gracefully with proper error structure
        assert response.status_code in [200, 400, 500]

        if response.status_code != 200:
            data = response.json()
            assert "detail" in data
