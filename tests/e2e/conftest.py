"""Pytest configuration and fixtures for E2E tests."""

import shutil
import sys
import tempfile
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from mockhaus.server.app import app


class E2EClient:
    """Enhanced test client for E2E operations."""

    def __init__(self, test_client: TestClient) -> None:
        """Initialize the E2E client."""
        self.client = test_client
        self.active_sessions: list[str] = []

    def create_session(self, session_type: str = "memory", ttl_seconds: int = 3600, **kwargs: Any) -> dict[str, Any]:
        """Create a new session for testing."""
        session_data = {"type": session_type, "ttl_seconds": ttl_seconds, **kwargs}

        response = self.client.post("/api/v1/sessions", json=session_data)
        assert response.status_code == 200, f"Failed to create session: {response.text}"

        result = response.json()
        session_id = result["session"]["session_id"]
        self.active_sessions.append(session_id)

        return result["session"]

    def execute_query(self, sql: str, session_id: str) -> dict[str, Any]:
        """Execute a SQL query in a specific session."""
        query_data = {"sql": sql, "session_id": session_id}

        response = self.client.post("/api/v1/query", json=query_data)

        if response.status_code != 200:
            # For debugging, return error details
            return {
                "success": False,
                "error": response.json() if response.status_code != 500 else {"detail": response.text},
                "status_code": response.status_code,
            }

        return response.json()

    def get_session_info(self, session_id: str) -> dict[str, Any]:
        """Get information about a session."""
        response = self.client.get(f"/api/v1/sessions/{session_id}")
        assert response.status_code == 200, f"Failed to get session info: {response.text}"
        return response.json()["session"]

    def list_sessions(self) -> dict[str, Any]:
        """List all active sessions."""
        response = self.client.get("/api/v1/sessions")
        assert response.status_code == 200, f"Failed to list sessions: {response.text}"
        return response.json()

    def terminate_session(self, session_id: str) -> None:
        """Terminate a specific session."""
        response = self.client.delete(f"/api/v1/sessions/{session_id}")
        assert response.status_code == 200, f"Failed to terminate session: {response.text}"

        if session_id in self.active_sessions:
            self.active_sessions.remove(session_id)

    def cleanup_all_sessions(self) -> None:
        """Clean up all active sessions created by this client."""
        for session_id in self.active_sessions.copy():
            try:
                self.terminate_session(session_id)
            except Exception as e:
                print(f"Warning: Failed to cleanup session {session_id}: {e}")

        self.active_sessions.clear()


class SQLWorkflowExecutor:
    """Executor for SQL workflow scripts."""

    def __init__(self, e2e_client: E2EClient, session_id: str) -> None:
        """Initialize the workflow executor."""
        self.client = e2e_client
        self.session_id = session_id
        self.execution_log: list[dict[str, Any]] = []

    def execute_sql_file(self, sql_file_path: Path) -> list[dict[str, Any]]:
        """Execute a complete SQL file step by step."""
        sql_content = sql_file_path.read_text()

        # Split SQL into individual statements (basic implementation)
        statements = self._split_sql_statements(sql_content)

        results = []
        for i, statement in enumerate(statements):
            if statement.strip():
                step_name = f"Step {i + 1}"
                result = self.execute_sql_step(statement, step_name)
                results.append(result)

                # Stop execution if there's an error
                if not result.get("success", False):
                    print(f"Stopping execution at {step_name} due to error: {result.get('error')}")
                    break

        return results

    def execute_sql_step(self, sql: str, step_name: str) -> dict[str, Any]:
        """Execute a single SQL step."""
        result = self.client.execute_query(sql, self.session_id)

        # Add step metadata
        result["step_name"] = step_name
        result["sql"] = sql

        # Log the execution
        self.execution_log.append(result)

        return result

    def get_execution_summary(self) -> dict[str, Any]:
        """Get a summary of all executed steps."""
        total_steps = len(self.execution_log)
        successful_steps = sum(1 for step in self.execution_log if step.get("success", False))
        failed_steps = total_steps - successful_steps

        return {"total_steps": total_steps, "successful_steps": successful_steps, "failed_steps": failed_steps, "execution_log": self.execution_log}

    def _split_sql_statements(self, sql_content: str) -> list[str]:
        """Split SQL content into individual statements."""
        # Remove comments and empty lines
        lines = []
        for line in sql_content.split("\n"):
            line = line.strip()
            if line and not line.startswith("--"):
                lines.append(line)

        # Join lines and split by semicolon
        full_sql = " ".join(lines)
        return [stmt.strip() for stmt in full_sql.split(";") if stmt.strip()]


class DataValidator:
    """Validator for data integrity checks."""

    def __init__(self, e2e_client: E2EClient, session_id: str) -> None:
        """Initialize the data validator."""
        self.client = e2e_client
        self.session_id = session_id

    def validate_record_count(self, table: str, expected: int) -> bool:
        """Validate the record count in a table."""
        result = self.client.execute_query(f"SELECT COUNT(*) as count FROM {table}", self.session_id)

        if not result.get("success", False):
            print(f"Failed to query table {table}: {result.get('error')}")
            return False

        actual_count = result["data"][0]["count"]
        return actual_count == expected

    def get_record_count(self, table: str) -> int:
        """Get the actual record count from a table."""
        result = self.client.execute_query(f"SELECT COUNT(*) as count FROM {table}", self.session_id)

        if not result.get("success", False):
            raise ValueError(f"Failed to query table {table}: {result.get('error')}")

        return result["data"][0]["count"]

    def get_table_data(self, table: str, order_by: str = None) -> list[dict[str, Any]]:
        """Get all data from a table."""
        sql = f"SELECT * FROM {table}"
        if order_by:
            sql += f" ORDER BY {order_by}"

        result = self.client.execute_query(sql, self.session_id)

        if not result.get("success", False):
            raise ValueError(f"Failed to query table {table}: {result.get('error')}")

        return result["data"]

    def validate_data_integrity(self, table: str, constraints: dict[str, Any]) -> bool:
        """Validate data integrity constraints."""
        try:
            data = self.get_table_data(table)

            for constraint_name, constraint_check in constraints.items():
                if not constraint_check(data):
                    print(f"Data integrity check failed: {constraint_name}")
                    return False

            return True
        except Exception as e:
            print(f"Data integrity validation failed: {e}")
            return False


@pytest.fixture(scope="function")
def e2e_client() -> Generator[E2EClient, None, None]:
    """Create an E2E test client with automatic cleanup."""
    with TestClient(app) as test_client:
        client = E2EClient(test_client)
        yield client
        # Cleanup all sessions created during the test
        client.cleanup_all_sessions()


@pytest.fixture(scope="function")
def temp_stage_files() -> Generator[tuple[Path, Path], None, None]:
    """Create temporary CSV files for staging operations."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Copy fixture files to temporary directory
        fixtures_dir = Path(__file__).parent / "fixtures" / "data_pipeline"

        initial_file = temp_path / "customer_initial_data.csv"
        incremental_file = temp_path / "customer_incremental_data.csv"

        shutil.copy2(fixtures_dir / "customer_initial_data.csv", initial_file)
        shutil.copy2(fixtures_dir / "customer_incremental_data.csv", incremental_file)

        yield initial_file, incremental_file


@pytest.fixture(scope="function")
def workflow_executor(e2e_client: E2EClient) -> Generator[tuple[SQLWorkflowExecutor, str], None, None]:
    """Create a workflow executor with an active session."""
    # Create a memory session for the test
    session = e2e_client.create_session(type="memory")
    session_id = session["session_id"]

    executor = SQLWorkflowExecutor(e2e_client, session_id)
    yield executor, session_id


@pytest.fixture(scope="function")
def data_validator(e2e_client: E2EClient) -> Generator[tuple[DataValidator, str], None, None]:
    """Create a data validator with an active session."""
    # Create a memory session for the test
    session = e2e_client.create_session(type="memory")
    session_id = session["session_id"]

    validator = DataValidator(e2e_client, session_id)
    yield validator, session_id


# Configure E2E-specific pytest markers
def pytest_configure(config):
    """Configure E2E-specific pytest markers."""
    config.addinivalue_line("markers", "e2e: mark test as end-to-end test (may be slow)")
    config.addinivalue_line("markers", "data_pipeline: mark test as data pipeline E2E test")
