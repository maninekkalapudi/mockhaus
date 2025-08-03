"""Pytest configuration and shared fixtures for all tests."""

import sys
from pathlib import Path

import pytest

# Add src to path for all test modules
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Set up the test environment."""
    # Ensure we're using the test environment
    import os

    os.environ.setdefault("MOCKHAUS_ENV", "test")

    yield

    # Cleanup after all tests
    pass


@pytest.fixture(autouse=True)
def reset_state():
    """Reset any global state before each test."""
    # Import here to avoid issues if modules aren't available
    try:
        from mockhaus.server.state import server_state

        server_state.shutdown()
    except ImportError:
        pass

    yield

    # Cleanup after test
    try:
        from mockhaus.server.state import server_state

        server_state.shutdown()
    except ImportError:
        pass


# Add pytest markers for different test types
def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line("markers", "integration: mark test as integration test")
    config.addinivalue_line("markers", "slow: mark test as slow running")
    config.addinivalue_line("markers", "unit: mark test as unit test")
    config.addinivalue_line("markers", "server: mark test as server test")


def pytest_collection_modifyitems(config, items):  # noqa: ARG001
    """Modify test items during collection."""
    # Mark tests based on their location
    for item in items:
        if "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)
        elif "unit" in str(item.fspath):
            item.add_marker(pytest.mark.unit)
        elif "server" in str(item.fspath):
            item.add_marker(pytest.mark.server)
