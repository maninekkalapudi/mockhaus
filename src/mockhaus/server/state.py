"""Global server state management for persistent connections."""

import os
from collections.abc import Generator
from contextlib import contextmanager

from ..executor import MockhausExecutor


class ServerState:
    """Manages global server state including persistent database connections."""

    def __init__(self) -> None:
        self._executor: MockhausExecutor | None = None
        self._initialized = False

    def get_executor(self) -> MockhausExecutor:
        """Get or create the global executor instance."""
        if not self._initialized:
            self._initialize()

        if self._executor is None:
            raise RuntimeError("Server executor not initialized")

        return self._executor

    def _initialize(self) -> None:
        """Initialize the global executor for server mode."""
        # Ensure server mode is set
        os.environ["MOCKHAUS_SERVER_MODE"] = "true"

        # Create persistent executor
        self._executor = MockhausExecutor(database_path=None)
        self._executor.connect()

        # Create sample data in main database
        self._executor.create_sample_data()

        self._initialized = True

    def shutdown(self) -> None:
        """Shutdown the server and cleanup resources."""
        if self._executor:
            self._executor.disconnect()
            self._executor = None
        self._initialized = False


# Global server state instance
server_state = ServerState()


@contextmanager
def get_server_executor() -> Generator[MockhausExecutor, None, None]:
    """Context manager to get the persistent server executor."""
    executor = server_state.get_executor()
    try:
        yield executor
    except Exception:
        # Don't disconnect on error - keep connection alive
        raise
