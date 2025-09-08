"""
This module provides a global state manager for the Mockhaus server.

It defines the `ServerState` class, which holds a singleton instance of the
`ConcurrentSessionManager`. This allows the session manager to be easily
accessed from different parts of the application (like API routes) without
using global variables directly. The state is initialized lazily on first access.
"""

import os

from .concurrent_session_manager import ConcurrentSessionManager
from .session_context import SessionContext


class ServerState:
    """
    Manages the global state of the server, primarily the session manager.

    This class follows a singleton-like pattern, providing a single point of
    access to the `ConcurrentSessionManager`. It handles the initialization
    and graceful shutdown of the session manager.
    """

    def __init__(self) -> None:
        """Initializes the ServerState."""
        self._initialized = False
        self._session_manager: ConcurrentSessionManager | None = None

    async def _initialize(self) -> None:
        """
        Initializes the session manager with configuration from environment variables.
        This method is called automatically on the first access to the session manager.
        """
        # Load configuration from environment variables
        max_sessions = int(os.environ.get("MOCKHAUS_MAX_SESSIONS", "100"))
        default_ttl = int(os.environ.get("MOCKHAUS_SESSION_TTL", "3600"))
        cleanup_interval = int(os.environ.get("MOCKHAUS_CLEANUP_INTERVAL", "300"))

        self._session_manager = ConcurrentSessionManager(max_sessions=max_sessions, default_ttl=default_ttl, cleanup_interval=cleanup_interval)
        # Start the session manager (starts background cleanup)
        await self._session_manager.start()

        self._initialized = True

    async def get_session_manager(self) -> ConcurrentSessionManager:
        """
        Returns the singleton instance of the `ConcurrentSessionManager`.

        If the manager has not been initialized, it will be initialized on the
        first call to this method.

        Returns:
            The `ConcurrentSessionManager` instance.
        """
        if not self._initialized:
            await self._initialize()

        if not self._session_manager:
            raise RuntimeError("Session manager not initialized")

        return self._session_manager

    async def get_or_create_session(self, session_id: str | None = None) -> SessionContext:
        """
        A convenience method to get or create a session context directly from the state.

        Args:
            session_id: The ID of the session to get or create.

        Returns:
            A `SessionContext` instance.
        """
        session_manager = await self.get_session_manager()
        return await session_manager.get_or_create_session(session_id)

    async def shutdown(self) -> None:
        """
        Gracefully shuts down the server state and cleans up resources.

        This method should be called during the application shutdown process to ensure
        that all active sessions are terminated and background tasks are stopped.
        """
        if self._session_manager:
            await self._session_manager.shutdown()
            self._session_manager = None
        self._initialized = False


# Global server state instance
server_state = ServerState()
