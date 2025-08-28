"""Global server state management for session-based architecture."""

import os

from .concurrent_session_manager import ConcurrentSessionManager
from .session_context import SessionContext


class ServerState:
    """Manages global server state for session-based architecture."""

    def __init__(self) -> None:
        self._initialized = False
        self._session_manager: ConcurrentSessionManager | None = None

    async def _initialize(self) -> None:
        """Initialize the session manager."""
        # Load configuration from environment variables
        max_sessions = int(os.environ.get("MOCKHAUS_MAX_SESSIONS", "100"))
        default_ttl = int(os.environ.get("MOCKHAUS_SESSION_TTL", "3600"))
        cleanup_interval = int(os.environ.get("MOCKHAUS_CLEANUP_INTERVAL", "300"))

        self._session_manager = ConcurrentSessionManager(max_sessions=max_sessions, default_ttl=default_ttl, cleanup_interval=cleanup_interval)
        # Start the session manager (starts background cleanup)
        await self._session_manager.start()

        self._initialized = True

    async def get_session_manager(self) -> ConcurrentSessionManager:
        """Get the session manager."""
        if not self._initialized:
            await self._initialize()

        if not self._session_manager:
            raise RuntimeError("Session manager not initialized")

        return self._session_manager

    async def get_or_create_session(self, session_id: str | None = None) -> SessionContext:
        """Get or create a session context."""
        session_manager = await self.get_session_manager()
        return await session_manager.get_or_create_session(session_id)

    async def shutdown(self) -> None:
        """Shutdown the server and cleanup resources."""
        if self._session_manager:
            await self._session_manager.shutdown()
            self._session_manager = None
        self._initialized = False


# Global server state instance
server_state = ServerState()
