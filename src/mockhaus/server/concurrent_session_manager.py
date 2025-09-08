"""
This module provides a manager for handling multiple, isolated user sessions.

It defines the `ConcurrentSessionManager`, which is responsible for creating,
managing, and terminating `SessionContext` objects. Each session provides an
isolated environment with its own DuckDB database connection and state, allowing
for concurrent use of the Mockhaus server without interference between users.

The manager handles session lifecycle, including TTL-based expiration, LRU eviction
under memory pressure, and graceful shutdown.
"""

import asyncio
import contextlib
import logging
import uuid

from .models.session import SessionConfig, SessionStorageConfig, SessionType
from .session_context import SessionContext
from .storage import LocalFileBackend, StorageBackend, StorageConfig, TempFileBackend

logger = logging.getLogger(__name__)


class ConcurrentSessionManager:
    """
    Manages multiple concurrent and isolated user sessions.

    This class is the core of the server's multi-tenancy capability. It maintains
    a dictionary of active sessions, handles session creation with different storage
    backends, and enforces policies like maximum session limits and TTLs.
    """

    def __init__(self, max_sessions: int = 100, default_ttl: int = 3600, cleanup_interval: int = 300):
        """
        Initializes the session manager.

        Args:
            max_sessions: The maximum number of concurrent sessions allowed.
            default_ttl: The default time-to-live for sessions in seconds.
            cleanup_interval: The interval in seconds for the background task
                              to run and clean up expired sessions.
        """
        self._sessions: dict[str, SessionContext] = {}
        self._sessions_lock = asyncio.Lock()
        self._max_sessions = max_sessions
        self._default_ttl = default_ttl
        self._cleanup_interval = cleanup_interval
        self._cleanup_task: asyncio.Task | None = None
        self._shutdown_event = asyncio.Event()
        self._started = False

    async def get_or_create_session(
        self,
        session_id: str | None = None,
        ttl_seconds: int | None = None,
        session_type: SessionType = SessionType.MEMORY,
        storage_config: SessionStorageConfig | None = None,
    ) -> SessionContext:
        """
        Gets an existing session or creates a new one.

        If a `session_id` is provided and valid, it returns the existing session.
        Otherwise, it creates a new session with the specified configuration.
        If the session limit is reached, it attempts to clean up expired sessions
        or evict the least recently used one.

        Args:
            session_id: An optional ID to retrieve an existing session.
            ttl_seconds: An optional TTL for a new session.
            session_type: The type of session to create (in-memory or persistent).
            storage_config: Configuration for the storage backend if the session
                            is persistent.

        Returns:
            The `SessionContext` for the requested or newly created session.

        Raises:
            RuntimeError: If the maximum number of sessions is reached and no
                          session can be evicted.
            ValueError: If a persistent session is requested without storage config.
        """
        async with self._sessions_lock:
            # If session_id provided, try to get existing session
            if session_id and session_id in self._sessions:
                context = self._sessions[session_id]
                if context.is_active():
                    context.config.update_last_accessed()
                    return context
                # Session expired or inactive, remove it
                await self._remove_session(session_id)

            # Create new session
            if len(self._sessions) >= self._max_sessions:
                # Try to clean up expired sessions first
                await self._cleanup_expired_sessions()

                # If still at limit after cleanup, try LRU eviction
                if len(self._sessions) >= self._max_sessions:
                    evicted = await self._evict_lru_session()
                    if not evicted:
                        raise RuntimeError(f"Maximum number of sessions ({self._max_sessions}) reached and no sessions could be evicted")
                    logger.info("Evicted LRU session to make room for new session")

            # Generate new session ID if not provided
            if not session_id:
                session_id = str(uuid.uuid4())

            # Create storage backend if needed
            storage_backend = None
            if session_type == SessionType.PERSISTENT:
                if not storage_config:
                    raise ValueError("Persistent session requires storage configuration")
                storage_backend = await self._create_storage_backend(storage_config)

            # Create new session config
            config = SessionConfig(
                session_id=session_id, type=session_type, ttl_seconds=ttl_seconds or self._default_ttl, storage_config=storage_config
            )

            # Create new session context with storage backend
            context = SessionContext(config, storage_backend)
            self._sessions[session_id] = context

            logger.info(f"Created {session_type.value} session {session_id}")

            return context

    async def get_session(self, session_id: str) -> SessionContext | None:
        """
        Gets an existing session by its ID.

        Args:
            session_id: The ID of the session to retrieve.

        Returns:
            The `SessionContext` if it is found and active, otherwise None.
        """
        async with self._sessions_lock:
            if session_id in self._sessions:
                context = self._sessions[session_id]
                if context.is_active():
                    return context
                # Remove expired session
                await self._remove_session(session_id)
            return None

    async def terminate_session(self, session_id: str) -> bool:
        """
        Terminate and remove a session.

        Args:
            session_id: Session ID to terminate

        Returns:
            True if session was found and terminated
        """
        async with self._sessions_lock:
            return await self._remove_session(session_id)

    async def list_sessions(self) -> dict[str, dict]:
        """
        List all active sessions.

        Returns:
            Dictionary of session info by session ID
        """
        async with self._sessions_lock:
            sessions_info = {}
            for session_id, context in list(self._sessions.items()):
                if context.is_active():
                    sessions_info[session_id] = context.get_info()
                else:
                    # Remove expired session while we're here
                    await self._remove_session(session_id)
            return sessions_info

    async def cleanup_expired_sessions(self) -> int:
        """
        Clean up all expired sessions.

        Returns:
            Number of sessions cleaned up
        """
        async with self._sessions_lock:
            return await self._cleanup_expired_sessions()

    async def _cleanup_expired_sessions(self) -> int:
        """Internal method to clean up expired sessions (no lock)."""
        expired_ids = []
        for session_id, context in self._sessions.items():
            if not context.is_active():
                expired_ids.append(session_id)

        for session_id in expired_ids:
            await self._remove_session(session_id)

        return len(expired_ids)

    async def _remove_session(self, session_id: str) -> bool:
        """Internal method to remove a session (no lock)."""
        if session_id in self._sessions:
            context = self._sessions[session_id]
            await context.close()
            del self._sessions[session_id]
            return True
        return False

    async def _create_storage_backend(self, storage_config: SessionStorageConfig) -> StorageBackend:
        """
        Creates a storage backend instance based on the provided configuration.

        Args:
            storage_config: The configuration for the storage backend.

        Returns:
            An initialized `StorageBackend` instance.
        
        Raises:
            ValueError: If the storage type is unsupported.
        """
        storage_type = storage_config.type.lower()

        # Create storage config for backend
        backend_config = StorageConfig(
            type=storage_type, path=storage_config.path, credentials=storage_config.credentials, options=storage_config.options
        )

        # Create appropriate backend
        backend: StorageBackend
        if storage_type == "local":
            backend = LocalFileBackend(backend_config)
        elif storage_type == "temp":
            backend = TempFileBackend(backend_config)
        else:
            raise ValueError(f"Unsupported storage type: {storage_type}")

        # Initialize the backend
        await backend.initialize()
        logger.info(f"Created {storage_type} storage backend at {storage_config.path}")

        return backend

    async def _evict_lru_session(self) -> bool:
        """
        Evicts the least recently used (LRU) session to make space.

        A session is eligible for eviction only if it is not currently locked
        (i.e., not in the middle of an operation).

        Returns:
            True if a session was successfully evicted, False otherwise.
        """
        if not self._sessions:
            return False

        # Find the LRU session (oldest last_accessed time)
        lru_session_id = None
        oldest_time = None

        for session_id, context in self._sessions.items():
            # Skip if session is being actively used (lock is held)
            if context.lock.locked():
                continue

            last_accessed = context.config.last_accessed
            if oldest_time is None or last_accessed < oldest_time:
                oldest_time = last_accessed
                lru_session_id = session_id

        # Evict the LRU session if found
        if lru_session_id:
            logger.info(f"Evicting LRU session {lru_session_id} (last accessed: {oldest_time})")
            await self._remove_session(lru_session_id)
            return True

        # No session could be evicted (all are locked/in use)
        return False

    async def start(self) -> None:
        """
        Starts the session manager and its background cleanup task.
        """
        if self._started:
            return

        logger.info(f"Starting session manager with cleanup interval {self._cleanup_interval}s")
        self._started = True
        self._shutdown_event.clear()

        # Start background cleanup task
        self._cleanup_task = asyncio.create_task(self._background_cleanup())

    async def shutdown(self) -> None:
        """
        Shuts down the session manager, terminating all active sessions and stopping
        background tasks.
        """
        if not self._started:
            return

        logger.info("Shutting down session manager")
        self._started = False
        self._shutdown_event.set()

        # Stop background cleanup task
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            with contextlib.suppress(TimeoutError, asyncio.CancelledError, RuntimeError):
                # Use timeout to avoid hanging, ignore cancellation/timeout errors during cleanup
                await asyncio.wait_for(self._cleanup_task, timeout=1.0)

        # Clean up all sessions
        async with self._sessions_lock:
            logger.info(f"Cleaning up {len(self._sessions)} active sessions")
            for session_id in list(self._sessions.keys()):
                await self._remove_session(session_id)

    async def _background_cleanup(self) -> None:
        """
        A background task that periodically cleans up expired sessions.
        """
        logger.info("Starting background session cleanup task")

        while not self._shutdown_event.is_set():
            try:
                # Wait for cleanup interval or shutdown
                try:
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=self._cleanup_interval)
                    # Shutdown was signaled
                    break
                except TimeoutError:
                    # Timeout reached, perform cleanup
                    pass

                # Perform cleanup
                cleaned_count = await self.cleanup_expired_sessions()
                if cleaned_count > 0:
                    logger.info(f"Background cleanup removed {cleaned_count} expired sessions")

            except asyncio.CancelledError:
                logger.info("Background cleanup task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in background cleanup: {e}")
                # Continue running despite errors
                await asyncio.sleep(60)  # Wait 1 minute before retrying

        logger.info("Background session cleanup task stopped")

    def get_stats(self) -> dict:
        """
        Returns statistics about the session manager's state.

        Returns:
            A dictionary containing statistics like active sessions, max sessions,
            and cleanup task status.
        """
        # Calculate session usage percentage
        usage_percentage = (len(self._sessions) / self._max_sessions * 100) if self._max_sessions > 0 else 0

        return {
            "active_sessions": len(self._sessions),
            "max_sessions": self._max_sessions,
            "usage_percentage": round(usage_percentage, 2),
            "default_ttl": self._default_ttl,
            "cleanup_interval": self._cleanup_interval,
            "background_cleanup_running": self._cleanup_task is not None and not self._cleanup_task.done(),
            "started": self._started,
            "eviction_policy": "LRU (Least Recently Used)",
        }

    def get_session_details(self) -> list[dict]:
        """
        Returns detailed information about all active sessions for monitoring.

        Returns:
            A list of dictionaries, where each dictionary contains detailed
            information about a single session.
        """
        session_details = []
        for _session_id, context in self._sessions.items():
            info = context.get_info()
            info["locked"] = context.lock.locked()
            session_details.append(info)

        # Sort by last_accessed (most recent first)
        session_details.sort(key=lambda x: x["last_accessed"], reverse=True)
        return session_details