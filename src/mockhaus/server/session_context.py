"""
This module defines the context for a single, isolated user session.

It contains the `SessionContext` class, which encapsulates a `MockhausExecutor`
instance, a session configuration, and an optional storage backend. Each session
runs in its own context, ensuring that database connections, transactions, and
state are not shared between concurrent users. It also manages the lifecycle
of the executor and handles thread-safe execution of SQL queries.
"""

import asyncio
import contextlib
import logging
from typing import Any

from ..executor import MockhausExecutor
from .models.session import SessionConfig, SessionType
from .storage import StorageBackend

logger = logging.getLogger(__name__)


class SessionContext:
    """
    Manages the context for a single user session, providing an isolated
    environment with its own DuckDB connection and state.

    This class is responsible for creating and managing a `MockhausExecutor`
    instance that is unique to the session. It handles both in-memory and
    persistent sessions, using a storage backend to manage the database file
    for the latter. All operations within the session are protected by an
    asynchronous lock to ensure thread safety.
    """

    def __init__(self, config: SessionConfig, storage_backend: StorageBackend | None = None):
        """
        Initializes the session context.

        Args:
            config: The configuration object for the session.
            storage_backend: An optional storage backend for persistent sessions.
        """
        self.config = config
        self.storage_backend = storage_backend
        self.lock = asyncio.Lock()  # For thread safety
        self._executor: MockhausExecutor | None = None
        self._is_active = True
        self._db_path: str | None = None

    @property
    def session_id(self) -> str:
        """Returns the unique identifier for the session."""
        return self.config.session_id

    async def get_executor(self) -> MockhausExecutor:
        """
        Gets or creates the `MockhausExecutor` for this session.

        If the executor has not yet been created, this method initializes it,
        connecting to either an in-memory database or a persistent file based
        on the session's configuration.

        Returns:
            The `MockhausExecutor` instance for this session.
        """
        if self._executor is None:
            # Create isolated executor for this session
            self._executor = MockhausExecutor()

            if self.config.type == SessionType.MEMORY:
                # For memory sessions, use in-memory database
                self._executor.connect()  # Uses :memory: by default
                logger.debug(f"Session {self.session_id}: Connected to in-memory database")
            elif self.config.type == SessionType.PERSISTENT:
                # For persistent sessions, use storage backend
                if not self.storage_backend:
                    raise RuntimeError(f"Session {self.session_id}: Persistent session requires storage backend")

                # Get the database file path from storage
                self._db_path = await self.storage_backend.get_database_path()

                # Connect to the persistent database file
                if self._db_path is None:
                    raise RuntimeError(f"Session {self.session_id}: Storage backend returned None for database path")
                self._executor.database_path = self._db_path
                self._executor.connect()
                logger.info(f"Session {self.session_id}: Connected to persistent database at {self._db_path}")

        return self._executor

    async def execute_sql(self, sql: str) -> dict[str, Any]:
        """
        Executes a SQL query within this session's context with thread safety.

        This method acquires a lock to ensure that only one operation can be
        performed at a time within the session. It updates the session's last
        accessed time and handles syncing data to storage for persistent sessions.

        Args:
            sql: The SQL query string to execute.

        Returns:
            A dictionary containing the result of the execution.
        """
        async with self.lock:
            # Update last accessed time
            self.config.update_last_accessed()

            # Execute the SQL
            try:
                executor = await self.get_executor()
                result = executor.execute_snowflake_sql(sql)

                # For persistent sessions, sync after write operations
                if self.config.type == SessionType.PERSISTENT and self.storage_backend:
                    # Check if this was a write operation (simple heuristic)
                    sql_lower = sql.lower().strip()
                    if any(sql_lower.startswith(op) for op in ["create", "insert", "update", "delete", "drop", "alter", "copy"]):
                        await self.storage_backend.sync_to_storage()
                        logger.debug(f"Session {self.session_id}: Synced to storage after write operation")
                return {
                    "success": result.success,
                    "data": result.data if result.data else [],
                    "columns": result.columns if result.columns else [],
                    "row_count": result.row_count,
                    "translated_sql": result.translated_sql,
                    "execution_time_ms": result.execution_time_ms,
                    "session_id": self.session_id,
                    "error": result.error,
                }
            except Exception as e:
                return {
                    "success": False,
                    "error": str(e),
                    "session_id": self.session_id,
                    "data": [],
                    "columns": [],
                    "row_count": 0,
                    "translated_sql": "",
                    "execution_time_ms": 0.0,
                }

    def is_expired(self) -> bool:
        """Check if session has expired."""
        return self.config.is_expired()

    def is_active(self) -> bool:
        """Check if session is still active."""
        return self._is_active and not self.is_expired()

    async def close(self) -> None:
        """
        Closes the session and cleans up all associated resources.

        This includes syncing any final data for persistent sessions, disconnecting
        the database executor, and cleaning up the storage backend.
        """
        async with self.lock:
            self._is_active = False

            # Sync persistent sessions before closing
            if self.config.type == SessionType.PERSISTENT and self.storage_backend:
                try:
                    await self.storage_backend.sync_to_storage()
                    logger.debug(f"Session {self.session_id}: Final sync before close")
                except Exception as e:
                    logger.error(f"Session {self.session_id}: Failed to sync on close: {e}")

            # Close executor
            if self._executor:
                with contextlib.suppress(Exception):
                    # Ignore all errors during cleanup
                    self._executor.disconnect()
                self._executor = None

            # Cleanup storage backend
            if self.storage_backend:
                try:
                    await self.storage_backend.cleanup()
                except Exception as e:
                    logger.error(f"Session {self.session_id}: Failed to cleanup storage: {e}")

    def get_info(self) -> dict[str, Any]:
        """
        Returns a dictionary with information about the session.

        Returns:
            A dictionary containing the session's configuration and active status.
        """
        info = self.config.to_dict()
        info["is_active"] = self._is_active
        return info

    async def __aenter__(self) -> "SessionContext":
        """Async context manager entry."""
        await self.lock.acquire()
        self.config.update_last_accessed()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        self.lock.release()
