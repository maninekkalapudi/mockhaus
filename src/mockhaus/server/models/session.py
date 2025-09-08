"""
This module defines the data models related to user sessions.

It includes dataclasses and enums that structure the configuration and state
of a session, such as its type (memory or persistent), storage configuration,
and lifecycle metadata like TTL and last accessed time.
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any


class SessionType(Enum):
    """
    Enumerates the types of sessions supported by the server.

    Attributes:
        MEMORY: A session that exists only in memory and is lost on termination.
        PERSISTENT: A session whose state is saved to a storage backend.
    """

    MEMORY = "memory"
    PERSISTENT = "persistent"


@dataclass
class SessionStorageConfig:
    """
    Represents the configuration for a persistent session's storage backend.

    Attributes:
        type: The type of storage backend (e.g., 'local', 'temp', 's3').
        path: The path or identifier for the storage location (e.g., a file path).
        credentials: A dictionary of credentials required to access the storage.
        options: A dictionary of other backend-specific options.
    """

    type: str  # "local", "temp", "s3", etc.
    path: str  # Storage path
    credentials: dict[str, Any] | None = None
    options: dict[str, Any] | None = None


@dataclass
class SessionConfig:
    """
    Represents the configuration and state of a single user session.

    Attributes:
        session_id: The unique identifier for the session.
        type: The type of the session (memory or persistent).
        created_at: The timestamp when the session was created.
        last_accessed: The timestamp of the last access to the session.
        ttl_seconds: The time-to-live for the session in seconds.
        metadata: A dictionary for storing arbitrary session metadata.
        storage_config: The storage configuration, for persistent sessions only.
    """

    session_id: str
    type: SessionType = SessionType.MEMORY
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    last_accessed: datetime = field(default_factory=lambda: datetime.now(UTC))
    ttl_seconds: int | None = 3600  # Default 1 hour TTL
    metadata: dict[str, Any] = field(default_factory=dict)
    storage_config: SessionStorageConfig | None = None  # For persistent sessions

    def update_last_accessed(self) -> None:
        """Update the last accessed timestamp."""
        self.last_accessed = datetime.now(UTC)

    def is_expired(self) -> bool:
        """
        Checks if the session has expired based on its TTL.

        Returns:
            True if the session has expired, False otherwise.
        """
        if self.ttl_seconds is None:
            return False

        elapsed = (datetime.now(UTC) - self.last_accessed).total_seconds()
        return elapsed > self.ttl_seconds

    def to_dict(self) -> dict[str, Any]:
        """
        Converts the session configuration to a dictionary for API responses.

        Returns:
            A dictionary representation of the session's state.
        """
        return {
            "session_id": self.session_id,
            "type": self.type.value,
            "created_at": self.created_at.isoformat(),
            "last_accessed": self.last_accessed.isoformat(),
            "ttl_seconds": self.ttl_seconds,
            "metadata": self.metadata,
            "is_expired": self.is_expired(),
        }
