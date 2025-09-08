"""Session data models for Mockhaus server."""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any


class SessionType(Enum):
    """Types of sessions supported."""

    MEMORY = "memory"
    PERSISTENT = "persistent"


@dataclass
class SessionStorageConfig:
    """Configuration for persistent session storage."""

    type: str  # "local", "temp", "s3", etc.
    path: str  # Storage path
    credentials: dict[str, Any] | None = None
    options: dict[str, Any] | None = None


@dataclass
class SessionConfig:
    """Configuration for a session."""

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
        """Check if session has expired based on TTL."""
        if self.ttl_seconds is None:
            return False

        elapsed = (datetime.now(UTC) - self.last_accessed).total_seconds()
        return elapsed > self.ttl_seconds

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for API responses."""
        return {
            "session_id": self.session_id,
            "type": self.type.value,
            "created_at": self.created_at.isoformat(),
            "last_accessed": self.last_accessed.isoformat(),
            "ttl_seconds": self.ttl_seconds,
            "metadata": self.metadata,
            "is_expired": self.is_expired(),
        }
