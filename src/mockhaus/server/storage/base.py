"""Base storage backend abstraction for persistent sessions."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass
class StorageConfig:
    """Configuration for storage backend."""

    type: str  # "local", "s3", "gcs", "azure", "vercel"
    path: str  # Storage path/location
    credentials: dict[str, Any] | None = None
    options: dict[str, Any] | None = None


class StorageBackend(ABC):
    """Abstract base class for session storage backends."""

    def __init__(self, config: StorageConfig):
        """Initialize storage backend with configuration."""
        self.config = config
        self._local_cache_path: str | None = None

    @abstractmethod
    async def initialize(self) -> None:
        """Initialize the storage backend (create directories, connect to service, etc.)."""
        pass

    @abstractmethod
    async def get_database_path(self) -> str:
        """
        Get the local path to the database file.
        For remote storage, this downloads the file to a local cache.

        Returns:
            Local file path to the DuckDB database
        """
        pass

    @abstractmethod
    async def sync_to_storage(self) -> None:
        """
        Sync local changes back to the storage backend.
        For local storage, this is a no-op.
        For remote storage, this uploads the file.
        """
        pass

    @abstractmethod
    async def exists(self) -> bool:
        """Check if the database file exists in storage."""
        pass

    @abstractmethod
    async def delete(self) -> None:
        """Delete the database file from storage."""
        pass

    @abstractmethod
    async def cleanup(self) -> None:
        """Clean up local resources (temp files, connections, etc.)."""
        pass

    @abstractmethod
    def get_info(self) -> dict[str, Any]:
        """Get information about the storage backend."""
        pass
