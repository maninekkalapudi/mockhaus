"""
This module defines the abstract base classes and data models for storage backends.

It provides the `StorageBackend` abstract base class (ABC), which defines the
common interface that all storage implementations (e.g., local, S3, temporary)
must adhere to. This ensures that the session management layer can interact
with different storage mechanisms in a consistent way.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass
class StorageConfig:
    """
    Represents the configuration for a storage backend.

    Attributes:
        type: The type of the storage backend (e.g., 'local', 's3').
        path: The path or location identifier for the storage (e.g., a file path
              or an S3 bucket key).
        credentials: A dictionary of credentials needed to access the storage.
        options: A dictionary of other backend-specific options.
    """

    type: str  # "local", "s3", "gcs", "azure", "vercel"
    path: str  # Storage path/location
    credentials: dict[str, Any] | None = None
    options: dict[str, Any] | None = None


class StorageBackend(ABC):
    """
    An abstract base class for session storage backends.

    This class defines the contract for all storage backends that manage the
    persistence of session data, particularly the DuckDB database file.
    """

    def __init__(self, config: StorageConfig):
        """
        Initializes the storage backend with its configuration.

        Args:
            config: A `StorageConfig` object with the backend details.
        """
        self.config = config
        self._local_cache_path: str | None = None

    @abstractmethod
    async def initialize(self) -> None:
        """
        Initializes the storage backend.

        This method should handle any setup required, such as creating directories
        or connecting to a remote service.
        """
        pass

    @abstractmethod
    async def get_database_path(self) -> str:
        """
        Gets the local file system path to the database file.

        For remote storage backends, this method is responsible for downloading
        the database file to a local cache and returning the path to that cache.

        Returns:
            The absolute local file path to the DuckDB database file.
        """
        pass

    @abstractmethod
    async def sync_to_storage(self) -> None:
        """
        Syncs local changes back to the persistent storage.

        For local storage, this may be a no-op. For remote storage, this method
        should handle uploading the local database file to the remote store.
        """
        pass

    @abstractmethod
    async def exists(self) -> bool:
        """Checks if the database file exists in the storage backend."""
        pass

    @abstractmethod
    async def delete(self) -> None:
        """Deletes the database file from the storage backend."""
        pass

    @abstractmethod
    async def cleanup(self) -> None:
        """
        Cleans up any local resources used by the backend.

        This can include deleting temporary files or closing connections.
        """
        pass

    @abstractmethod
    def get_info(self) -> dict[str, Any]:
        """
        Returns a dictionary with information about the storage backend.

        This is useful for monitoring and debugging purposes.

        Returns:
            A dictionary containing details about the backend's state.
        """
        pass