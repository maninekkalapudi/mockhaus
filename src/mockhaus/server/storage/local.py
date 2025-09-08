"""Local file storage backend for persistent sessions."""

import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

from .base import StorageBackend, StorageConfig

logger = logging.getLogger(__name__)


class LocalFileBackend(StorageBackend):
    """Local file system storage backend for development and testing."""

    def __init__(self, config: StorageConfig):
        """Initialize local file backend."""
        super().__init__(config)
        # Parse the path - could be absolute or relative
        self.storage_path = Path(config.path).expanduser().resolve()
        self.db_file_path = self.storage_path

        # If path doesn't end with .db, add it
        if not str(self.db_file_path).endswith(".db"):
            self.db_file_path = self.db_file_path.with_suffix(".db")

        logger.info(f"LocalFileBackend initialized with path: {self.db_file_path}")

    async def initialize(self) -> None:
        """Create parent directories if they don't exist."""
        parent_dir = self.db_file_path.parent
        if not parent_dir.exists():
            parent_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created directory: {parent_dir}")

    async def get_database_path(self) -> str:
        """Return the local database file path."""
        # For local backend, we use the file directly
        return str(self.db_file_path)

    async def sync_to_storage(self) -> None:
        """No-op for local storage as we work directly with the file."""
        # DuckDB writes directly to the file, no sync needed
        logger.debug(f"Local file {self.db_file_path} is automatically synced")

    async def exists(self) -> bool:
        """Check if the database file exists."""
        exists = self.db_file_path.exists()
        logger.debug(f"Database file {self.db_file_path} exists: {exists}")
        return exists

    async def delete(self) -> None:
        """Delete the database file."""
        if self.db_file_path.exists():
            try:
                os.remove(self.db_file_path)
                logger.info(f"Deleted database file: {self.db_file_path}")

                # Also remove WAL and other DuckDB auxiliary files
                wal_file = Path(str(self.db_file_path) + ".wal")
                if wal_file.exists():
                    os.remove(wal_file)
                    logger.debug(f"Deleted WAL file: {wal_file}")
            except Exception as e:
                logger.error(f"Failed to delete database file: {e}")
                raise

    async def cleanup(self) -> None:
        """Clean up local resources (no-op for local backend)."""
        # Nothing to clean up for local storage
        logger.debug("LocalFileBackend cleanup (no-op)")

    def get_info(self) -> dict[str, Any]:
        """Get information about the storage backend."""
        info = {"type": "local", "path": str(self.db_file_path), "exists": self.db_file_path.exists()}

        if self.db_file_path.exists():
            stat = self.db_file_path.stat()
            info["size_bytes"] = stat.st_size
            info["size_mb"] = round(stat.st_size / (1024 * 1024), 2)
            info["modified"] = stat.st_mtime

        return info


class TempFileBackend(StorageBackend):
    """Temporary file storage backend for ephemeral persistent sessions."""

    def __init__(self, config: StorageConfig):
        """Initialize temp file backend."""
        super().__init__(config)
        self.temp_dir: str | None = None
        self.db_file_path: Path | None = None
        logger.info("TempFileBackend initialized")

    async def initialize(self) -> None:
        """Create a temporary directory and file."""
        if not self.temp_dir:
            self.temp_dir = tempfile.mkdtemp(prefix="mockhaus_session_")
            path_name = self.config.path or "session"
            self.db_file_path = Path(self.temp_dir) / f"{path_name}.db"
            logger.info(f"Created temp database at: {self.db_file_path}")

    async def get_database_path(self) -> str:
        """Return the temporary database file path."""
        if not self.db_file_path:
            await self.initialize()
        return str(self.db_file_path)

    async def sync_to_storage(self) -> None:
        """No-op for temp storage."""
        logger.debug("TempFileBackend sync (no-op)")

    async def exists(self) -> bool:
        """Check if the temp database file exists."""
        if not self.db_file_path:
            return False
        return self.db_file_path.exists()

    async def delete(self) -> None:
        """Delete the temporary database file and directory."""
        await self.cleanup()

    async def cleanup(self) -> None:
        """Clean up temporary files and directory."""
        if self.temp_dir and Path(self.temp_dir).exists():
            try:
                shutil.rmtree(self.temp_dir)
                logger.info(f"Cleaned up temp directory: {self.temp_dir}")
            except Exception as e:
                logger.error(f"Failed to cleanup temp directory: {e}")
            finally:
                self.temp_dir = None
                self.db_file_path = None

    def get_info(self) -> dict[str, Any]:
        """Get information about the storage backend."""
        from typing import Any

        info: dict[str, Any] = {
            "type": "temp",
            "temp_dir": str(self.temp_dir) if self.temp_dir else None,
            "path": str(self.db_file_path) if self.db_file_path else None,
            "exists": self.db_file_path.exists() if self.db_file_path else False,
        }

        if self.db_file_path and self.db_file_path.exists():
            stat = self.db_file_path.stat()
            info["size_bytes"] = stat.st_size
            info["size_mb"] = round(stat.st_size / (1024 * 1024), 2)

        return info
