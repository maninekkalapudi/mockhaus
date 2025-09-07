"""
This module manages Snowflake stages for data ingestion in Mockhaus.

It simulates Snowflake's stage functionality by mapping stage names to local
file system directories. Metadata about each stage is stored in a DuckDB
system table, allowing for persistence and management of stages.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import duckdb


@dataclass
class Stage:
    """
    Represents a Snowflake stage and its properties.

    Attributes:
        name: The name of the stage.
        stage_type: The type of stage (e.g., 'USER', 'EXTERNAL').
        url: The URL for an external stage.
        local_path: The local file system path that represents the stage.
        properties: A dictionary of properties for the stage.
        created_at: The timestamp when the stage was created.
    """

    name: str
    stage_type: str  # 'INTERNAL', 'USER', 'EXTERNAL', 'TABLE'
    url: str | None = None
    local_path: str = ""
    properties: dict[str, Any] | None = None
    created_at: str | None = None

    def __post_init__(self) -> None:
        if self.properties is None:
            self.properties = {}


class MockStageManager:
    """
    Manages Snowflake stages by mapping them to the local file system.
    """

    def __init__(self, connection: duckdb.DuckDBPyConnection) -> None:
        """
        Initializes the stage manager.

        Args:
            connection: An active DuckDB connection.
        """
        self.connection = connection
        # Define base paths for different types of stages
        self.base_path = Path.home() / ".mockhaus"
        self.stages_path = self.base_path / "stages"
        self.tables_path = self.base_path / "tables"
        self.user_path = self.base_path / "user"
        self.external_path = self.base_path / "external"

        # Create base directories
        self._ensure_directories()
        self._create_system_tables()

    def _ensure_directories(self) -> None:
        """Creates the necessary base directories for stage operations."""
        for path in [self.base_path, self.stages_path, self.tables_path, self.user_path, self.external_path]:
            path.mkdir(parents=True, exist_ok=True)

    def _create_system_tables(self) -> None:
        """Creates the `mockhaus_stages` table for storing stage metadata."""
        create_stages_table = """
        CREATE TABLE IF NOT EXISTS mockhaus_stages (
            name VARCHAR PRIMARY KEY,
            stage_type VARCHAR NOT NULL,
            url VARCHAR,
            local_path VARCHAR NOT NULL,
            properties JSON,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        self.connection.execute(create_stages_table)

    def create_stage(self, name: str, stage_type: str = "USER", url: str | None = None, properties: dict[str, Any] | None = None) -> Stage:
        """
        Creates a new stage.

        Args:
            name: The name of the stage.
            stage_type: The type of stage.
            url: The URL for an external stage.
            properties: A dictionary of stage properties.

        Returns:
            A `Stage` object representing the new stage.
        """
        if properties is None:
            properties = {}

        # Determine local path based on stage type and URL
        local_path = self._determine_local_path(name, stage_type, url)

        # Create the directory
        Path(local_path).mkdir(parents=True, exist_ok=True)

        # Create stage object
        stage = Stage(name=name, stage_type=stage_type, url=url, local_path=local_path, properties=properties)

        # Store in system table
        self._store_stage_metadata(stage)

        return stage

    def _determine_local_path(self, name: str, stage_type: str, url: str | None) -> str:
        """Determines the local file system path for a stage."""
        if stage_type == "USER":
            return str(self.user_path / name)
        if stage_type == "INTERNAL" or stage_type == "TABLE":
            return str(self.tables_path / name)
        if stage_type == "EXTERNAL":
            if url:
                # Parse URL to create meaningful local path
                parsed = urlparse(url)
                if parsed.scheme in ["s3", "gcs", "azure"]:
                    return str(self.external_path / parsed.scheme / (parsed.netloc + parsed.path).strip("/"))
                if parsed.scheme == "file":
                    # For file:// URLs, use the actual local path
                    return parsed.path
                return str(self.external_path / name)
            return str(self.external_path / name)
        # Default to named stage
        return str(self.stages_path / name)

    def _store_stage_metadata(self, stage: Stage) -> None:
        """Store stage metadata in system table."""
        insert_sql = """
        INSERT OR REPLACE INTO mockhaus_stages
        (name, stage_type, url, local_path, properties)
        VALUES (?, ?, ?, ?, ?)
        """
        import json

        self.connection.execute(insert_sql, [stage.name, stage.stage_type, stage.url, stage.local_path, json.dumps(stage.properties)])

    def get_stage(self, name: str) -> Stage | None:
        """
        Retrieves a stage by its name.

        Args:
            name: The name of the stage.

        Returns:
            A `Stage` object if found, otherwise None.
        """
        result = self.connection.execute("SELECT * FROM mockhaus_stages WHERE name = ?", [name]).fetchone()

        if not result:
            return None

        import json

        return Stage(
            name=result[0],
            stage_type=result[1],
            url=result[2],
            local_path=result[3],
            properties=json.loads(result[4] or '{}'),
            created_at=result[5],
        )

    def list_stages(self) -> list[Stage]:
        """Lists all created stages."""
        results = self.connection.execute("SELECT * FROM mockhaus_stages").fetchall()
        stages = []

        import json
        return [Stage(name=r[0], stage_type=r[1], url=r[2], local_path=r[3], properties=json.loads(r[4] or '{}'), created_at=r[5]) for r in results]

    def drop_stage(self, name: str) -> bool:
        """
        Drops a stage by its name.

        Args:
            name: The name of the stage to drop.

        Returns:
            True if the stage was dropped, False if it was not found.
        """
        if not self.get_stage(name):
            return False

        # Remove from system table
        self.connection.execute("DELETE FROM mockhaus_stages WHERE name = ?", [name])

        # Optionally remove directory (commented out for safety)
        # shutil.rmtree(stage.local_path, ignore_errors=True)

        return True

    def resolve_stage_path(self, stage_reference: str) -> str | None:
        """
        Resolves a Snowflake stage reference to a local file path.

        Args:
            stage_reference: The stage reference (e.g., '@my_stage/file.csv').

        Returns:
            The resolved local file path as a string, or None if invalid.
        """
        if not stage_reference.startswith("@"):
            return None

        # Remove the @ prefix
        ref = stage_reference[1:]

        # Handle user stage (@~)
        if ref.startswith("~/"):
            file_path = ref[2:]  # Remove ~/
            return str(self.user_path / file_path)

        # Handle table internal stage (@%table_name)
        if ref.startswith("%"):
            # Extract table name and file path
            parts = ref[1:].split("/", 1)  # Remove %
            table_name = parts[0]
            file_path = parts[1] if len(parts) > 1 else ""
            return str(self.tables_path / table_name / file_path)

        # Handle named stage (@stage_name/path)
        parts = ref.split("/", 1)
        stage_name = parts[0]
        file_path = parts[1] if len(parts) > 1 else ""

        # Look up stage
        stage = self.get_stage(stage_name)
        if stage:
            return str(Path(stage.local_path) / file_path)

        # If stage doesn't exist, assume it's a named stage
        return str(self.stages_path / stage_name / file_path)

    def list_stage_files(self, stage_reference: str, pattern: str = "*") -> list[str]:
        """
        Lists files within a stage directory that match a given pattern.

        Args:
            stage_reference: The stage to list files from.
            pattern: A glob pattern to filter files.

        Returns:
            A list of file paths.
        """
        base_path = self.resolve_stage_path(stage_reference)
        if not base_path:
            return []

        # Remove filename if present, get directory
        path = Path(base_path)
        if not path.is_dir():
            path = path.parent

        if not path.exists():
            return []

        # List files matching pattern
        files = []
        for file_path in path.glob(pattern):
            if file_path.is_file():
                files.append(str(file_path))

        return sorted(files)

    def validate_stage_access(self, stage_reference: str) -> bool:
        """Validate that a stage reference can be accessed."""
        resolved_path = self.resolve_stage_path(stage_reference)
        if not resolved_path:
            return False

        path = Path(resolved_path)
        # Check if it's a file or if parent directory exists
        return path.exists() or path.parent.exists()

    def ensure_stage_directory(self, stage_reference: str) -> str:
        """
        Ensures that the directory for a stage reference exists, creating it
        if necessary.

        Args:
            stage_reference: The stage reference.

        Returns:
            The resolved local path of the stage directory.
        """
        resolved_path = self.resolve_stage_path(stage_reference)
        if not resolved_path:
            raise ValueError(f"Invalid stage reference: {stage_reference}")

        path = Path(resolved_path)
        # If it looks like a file path, create parent directory
        if path.suffix != "" or "." in path.name:
            path.parent.mkdir(parents=True, exist_ok=True)
        else:
            path.mkdir(parents=True, exist_ok=True)

        return resolved_path
