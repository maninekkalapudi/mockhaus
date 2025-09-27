"""Stage management for Mockhaus data ingestion."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import duckdb
import os


@dataclass
class Stage:
    """Represents a Snowflake stage in Mockhaus."""

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
    """Manages Snowflake stages using local file system."""

    def __init__(self, connection: duckdb.DuckDBPyConnection) -> None:
        """Initialize stage manager with DuckDB connection."""
        self.connection = connection
        self.base_path = Path.home() / ".mockhaus"
        self.stages_path = self.base_path / "stages"
        self.tables_path = self.base_path / "tables"
        self.user_path = self.base_path / "user"
        self.external_path = self.base_path / "external"

        # Create base directories
        self._ensure_directories()
        self._create_system_tables()

    def _ensure_directories(self) -> None:
        """Create necessary directories for stage operations."""
        for path in [self.base_path, self.stages_path, self.tables_path, self.user_path, self.external_path]:
            path.mkdir(parents=True, exist_ok=True)

    def _create_system_tables(self) -> None:
        """Create system tables for stage metadata."""
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
        """Create a new stage."""
        if properties is None:
            properties = {}

        # Determine local path based on stage type and URL
        local_path = self._determine_local_path(name, stage_type, url)

        # Create the directory
        Path(local_path).mkdir(parents=True, exist_ok=True)

        # Create stage object
        if url and url.startswith("file://"): 
            url = "file://" + str(Path(url[7:]).as_posix())
        stage = Stage(name=name, stage_type=stage_type, url=url, local_path=local_path, properties=properties)

        # Store in system table
        self._store_stage_metadata(stage)

        return stage

    def _determine_local_path(self, name: str, stage_type: str, url: str | None) -> str:
        """Determine local path for a stage based on type and URL."""
        if stage_type == "USER":
            return str(self.user_path / name)
        if stage_type == "INTERNAL" or stage_type == "TABLE":
            return str(self.tables_path / name)
        if stage_type == "EXTERNAL":
            if url:
                # Parse URL to create meaningful local path
                parsed = urlparse(url)
                if parsed.scheme in ["s3", "gcs", "azure"]:
                    path = os.path.join(self.external_path, parsed.scheme, parsed.netloc, parsed.path.strip("/"))
                    return os.path.normpath(path)
                if parsed.scheme == "file":
                    # For file:// URLs, use the actual local path
                    path = parsed.path
                    if parsed.netloc:
                        path = parsed.netloc + path
                    return str(Path(path))
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
        """Get stage by name."""
        result = self.connection.execute("SELECT * FROM mockhaus_stages WHERE name = ?", [name]).fetchone()

        if not result:
            return None

        import json

        return Stage(
            name=result[0],
            stage_type=result[1],
            url=result[2],
            local_path=result[3],
            properties=json.loads(result[4]) if result[4] else {},
            created_at=result[5],
        )

    def list_stages(self) -> list[Stage]:
        """List all stages."""
        results = self.connection.execute("SELECT * FROM mockhaus_stages").fetchall()
        stages = []

        import json

        for result in results:
            stages.append(
                Stage(
                    name=result[0],
                    stage_type=result[1],
                    url=result[2],
                    local_path=result[3],
                    properties=json.loads(result[4]) if result[4] else {},
                    created_at=result[5],
                )
            )

        return stages

    def drop_stage(self, name: str) -> bool:
        """Drop a stage and optionally remove its directory."""
        stage = self.get_stage(name)
        if not stage:
            return False

        # Remove from system table
        self.connection.execute("DELETE FROM mockhaus_stages WHERE name = ?", [name])

        # Optionally remove directory (commented out for safety)
        # shutil.rmtree(stage.local_path, ignore_errors=True)

        return True

    def resolve_stage_path(self, stage_reference: str) -> str | None:
        """
        Resolve a Snowflake stage reference to local file path.

        Examples:
        - @my_stage/file.csv → ~/.mockhaus/stages/my_stage/file.csv
        - @%table_name/file.csv → ~/.mockhaus/tables/table_name/file.csv
        - @~/file.csv → ~/.mockhaus/user/file.csv
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
        """List files in a stage directory."""
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
        """Ensure stage directory exists and return the path."""
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
