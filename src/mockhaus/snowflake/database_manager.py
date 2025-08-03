"""Database management for Snowflake DDL commands."""

import os
import re
from pathlib import Path

import duckdb


class SnowflakeDatabaseManager:
    """Handles Snowflake database DDL commands like CREATE DATABASE, USE DATABASE."""

    def __init__(self, connection: duckdb.DuckDBPyConnection | None = None, base_path: str = "./databases", in_memory: bool = False):
        """
        Initialize database manager.

        Args:
            connection: DuckDB connection for in-memory mode
            base_path: Directory where database files will be stored (file mode only)
            in_memory: If True, use ATTACH for in-memory databases
        """
        self.connection = connection
        self.in_memory = in_memory
        self.attached_databases: set[str] = set()
        self.current_database: str | None = None

        if not in_memory:
            # File-based mode: create directory for database files
            self.base_path = Path(base_path)
            self.base_path.mkdir(exist_ok=True)
        else:
            # In-memory mode: current database starts as "main"
            self.current_database = "main"

    def is_database_ddl(self, sql: str) -> bool:
        """
        Check if SQL is a database DDL command.

        Args:
            sql: SQL statement to check

        Returns:
            True if it's a database DDL command
        """
        sql_upper = sql.strip().upper()
        return (
            sql_upper.startswith("CREATE DATABASE")
            or sql_upper.startswith("DROP DATABASE")
            or sql_upper.startswith("USE DATABASE")
            or sql_upper.startswith("USE ")
            or sql_upper.startswith("SHOW DATABASES")
        )

    def execute_database_ddl(self, sql: str) -> dict:
        """
        Execute database DDL command.

        Args:
            sql: Database DDL SQL to execute

        Returns:
            Result dictionary with success status and message
        """
        sql_clean = sql.strip().rstrip(";")
        sql_upper = sql_clean.upper()

        if sql_upper.startswith("CREATE DATABASE"):
            return self._create_database(sql_clean)
        if sql_upper.startswith("DROP DATABASE"):
            return self._drop_database(sql_clean)
        if sql_upper.startswith("USE DATABASE") or sql_upper.startswith("USE "):
            return self._use_database(sql_clean)
        if sql_upper.startswith("SHOW DATABASES"):
            return self._show_databases()
        return {"success": False, "error": f"Unsupported database DDL: {sql}"}

    def _create_database(self, sql: str) -> dict:
        """Handle CREATE DATABASE command."""
        # Parse database name from SQL
        # Supports: CREATE DATABASE my_db, CREATE DATABASE "my db", CREATE DATABASE IF NOT EXISTS my_db
        pattern = r'CREATE\s+DATABASE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:"([^"]+)"|(\w+))'
        match = re.search(pattern, sql, re.IGNORECASE)

        if not match:
            return {"success": False, "error": "Invalid CREATE DATABASE syntax. Use: CREATE DATABASE database_name"}

        # Get database name (quoted or unquoted) and normalize to lowercase
        db_name_raw = match.group(1) if match.group(1) else match.group(2)
        db_name = db_name_raw.lower()  # Case-insensitive storage

        # Check if IF NOT EXISTS was specified
        if_not_exists = "IF NOT EXISTS" in sql.upper()

        if self.in_memory:
            # In-memory mode: use ATTACH
            if db_name in self.attached_databases or db_name == "main":
                if not if_not_exists:
                    return {"success": False, "error": f"Database '{db_name_raw}' already exists"}
                return {"success": True, "message": f"Database '{db_name_raw}' already exists (IF NOT EXISTS specified)"}

            try:
                if self.connection is None:
                    return {"success": False, "error": "No database connection available"}
                self.connection.execute(f"ATTACH ':memory:' AS {db_name}")
                self.attached_databases.add(db_name)
                return {"success": True, "message": f"Database '{db_name_raw}' created (in-memory)"}
            except Exception as e:
                return {"success": False, "error": f"Failed to create database '{db_name_raw}': {str(e)}"}
        else:
            # File-based mode (original implementation)
            db_file = self.base_path / f"{db_name}.db"

            if db_file.exists() and not if_not_exists:
                return {"success": False, "error": f"Database '{db_name_raw}' already exists"}

            if db_file.exists() and if_not_exists:
                return {
                    "success": True,
                    "message": f"Database '{db_name_raw}' already exists (IF NOT EXISTS specified)",
                    "database_path": str(db_file),
                }

            # Create the database file by creating an empty DuckDB file
            try:
                conn = duckdb.connect(str(db_file))
                conn.execute("CREATE TABLE _mockhaus_metadata (created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                conn.close()

                return {"success": True, "message": f"Database '{db_name_raw}' created successfully", "database_path": str(db_file)}
            except Exception as e:
                return {"success": False, "error": f"Failed to create database '{db_name_raw}': {str(e)}"}

    def _drop_database(self, sql: str) -> dict:
        """Handle DROP DATABASE command."""
        # Parse database name
        pattern = r'DROP\s+DATABASE\s+(?:IF\s+EXISTS\s+)?(?:"([^"]+)"|(\w+))'
        match = re.search(pattern, sql, re.IGNORECASE)

        if not match:
            return {"success": False, "error": "Invalid DROP DATABASE syntax. Use: DROP DATABASE database_name"}

        db_name_raw = match.group(1) if match.group(1) else match.group(2)
        db_name = db_name_raw.lower()  # Case-insensitive storage

        if_exists = "IF EXISTS" in sql.upper()

        if self.in_memory:
            # In-memory mode: use DETACH
            if db_name == "main":
                return {"success": False, "error": "Cannot drop main database"}

            if db_name not in self.attached_databases:
                if not if_exists:
                    return {"success": False, "error": f"Database '{db_name_raw}' does not exist"}
                return {"success": True, "message": f"Database '{db_name_raw}' does not exist (IF EXISTS specified)"}

            try:
                if self.connection is None:
                    return {"success": False, "error": "No database connection available"}
                # Always switch to memory (main) before detaching any database
                self.connection.execute("USE memory")
                self.current_database = "main"

                self.connection.execute(f"DETACH {db_name}")
                self.attached_databases.remove(db_name)

                return {"success": True, "message": f"Database '{db_name_raw}' dropped successfully"}
            except Exception as e:
                return {"success": False, "error": f"Failed to drop database '{db_name_raw}': {str(e)}"}
        else:
            # File-based mode (original implementation)
            db_file = self.base_path / f"{db_name}.db"

            if not db_file.exists() and not if_exists:
                return {"success": False, "error": f"Database '{db_name_raw}' does not exist"}

            if not db_file.exists() and if_exists:
                return {"success": True, "message": f"Database '{db_name_raw}' does not exist (IF EXISTS specified)"}

            try:
                os.remove(db_file)
                if self.current_database == db_name:
                    self.current_database = None

                return {"success": True, "message": f"Database '{db_name_raw}' dropped successfully"}
            except Exception as e:
                return {"success": False, "error": f"Failed to drop database '{db_name_raw}': {str(e)}"}

    def _use_database(self, sql: str) -> dict:
        """Handle USE DATABASE command."""
        # Parse database name - supports both "USE database_name" and "USE DATABASE database_name"
        pattern = r'USE\s+(?:DATABASE\s+)?(?:"([^"]+)"|(\w+))'
        match = re.search(pattern, sql, re.IGNORECASE)

        if not match:
            return {"success": False, "error": "Invalid USE DATABASE syntax. Use: USE database_name or USE DATABASE database_name"}

        db_name_raw = match.group(1) if match.group(1) else match.group(2)
        db_name = db_name_raw.lower()  # Case-insensitive storage

        if self.in_memory:
            # In-memory mode: check if database exists (main always exists)
            if db_name != "main" and db_name not in self.attached_databases:
                return {"success": False, "error": f"Database '{db_name_raw}' does not exist. Create it first with: CREATE DATABASE {db_name_raw}"}

            try:
                if self.connection is None:
                    return {"success": False, "error": "No database connection available"}
                # Map "main" to "memory" for DuckDB
                actual_db_name = "memory" if db_name == "main" else db_name
                self.connection.execute(f"USE {actual_db_name}")
                self.current_database = db_name
                return {"success": True, "message": f"Using database '{db_name_raw}'"}
            except Exception as e:
                return {"success": False, "error": f"Failed to switch to database '{db_name_raw}': {str(e)}"}
        else:
            # File-based mode (original implementation)
            db_file = self.base_path / f"{db_name}.db"

            if not db_file.exists():
                return {"success": False, "error": f"Database '{db_name_raw}' does not exist. Create it first with: CREATE DATABASE {db_name_raw}"}

            self.current_database = db_name
            return {"success": True, "message": f"Using database '{db_name_raw}'", "database_path": str(db_file)}

    def _show_databases(self) -> dict:
        """Handle SHOW DATABASES command."""
        try:
            if self.in_memory:
                # In-memory mode: show main database and attached databases
                databases = []

                # Always include main database
                databases.append(
                    {
                        "name": "main",
                        "current": self.current_database == "main",
                        "type": "in-memory",
                        "size_mb": 0.0,  # In-memory databases don't have file size
                    }
                )

                # Add attached databases
                for db_name in sorted(self.attached_databases):
                    databases.append({"name": db_name, "current": self.current_database == db_name, "type": "in-memory", "size_mb": 0.0})

                return {"success": True, "databases": databases, "current_database": self.current_database}
            # File-based mode (original implementation)
            db_files = list(self.base_path.glob("*.db"))
            databases = []

            for db_file in sorted(db_files):
                db_name = db_file.stem
                is_current = db_name == self.current_database

                # Get file size
                size_mb = db_file.stat().st_size / (1024 * 1024)

                databases.append({"name": db_name, "current": is_current, "size_mb": round(size_mb, 2), "path": str(db_file)})

            return {"success": True, "databases": databases, "current_database": self.current_database}
        except Exception as e:
            return {"success": False, "error": f"Failed to list databases: {str(e)}"}

    def get_current_database_path(self) -> str | None:
        """
        Get the file path of the current database.

        Returns:
            Path to current database file, or None if in-memory mode or no database selected
        """
        if self.in_memory:
            # In-memory mode: no file paths
            return None

        if not self.current_database:
            return None

        db_file = self.base_path / f"{self.current_database}.db"
        return str(db_file) if db_file.exists() else None
