"""
This module provides a manager for handling Snowflake database DDL commands.

It is responsible for emulating commands like `CREATE DATABASE`, `DROP DATABASE`,
`USE DATABASE`, and `SHOW DATABASES`. It manages the database files on the
local filesystem, typically within a `databases` directory.
"""

import re

import duckdb


class SnowflakeDatabaseManager:
    """Handles Snowflake database DDL commands like CREATE DATABASE, USE DATABASE."""

    def __init__(self, connection: duckdb.DuckDBPyConnection | None = None):
        """
        Initialize database manager.

        Args:
            connection: DuckDB connection
            in_memory: Always True now (kept for API compatibility)
        """
        self.connection = connection
        self.attached_databases: set[str] = set()
        # Current database starts as "main"
        self.current_database = "main"

    def is_database_ddl(self, sql: str) -> bool:
        """
        Check if SQL is a database DDL command.

        Args:
            sql: SQL statement to check

        Returns:
            True if the statement is a database DDL command, False otherwise.
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
            A dictionary with the result of the operation.
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

        # Check if database already exists
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

        # Cannot drop main database
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

    def _use_database(self, sql: str) -> dict:
        """Handle USE DATABASE command."""
        # Parse database name - supports both "USE database_name" and "USE DATABASE database_name"
        pattern = r'USE\s+(?:DATABASE\s+)?(?:"([^"]+)"|(\w+))'
        match = re.search(pattern, sql, re.IGNORECASE)

        if not match:
            return {"success": False, "error": "Invalid USE DATABASE syntax. Use: USE database_name or USE DATABASE database_name"}

        db_name_raw = match.group(1) if match.group(1) else match.group(2)
        db_name = db_name_raw.lower()  # Case-insensitive storage

        # Check if database exists (main always exists)
        if db_name != "main" and db_name not in self.attached_databases:
            return {"success": False, "error": f"Database '{db_name_raw}' does not exist. Create it first with: CREATE DATABASE {db_name_raw}"}

        try:
            if self.connection is None:
                return {"success": False, "error": "No database connection available"}
            # Map "main" to "memory" for DuckDB
            actual_db_name = "memory" if db_name == "main" else db_name
            self.connection.execute(f"USE {actual_db_name}")
            self.current_database = db_name
            return {"success": True, "message": f"Using database '{db_name_raw}'", "database_name": db_name}
        except Exception as e:
            return {"success": False, "error": f"Failed to switch to database '{db_name_raw}': {str(e)}"}

    def _show_databases(self) -> dict:
        """Handle SHOW DATABASES command."""
        try:
            # Show main database and attached databases
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
        except Exception as e:
            return {"success": False, "error": f"Failed to list databases: {str(e)}"}

    def get_current_database_name(self) -> str | None:
        """
        Get the name of the current database.

        Returns:
            Name of current database, or None if no database selected
        """
        return self.current_database
