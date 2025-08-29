#!/usr/bin/env python3
"""Enhanced interactive REPL client for Mockhaus server using prompt_toolkit."""
# ruff: noqa: T201

import os
from typing import Any, cast

import requests

# Try to import prompt_toolkit for enhanced features
try:
    from prompt_toolkit import prompt
    from prompt_toolkit.completion import WordCompleter
    from prompt_toolkit.history import FileHistory
    from prompt_toolkit.key_binding import KeyBindings
    from prompt_toolkit.shortcuts import CompleteStyle

    PROMPT_TOOLKIT_AVAILABLE = True
except ImportError:
    PROMPT_TOOLKIT_AVAILABLE = False


class EnhancedMockhausClient:
    """Enhanced HTTP client for Mockhaus server with advanced terminal features."""

    def __init__(
        self,
        base_url: str = "http://localhost:8080",
        session_type: str = "memory",
        session_id: str | None = None,
        session_ttl: int | None = None,
        persistent_path: str | None = None,
    ):
        """
        Initialize client with enhanced terminal features.

        Args:
            base_url: Base URL of Mockhaus server
            session_type: Type of session ("memory" or "persistent")
            session_id: Specific session ID to use (optional)
            session_ttl: Session TTL in seconds (optional)
            persistent_path: Path for persistent session storage (optional)
        """
        self.base_url = base_url
        self.session = requests.Session()
        self.session_id: str | None = session_id
        self.session_type = session_type
        self.session_ttl = session_ttl
        self.persistent_path = persistent_path
        self.current_database: str | None = None

        if PROMPT_TOOLKIT_AVAILABLE:
            self._setup_enhanced_features()

    def initialize_session(self) -> bool:
        """
        Initialize the session at startup.

        Returns:
            True if session was created/connected successfully
        """
        if self.session_id:
            # Try to connect to existing session
            session_info = self.get_session_info()
            if session_info:
                print(f"✅ Connected to existing session: {self.session_id[:8]}...")
                return True
            print(f"⚠️  Session {self.session_id[:8]}... not found, creating new session")
            self.session_id = None  # Clear invalid session ID

        # Create new session
        session_result = self.create_session()
        if session_result.get("success") and self.session_id:
            print(f"✅ Created new {self.session_type} session: {self.session_id[:8]}...")
            return True
        error_msg = session_result.get("message", "Unknown error")
        if "detail" in session_result and isinstance(session_result["detail"], dict):
            error_msg = session_result["detail"].get("detail", error_msg)
        print(f"❌ Failed to create session: {error_msg}")
        return False

    def _setup_enhanced_features(self) -> None:
        """Setup enhanced terminal features when prompt_toolkit is available."""
        # Setup persistent history file
        history_file = os.path.expanduser("~/.mockhaus_history")
        self.history = FileHistory(history_file)

        # SQL keywords for auto-completion
        sql_keywords = [
            "SELECT",
            "FROM",
            "WHERE",
            "CREATE",
            "DATABASE",
            "TABLE",
            "INSERT",
            "UPDATE",
            "DELETE",
            "DROP",
            "USE",
            "SHOW",
            "DATABASES",
            "TABLES",
            "INT",
            "INTEGER",
            "VARCHAR",
            "DECIMAL",
            "PRIMARY",
            "KEY",
            "IF",
            "NOT",
            "EXISTS",
            "COPY",
            "INTO",
            "VALUES",
            "ORDER",
            "BY",
            "GROUP",
            "HAVING",
            "LIMIT",
            "OFFSET",
            "INNER",
            "LEFT",
            "RIGHT",
            "OUTER",
            "JOIN",
            "ON",
            "AS",
            "DISTINCT",
            "COUNT",
            "SUM",
            "AVG",
            "MAX",
            "MIN",
            "AND",
            "OR",
            "LIKE",
            "IN",
            "BETWEEN",
            "IS",
            "NULL",
            "ALTER",
            "ADD",
            "COLUMN",
            "CONSTRAINT",
            "INDEX",
            "UNION",
            "ALL",
            "CASE",
            "WHEN",
            "THEN",
            "ELSE",
            "END",
        ]

        self.sql_completer = WordCompleter(sql_keywords, ignore_case=True, match_middle=True)

        # Setup custom key bindings
        self.bindings = KeyBindings()
        self._setup_key_bindings()

    def _setup_key_bindings(self) -> None:
        """Setup custom key bindings for enhanced functionality."""

        @self.bindings.add("f5")
        def show_databases(event: Any) -> None:
            """F5: Insert SHOW DATABASES command"""
            event.app.current_buffer.insert_text("SHOW DATABASES;")

        @self.bindings.add("f6")
        def show_tables(event: Any) -> None:
            """F6: Insert query to show tables"""
            event.app.current_buffer.insert_text("SELECT name FROM sqlite_master WHERE type='table';")

        @self.bindings.add("c-l")
        def clear_screen(event: Any) -> None:
            """Ctrl+L: Clear screen"""
            event.app.output.clear()

    def create_session(self) -> dict[str, Any]:
        """
        Create a new session on the server.

        Returns:
            Session creation result dictionary
        """
        from typing import Any

        payload: dict[str, Any] = {"type": self.session_type}

        if self.session_ttl:
            payload["ttl_seconds"] = self.session_ttl

        if self.session_type == "persistent" and self.persistent_path:
            payload["storage"] = {"type": "local", "path": self.persistent_path}

        response = self.session.post(f"{self.base_url}/api/v1/sessions", json=payload)
        result = response.json()

        # Handle nested session structure from server
        if result.get("success") and "session" in result:
            session_data = result["session"]
            if "session_id" in session_data:
                self.session_id = session_data["session_id"]
        elif "session_id" in result:  # Handle flat structure (fallback)
            self.session_id = result["session_id"]

        return cast(dict[str, Any], result)

    def get_session_info(self) -> dict[str, Any] | None:
        """
        Get information about the current session.

        Returns:
            Session info dictionary or None if no session
        """
        if not self.session_id:
            return None

        response = self.session.get(f"{self.base_url}/api/v1/sessions/{self.session_id}")

        if response.status_code == 404:
            return None

        result = response.json()
        return cast(dict[str, Any], result)

    def terminate_session(self) -> bool:
        """
        Terminate the current session on the server.

        Returns:
            True if session was terminated successfully
        """
        if not self.session_id:
            return False

        response = self.session.delete(f"{self.base_url}/api/v1/sessions/{self.session_id}")

        if response.status_code == 200:
            self.session_id = None
            self.current_database = None
            return True

        return False

    def list_sessions(self) -> dict[str, Any]:
        """
        List all active sessions on the server.

        Returns:
            Dictionary of session information
        """
        response = self.session.get(f"{self.base_url}/api/v1/sessions")
        return cast(dict[str, Any], response.json())

    def query(self, sql: str, database: str | None = None) -> dict[str, Any]:
        """
        Execute SQL query against Mockhaus server.

        Args:
            sql: SQL query to execute
            database: Optional database file path

        Returns:
            Query result dictionary
        """
        if not self.session_id:
            return {"success": False, "error": "No active session. Session should be created at startup."}

        payload = {"sql": sql, "database": database, "session_id": self.session_id}

        response = self.session.post(f"{self.base_url}/api/v1/query", json=payload)
        result = response.json()

        # Update current database info from response (but not session_id)
        if result.get("success") and "current_database" in result:
            self.current_database = result.get("current_database")

        return cast(dict[str, Any], result)

    def health(self) -> dict[str, Any]:
        """
        Check server health.

        Returns:
            Health status dictionary
        """
        response = self.session.get(f"{self.base_url}/api/v1/health")
        return cast(dict[str, Any], response.json())

    def get_input(self, base_prompt: str) -> str:
        """
        Get user input with enhanced features, supporting both single and multi-line input.

        Args:
            base_prompt: The prompt to display

        Returns:
            User input string
        """
        if not PROMPT_TOOLKIT_AVAILABLE:
            # Fallback to basic input
            return input(base_prompt)

        lines: list[str] = []
        continuation_prompt = " " * (len(base_prompt) - 3) + "... "

        try:
            while True:
                current_prompt = base_prompt if not lines else continuation_prompt

                line = prompt(
                    current_prompt,
                    history=self.history if not lines else None,  # Only use history for first line
                    completer=self.sql_completer,
                    complete_style=CompleteStyle.READLINE_LIKE,
                    multiline=False,  # Single line input
                    complete_while_typing=False,
                    key_bindings=self.bindings,
                    wrap_lines=True,
                ).strip()

                # Empty line handling
                if not line and lines:
                    # Empty line with existing content - execute
                    break
                if not line and not lines:
                    # Empty line with no content - continue
                    continue

                lines.append(line)

                # If line ends with semicolon, we're done
                if line.endswith(";"):
                    break

            return " ".join(lines)

        except (EOFError, KeyboardInterrupt):
            if lines:
                return " ".join(lines)
            raise


def format_results(result: dict) -> str:
    """
    Format query results for display.

    Args:
        result: Query result dictionary from server

    Returns:
        Formatted string for display
    """
    if not result.get("success"):
        error_detail = result.get("detail", {})
        error_msg = error_detail.get("detail", "Unknown error") if isinstance(error_detail, dict) else str(error_detail)
        return f"❌ Error: {error_msg}"

    data = result.get("data", [])
    if not data:
        return "✅ Query executed successfully (no results)"

    # Dynamic table formatting with proper column widths
    if len(data) > 0:
        headers = list(data[0].keys())
        output = []

        # Calculate column widths based on content
        col_widths = {}
        display_data = data[:10]  # Only calculate for displayed rows

        for header in headers:
            # Start with header width
            max_width = len(str(header))

            # Check data widths
            for row in display_data:
                value = row.get(header, "")
                str_value = str(value) if value is not None else ""
                max_width = max(max_width, len(str_value))

            # Cap at reasonable maximum, but allow more than 12 chars
            col_widths[header] = min(max_width, 50)

        # Header row
        header_parts = []
        separator_parts = []
        for header in headers:
            width = col_widths[header]
            header_parts.append(f"{header:<{width}}")
            separator_parts.append("-" * width)

        output.append(" | ".join(header_parts))
        output.append("-+-".join(separator_parts))

        # Data rows
        for row in display_data:
            row_parts = []
            for header in headers:
                value = row.get(header, "")
                str_value = str(value) if value is not None else ""
                width = col_widths[header]

                # Truncate if necessary but show more than before
                if len(str_value) > width:
                    str_value = str_value[: width - 3] + "..."

                row_parts.append(f"{str_value:<{width}}")
            output.append(" | ".join(row_parts))

        if len(data) > 10:
            output.append(f"... and {len(data) - 10} more rows")

        execution_time = result.get("execution_time", 0)
        output.append(f"\n✅ {len(data)} rows in {execution_time:.3f}s")

        return "\n".join(output)

    return "✅ Query executed successfully"


def print_help() -> None:
    """Print help information with enhanced features."""
    print("📖 Mockhaus REPL Help")
    print("=" * 30)
    print()
    print("Commands:")
    print("  help, ?          - Show this help")
    print("  health           - Check server health")
    print("  session          - Show current session info")
    print("  sessions         - List all active sessions")
    print("  quit, exit, q    - Exit REPL (terminates session)")
    print()
    print("SQL Examples:")
    print("  SHOW DATABASES;")
    print("  CREATE DATABASE test.db;")
    print("  USE test.db;")
    print("  CREATE TABLE users (id INT, name VARCHAR(50));")
    print("  INSERT INTO users VALUES (1, 'Alice');")
    print("  SELECT * FROM users;")
    print()
    if PROMPT_TOOLKIT_AVAILABLE:
        print("Enhanced Features:")
        print("  - Auto-completion (Tab)")
        print("  - Command history (Up/Down arrows)")
        print("  - F5: Insert 'SHOW DATABASES;'")
        print("  - F6: Insert table listing query")
        print("  - Ctrl+L: Clear screen")
        print()
    print("Multi-line queries: End with semicolon (;) or empty line to execute")
    print()


def get_multi_line_input_basic(prompt: str = "mockhaus> ", current_db: str | None = None) -> str:
    """
    Fallback multi-line input for when prompt_toolkit is not available.
    """
    lines: list[str] = []

    # Create dynamic prompt with database context
    base_prompt = f"mockhaus({current_db})> " if current_db else prompt

    continuation_prompt = " " * (len(base_prompt) - 3) + "... "

    try:
        while True:
            line = input(base_prompt).strip() if not lines else input(continuation_prompt).strip()

            if not line and lines:
                # Empty line with existing content - execute
                break

            if not line and not lines:
                # Empty line with no content - continue
                continue

            lines.append(line)

            # If line ends with semicolon, we're done
            if line.endswith(";"):
                break

    except EOFError:
        # Ctrl+D pressed
        if lines:
            return " ".join(lines)
        raise KeyboardInterrupt from None

    return " ".join(lines)


def main(session_type: str = "memory", session_id: str | None = None, session_ttl: int | None = None, persistent_path: str | None = None) -> None:
    """Enhanced interactive REPL for Mockhaus."""
    # Print startup message with enhanced features status
    print("🏠 Mockhaus Interactive REPL")
    print("=" * 40)

    if PROMPT_TOOLKIT_AVAILABLE:
        print("✅ Enhanced mode (with auto-completion and history)")
        print("   F5: Show databases, F6: Show tables, Ctrl+L: Clear screen")
    else:
        print("⚠️  Basic mode (prompt_toolkit not available)")

    print("   Type 'help' for commands, 'quit' or Ctrl+C to exit")

    # Show session configuration
    print()
    print("📋 Session Configuration:")
    print(f"   Type: {session_type}")
    if session_id:
        print(f"   ID: {session_id}")
    if session_ttl:
        print(f"   TTL: {session_ttl} seconds")
    if persistent_path:
        print(f"   Storage: {persistent_path}")
    print()

    # Allow custom server URL via environment variable
    server_url = os.getenv("MOCKHAUS_SERVER_URL", "http://localhost:8080")
    client = EnhancedMockhausClient(
        base_url=server_url, session_type=session_type, session_id=session_id, session_ttl=session_ttl, persistent_path=persistent_path
    )

    # Test connection
    try:
        health_result = client.health()
        print(f"🚀 Connected to Mockhaus server at {server_url}")
        print(f"   Server status: {health_result}")
    except Exception as e:
        print(f"❌ Cannot connect to Mockhaus server at {server_url}")
        print(f"   Error: {e}")
        print("   Please make sure the server is running with: python -m mockhaus.server")
        return

    # Initialize session
    print("🔗 Initializing session...")
    if not client.initialize_session():
        print("❌ Failed to initialize session. Exiting.")
        return

    while True:
        try:
            # Create dynamic prompt with database context
            base_prompt = f"mockhaus({client.current_database})> " if client.current_database else "mockhaus> "

            if PROMPT_TOOLKIT_AVAILABLE:
                query = client.get_input(base_prompt).strip()
            else:
                query = get_multi_line_input_basic(current_db=client.current_database).strip()

            if not query or query.strip() == "":
                continue

            if query.lower() in ["quit", "exit", "q"]:
                break

            if query.lower() in ["help", "?"]:
                print_help()
                continue

            if query.lower() == "health":
                health_result = client.health()
                print(f"✅ Server health: {health_result}")
                continue

            if query.lower() == "session":
                session_info = client.get_session_info()
                if session_info and session_info.get("success") and "session" in session_info:
                    session_data = session_info["session"]
                    print("📋 Current Session Info:")
                    print(f"   ID: {session_data.get('session_id', 'N/A')}")
                    print(f"   Type: {session_data.get('type', 'N/A')}")
                    print(f"   Created: {session_data.get('created_at', 'N/A')}")
                    print(f"   Last Accessed: {session_data.get('last_accessed', 'N/A')}")
                    print(f"   TTL: {session_data.get('ttl_seconds', 'N/A')} seconds")
                    print(f"   Active: {session_data.get('is_active', 'N/A')}")
                    if session_data.get("storage_config"):
                        storage = session_data["storage_config"]
                        print(f"   Storage: {storage.get('type', 'N/A')} at {storage.get('path', 'N/A')}")
                else:
                    print("❌ No active session")
                continue

            if query.lower() == "sessions":
                sessions_result = client.list_sessions()
                if sessions_result.get("success"):
                    session_details = sessions_result.get("session_details", [])
                    if session_details:
                        print(f"📋 Active Sessions ({len(session_details)}):")
                        for info in session_details:
                            session_id = info.get("session_id", "unknown")
                            session_type = info.get("type", "unknown")
                            last_accessed = info.get("last_accessed", "N/A")
                            # Ensure session_id is not None before slicing
                            display_id = session_id[:8] + "..." if session_id and len(session_id) > 8 else session_id or "unknown"
                            print(f"   {display_id} - {session_type} ({last_accessed})")
                    else:
                        print("📋 No active sessions")
                else:
                    print(f"❌ Failed to list sessions: {sessions_result.get('error', 'Unknown error')}")
                continue

            # Execute SQL query
            result = client.query(query)
            print(format_results(result))

        except KeyboardInterrupt:
            print("\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            import traceback

            traceback.print_exc()
            continue

    # Clean up session when exiting
    try:
        if client.session_id:
            print("🧹 Cleaning up session...")
            if client.terminate_session():
                print("✅ Session terminated")
            else:
                print("⚠️  Failed to terminate session")
    except Exception as e:
        print(f"⚠️  Error during cleanup: {e}")


if __name__ == "__main__":
    main()
