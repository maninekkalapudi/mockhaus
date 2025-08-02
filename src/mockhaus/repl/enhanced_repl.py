#!/usr/bin/env python3
"""Enhanced interactive REPL client for Mockhaus server using prompt_toolkit."""

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

    def __init__(self, base_url: str = "http://localhost:8080"):
        """
        Initialize client with enhanced terminal features.

        Args:
            base_url: Base URL of Mockhaus server
        """
        self.base_url = base_url
        self.session = requests.Session()
        self.session_id: str | None = None
        self.current_database: str | None = None

        if PROMPT_TOOLKIT_AVAILABLE:
            self._setup_enhanced_features()

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

    def query(self, sql: str, database: str | None = None) -> dict[str, Any]:
        """
        Execute SQL query against Mockhaus server.

        Args:
            sql: SQL query to execute
            database: Optional database file path

        Returns:
            Query result dictionary
        """
        payload = {"sql": sql, "database": database}
        if self.session_id:
            payload["session_id"] = self.session_id

        response = self.session.post(f"{self.base_url}/api/v1/query", json=payload)
        result = response.json()

        # Update session info from response
        if result.get("success") and "session_id" in result:
            self.session_id = result["session_id"]
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


def main() -> None:
    """Enhanced interactive REPL for Mockhaus."""
    # Print startup message with enhanced features status

    if PROMPT_TOOLKIT_AVAILABLE:
        pass
    else:
        pass

    # Allow custom server URL via environment variable
    server_url = os.getenv("MOCKHAUS_SERVER_URL", "http://localhost:8080")
    client = EnhancedMockhausClient(server_url)

    # Test connection
    try:
        client.health()
    except Exception:
        return

    while True:
        try:
            # Create dynamic prompt with database context
            base_prompt = f"mockhaus({client.current_database})> " if client.current_database else "mockhaus> "

            if PROMPT_TOOLKIT_AVAILABLE:
                query = client.get_input(base_prompt).strip()
            else:
                query = get_multi_line_input_basic(current_db=client.current_database).strip()

            if not query:
                continue

            if query.lower() in ["quit", "exit", "q"]:
                break

            if query.lower() in ["help", "?"]:
                print_help()
                continue

            if query.lower() == "health":
                client.health()
                continue

            # Execute SQL query
            client.query(query)

        except KeyboardInterrupt:
            break
        except Exception:
            pass


if __name__ == "__main__":
    main()
