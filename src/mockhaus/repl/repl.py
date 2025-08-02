#!/usr/bin/env python3
"""Interactive REPL client for Mockhaus server."""

from typing import Any, cast

import requests

# Enable readline for better input editing (backspace, arrow keys, history)
try:
    import readline

    # Enable history and basic editing
    readline.parse_and_bind("tab: complete")
    readline.parse_and_bind("set editing-mode emacs")
except ImportError:
    # readline not available on this system
    pass


class MockhausClient:
    """HTTP client for Mockhaus server."""

    def __init__(self, base_url: str = "http://localhost:8080"):
        """
        Initialize client.

        Args:
            base_url: Base URL of Mockhaus server
        """
        self.base_url = base_url
        self.session = requests.Session()
        self.session_id: str | None = None
        self.current_database: str | None = None

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

    # Simple table formatting
    if len(data) > 0:
        headers = list(data[0].keys())
        output = []

        # Header row
        header_line = " | ".join(f"{h:>12}" for h in headers)
        output.append(header_line)
        output.append("-" * len(header_line))

        # Data rows (limit to first 10)
        for row in data[:10]:
            row_values = []
            for header in headers:
                value = row.get(header, "")
                # Truncate long values
                str_value = str(value)[:12] if value is not None else ""
                row_values.append(f"{str_value:>12}")
            output.append(" | ".join(row_values))

        if len(data) > 10:
            output.append(f"... and {len(data) - 10} more rows")

        execution_time = result.get("execution_time", 0)
        output.append(f"\n✅ {len(data)} rows in {execution_time:.3f}s")

        return "\n".join(output)

    return "✅ Query executed successfully"


def print_help() -> None:
    """Print help information."""


def get_multi_line_input(prompt: str = "mockhaus> ", current_db: str | None = None) -> str:
    """
    Get potentially multi-line SQL input.
    Continues reading until a line ends with semicolon or user presses Enter twice.
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
    """Interactive REPL for Mockhaus."""

    # Allow custom server URL via environment variable
    import os

    server_url = os.getenv("MOCKHAUS_SERVER_URL", "http://localhost:8080")
    client = MockhausClient(server_url)

    # Test connection
    try:
        client.health()
    except Exception:
        return

    while True:
        try:
            query = get_multi_line_input(current_db=client.current_database).strip()

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
