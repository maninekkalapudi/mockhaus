#!/usr/bin/env python3
"""Interactive REPL client for Mockhaus server."""

import json
import requests
from typing import Optional


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
    
    def query(self, sql: str, database: Optional[str] = None) -> dict:
        """
        Execute SQL query against Mockhaus server.
        
        Args:
            sql: SQL query to execute
            database: Optional database file path
            
        Returns:
            Query result dictionary
        """
        response = self.session.post(
            f"{self.base_url}/api/v1/query",
            json={"sql": sql, "database": database}
        )
        return response.json()
    
    def health(self) -> dict:
        """
        Check server health.
        
        Returns:
            Health status dictionary
        """
        response = self.session.get(f"{self.base_url}/api/v1/health")
        return response.json()


def format_results(result: dict) -> str:
    """
    Format query results for display.
    
    Args:
        result: Query result dictionary from server
        
    Returns:
        Formatted string for display
    """
    if not result.get("success"):
        error_detail = result.get('detail', {})
        if isinstance(error_detail, dict):
            error_msg = error_detail.get('detail', 'Unknown error')
        else:
            error_msg = str(error_detail)
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
                value = row.get(header, '')
                # Truncate long values
                str_value = str(value)[:12] if value is not None else ''
                row_values.append(f"{str_value:>12}")
            output.append(" | ".join(row_values))
        
        if len(data) > 10:
            output.append(f"... and {len(data) - 10} more rows")
        
        execution_time = result.get("execution_time", 0)
        output.append(f"\n✅ {len(data)} rows in {execution_time:.3f}s")
        
        return "\n".join(output)
    
    return "✅ Query executed successfully"


def print_help():
    """Print help information."""
    help_text = """
Available commands:
  <SQL>     Execute SQL query (e.g., SELECT * FROM sample_customers)
  health    Check server health status
  help      Show this help message
  quit      Exit the REPL (or Ctrl+C)
  
Examples:
  SELECT * FROM sample_customers LIMIT 5
  CREATE STAGE my_stage URL = 's3://bucket/path/'
  COPY INTO customers FROM '@my_stage/data.csv'
"""
    print(help_text)


def main():
    """Interactive REPL for Mockhaus."""
    print("🏠 Mockhaus Interactive Client")
    print("Type SQL queries, 'health' for server status, 'help' for commands, or 'quit' to exit")
    print("-" * 70)
    
    # Allow custom server URL via environment variable
    import os
    server_url = os.getenv("MOCKHAUS_SERVER_URL", "http://localhost:8080")
    client = MockhausClient(server_url)
    
    # Test connection
    try:
        health = client.health()
        print(f"✅ Connected to Mockhaus v{health['version']} at {server_url}")
    except Exception as e:
        print(f"❌ Cannot connect to server: {e}")
        print("Make sure the server is running: uv run mockhaus serve")
        print(f"Server URL: {server_url}")
        return
    
    print()
    
    while True:
        try:
            query = input("mockhaus> ").strip()
            
            if not query:
                continue
            
            if query.lower() in ['quit', 'exit', 'q']:
                print("👋 Goodbye!")
                break
            
            if query.lower() in ['help', '?']:
                print_help()
                continue
            
            if query.lower() == 'health':
                result = client.health()
                uptime_minutes = result['uptime'] / 60
                print(f"✅ Server healthy - uptime: {uptime_minutes:.1f} minutes")
                continue
            
            # Execute SQL query
            result = client.query(query)
            print(format_results(result))
            print()
            
        except KeyboardInterrupt:
            print("\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()