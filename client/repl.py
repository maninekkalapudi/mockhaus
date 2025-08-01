#!/usr/bin/env python3
"""Interactive REPL client for Mockhaus server."""

import json
import requests
from typing import Optional

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
        self.session_id: Optional[str] = None
        self.current_database: Optional[str] = None
    
    def query(self, sql: str, database: Optional[str] = None) -> dict:
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
            
        response = self.session.post(
            f"{self.base_url}/api/v1/query",
            json=payload
        )
        result = response.json()
        
        # Update session info from response
        if result.get("success") and "session_id" in result:
            self.session_id = result["session_id"]
            self.current_database = result.get("current_database")
        
        return result
    
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
  <SQL>                Execute SQL query (any DDL, DML, or SELECT)
  health               Check server health status
  help                 Show this help message
  quit                 Exit the REPL (or Ctrl+C)
  
Database Management (Snowflake-style):
  CREATE DATABASE <name>    Create a new persistent database
  USE DATABASE <name>       Switch to an existing database
  USE <name>               Switch to an existing database (short form)
  SHOW DATABASES           List all available databases
  DROP DATABASE <name>     Delete a database
  
Multi-line queries:
  - End with semicolon (;) for immediate execution
  - Press Enter twice to execute without semicolon
  - Use backspace, arrow keys, and command history
  
🎯 Quick Start with Persistent Tables:
  1. CREATE DATABASE my_project;    -- Create a database file
  2. USE DATABASE my_project;       -- Switch to it
  3. CREATE TABLE ...;              -- Now tables will persist!
  
Sample data available:
  sample_customers - Pre-loaded customer data for testing
  
Examples:
  -- Create and use a persistent database 
  CREATE DATABASE analytics;
  USE DATABASE analytics;
  
  -- Now create tables that will persist
  CREATE TABLE employees (
      id INTEGER PRIMARY KEY,
      name VARCHAR(100),
      department VARCHAR(50),
      salary DECIMAL(10,2)
  );
  
  -- Insert data (persists in database file)
  INSERT INTO employees VALUES 
      (1, 'Alice', 'Engineering', 95000),
      (2, 'Bob', 'Marketing', 65000);
  
  -- Query your data (will work in future sessions)
  SELECT * FROM employees WHERE department = 'Engineering';
  
  -- List all tables in current database
  SELECT name FROM sqlite_master WHERE type='table';
  
  -- See all your databases
  SHOW DATABASES;
"""
    print(help_text)


def get_multi_line_input(prompt: str = "mockhaus> ", current_db: Optional[str] = None) -> str:
    """
    Get potentially multi-line SQL input.
    Continues reading until a line ends with semicolon or user presses Enter twice.
    """
    lines = []
    
    # Create dynamic prompt with database context
    if current_db:
        base_prompt = f"mockhaus({current_db})> "
    else:
        base_prompt = prompt
    
    continuation_prompt = " " * (len(base_prompt) - 3) + "... "
    
    try:
        while True:
            if not lines:
                line = input(base_prompt).strip()
            else:
                line = input(continuation_prompt).strip()
            
            if not line and lines:
                # Empty line with existing content - execute
                break
                
            if not line and not lines:
                # Empty line with no content - continue
                continue
                
            lines.append(line)
            
            # If line ends with semicolon, we're done
            if line.endswith(';'):
                break
                
    except EOFError:
        # Ctrl+D pressed
        if lines:
            return ' '.join(lines)
        else:
            raise KeyboardInterrupt
    
    return ' '.join(lines)


def main():
    """Interactive REPL for Mockhaus."""
    print("🏠 Mockhaus Interactive Client")
    print("Type SQL queries, 'health' for server status, 'help' for commands, or 'quit' to exit")
    print("💡 Multi-line queries: End with ';' or press Enter twice")
    print("💡 Use backspace, arrow keys, and command history")
    print("-" * 80)
    
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
            query = get_multi_line_input(current_db=client.current_database).strip()
            
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