#!/usr/bin/env python3
"""Enhanced interactive REPL client for Mockhaus server using prompt_toolkit."""

import json
import os
import requests
from typing import Optional

# Try to import prompt_toolkit for enhanced features
try:
    from prompt_toolkit import prompt
    from prompt_toolkit.history import FileHistory
    from prompt_toolkit.completion import WordCompleter
    from prompt_toolkit.shortcuts import CompleteStyle
    from prompt_toolkit.key_binding import KeyBindings
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
        self.session_id: Optional[str] = None
        self.current_database: Optional[str] = None
        
        if PROMPT_TOOLKIT_AVAILABLE:
            self._setup_enhanced_features()
        
    def _setup_enhanced_features(self):
        """Setup enhanced terminal features when prompt_toolkit is available."""
        # Setup persistent history file
        history_file = os.path.expanduser('~/.mockhaus_history')
        self.history = FileHistory(history_file)
        
        # SQL keywords for auto-completion
        sql_keywords = [
            'SELECT', 'FROM', 'WHERE', 'CREATE', 'DATABASE', 'TABLE',
            'INSERT', 'UPDATE', 'DELETE', 'DROP', 'USE', 'SHOW',
            'DATABASES', 'TABLES', 'INT', 'INTEGER', 'VARCHAR', 'DECIMAL',
            'PRIMARY', 'KEY', 'IF', 'NOT', 'EXISTS', 'COPY', 'INTO',
            'VALUES', 'ORDER', 'BY', 'GROUP', 'HAVING', 'LIMIT',
            'OFFSET', 'INNER', 'LEFT', 'RIGHT', 'OUTER', 'JOIN',
            'ON', 'AS', 'DISTINCT', 'COUNT', 'SUM', 'AVG', 'MAX', 'MIN',
            'AND', 'OR', 'LIKE', 'IN', 'BETWEEN', 'IS', 'NULL',
            'ALTER', 'ADD', 'COLUMN', 'CONSTRAINT', 'INDEX',
            'UNION', 'ALL', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END'
        ]
        
        self.sql_completer = WordCompleter(
            sql_keywords,
            ignore_case=True,
            match_middle=True
        )
        
        # Setup custom key bindings
        self.bindings = KeyBindings()
        self._setup_key_bindings()
    
    def _setup_key_bindings(self):
        """Setup custom key bindings for enhanced functionality."""
        @self.bindings.add('f5')
        def show_databases(event):
            """F5: Insert SHOW DATABASES command"""
            event.app.current_buffer.insert_text('SHOW DATABASES;')
        
        @self.bindings.add('f6')
        def show_tables(event):
            """F6: Insert query to show tables"""
            event.app.current_buffer.insert_text("SELECT name FROM sqlite_master WHERE type='table';")
        
        @self.bindings.add('c-l')
        def clear_screen(event):
            """Ctrl+L: Clear screen"""
            event.app.output.clear()
    
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
        
        lines = []
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
                    wrap_lines=True
                ).strip()
                
                # Empty line handling
                if not line and lines:
                    # Empty line with existing content - execute
                    break
                elif not line and not lines:
                    # Empty line with no content - continue
                    continue
                
                lines.append(line)
                
                # If line ends with semicolon, we're done
                if line.endswith(';'):
                    break
                    
            return ' '.join(lines)
            
        except (EOFError, KeyboardInterrupt):
            if lines:
                return ' '.join(lines)
            else:
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
    """Print help information with enhanced features."""
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
  
Enhanced Features (when prompt_toolkit is available):
  🚀 Auto-completion      SQL keywords with Tab completion
  📚 Persistent history   Commands saved across sessions
  🔍 History search      Use Ctrl+R to search command history
  ⌨️  Custom shortcuts    F5=SHOW DATABASES, F6=SHOW TABLES, Ctrl+L=clear
  📝 Multi-line editing   Better editing experience for complex queries
  
Multi-line queries:
  - End with semicolon (;) for immediate execution
  - Press Ctrl+D or Ctrl+C to execute/exit
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


def get_multi_line_input_basic(prompt: str = "mockhaus> ", current_db: Optional[str] = None) -> str:
    """
    Fallback multi-line input for when prompt_toolkit is not available.
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
    """Enhanced interactive REPL for Mockhaus."""
    # Print startup message with enhanced features status
    print("🏠 Mockhaus Interactive Client")
    
    if PROMPT_TOOLKIT_AVAILABLE:
        print("🚀 Enhanced REPL loaded (prompt_toolkit)")
        print("   Features: Auto-completion, persistent history, advanced editing")
        print("   Shortcuts: F5=SHOW DATABASES, F6=SHOW TABLES, Ctrl+L=clear, Ctrl+R=search")
    else:
        print("📱 Basic REPL loaded (prompt_toolkit not available)")
        print("   Install with: uv add prompt-toolkit")
    
    print("Type SQL queries, 'health' for server status, 'help' for commands, or 'quit' to exit")
    print("-" * 80)
    
    # Allow custom server URL via environment variable
    server_url = os.getenv("MOCKHAUS_SERVER_URL", "http://localhost:8080")
    client = EnhancedMockhausClient(server_url)
    
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
            # Create dynamic prompt with database context
            if client.current_database:
                base_prompt = f"mockhaus({client.current_database})> "
            else:
                base_prompt = "mockhaus> "
            
            if PROMPT_TOOLKIT_AVAILABLE:
                query = client.get_input(base_prompt).strip()
            else:
                query = get_multi_line_input_basic(current_db=client.current_database).strip()
            
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