# Mockhaus Client

Interactive client tools for connecting to Mockhaus HTTP server.

## REPL Client

Interactive command-line client for executing SQL queries against Mockhaus server.

### Usage

1. **Start Mockhaus server:**
   ```bash
   uv run mockhaus serve --host localhost --port 8080
   ```

2. **Install client dependencies:**
   ```bash
   pip install requests
   # or add to project dependencies
   uv add requests
   ```

3. **Run the REPL client:**
   ```bash
   uv run mockhaus repl
   ```

### Features

- **Interactive SQL execution** - Type SQL queries and see formatted results
- **Health monitoring** - Check server status with `health` command
- **Error handling** - Clear error messages for failed queries
- **Formatted output** - Tabular display of query results
- **Connection testing** - Automatic server connectivity check on startup

### Available Commands

- `<SQL>` - Execute any SQL query
- `health` - Check server health and uptime
- `help` - Show available commands
- `quit` / `exit` / `q` - Exit the REPL
- `Ctrl+C` - Exit the REPL

### Configuration

Set custom server URL via environment variable:
```bash
export MOCKHAUS_SERVER_URL=http://localhost:8081
uv run mockhaus repl
```

### Example Session

```
🏠 Mockhaus Interactive Client
Type SQL queries, 'health' for server status, 'help' for commands, or 'quit' to exit
----------------------------------------------------------------------
✅ Connected to Mockhaus v0.3.0 at http://localhost:8080

mockhaus> SELECT * FROM sample_customers LIMIT 3
customer_id | customer_name | account_balance | signup_date |    is_active
---------------------------------------------------------------------------
          1 |         Alice |          1500.0 |  2023-01-15 |         True
          2 |           Bob |          2300.0 |  2023-02-20 |         True
          3 |       Charlie |           850.0 |  2023-03-10 |        False

✅ 3 rows in 0.045s

mockhaus> CREATE STAGE my_stage URL = 's3://test-bucket/data/'
✅ Query executed successfully (no results)

mockhaus> health
✅ Server healthy - uptime: 5.2 minutes

mockhaus> quit
👋 Goodbye!
```

### Error Handling

The client provides clear error messages for:
- Server connection failures
- SQL syntax errors
- Query execution errors
- Network timeouts

### Integration

The `MockhausClient` class can be imported and used in other Python scripts:

```python
from mockhaus.repl import MockhausClient

client = MockhausClient("http://localhost:8080")
result = client.query("SELECT COUNT(*) FROM sample_customers")
print(result)
```