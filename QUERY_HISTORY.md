# Query History Documentation

Mockhaus tracks the history of SQL queries executed during a server session. This helps with debugging, performance analysis, and understanding query patterns.

## Overview

Query history captures:
- Original SQL text and any translations performed
- Execution time and performance metrics
- Success/failure status and error details
- Timestamp and session context

**Important:** Query history is stored in-memory and only available during an active server session. When the server restarts, all history is lost.

## Getting Started

### Automatic History Tracking

Query history is automatically enabled when you run the Mockhaus server:

```bash
# Start the server
uv run mockhaus serve

# History is now being tracked for all queries executed through the server
```

### Accessing History

Query history can be accessed in two ways:

1. **Through the REPL** (connected to the running server):
```bash
# Connect to the running server
uv run python -m mockhaus.repl.enhanced_repl

# Query the history table directly
SELECT original_sql, status, execution_time_ms, timestamp 
FROM __mockhaus__.query_history 
ORDER BY timestamp DESC 
LIMIT 10;
```

2. **Through SQL queries** via the server API:
```bash
# Execute a query to view history
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM __mockhaus__.query_history ORDER BY timestamp DESC LIMIT 5"}'
```

## Query History Schema

The `__mockhaus__.query_history` table contains:

| Column | Type | Description |
|--------|------|-------------|
| query_id | VARCHAR | Unique identifier for the query |
| original_sql | TEXT | The original Snowflake SQL query |
| translated_sql | TEXT | The translated DuckDB SQL (if different) |
| status | VARCHAR | SUCCESS or ERROR |
| error_message | TEXT | Error details (if failed) |
| execution_time_ms | FLOAT | Query execution time in milliseconds |
| row_count | INTEGER | Number of rows returned |
| timestamp | TIMESTAMP | When the query was executed |
| session_id | VARCHAR | Server session identifier |
| database_name | VARCHAR | Database context |
| user_id | VARCHAR | User identifier (if available) |

## Common Queries

### Recent Queries
```sql
-- Last 10 queries
SELECT query_id, original_sql, status, execution_time_ms, timestamp
FROM __mockhaus__.query_history
ORDER BY timestamp DESC
LIMIT 10;
```

### Failed Queries
```sql
-- Recent errors
SELECT timestamp, original_sql, error_message
FROM __mockhaus__.query_history
WHERE status = 'ERROR'
ORDER BY timestamp DESC
LIMIT 5;
```

### Performance Analysis
```sql
-- Slowest queries
SELECT original_sql, execution_time_ms, row_count
FROM __mockhaus__.query_history
WHERE status = 'SUCCESS'
ORDER BY execution_time_ms DESC
LIMIT 10;

-- Query statistics
SELECT 
    COUNT(*) as total_queries,
    COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) as successful,
    COUNT(CASE WHEN status = 'ERROR' THEN 1 END) as failed,
    AVG(execution_time_ms) as avg_time_ms,
    MAX(execution_time_ms) as max_time_ms
FROM __mockhaus__.query_history;
```

### Search by Pattern
```sql
-- Find queries on specific table
SELECT query_id, original_sql, timestamp
FROM __mockhaus__.query_history
WHERE LOWER(original_sql) LIKE '%customers%'
ORDER BY timestamp DESC;
```

## Session Lifecycle

1. **Server Start**: A new in-memory database is created with an empty history table
2. **Query Execution**: Each query through the server is automatically logged
3. **Session Duration**: History accumulates as long as the server runs
4. **Server Stop/Restart**: All history is lost when the server stops

## Best Practices

1. **Export Important Queries**: If you need to preserve query history, export it before stopping the server:
   ```sql
   -- Export to CSV format
   COPY (SELECT * FROM __mockhaus__.query_history) 
   TO 'query_history_export.csv' (FORMAT CSV, HEADER);
   ```

2. **Monitor Long Sessions**: For long-running server sessions, periodically check the size of the history table

3. **Use for Debugging**: Query history is excellent for debugging failed queries and understanding performance patterns

## Limitations

- **No Persistence**: History is lost on server restart
- **Memory Usage**: Very long sessions may accumulate significant history
- **CLI Mode**: History is not useful in CLI mode as each command is isolated
- **No Configuration**: History tracking cannot be disabled or configured

## Example Session

```bash
# Terminal 1: Start the server
$ uv run mockhaus serve
Starting Mockhaus server at http://localhost:8080
⚠️  Server running in IN-MEMORY mode - all data is ephemeral

# Terminal 2: Connect REPL and run queries
$ uv run python -m mockhaus.repl.enhanced_repl
mockhaus> SELECT COUNT(*) FROM sample_customers;
mockhaus> SELECT * FROM sample_customers WHERE account_balance > 1000;

# Check history
mockhaus> SELECT query_id, original_sql, execution_time_ms 
         FROM __mockhaus__.query_history 
         ORDER BY timestamp DESC;
```

---

For more information about the Mockhaus architecture, see the main [README](README.md).