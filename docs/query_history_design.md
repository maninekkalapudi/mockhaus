# Query History Design

## Overview

Mockhaus maintains a comprehensive query history to aid in debugging and development workflows. All queries executed through the proxy are tracked with full context, enabling developers to understand query patterns, debug issues, and optimize performance.

## Storage Architecture

### DuckDB Backend
- Dedicated DuckDB database for query history
- Default location: `~/.mockhaus/history.duckdb` (configurable)
- Separate from main execution database to avoid conflicts
- Automatic rotation based on size/age
- Concurrent read access, serialized writes
- Leverages DuckDB's columnar storage for efficient analytics

### Schema Design

```sql
-- Using DuckDB's native types and features
CREATE TABLE query_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    query_id UUID NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    session_id VARCHAR(255),
    connection_id VARCHAR(255),
    
    -- Query details
    original_sql TEXT NOT NULL,
    translated_sql TEXT,
    query_type VARCHAR(50), -- SELECT, INSERT, UPDATE, etc.
    
    -- Context
    database_name VARCHAR(255),
    schema_name VARCHAR(255),
    user VARCHAR(255),
    warehouse VARCHAR(255),
    
    -- Execution details
    status VARCHAR(50), -- SUCCESS, ERROR, CANCELLED
    error_message TEXT,
    error_code VARCHAR(50),
    execution_time_ms INTEGER,
    rows_affected INTEGER,
    
    -- Metadata
    client_info JSON, -- User agent, version, etc.
    query_tags JSON,  -- Custom tags for filtering
);

-- DuckDB doesn't need explicit indexes for analytical queries
-- but we can create views for common access patterns
CREATE VIEW recent_queries AS 
SELECT * FROM query_history 
ORDER BY timestamp DESC 
LIMIT 1000;

CREATE TABLE query_metrics (
    query_id UUID PRIMARY KEY,
    parse_time_ms INTEGER,
    translation_time_ms INTEGER,
    execution_time_ms INTEGER,
    total_time_ms INTEGER,
    memory_usage_bytes BIGINT,
    cpu_usage_percent DOUBLE
);

-- Analytical view for performance analysis
CREATE VIEW query_performance AS
SELECT 
    h.*,
    m.parse_time_ms,
    m.translation_time_ms,
    m.execution_time_ms,
    m.total_time_ms
FROM query_history h
JOIN query_metrics m ON h.query_id = m.query_id;
```

## API Design

### Recording Queries

```python
class QueryHistory:
    def record_query(
        self,
        original_sql: str,
        translated_sql: str,
        context: QueryContext,
        result: QueryResult
    ) -> str:
        """Record a query execution with full context."""
        
    def record_error(
        self,
        original_sql: str,
        error: Exception,
        context: QueryContext
    ) -> str:
        """Record a failed query with error details."""
```

### Retrieving History

```python
class QueryHistoryAPI:
    def get_recent(self, limit: int = 100) -> List[QueryRecord]:
        """Get recent queries."""
        
    def search(
        self,
        text: Optional[str] = None,
        status: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        database: Optional[str] = None
    ) -> List[QueryRecord]:
        """Search query history with filters."""
        
    def get_by_id(self, query_id: str) -> QueryRecord:
        """Get specific query by ID."""
        
    def get_statistics(
        self,
        start_time: datetime,
        end_time: datetime
    ) -> QueryStatistics:
        """Get query statistics for time period."""
```

## Features

### 1. Query Replay
```python
# Replay a previous query
history = connection.get_history()
query = history.get_by_id("abc-123")
result = connection.replay_query(query)
```

### 2. Pattern Analysis
```python
# Find slow queries
slow_queries = history.search(
    min_execution_time_ms=1000
)

# Find failed queries by pattern
errors = history.search(
    status="ERROR",
    text="JOIN"
)
```

### 3. Export Capabilities
```python
# Export to various formats
history.export_json("queries.json", start_time=yesterday)
history.export_csv("queries.csv", columns=["timestamp", "sql", "duration"])
history.export_markdown("report.md", template="debug_report")
```

### 4. CLI Integration
```bash
# View recent queries
mockhaus history --recent 20

# Search queries
mockhaus history --search "SELECT * FROM users"

# Show query details
mockhaus history --id abc-123 --verbose

# Export history
mockhaus history --export json --output queries.json
```

### 5. Debug Mode
```python
# Enable verbose debugging
connection = mockhaus.connect(debug=True)

# This will:
# - Log all SQL transformations step-by-step
# - Track memory usage
# - Record detailed timing for each phase
# - Capture query execution plans
```

## Configuration

```yaml
# .mockhaus/config.yaml
history:
  enabled: true
  storage:
    type: duckdb
    path: ~/.mockhaus/history.duckdb
    max_size_mb: 1000
    retention_days: 30
    # DuckDB-specific settings
    memory_limit: 256MB
    threads: 2
  
  capture:
    query_text: true
    execution_plan: false
    result_preview: true
    result_preview_rows: 10
    
  privacy:
    redact_literals: false  # For sensitive environments
    exclude_patterns:
      - ".*password.*"
      - ".*secret.*"
```

## Performance Considerations

1. **Async Recording**: Query history is recorded asynchronously to avoid impacting query performance
2. **Batching**: Multiple queries can be batched for efficient storage
3. **Compression**: Large queries are compressed before storage
4. **Indexing**: Strategic indexes for common search patterns
5. **Cleanup**: Automatic cleanup of old records based on retention policy

## Security & Privacy

1. **Local Storage**: All history is stored locally, never transmitted
2. **Redaction**: Optional redaction of sensitive literals
3. **Encryption**: Optional encryption at rest
4. **Access Control**: File-system based permissions
5. **Audit Trail**: Track who accessed query history

## Use Cases

### Development Debugging
```python
# Find all queries that modified a table
history.search(
    query_type="UPDATE",
    text="users_table"
)
```

### Performance Analysis
```python
# Analyze query performance over time
stats = history.get_statistics(
    start_time=last_week,
    end_time=now,
    group_by="hour"
)
```

### Test Generation
```python
# Generate test cases from real queries
queries = history.get_recent(limit=100)
test_generator.create_tests(queries)
```

### Migration Validation
```python
# Compare queries before/after migration
before = history.search(tag="pre-migration")
after = history.search(tag="post-migration")
validator.compare(before, after)
```