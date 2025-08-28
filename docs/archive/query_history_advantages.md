# Why DuckDB for Query History

## Advantages of Using DuckDB for Query History

### 1. Single Technology Stack
- **Simplified Architecture**: One database engine for both execution and history
- **Reduced Dependencies**: No need for SQLite as an additional dependency
- **Consistent SQL Dialect**: Same SQL syntax for querying both data and history
- **Unified Connection Management**: Single connection pool and configuration

### 2. Superior Analytical Capabilities
- **Columnar Storage**: Perfect for analytical queries on history data
- **Fast Aggregations**: Analyze query patterns, performance trends efficiently
- **Window Functions**: Advanced analytics on query sequences and patterns
- **Native JSON Support**: Better handling of query metadata and tags

### 3. Performance Benefits
```sql
-- Example: Find top 10 slowest queries by pattern
SELECT 
    REGEXP_EXTRACT(original_sql, '^(SELECT|INSERT|UPDATE|DELETE).*FROM\s+(\w+)', 2) as table_name,
    COUNT(*) as query_count,
    AVG(execution_time_ms) as avg_time_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY execution_time_ms) as p95_time_ms
FROM query_history
WHERE timestamp > NOW() - INTERVAL '24 hours'
GROUP BY table_name
ORDER BY p95_time_ms DESC
LIMIT 10;
```

### 4. Advanced History Analysis
```sql
-- Time-series analysis of query load
SELECT 
    DATE_TRUNC('hour', timestamp) as hour,
    query_type,
    COUNT(*) as queries,
    SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) as errors,
    AVG(execution_time_ms) as avg_duration
FROM query_history
WHERE timestamp > NOW() - INTERVAL '7 days'
GROUP BY hour, query_type
ORDER BY hour, query_type;
```

### 5. Efficient Storage
- **Compression**: DuckDB's columnar format compresses repetitive SQL text efficiently
- **Partitioning**: Can partition by date for efficient retention management
- **Vectorized Operations**: Fast bulk inserts and deletes for history management

### 6. Export Flexibility
```sql
-- Export to Parquet for external analysis
COPY (
    SELECT * FROM query_history 
    WHERE timestamp > NOW() - INTERVAL '1 day'
) TO 'query_history_export.parquet' (FORMAT PARQUET);

-- Export to CSV for sharing
COPY (
    SELECT timestamp, original_sql, execution_time_ms, status
    FROM query_history
) TO 'queries.csv' (FORMAT CSV, HEADER);
```

### 7. In-Process Analytics
- **No Network Overhead**: History queries run in the same process
- **Shared Memory**: Can join history with actual data tables if needed
- **Real-time Analysis**: Instant insights without data movement

## Implementation Benefits

### Unified Query Interface
```python
class MockhausConnection:
    def __init__(self):
        # Single DuckDB instance
        self.db = duckdb.connect('mockhaus.db')
        self.history_db = self.db  # Same connection!
    
    def execute(self, sql):
        # Execute user query
        result = self._execute_user_query(sql)
        
        # Record in history using same connection
        self.history_db.execute("""
            INSERT INTO query_history (...) VALUES (...)
        """)
        
        return result
```

### Powerful Debugging Queries
```sql
-- Find all queries that touched a specific table today
WITH query_tables AS (
    SELECT 
        query_id,
        original_sql,
        timestamp,
        -- Extract all table names using regex
        REGEXP_EXTRACT_ALL(LOWER(original_sql), 'from\s+([a-z_]+\.)?([a-z_]+)') as tables
    FROM query_history
    WHERE timestamp::DATE = CURRENT_DATE
)
SELECT DISTINCT
    query_id,
    timestamp,
    original_sql
FROM query_tables
WHERE list_contains(tables, 'users_table');
```

### History-Driven Testing
```sql
-- Generate test cases from real query patterns
SELECT 
    query_type,
    -- Anonymize queries for test cases
    REGEXP_REPLACE(original_sql, '\d+', 'X') as pattern,
    COUNT(*) as frequency,
    ARRAY_AGG(DISTINCT database_name || '.' || schema_name) as contexts
FROM query_history
WHERE status = 'SUCCESS'
GROUP BY query_type, pattern
HAVING COUNT(*) > 10
ORDER BY frequency DESC;
```

## Conclusion

Using DuckDB for query history transforms it from a simple log into a powerful analytical tool. The combination of:
- Columnar storage for efficient history data
- SQL analytics for pattern discovery  
- Same engine for execution and analysis
- Native support for complex data types

Makes DuckDB the ideal choice for Mockhaus's query history feature.