# Query History Documentation

Mockhaus automatically tracks and stores the history of all SQL queries you execute. This comprehensive audit trail helps with debugging, performance analysis, and understanding query patterns.

## Overview

Query history captures detailed information about every SQL query including:
- Original SQL text and any translations performed
- Execution time and performance metrics
- Success/failure status and error details
- Timestamp and session context
- Database and user information

## Getting Started

### Automatic History Tracking

Query history is **enabled by default** and works automatically when you:
- Use the Mockhaus REPL (`python -m mockhaus.repl.enhanced_repl`)
- Execute queries via the CLI (`python -m mockhaus.cli query "SELECT 1"`)
- Use the Mockhaus server API

No setup required - just start using Mockhaus and your queries will be tracked!

### History Storage Location

Query history is stored in: `~/.mockhaus/history.duckdb`

This is separate from your application databases and won't interfere with your data.

## Viewing Query History

### Recent Queries

View your most recent queries:

```bash
# Show last 10 queries
mockhaus history recent

# Show last 20 queries with full SQL text
mockhaus history recent -n 20 --verbose
```

### Search Queries

Find specific queries using powerful search filters:

```bash
# Search for queries containing "SELECT"
mockhaus history search --text "SELECT"

# Find failed queries from the last 3 days
mockhaus history search --status ERROR --days 3

# Search for INSERT queries in the last week
mockhaus history search --type INSERT --days 7

# Combine filters: find SELECT queries with "users" in last 24 hours
mockhaus history search --text "users" --type SELECT --days 1
```

### Query Details

Get detailed information about a specific query:

```bash
# Show full details for a query (use ID from recent/search results)
mockhaus history show abc123-def456-789
```

This shows:
- Complete SQL text (original and translated)
- Execution timing and performance
- Error messages (if failed)
- Session context and metadata

## Performance Analytics

### Query Statistics

Get insights into your query patterns and performance:

```bash
# Statistics for today
mockhaus history stats

# Statistics for the last week
mockhaus history stats --days 7

# Statistics for the last month  
mockhaus history stats --days 30
```

Statistics include:
- Total queries executed
- Success/failure rates
- Average and 95th percentile execution times
- Breakdown by query type (SELECT, INSERT, etc.)
- Error patterns and frequency

## Managing History

### Clearing Old Queries

Remove old queries to save space:

```bash
# Clear all queries older than 30 days
mockhaus history clear --before 2024-01-01

# Clear ALL query history (with confirmation)
mockhaus history clear

# Skip confirmation prompt
mockhaus history clear --force
```

### Exporting History

Export your query history for analysis:

```bash
# Export last 7 days to JSON
mockhaus history export --format json --output queries.json --days 7

# Export all history to CSV
mockhaus history export --format csv --output all_queries.csv
```

## Configuration

### Basic Settings

Create `mockhaus.config.json` in your project or home directory:

```json
{
  "history": {
    "enabled": true,
    "retention_days": 30,
    "max_size_mb": 1000
  }
}
```

### Environment Variables

Control history via environment variables:

```bash
# Enable/disable history tracking
export MOCKHAUS_HISTORY_ENABLED=true

# Set custom history database location
export MOCKHAUS_HISTORY_DB_PATH="/path/to/my/history.db"

# Set retention period (days)
export MOCKHAUS_HISTORY_RETENTION_DAYS=60
```


## Advanced Usage

### Direct Database Access

You can query the history database directly if needed:

```sql
-- In REPL, attach the history database
ATTACH '~/.mockhaus/history.duckdb' AS history_db;

-- Query your history directly
SELECT original_sql, status, execution_time_ms, timestamp 
FROM history_db.__mockhaus__.query_history 
ORDER BY timestamp DESC 
LIMIT 10;
```

### Integration with Tools

The history data can be integrated with monitoring tools:

```bash
# Get recent errors for alerting
mockhaus history search --status ERROR --days 1 --limit 5

# Export performance data for dashboards
mockhaus history export --format json --days 7
```

## Common Use Cases

### Debugging Failed Queries

When a query fails, find and examine it:

```bash
# Find recent failed queries
mockhaus history search --status ERROR --days 1

# Get full details including error message
mockhaus history show <query-id>
```

### Performance Analysis

Identify slow queries and patterns:

```bash
# Get performance overview
mockhaus history stats --days 7

# Find long-running queries (exported data shows execution times)
mockhaus history export --format json --days 7
```

### Audit and Compliance

Track all database activity:

```bash
# Export complete audit trail
mockhaus history export --format csv --output audit_trail.csv

# Search for specific table access
mockhaus history search --text "users_table" --days 30
```

### Development Workflow

Review and replay queries during development:

```bash
# See what queries you ran today
mockhaus history recent --verbose

# Find that complex query you wrote yesterday
mockhaus history search --text "JOIN" --days 2
```

## Troubleshooting

### History Not Working

If queries aren't being tracked:

1. Check if history is enabled:
   ```bash
   # Should show recent queries
   mockhaus history recent
   ```

2. Verify configuration:
   ```bash
   # Check environment variables
   echo $MOCKHAUS_HISTORY_ENABLED
   ```

3. Check permissions on history directory:
   ```bash
   ls -la ~/.mockhaus/
   ```

### Performance Impact

Query history has minimal performance impact:
- Adds ~1-2ms per query
- Uses separate database connection
- Asynchronous writing (configurable)

### Storage Management

Monitor history database size:

```bash
# Check size
du -h ~/.mockhaus/history.duckdb

# Clear old entries if needed
mockhaus history clear --before $(date -d '30 days ago' +%Y-%m-%d)
```

## Best Practices

1. **Regular Cleanup**: Set up periodic cleanup of old queries
2. **Export Important Data**: Export critical query history before cleanup
3. **Monitor Size**: Keep an eye on history database size
4. **Use Search**: Leverage search filters to find queries quickly
5. **Privacy**: Configure exclusion patterns for sensitive queries

## Frequently Asked Questions

**Q: Does query history slow down my queries?**
A: No, history tracking adds minimal overhead (~1-2ms) and doesn't block query execution.

**Q: Can I disable history for specific queries?**
A: Not per-query, but you can disable it entirely or use exclusion patterns for sensitive queries.

**Q: Where is the history stored?**
A: In `~/.mockhaus/history.duckdb`, separate from your application databases.

**Q: Can I see history from the REPL?**
A: Use the CLI commands (`mockhaus history recent`) or attach the history database in your REPL session.

**Q: How do I backup my query history?**
A: Copy the `~/.mockhaus/history.duckdb` file or use `mockhaus history export`.

---

For technical details about the query history system, see the developer documentation.