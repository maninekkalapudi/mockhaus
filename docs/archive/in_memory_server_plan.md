# In-Memory Server Architecture Plan

## Overview

This document outlines the plan to modify Mockhaus server to operate entirely in-memory while maintaining persistent query history. This approach ensures complete data isolation between server restarts while preserving audit trails.

## Goals

1. **Ephemeral Data**: All databases and tables exist only in memory
2. **Multi-Database Support**: Full CREATE/DROP/USE DATABASE functionality
3. **Persistent History**: Query history remains in persistent storage
4. **Zero Disk Footprint**: No database files created (except history)
5. **Snowflake Compatibility**: Maintain full SQL compatibility

## Current Architecture

The current implementation:
- Creates persistent DuckDB files in `./databases/` directory
- Stores query history in `~/.mockhaus/history.duckdb`
- Supports switching between persistent databases
- Maintains session state for database context

## Proposed Architecture

### High-Level Design

```
┌─────────────┐     ┌──────────────────┐     ┌─────────────────────┐
│   Client    │────▶│  Mockhaus Server │────▶│ Main DuckDB Memory  │
│   (HTTP)    │     │                  │     │                     │
└─────────────┘     └──────────────────┘     ├─────────────────────┤
                            │                 │ ATTACH db1 :memory: │
                            │                 ├─────────────────────┤
                            │                 │ ATTACH db2 :memory: │
                            ▼                 ├─────────────────────┤
                    ┌──────────────────┐     │ ATTACH db3 :memory: │
                    │ History DuckDB   │     └─────────────────────┘
                    │ (Persistent)     │
                    └──────────────────┘
```

### DuckDB ATTACH Mechanism

DuckDB supports attaching multiple databases to a single connection:

```sql
-- Attach an in-memory database with an alias
ATTACH ':memory:' AS my_database;

-- Create tables in the attached database
CREATE TABLE my_database.users (id INT, name VARCHAR);

-- Switch default database context
USE my_database;

-- Query across databases
SELECT * FROM main.table1 JOIN my_database.table2 ON ...;

-- Detach database (removes it completely if in-memory)
DETACH my_database;
```

## Implementation Plan

### 1. Modify SnowflakeDatabaseManager

Create an in-memory mode for the database manager:

```python
class SnowflakeDatabaseManager:
    def __init__(self, connection: duckdb.DuckDBPyConnection | None = None, 
                 in_memory: bool = True):
        self.connection = connection
        self.in_memory = in_memory
        self.attached_databases: set[str] = set()
        self.current_database = "main"
        
        if not in_memory:
            # Original file-based implementation
            self.base_path = Path("./databases")
            self.base_path.mkdir(exist_ok=True)
```

### 2. Database DDL Commands

#### CREATE DATABASE
```python
def _create_database(self, sql: str) -> dict:
    db_name = self._parse_db_name(sql)
    
    if self.in_memory:
        try:
            # Attach a new in-memory database
            self.connection.execute(f"ATTACH ':memory:' AS {db_name}")
            self.attached_databases.add(db_name)
            return {
                "success": True, 
                "message": f"Database '{db_name}' created (in-memory)"
            }
        except Exception as e:
            return {"success": False, "error": str(e)}
```

#### DROP DATABASE
```python
def _drop_database(self, sql: str) -> dict:
    db_name = self._parse_db_name(sql)
    
    if self.in_memory:
        if db_name not in self.attached_databases:
            return {"success": False, "error": f"Database '{db_name}' not found"}
        
        try:
            self.connection.execute(f"DETACH {db_name}")
            self.attached_databases.remove(db_name)
            return {
                "success": True,
                "message": f"Database '{db_name}' dropped"
            }
        except Exception as e:
            return {"success": False, "error": str(e)}
```

#### USE DATABASE
```python
def _use_database(self, sql: str) -> dict:
    db_name = self._parse_db_name(sql)
    
    if self.in_memory:
        # Check if database exists (main always exists)
        if db_name != "main" and db_name not in self.attached_databases:
            return {
                "success": False, 
                "error": f"Database '{db_name}' not found"
            }
        
        try:
            self.connection.execute(f"USE {db_name}")
            self.current_database = db_name
            return {
                "success": True,
                "message": f"Using database '{db_name}'"
            }
        except Exception as e:
            return {"success": False, "error": str(e)}
```

#### SHOW DATABASES
```python
def _show_databases(self) -> dict:
    if self.in_memory:
        databases = [{"name": "main", "current": self.current_database == "main"}]
        
        for db_name in sorted(self.attached_databases):
            databases.append({
                "name": db_name,
                "current": self.current_database == db_name
            })
        
        return {"success": True, "databases": databases}
```

### 3. MockhausExecutor Changes

```python
class MockhausExecutor:
    def __init__(self, database_path: str | None = None, ...):
        # Detect server mode
        self.server_mode = os.environ.get("MOCKHAUS_SERVER_MODE") == "true"
        
        # Force in-memory for server mode
        if self.server_mode:
            self.database_path = None  # Always use :memory:
        else:
            self.database_path = database_path
        
        # ... rest of initialization ...
    
    def _setup_database(self) -> None:
        """Set up the database with initial configuration."""
        if not self._connection:
            return
        
        # Initialize database manager with connection
        in_memory = self.server_mode or self.database_path is None
        self._database_manager = SnowflakeDatabaseManager(
            self._connection, 
            in_memory=in_memory
        )
```

### 4. Server Route Updates

```python
# In server startup, set environment variable
os.environ["MOCKHAUS_SERVER_MODE"] = "true"

# In query route handler
async def execute_query(request: QueryRequest) -> Any:
    # Always use None for database_path in server mode
    # The executor will detect server mode and use in-memory
    with MockhausExecutor(None) as executor:
        # Initial sample data in main database
        executor.create_sample_data()
        
        result = executor.execute_snowflake_sql(request.sql)
        # ... rest of handler ...
```

### 5. Session Management

Since databases are ephemeral, session management becomes simpler:
- No need to track database paths
- Only track current database name
- Sessions reset on server restart

## Benefits

1. **Security**: No persistent data files that could leak sensitive information
2. **Isolation**: Complete separation between server instances
3. **Performance**: In-memory operations are faster
4. **Simplicity**: No file system management needed
5. **Testing**: Each test run starts fresh

## Considerations

### Memory Usage
- Each attached database consumes memory
- Need to monitor total memory usage
- Consider adding limits on number of databases

### Multi-User Scenarios
- All users share the same in-memory databases
- Consider adding namespace/schema isolation
- Future enhancement: per-session databases

### Data Persistence
- Users must understand data is ephemeral
- Provide clear warnings in UI/API
- Consider export functionality for important data

## Testing Plan

1. **Basic Functionality**:
   - CREATE DATABASE creates in-memory database
   - USE DATABASE switches context
   - DROP DATABASE removes database
   - SHOW DATABASES lists all databases

2. **Cross-Database Queries**:
   - JOIN between databases
   - Fully qualified table names
   - Database.schema.table references

3. **Edge Cases**:
   - Maximum databases limit
   - Memory pressure scenarios
   - Concurrent access patterns

4. **Query History**:
   - Verify history persists across restarts
   - Check history tracks database context
   - Ensure no data leakage

## Migration Path

1. Add `MOCKHAUS_SERVER_MODE` environment variable
2. Implement in-memory mode in SnowflakeDatabaseManager
3. Update MockhausExecutor to detect server mode
4. Test thoroughly with existing test suite
5. Update documentation and examples
6. Add server startup banner about ephemeral nature

## Future Enhancements

1. **Per-User Isolation**: Separate databases per user/session
2. **Memory Limits**: Configurable memory quotas
3. **Data Export**: Save database state before shutdown
4. **Warm Start**: Option to restore from snapshot
5. **Monitoring**: Memory usage metrics and alerts

## Conclusion

This architecture provides the best balance of:
- Full Snowflake SQL compatibility
- Complete data ephemerality
- Persistent audit trail via query history
- Simple and secure operation

The use of DuckDB's ATTACH functionality enables elegant multi-database support while maintaining the in-memory guarantee.