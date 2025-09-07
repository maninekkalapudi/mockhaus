# Statement Handle Storage Strategy for Snowflake REST API Proxy

## Overview
Statement handles are unique identifiers (UUIDs) that track the lifecycle of SQL statements submitted through the Snowflake API. They need to be stored persistently to support async execution, result retrieval, and cancellation.

## Storage Requirements

1. **Fast lookups** - O(1) access by statement handle
2. **TTL support** - Automatic expiration of old statements
3. **Persistence options** - Both in-memory and durable storage
4. **Multi-session aware** - Statements tied to user sessions
5. **Concurrent access** - Thread-safe operations
6. **Result storage** - Ability to store large result sets

## Recommended Storage Architecture

### Option 1: Session-Scoped Storage (Recommended)
Store statement handles within the existing `SessionContext`, leveraging the current session management infrastructure.

**Implementation:**
```python
# In src/mockhaus/server/session_context.py
class SessionContext:
    def __init__(self, config: SessionConfig, storage_backend: StorageBackend | None = None):
        # ... existing code ...
        self._statements: Dict[str, StatementContext] = {}
        self._statements_lock = asyncio.Lock()
        
    async def create_statement(self, sql: str, config: StatementConfig) -> str:
        """Create a new statement and return its handle."""
        async with self._statements_lock:
            statement_handle = str(uuid.uuid4())
            self._statements[statement_handle] = StatementContext(
                handle=statement_handle,
                sql=sql,
                status=StatementStatus.SUBMITTED,
                created_at=datetime.utcnow(),
                config=config
            )
            return statement_handle
    
    async def get_statement(self, handle: str) -> Optional[StatementContext]:
        """Retrieve statement by handle."""
        async with self._statements_lock:
            return self._statements.get(handle)
```

**Advantages:**
- Leverages existing session isolation
- Automatic cleanup when session expires
- Works with both memory and persistent sessions
- Natural security boundary (users can only access their own statements)

**Storage Backend:**
- **In-memory sessions**: Statements stored in Python dict
- **Persistent sessions**: Statements stored in DuckDB tables within session database

### Option 2: Global Statement Store with Redis
Use Redis as a centralized statement store across all sessions.

**Implementation:**
```python
# In src/mockhaus/server/snowflake_api/statement_store.py
class RedisStatementStore:
    def __init__(self, redis_url: str):
        self.redis = aioredis.from_url(redis_url)
        
    async def save_statement(self, handle: str, statement: StatementData) -> None:
        # Store with TTL
        await self.redis.setex(
            f"statement:{handle}",
            ttl_seconds=3600,  # 1 hour default
            value=statement.json()
        )
        
        # Add to session index
        await self.redis.sadd(f"session:{statement.session_id}:statements", handle)
    
    async def get_statement(self, handle: str) -> Optional[StatementData]:
        data = await self.redis.get(f"statement:{handle}")
        return StatementData.parse_raw(data) if data else None
```

**Advantages:**
- Highly scalable
- Survives server restarts
- Built-in TTL support
- Can be shared across multiple Mockhaus instances

**Disadvantages:**
- Requires Redis infrastructure
- Additional operational complexity
- Network latency for every operation

### Option 3: Hybrid Approach (Best of Both Worlds)
Combine session-scoped storage with optional Redis backing for production deployments.

**Implementation:**
```python
# In src/mockhaus/server/snowflake_api/statement_manager.py
class StatementManager:
    def __init__(self, storage_type: str = "session"):
        self.storage_type = storage_type
        self.redis_store = RedisStatementStore() if storage_type == "redis" else None
        
    async def store_statement(self, session: SessionContext, statement: StatementData) -> str:
        handle = str(uuid.uuid4())
        
        # Always store in session for fast access
        await session.add_statement(handle, statement)
        
        # Optionally persist to Redis
        if self.redis_store:
            await self.redis_store.save_statement(handle, statement)
            
        return handle
```

## Storage Schema for Persistent Sessions

For persistent sessions using DuckDB, create statement tracking tables:

```sql
-- Statement metadata table
CREATE TABLE IF NOT EXISTS statements (
    handle VARCHAR PRIMARY KEY,
    session_id VARCHAR NOT NULL,
    sql TEXT NOT NULL,
    status VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT,
    error_code VARCHAR,
    result_row_count INTEGER,
    result_metadata JSON,
    -- Indexing for fast lookups
    INDEX idx_session_id (session_id),
    INDEX idx_created_at (created_at)
);

-- Statement results table (for large result sets)
CREATE TABLE IF NOT EXISTS statement_results (
    handle VARCHAR NOT NULL,
    batch_number INTEGER NOT NULL,
    result_data JSON NOT NULL,
    PRIMARY KEY (handle, batch_number),
    FOREIGN KEY (handle) REFERENCES statements(handle) ON DELETE CASCADE
);
```

## Statement Lifecycle Management

### Creation Flow
1. User submits SQL via POST /api/v2/statements
2. Statement handle generated (UUID)
3. Statement stored in session context
4. Background execution task created
5. Handle returned to user immediately

### Storage During Execution
```python
class StatementContext:
    handle: str
    sql: str
    status: StatementStatus  # SUBMITTED -> RUNNING -> SUCCEEDED/FAILED
    created_at: datetime
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    
    # Execution details
    task: Optional[asyncio.Task]
    cancel_event: asyncio.Event
    
    # Results
    result_metadata: Optional[ResultSetMetadata]
    result_rows: Optional[List[Dict]]  # For small results
    result_batch_count: int = 0  # For paginated large results
    
    # Error information
    error_code: Optional[str]
    error_message: Optional[str]
    sql_state: str = "00000"
```

### Result Storage Strategy

**Small Results (< 10MB):**
- Store directly in StatementContext.result_rows
- Return complete result set in single API response

**Large Results (>= 10MB):**
- Store in batches in statement_results table
- Implement pagination in GET endpoint
- Support streaming/chunked responses

**Result Expiration:**
- Results expire with statement TTL (default 1 hour)
- Configurable per-statement via timeout parameter
- Background cleanup task removes expired statements

## Configuration Options

```yaml
snowflake_api:
  statement_storage:
    type: "session"  # or "redis" or "hybrid"
    
    # Statement TTL settings
    default_ttl_seconds: 3600
    max_ttl_seconds: 86400
    
    # Result storage limits
    max_result_size_mb: 100
    result_batch_size: 10000
    
    # Redis configuration (if used)
    redis:
      url: "redis://localhost:6379"
      key_prefix: "mockhaus:statements:"
      
    # Cleanup settings
    cleanup_interval_seconds: 300
    max_statements_per_session: 1000
```

## Migration Path

### Phase 1: Session-Scoped Storage
- Implement statement storage within SessionContext
- Use in-memory dict for memory sessions
- Use DuckDB tables for persistent sessions

### Phase 2: Add Redis Support (Optional)
- Add Redis backing for high-availability deployments
- Implement statement replication across instances
- Support statement migration between servers

### Phase 3: Advanced Features
- Statement result caching
- Query result sharing between users
- Statement history and audit logging

## Benefits of Session-Scoped Approach

1. **Simplicity** - No additional infrastructure required
2. **Security** - Natural isolation between users
3. **Performance** - No network calls for statement access
4. **Consistency** - Statements and data in same storage
5. **Existing Features** - Leverages TTL, cleanup, persistence from session manager

## Implementation Priority

1. **Start with Option 1** (Session-Scoped Storage)
   - Fastest to implement
   - Covers 90% of use cases
   - No operational overhead

2. **Add Redis later if needed** for:
   - Multi-instance deployments
   - High availability requirements
   - Cross-session statement sharing
   - Very long-running queries (> session TTL)