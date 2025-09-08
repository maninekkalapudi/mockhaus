# Snowflake REST API Proxy Implementation Plan

## Project Overview

Create a REST API proxy that implements Snowflake's SQL API endpoints (`/api/v2/statements/*`) but routes the execution through Mockhaus's existing translation and execution engine. This will allow existing Snowflake client applications and tools to connect to Mockhaus seamlessly without code changes.

## Current Mockhaus Architecture Analysis

Mockhaus is already well-positioned for this enhancement:

- **Core Translation Engine**: `SnowflakeToDuckDBTranslator` handles SQL translation
- **Execution Engine**: `MockhausExecutor` orchestrates query execution 
- **Server Infrastructure**: FastAPI server with session management
- **Multi-Session Support**: Concurrent session manager with persistent/in-memory options
- **Data Ingestion**: Full COPY INTO, STAGE, and FILE FORMAT support

## Target API Specification

### Endpoints to Implement

1. **POST /api/v2/statements** - Submit SQL statement
2. **GET /api/v2/statements/{statementHandle}** - Get statement status/results  
3. **POST /api/v2/statements/{statementHandle}/cancel** - Cancel statement

### Authentication Support
- JWT Key Pair Authentication (primary)
- OAuth Bearer tokens (secondary)
- Custom API key for development (Mockhaus-specific)

### Request/Response Formats
All endpoints use JSON request/response bodies matching Snowflake's exact schema.

## Implementation Plan

### Phase 1: Core API Infrastructure

#### 1.1 Statement Management System
**Files to create:**
- `src/mockhaus/server/snowflake_api/statement_manager.py`
- `src/mockhaus/server/snowflake_api/models.py`

**Functionality:**
- Statement handle generation (UUID-based)
- Statement lifecycle management (SUBMITTED → RUNNING → SUCCEEDED/FAILED/CANCELED)
- Result caching and retrieval
- Timeout handling
- Cancellation support

#### 1.2 Snowflake API Route Handler
**Files to create:**
- `src/mockhaus/server/snowflake_api/routes.py`
- `src/mockhaus/server/snowflake_api/__init__.py`

**Endpoints:**
```python
@router.post("/api/v2/statements")
async def submit_statement(request: StatementRequest) -> StatementResponse

@router.get("/api/v2/statements/{statement_handle}")  
async def get_statement_status(statement_handle: str) -> StatementResponse

@router.post("/api/v2/statements/{statement_handle}/cancel")
async def cancel_statement(statement_handle: str) -> CancellationResponse
```

#### 1.3 Integration with Existing Server
**Files to modify:**
- `src/mockhaus/server/app.py` - Add new router
- `src/mockhaus/server/session_context.py` - Extend for statement tracking

### Phase 2: Authentication Layer

#### 2.1 Authentication Middleware
**Files to create:**
- `src/mockhaus/server/auth/jwt_handler.py`
- `src/mockhaus/server/auth/oauth_handler.py`  
- `src/mockhaus/server/auth/middleware.py`

**Features:**
- JWT token validation (RSA256 signature verification)
- OAuth bearer token validation
- Custom API key support for development
- User context extraction from tokens
- Session-to-user mapping

#### 2.2 Configuration Management
**Files to modify:**
- Add authentication configuration options
- JWT public key configuration
- OAuth introspection endpoint configuration

### Phase 3: Data Model Mapping

#### 3.1 Request/Response Models  
**In `src/mockhaus/server/snowflake_api/models.py`:**

```python
class StatementRequest(BaseModel):
    statement: str
    timeout: Optional[int] = None
    database: Optional[str] = None
    schema: Optional[str] = None  
    warehouse: Optional[str] = None
    role: Optional[str] = None

class StatementResponse(BaseModel):
    statementHandle: str
    status: StatementStatus  # SUBMITTED, RUNNING, SUCCEEDED, FAILED, CANCELED
    sqlState: str
    dateTime: str
    message: Optional[str] = None
    errorCode: Optional[str] = None
    errorMessage: Optional[str] = None
    resultSetMetaData: Optional[ResultSetMetadata] = None
    resultSet: Optional[List[Dict[str, Any]]] = None
```

#### 3.2 DuckDB Result Mapping
**Files to create:**
- `src/mockhaus/server/snowflake_api/result_mapper.py`

**Functionality:**
- Convert DuckDB column types to Snowflake type names
- Format result sets as JSON objects
- Handle NULL values consistently  
- Map DuckDB errors to Snowflake error codes

### Phase 4: Async Execution Engine

#### 4.1 Async Statement Executor
**Files to create:**
- `src/mockhaus/server/snowflake_api/async_executor.py`

**Features:**
- Background task execution using asyncio
- Statement queuing and priority management
- Progress tracking and status updates
- Resource usage monitoring
- Cancellation support via asyncio.CancelledError

#### 4.2 Result Streaming for Large Queries
**Functionality:**
- Stream large result sets without loading everything into memory
- Pagination support for result retrieval
- Configurable result set size limits

### Phase 5: Session and Context Management

#### 5.1 Enhanced Session Context
**Files to modify:**
- `src/mockhaus/server/session_context.py`

**New features:**
- Database/schema/warehouse/role context per session
- Statement history tracking
- User-specific session isolation
- Session cleanup on user disconnect

#### 5.2 Multi-User Session Management
**Files to modify:**
- `src/mockhaus/server/concurrent_session_manager.py`

**Enhancements:**
- User-based session partitioning
- Per-user session limits
- Session sharing controls
- User activity tracking

### Phase 6: Error Handling and Observability

#### 6.1 Error Mapping
**Files to create:**
- `src/mockhaus/server/snowflake_api/error_mapper.py`

**Functionality:**
- Map common DuckDB errors to Snowflake error codes
- Consistent error message formatting
- SQL state code generation
- Error categorization (syntax, permission, resource, etc.)

#### 6.2 Logging and Metrics
**Features:**
- Request/response logging with correlation IDs
- Query execution metrics (duration, rows processed)
- Authentication success/failure tracking
- Error rate monitoring
- Session lifecycle events

### Phase 7: Testing and Validation

#### 7.1 Unit Tests
**Files to create:**
- `tests/unit/server/snowflake_api/test_statement_manager.py`
- `tests/unit/server/snowflake_api/test_routes.py`
- `tests/unit/server/auth/test_jwt_handler.py`
- `tests/unit/server/snowflake_api/test_result_mapper.py`

#### 7.2 Integration Tests  
**Files to create:**
- `tests/integration/test_snowflake_api_endpoints.py`
- `tests/integration/test_snowflake_api_auth.py`
- `tests/integration/test_multi_user_sessions.py`

#### 7.3 Compatibility Testing
**Test scenarios:**
- Popular Snowflake client libraries (Python, Node.js, JDBC)
- BI tools (Tableau, Power BI, Looker)
- Data pipeline tools (dbt, Airflow)
- Concurrent multi-user access patterns

## Configuration Options

### New Server Configuration
```json
{
  "snowflake_api": {
    "enabled": true,
    "auth": {
      "jwt_public_key_path": "/path/to/jwt/public.key",
      "oauth_introspection_url": "https://oauth.provider/introspect",
      "allow_api_key_auth": true
    },
    "limits": {
      "max_concurrent_statements": 100,
      "statement_timeout_seconds": 3600,
      "result_set_max_rows": 100000
    }
  }
}
```

## Backwards Compatibility

- Existing Mockhaus API (`/api/v1/*`) remains unchanged
- Current CLI and REPL clients continue to work
- Existing session management is extended, not replaced
- Configuration is additive, no breaking changes

## Deployment Considerations

### Development Mode
- API key authentication for easy testing
- Relaxed CORS policies
- Enhanced debug logging
- Mock authentication for testing

### Production Mode  
- Mandatory JWT/OAuth authentication
- Strict CORS policies
- Audit logging
- Rate limiting and resource controls

## Success Metrics

1. **API Compatibility**: 100% compatibility with Snowflake SQL API specification
2. **Client Support**: Major Snowflake clients work without modification
3. **Performance**: Query execution overhead < 10ms vs direct Mockhaus API
4. **Concurrent Users**: Support 50+ concurrent users with 1000+ active statements
5. **Error Handling**: Clear, actionable error messages matching Snowflake patterns

## Implementation Timeline

- **Phase 1-2**: Core API + Auth (1-2 weeks)
- **Phase 3-4**: Data models + Async execution (1 week)  
- **Phase 5**: Session management enhancements (1 week)
- **Phase 6**: Error handling + observability (1 week)
- **Phase 7**: Testing + validation (1-2 weeks)

**Total estimated time: 5-7 weeks**

## Future Enhancements

1. **Query Result Caching**: Cache frequently accessed results
2. **Query Optimization**: Query plan analysis and optimization suggestions  
3. **Advanced Authentication**: SAML, LDAP integration
4. **Multi-Tenant Support**: Organization-level isolation
5. **Query Governance**: Query cost estimation, resource usage controls
6. **Real-time Monitoring**: WebSocket-based query progress updates