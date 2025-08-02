# Mockhaus Server Architecture Plan

**Status**: Planning Phase  
**Version**: v0.3.0 Target  
**Last Updated**: August 1, 2025

## Executive Summary

This document outlines the plan to evolve Mockhaus from a CLI-only tool to a full-featured server that can act as a Snowflake proxy. The server will accept Snowflake SQL queries via multiple protocols and execute them against a local DuckDB backend, enabling cost-effective development and testing.

## Current State (v0.2.0)

### Existing Architecture
```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   CLI Client    │───▶│  MockhausExecutor │───▶│     DuckDB      │
│  (click-based)  │    │  (Query Engine)   │    │   (Storage)     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │ Snowflake Translator │
                    │ - SELECT queries    │
                    │ - Data ingestion    │
                    │ - COPY INTO        │
                    │ - Stages/Formats   │
                    └──────────────────┘
```

### Core Components
- **CLI Interface** (`cli.py`): Command-line interface with rich formatting
- **Query Executor** (`executor.py`): Main execution engine with DuckDB backend
- **SQL Translator** (`snowflake/translator.py`): Converts Snowflake SQL to DuckDB SQL
- **Data Ingestion** (`snowflake/ingestion.py`): Handles COPY INTO, stages, and file formats
- **AST Parser** (`snowflake/ast_parser.py`): SQLGlot-based parsing for complex queries

## Target Architecture (v0.3.0)

### HTTP + CLI Server
```
┌──────────────┐                    ┌──────────────┐
│ HTTP Client  │                    │  CLI Client  │
│   (REST)     │                    │   (click)    │
└──────┬───────┘                    └──────┬───────┘
       │                                   │
       ▼                                   ▼
┌─────────────────────────────────────────────────┐
│              Mockhaus Server                    │
├─────────────────────────────────────────────────┤
│  Protocol Layer                                 │
│  ├─ HTTP/REST Handler (FastAPI)                 │
│  └─ CLI Handler (existing)                      │
├─────────────────────────────────────────────────┤
│  Business Logic Layer                           │
│  ├─ Query Router & Validator                    │
│  ├─ Session Manager (HTTP sessions)             │
│  ├─ Connection Pool Manager                     │
│  └─ Query History & Metrics                     │
├─────────────────────────────────────────────────┤
│  Data Processing Layer (existing)               │
│  ├─ MockhausExecutor                            │
│  ├─ Snowflake Translator                        │
│  └─ Data Ingestion Engine                       │
├─────────────────────────────────────────────────┤
│  Storage Layer                                  │
│  ├─ DuckDB (primary)                            │
│  ├─ Metadata Store (SQLite)                     │
│  └─ File System (stages)                        │
└─────────────────────────────────────────────────┘
```

## Implementation Plan

### Phase 1: HTTP/REST Server Foundation
**Target**: Basic REST API for query execution  
**Timeline**: 1-2 weeks

#### New Components
```
src/mockhaus/
├── server/
│   ├── __init__.py
│   ├── app.py              # FastAPI application setup
│   ├── routes/
│   │   ├── __init__.py
│   │   ├── query.py        # Query execution endpoints
│   │   ├── health.py       # Health check endpoints
│   │   └── admin.py        # Admin/management endpoints
│   ├── models/
│   │   ├── __init__.py
│   │   ├── request.py      # Request schemas
│   │   └── response.py     # Response schemas
│   └── middleware/
│       ├── __init__.py
│       ├── auth.py         # Authentication middleware
│       ├── logging.py      # Request logging
│       └── cors.py         # CORS handling
```

#### API Endpoints
```
POST /api/v1/query
  - Execute Snowflake SQL query (including DDL for stages/formats)
  - Request: { "sql": "SELECT * FROM table", "database": "optional" }
  - Response: { "success": true, "data": [...], "execution_time": 0.123 }

GET /api/v1/health
  - Health check endpoint
  - Response: { "status": "healthy", "version": "0.3.0" }

GET /api/v1/databases
  - List available databases
  - Response: { "databases": ["default", "test", "analytics"] }

GET /api/v1/databases/{db}/tables
  - List tables in a database
  - Response: { "tables": ["customers", "orders", "products"] }

GET /api/v1/query/history
  - Get query execution history
  - Response: { "queries": [...] }
```

#### Dependencies to Add
```toml
# pyproject.toml additions
dependencies = [
    # ... existing deps
    "fastapi>=0.104.0",
    "uvicorn[standard]>=0.24.0",
    "pydantic>=2.5.0",
    "python-multipart>=0.0.6",  # For form data
]
```

### Phase 2: Session Management & Connection Pooling
**Target**: Multi-client HTTP support with proper session isolation  
**Timeline**: 1 week

#### Session Manager
```python
# src/mockhaus/server/session.py
class SessionManager:
    """Manages client sessions and database connections."""
    
    def create_session(self, client_id: str) -> Session
    def get_session(self, session_id: str) -> Session
    def cleanup_session(self, session_id: str) -> None
    def list_active_sessions(self) -> List[Session]
```

#### Connection Pool
```python
# src/mockhaus/server/pool.py
class ConnectionPool:
    """Manages DuckDB connection pool for concurrent access."""
    
    def get_connection(self) -> DuckDBConnection
    def return_connection(self, conn: DuckDBConnection) -> None
    def health_check(self) -> bool
```

### Phase 3: Advanced Features
**Target**: Production-ready features  
**Timeline**: 1-2 weeks

#### Query History & Metrics
```python
# src/mockhaus/server/history.py
class QueryHistory:
    """Tracks query execution history and performance metrics."""
    
    def log_query(self, sql: str, execution_time: float, result_count: int)
    def get_history(self, limit: int = 100) -> List[QueryRecord]
    def get_metrics(self) -> MetricsResponse
```

#### Configuration Management
```python
# src/mockhaus/server/config.py
class ServerConfig:
    """Server configuration management."""
    
    host: str = "localhost"
    port: int = 8080
    max_connections: int = 100
    query_timeout: int = 300
    enable_cors: bool = True
    log_level: str = "INFO"
```

## Server Deployment Options

### Option 1: Standalone Server
```bash
# Start server
uv run mockhaus serve --host localhost --port 8080

# With configuration file
uv run mockhaus serve --config server.yaml

# Background daemon
uv run mockhaus serve --daemon --pid-file mockhaus.pid
```

### Option 2: Docker Container
```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY . .
RUN uv sync --frozen
EXPOSE 8080
CMD ["uv", "run", "mockhaus", "serve", "--host", "0.0.0.0"]
```

### Option 3: ASGI Server
```bash
# Production deployment with Gunicorn
gunicorn mockhaus.server.app:app -w 4 -k uvicorn.workers.UvicornWorker
```

## Database Architecture

### Multi-Database Support
```
Storage Structure:
├── databases/
│   ├── default.db          # Default database
│   ├── test.db            # Test database
│   └── analytics.db       # Analytics database
├── metadata/
│   ├── sessions.db        # Session metadata
│   ├── query_history.db   # Query execution history
│   └── server_config.db   # Server configuration
└── stages/
    ├── user_stage/        # User stage files
    ├── internal_stage/    # Internal stage files
    └── external/          # External stage references
```

### Connection Management
- **Read-only connections**: For SELECT queries (connection pool)
- **Write connections**: For DDL/DML operations (serialized)
- **Session isolation**: Each client session has isolated temporary objects

## Security Considerations

### Authentication Options
1. **No Auth** (development): Direct access for local development
2. **API Key**: Simple API key-based authentication
3. **JWT Tokens**: Standard JWT-based authentication
4. **Snowflake Compatible**: Username/password with Snowflake-style tokens

### Authorization
- **Database-level**: Control access to specific databases
- **Schema-level**: Control access to schemas within databases
- **Table-level**: Fine-grained table access control

### Data Protection
- **Query sanitization**: Prevent SQL injection
- **Resource limits**: Query timeout and memory limits
- **Audit logging**: Log all queries and access attempts

## Performance Considerations

### Query Optimization
- **Query caching**: Cache frequently executed queries
- **Result caching**: Cache query results for identical queries
- **Connection pooling**: Reuse database connections
- **Prepared statements**: Support for parameterized queries

### Scalability
- **Horizontal scaling**: Multiple server instances with load balancer
- **Vertical scaling**: Optimize memory usage and CPU utilization
- **Storage scaling**: Support for multiple DuckDB databases

### Monitoring
- **Metrics collection**: Query execution times, success rates, error rates
- **Health checks**: Database connectivity, memory usage, disk space
- **Alerting**: Integration with monitoring systems (Prometheus, Grafana)

## Testing Strategy

### Unit Tests
- Protocol handlers
- Session management
- Query translation
- Error handling

### Integration Tests
- End-to-end query execution
- Multi-client scenarios
- Database isolation
- Error recovery

### Performance Tests
- Concurrent query execution
- Memory usage under load
- Query performance benchmarks
- Connection pool efficiency

### Compatibility Tests
- JDBC driver compatibility
- Snowflake SQL feature parity
- Data type handling
- Error message compatibility

## Migration Path

### Backward Compatibility
- Existing CLI interface remains unchanged
- All current features continue to work
- Configuration files are optional
- In-memory databases still supported

### Upgrade Process
1. Install new dependencies: `uv sync`
2. Run existing tests: `uv run pytest`
3. Start server: `uv run mockhaus serve`
4. Test HTTP endpoints
5. Configure for production use

## Future Enhancements

### Phase 4: Advanced Protocol Support (Optional)
- **WebSocket**: Real-time query streaming
- **GraphQL**: Enhanced query API for metadata
- **Server-Sent Events**: Real-time query progress updates

### Phase 5: Distributed Features (Future)
- **Query federation**: Join data across multiple databases
- **Replication**: Read replicas for scaling
- **Clustering**: Multi-node deployment

### Phase 6: Integration Features (Future)
- **HTTP-based BI Tool Integration**: Custom connectors for HTTP-based tools
- **ETL Integration**: dbt-http-adapter, Airflow operators, Prefect tasks
- **Cloud Data Sources**: HTTP APIs for AWS, GCP, Azure data access

## Success Metrics

### Functional Metrics
- **Query compatibility**: 95%+ of Snowflake queries work without modification
- **Performance**: Query execution within 2x of native DuckDB performance
- **Reliability**: 99.9% uptime for server operations

### Adoption Metrics
- **HTTP client compatibility**: Support for any HTTP client (curl, Postman, custom apps)
- **Feature parity**: All core Snowflake SQL features supported via HTTP API
- **Documentation**: Complete REST API documentation and examples

## Risk Assessment

### Technical Risks
- **Performance overhead**: HTTP server layer may impact query performance
- **Concurrency issues**: Multiple HTTP clients accessing same database
- **Memory usage**: Connection pooling and session management overhead

### Mitigation Strategies
- **Simple implementation**: Focus on HTTP/REST only, avoiding complex protocols
- **Comprehensive testing**: Unit, integration, and performance tests
- **Monitoring**: Real-time performance and error monitoring
- **Fallback options**: CLI interface always available as backup

## Conclusion

The server architecture plan provides a clear path to evolve Mockhaus from a CLI tool to an HTTP-enabled Snowflake proxy server. The simplified approach focuses on HTTP and CLI support, avoiding complex protocol implementations while maintaining all core functionality.

The implementation prioritizes:
1. **Developer Experience**: Easy to use HTTP REST API
2. **Simplicity**: HTTP-only server architecture with existing CLI support
3. **Performance**: Efficient query execution and resource usage
4. **Reliability**: Robust error handling and monitoring
5. **Scalability**: Support for multiple HTTP clients and databases

This streamlined architecture positions Mockhaus as a practical solution for Snowflake development and testing workflows via HTTP APIs, while preserving the existing CLI functionality for direct usage.