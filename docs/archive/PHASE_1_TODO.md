# Phase 1: HTTP/REST Server Foundation - TODO List

**Branch**: `server-phase-1`  
**Target**: Basic REST API for query execution  
**Timeline**: 1-2 weeks  
**Status**: In Progress

## Overview

Phase 1 focuses on creating the foundational HTTP server infrastructure that will expose Mockhaus's existing SQL translation and execution capabilities via REST API endpoints. This phase maintains backward compatibility with the existing CLI while adding HTTP access.

## High Priority Tasks ✅

### 1. ✅ Git Branch Setup
- [x] Create `server-phase-1` branch
- [x] Switch to development branch

### 2. 📝 Documentation & Planning
- [x] Create Phase 1 todo list document
- [ ] Review and validate implementation approach

### 3. 🔧 Dependencies & Configuration
- [ ] Add FastAPI to `pyproject.toml`
- [ ] Add Uvicorn server to dependencies  
- [ ] Add Pydantic for request/response validation
- [ ] Update development dependencies if needed

### 4. 🏗️ Server Module Structure
Create the foundational server module structure:
```
src/mockhaus/server/
├── __init__.py
├── app.py              # FastAPI application setup
├── routes/
│   ├── __init__.py
│   ├── query.py        # Query execution endpoints
│   └── health.py       # Health check endpoints
├── models/
│   ├── __init__.py
│   ├── request.py      # Request schemas
│   └── response.py     # Response schemas
└── middleware/
    ├── __init__.py
    ├── logging.py      # Request logging
    └── cors.py         # CORS handling
```

### 5. 🚀 Core API Endpoints
- [ ] **POST /api/v1/query** - Execute Snowflake SQL query
  - Accept: `{ "sql": "SELECT * FROM table", "database": "optional" }`
  - Return: `{ "success": true, "data": [...], "execution_time": 0.123 }`
  - Handle all SQL types: SELECT, DDL (CREATE STAGE/FORMAT), DML, etc.
  
- [ ] **GET /api/v1/health** - Health check endpoint
  - Return: `{ "status": "healthy", "version": "0.3.0", "uptime": 3600 }`

## Medium Priority Tasks 🔶

### 6. 📋 Request/Response Models
- [ ] Define Pydantic schemas for query requests
- [ ] Define response models for successful queries
- [ ] Define error response models
- [ ] Add input validation for SQL queries
- [ ] Add database parameter validation

### 7. 🖥️ CLI Integration
- [ ] Add `serve` command to CLI: `mockhaus serve`
- [ ] Support host/port configuration: `--host localhost --port 8080`
- [ ] Support database configuration: `--database my_data.db`
- [ ] Add daemon mode support: `--daemon`

### 8. 🛡️ Error Handling & Middleware
- [ ] Add global exception handler
- [ ] Add request logging middleware
- [ ] Add CORS middleware for web client support
- [ ] Add request timeout handling
- [ ] Handle MockhausExecutor exceptions properly

## Low Priority Tasks 🔹

### 9. 🧪 Testing Infrastructure
- [ ] Create test structure for HTTP endpoints
- [ ] Add test for POST /api/v1/query endpoint
- [ ] Add test for GET /api/v1/health endpoint
- [ ] Add integration tests with MockhausExecutor
- [ ] Add error handling tests

### 10. 📚 Documentation Updates
- [ ] Update README.md with HTTP server usage
- [ ] Add API endpoint documentation
- [ ] Add examples of HTTP client usage (curl, Python requests)
- [ ] Document server configuration options

## Detailed Implementation Checklist

### FastAPI Application Setup (`src/mockhaus/server/app.py`)
```python
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="Mockhaus Server",
    description="Snowflake proxy with DuckDB backend",
    version="0.3.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
from .routes import query, health
app.include_router(query.router, prefix="/api/v1")
app.include_router(health.router, prefix="/api/v1")
```

### Query Endpoint Implementation (`src/mockhaus/server/routes/query.py`)
```python
from fastapi import APIRouter, HTTPException
from ..models.request import QueryRequest
from ..models.response import QueryResponse, ErrorResponse
from ...executor import MockhausExecutor

router = APIRouter()

@router.post("/query", response_model=QueryResponse)
async def execute_query(request: QueryRequest):
    """Execute Snowflake SQL query via HTTP API."""
    try:
        with MockhausExecutor(request.database) as executor:
            if request.database is None:
                executor.create_sample_data()
            
            result = executor.execute_snowflake_sql(request.sql)
            
            if result.success:
                return QueryResponse(
                    success=True,
                    data=result.data,
                    execution_time=result.execution_time,
                    translated_sql=result.translated_sql
                )
            else:
                raise HTTPException(
                    status_code=400,
                    detail=f"Query execution failed: {result.error_message}"
                )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )
```

### Request/Response Models (`src/mockhaus/server/models/`)
```python
# request.py
from pydantic import BaseModel
from typing import Optional

class QueryRequest(BaseModel):
    sql: str
    database: Optional[str] = None

# response.py
from pydantic import BaseModel
from typing import List, Dict, Any, Optional

class QueryResponse(BaseModel):
    success: bool
    data: Optional[List[Dict[str, Any]]] = None
    execution_time: Optional[float] = None
    translated_sql: Optional[str] = None
    message: Optional[str] = None

class HealthResponse(BaseModel):
    status: str
    version: str
    uptime: Optional[float] = None

class ErrorResponse(BaseModel):
    success: bool = False
    error: str
    detail: Optional[str] = None
```

### CLI Server Command (`src/mockhaus/cli.py`)
```python
@main.command()
@click.option("--host", default="localhost", help="Host to bind server")
@click.option("--port", default=8080, type=int, help="Port to bind server")
@click.option("--database", "-d", default=None, help="Default database file")
@click.option("--daemon", is_flag=True, help="Run as daemon")
def serve(host: str, port: int, database: str, daemon: bool) -> None:
    """Start Mockhaus HTTP server."""
    import uvicorn
    from .server.app import app
    
    # Configure default database if specified
    if database:
        # Store in app state or environment variable
        pass
    
    console.print(f"[green]Starting Mockhaus server at http://{host}:{port}[/green]")
    
    uvicorn.run(
        "mockhaus.server.app:app",
        host=host,
        port=port,
        reload=not daemon,
        log_level="info"
    )
```

## Testing Strategy

### Unit Tests
```python
# tests/test_server_routes.py
from fastapi.testclient import TestClient
from mockhaus.server.app import app

client = TestClient(app)

def test_health_endpoint():
    response = client.get("/api/v1/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"

def test_query_endpoint():
    response = client.post(
        "/api/v1/query",
        json={"sql": "SELECT 1 as test"}
    )
    assert response.status_code == 200
    assert response.json()["success"] == True
```

## Success Criteria

### Phase 1 Complete When:
- [ ] HTTP server starts successfully with `mockhaus serve`
- [ ] POST /api/v1/query executes SELECT queries correctly
- [ ] POST /api/v1/query handles DDL queries (CREATE STAGE, CREATE FORMAT)
- [ ] GET /api/v1/health returns proper status
- [ ] Error handling works for invalid SQL
- [ ] CORS is configured for web client access
- [ ] Basic tests pass
- [ ] Documentation is updated with HTTP usage examples

### Example Usage After Phase 1:
```bash
# Start server
uv run mockhaus serve --host localhost --port 8080

# Execute query via curl
curl -X POST http://localhost:8080/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM sample_customers LIMIT 5"}'

# Health check
curl http://localhost:8080/api/v1/health
```

## Next Steps After Phase 1
- Phase 2: Session Management & Connection Pooling
- Phase 3: Advanced Features (metrics, query history, admin endpoints)

---

**Note**: This todo list will be updated as tasks are completed. Each task should be marked as complete when finished and tested.