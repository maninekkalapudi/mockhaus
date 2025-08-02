# Mockhaus - Snowflake Proxy with DuckDB

## Executive Summary

Mockhaus is a flexible Snowflake proxy that translates Snowflake SQL to DuckDB SQL, enabling cost-effective local development and testing without connecting to actual Snowflake instances. It can run in two modes: as a lightweight in-memory database for ephemeral testing or as a persistent service for team environments. Mockhaus leverages sqlglot for SQL translation and DuckDB as the execution engine.

## Project Goals

1. **Cost Reduction**: Eliminate Snowflake compute costs during development
2. **Speed**: Faster iteration cycles with local execution
3. **Compatibility**: Support common Snowflake SQL patterns and functions
4. **Ease of Use**: Drop-in replacement for development environments
5. **Testing**: Enable unit testing of Snowflake SQL without cloud dependencies
6. **Debugging**: Comprehensive query history tracking for development insights

## Architecture Overview

### Dual-Mode Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Application    │────▶│   Mockhaus       │────▶│    DuckDB       │
│  (Python/JDBC)  │     │                  │     │ (In-Memory or   │
└─────────────────┘     └──────────────────┘     │   Persistent)   │
                               │                  └─────────────────┘
                               │
                               ▼
                        ┌──────────────────┐
                        │     sqlglot      │
                        │  (SQL Translator)│
                        └──────────────────┘
```

### Mode 1: In-Memory (Development/Testing)
- Ephemeral data - lost on restart
- Fast startup time
- Perfect for unit tests
- Isolated environments
- No persistence overhead

### Mode 2: Service (Team/Integration)
- Persistent data storage
- REST API + Snowflake protocol
- Multi-user support
- Shared test datasets
- Query history persistence
- Monitoring and metrics

## Operational Modes

### In-Memory Mode
```python
# Quick start for testing
import mockhaus

conn = mockhaus.connect(mode="memory")
conn.execute("CREATE TABLE test (id INT, name VARCHAR)")
conn.execute("INSERT INTO test VALUES (1, 'Alice')")
# Data exists only during this session
```

**Use Cases:**
- Unit tests
- CI/CD pipelines  
- Local development
- Isolated testing

### Service Mode
```python
# Connect to persistent Mockhaus service
import mockhaus

conn = mockhaus.connect(
    mode="service",
    host="mockhaus.dev.company.com",
    port=8080
)
# Data persists across connections
```

**Use Cases:**
- Shared development environments
- Integration testing
- Demo environments
- Training/onboarding
- Long-running test suites

### Mode Configuration
```yaml
# mockhaus.config.yaml
mode: service  # or "memory"

service:
  host: 0.0.0.0
  port: 8080
  storage:
    path: /var/lib/mockhaus/data
    max_size_gb: 100
  
  authentication:
    enabled: true
    method: token  # or "basic", "none"
  
  multi_tenancy:
    enabled: true
    isolation: database  # or "schema"

memory:
  max_memory_gb: 4
  temp_directory: /tmp/mockhaus
```

## Core Components

### 1. Connection Layer
- **Snowflake-Compatible Interface**
  - Implement snowflake-connector-python compatible API
  - Support connection parameters (account, warehouse, database, schema)
  - Mock authentication for local development
  - Session management and context switching

### 2. SQL Translation Engine
- **sqlglot Integration**
  - Parse Snowflake SQL dialect
  - Transform to DuckDB SQL dialect
  - Handle dialect-specific syntax differences
  - Custom function mapping registry

### 3. Schema Management
- **Three-Level Namespace**
  - Database → Schema → Table hierarchy
  - USE DATABASE/SCHEMA commands
  - Fully qualified names (db.schema.table)
  - Information schema compatibility

### 4. Data Type System
- **Type Mapping**
  ```
  Snowflake → DuckDB
  NUMBER    → DECIMAL/INTEGER
  VARCHAR   → VARCHAR
  TIMESTAMP → TIMESTAMP
  VARIANT   → JSON
  ARRAY     → LIST
  OBJECT    → STRUCT
  ```

### 5. Function Translation
- **Built-in Functions**
  - Date/time functions (DATEADD, DATEDIFF, etc.)
  - String functions (SPLIT_PART, REGEXP_SUBSTR, etc.)
  - Window functions
  - Aggregate functions
  - Semi-structured data functions (FLATTEN, GET_PATH, etc.)

### 6. Query History & Debugging
- **Query Tracking**
  - Store all executed queries with timestamps
  - Track original Snowflake SQL and translated DuckDB SQL
  - Record execution time and status (success/failure)
  - Capture error messages and stack traces
  - Session and connection context
- **History Management**
  - DuckDB tables for persistent storage
  - Configurable retention policies
  - Export capabilities (JSON, CSV, Parquet)
  - Search and filter functionality
  - Query replay for debugging
  - Analytical queries on history data

## Implementation Phases

### Phase 1: Foundation (Weeks 1-2)
- [ ] Basic project structure and packaging
- [ ] DuckDB connection management (both modes)
- [ ] Mode switching architecture
- [ ] Simple SELECT query translation
- [ ] Basic data type mapping
- [ ] Query history infrastructure setup
- [ ] Unit test framework

### Phase 2: Core SQL Support (Weeks 3-4)
- [ ] Connection interface implementation
- [ ] Database/schema context management
- [ ] Common SQL patterns (JOIN, GROUP BY, CTEs)
- [ ] Basic function translations
- [ ] Error handling and reporting
- [ ] Query history API and storage
- [ ] Service mode REST API
- [ ] Persistence layer for service mode

### Phase 3: Advanced Features (Weeks 5-6)
- [ ] Window functions
- [ ] Recursive CTEs
- [ ] VARIANT/JSON operations
- [ ] Table creation and DDL
- [ ] Temporary tables

### Phase 4: Snowflake Specifics (Weeks 7-8)
- [ ] FLATTEN and semi-structured queries
- [ ] Time travel syntax (AT/BEFORE)
- [ ] COPY INTO simulation
- [ ] Stage references (limited support)
- [ ] Stored procedures (basic support)

### Phase 5: Polish & Performance (Weeks 9-10)
- [ ] Performance optimizations
- [ ] Comprehensive test suite
- [ ] Documentation
- [ ] Example migrations
- [ ] CI/CD pipeline
- [ ] Docker image for service mode
- [ ] Kubernetes deployment manifests
- [ ] Monitoring and alerting setup

## Technical Challenges & Solutions

### 1. SQL Dialect Differences
**Challenge**: Snowflake has unique syntax and functions
**Solution**: 
- Maintain comprehensive function mapping registry
- Custom sqlglot transformers for Snowflake-specific patterns
- Fallback mechanisms with clear error messages

### 2. Semi-Structured Data
**Challenge**: VARIANT, OBJECT, ARRAY types
**Solution**:
- Map to DuckDB's JSON type
- Implement custom functions for path access
- Support dot notation and bracket notation

### 3. Case Sensitivity
**Challenge**: Snowflake's case-insensitive identifiers
**Solution**:
- Normalize all identifiers to uppercase
- Handle quoted identifiers appropriately
- Maintain mapping tables for original case

### 4. Missing Features
**Challenge**: Some Snowflake features have no DuckDB equivalent
**Solution**:
- Document limitations clearly
- Provide workarounds where possible
- Fail gracefully with helpful messages

## Success Metrics

1. **Compatibility**: Support 80% of common Snowflake SQL patterns
2. **Performance**: Sub-second response for typical development queries
3. **Adoption**: Easy integration with existing codebases
4. **Testing**: 90%+ test coverage
5. **Documentation**: Comprehensive guides and examples
6. **Debugging**: 100% query capture with <5ms overhead

## Limitations (Planned)

1. **Not Supported**:
   - External stages and cloud storage
   - Snowpipe and streaming
   - Data sharing
   - Snowflake's native query history (replaced with local history)
   - Role-based access control
   - Multi-cluster warehouses

2. **Limited Support**:
   - Time travel (basic simulation only)
   - Stored procedures (simple cases)
   - User-defined functions
   - Some specialized functions

## Development Environment

### Technology Stack
- **Language**: Python 3.10+
- **Core Dependencies**:
  - DuckDB (latest)
  - sqlglot (latest)
  - snowflake-connector-python (for interface compatibility)
  - FastAPI (for service mode)
  - uvicorn (ASGI server)
- **Testing**: pytest, hypothesis
- **Build**: uv, hatch
- **CI/CD**: GitHub Actions
- **Containerization**: Docker, Kubernetes

### Project Structure
```
mockhaus/
├── src/
│   └── mockhaus/
│       ├── __init__.py
│       ├── connection.py      # Snowflake-compatible connection
│       ├── translator.py      # SQL translation engine
│       ├── executor.py        # Query execution
│       ├── schema.py          # Schema management
│       ├── functions.py       # Function mappings
│       ├── types.py           # Type mappings
│       ├── history.py         # Query history tracking
│       ├── debug.py           # Debugging utilities
│       ├── modes/
│       │   ├── __init__.py
│       │   ├── memory.py      # In-memory mode implementation
│       │   └── service.py     # Service mode implementation
│       └── api/
│           ├── __init__.py
│           ├── routes.py      # REST API endpoints
│           └── models.py      # API data models
├── tests/
│   ├── unit/
│   ├── integration/
│   └── compatibility/         # Snowflake SQL test suite
├── examples/
│   ├── basic_usage.py
│   ├── service_deployment.py
│   ├── debugging_guide.py
│   └── migration_guide.md
├── deployment/
│   ├── docker/
│   │   └── Dockerfile
│   └── k8s/
│       ├── deployment.yaml
│       └── service.yaml
└── docs/
    ├── architecture.md
    ├── deployment.md
    ├── limitations.md
    ├── debugging.md
    └── api_reference.md
```

## Testing Strategy

### 1. Unit Tests
- Individual component testing
- SQL translation accuracy
- Function mapping correctness

### 2. Integration Tests
- End-to-end query execution
- Multi-statement transactions
- Schema operations

### 3. Compatibility Tests
- Real Snowflake SQL examples
- Common patterns from production
- Performance benchmarks

### 4. Regression Tests
- Automated test suite
- Version compatibility
- Breaking change detection

## Release Plan

### v0.1.0 (MVP)
- Basic SELECT queries
- Common functions
- Simple schema management

### v0.2.0
- Complex queries (CTEs, window functions)
- Better error messages
- Performance improvements

### v0.3.0
- Semi-structured data support
- DDL operations
- Extended function library

### v1.0.0
- Production-ready
- Comprehensive documentation
- Stable API
- Performance optimized

## Next Steps

1. **Proof of Concept**: Build minimal translator for SELECT queries
2. **Validate Approach**: Test with real Snowflake SQL examples
3. **Community Feedback**: Share early version for input
4. **Iterate**: Refine based on usage patterns
5. **Document**: Create migration guides and best practices

## Risk Mitigation

1. **Scope Creep**: Focus on development use cases only
2. **Compatibility**: Accept that 100% compatibility is not the goal
3. **Performance**: Set realistic expectations for complex queries
4. **Maintenance**: Plan for ongoing Snowflake feature additions

## Success Criteria

The project will be considered successful when:
1. Developers can run 80% of their Snowflake SQL locally
2. Test suites run 10x faster than against real Snowflake
3. Zero Snowflake costs for development environments
4. Clear documentation of supported/unsupported features
5. Active adoption in at least 5 projects
6. Query history enables debugging issues 50% faster

---

*This document serves as the high-level plan for Mockhaus. It will be updated as the project evolves and new requirements emerge.*