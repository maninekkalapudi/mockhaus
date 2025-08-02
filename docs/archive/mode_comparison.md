# Mockhaus Mode Comparison

## Overview

Mockhaus supports two operational modes to serve different use cases:

1. **In-Memory Mode**: Lightweight, ephemeral database for testing
2. **Service Mode**: Persistent, multi-user database service

## Mode Comparison Table

| Feature | In-Memory Mode | Service Mode |
|---------|----------------|--------------|
| **Data Persistence** | No (ephemeral) | Yes (persistent) |
| **Startup Time** | <100ms | ~5 seconds |
| **Multi-User** | No | Yes |
| **Network Access** | Local only | Network accessible |
| **Resource Usage** | Low | Medium-High |
| **Best For** | Unit tests, CI/CD | Team environments |
| **Configuration** | Minimal | Full featured |
| **Monitoring** | Basic | Comprehensive |
| **Scalability** | Single process | Horizontal scaling |

## In-Memory Mode Details

### Architecture
```
Process Memory
├── DuckDB Instance (transient)
├── Query Cache
├── Session State
└── Temporary Tables
```

### Characteristics
- **Zero Setup**: No configuration needed
- **Fast Iteration**: Instant startup and teardown
- **Isolated**: Each instance is completely separate
- **Resource Efficient**: Minimal memory footprint
- **CI/CD Friendly**: Perfect for automated testing

### Example Usage
```python
import mockhaus

# Unit test example
def test_user_query():
    with mockhaus.connect(mode="memory") as conn:
        conn.execute("CREATE TABLE users (id INT, name VARCHAR)")
        conn.execute("INSERT INTO users VALUES (1, 'Alice')")
        
        result = conn.execute("SELECT * FROM users WHERE id = 1")
        assert result[0]['name'] == 'Alice'
    # All data is automatically cleaned up
```

### Configuration
```yaml
# Minimal configuration for memory mode
mode: memory
memory:
  max_memory_mb: 512
  temp_path: /tmp/mockhaus
```

## Service Mode Details

### Architecture
```
Mockhaus Service
├── REST API (FastAPI)
│   ├── /query endpoint
│   ├── /admin endpoints
│   └── Health checks
├── WebSocket Server
│   └── Streaming results
├── Storage Layer
│   ├── Data files (DuckDB)
│   ├── Query history
│   └── User sessions
└── Monitoring
    ├── Prometheus metrics
    └── Structured logging
```

### Characteristics
- **Persistent Storage**: Data survives restarts
- **Multi-Tenant**: Supports multiple users/projects
- **REST API**: HTTP interface for any client
- **Monitoring**: Full observability
- **Scalable**: Can handle team workloads

### Example Usage
```python
import mockhaus

# Connect to shared service
conn = mockhaus.connect(
    mode="service",
    host="mockhaus.dev.internal",
    port=8080,
    token="dev-team-token"
)

# Data persists across connections
tables = conn.execute("SHOW TABLES").fetchall()
print(f"Found {len(tables)} existing tables")
```

### Deployment Example
```yaml
# docker-compose.yml
version: '3.8'
services:
  mockhaus:
    image: mockhaus:latest
    ports:
      - "8080:8080"
    volumes:
      - mockhaus-data:/var/lib/mockhaus
    environment:
      - MOCKHAUS_MODE=service
      - MOCKHAUS_AUTH_ENABLED=true
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 3

volumes:
  mockhaus-data:
```

## Choosing the Right Mode

### Use In-Memory Mode When:
- Running unit tests
- Developing locally
- CI/CD pipelines
- Learning/experimenting
- Need isolation between tests
- Want minimal setup

### Use Service Mode When:
- Setting up team environments
- Need persistent test data
- Running integration tests
- Building demo environments
- Sharing datasets across team
- Need monitoring/metrics

## Mode Switching

### Runtime Selection
```python
# Environment variable
os.environ['MOCKHAUS_MODE'] = 'service'

# Or connection parameter
conn = mockhaus.connect(mode="service")

# Or configuration file
# mockhaus.yaml
mode: service
```

### Migration Between Modes
```python
# Export from memory to service
memory_conn = mockhaus.connect(mode="memory")
# ... populate data ...

service_conn = mockhaus.connect(mode="service")
mockhaus.migrate(from_conn=memory_conn, to_conn=service_conn)
```

## Performance Considerations

### In-Memory Mode
- **Startup**: ~50ms
- **Query Overhead**: <1ms
- **Memory Usage**: 50-200MB base
- **Concurrent Queries**: Limited by CPU

### Service Mode
- **Startup**: 3-5 seconds
- **Query Overhead**: 5-10ms (network)
- **Memory Usage**: 500MB-2GB base
- **Concurrent Queries**: 100+ supported

## Security Implications

### In-Memory Mode
- No authentication needed
- Process-level isolation
- No network exposure
- Temporary data only

### Service Mode
- Token/basic authentication
- Network encryption (TLS)
- Role-based access control
- Audit logging
- Data encryption at rest

## Best Practices

### In-Memory Mode
1. Use context managers for automatic cleanup
2. Don't rely on data persistence
3. Keep datasets small for fast tests
4. Use for unit tests primarily

### Service Mode
1. Set up proper authentication
2. Monitor resource usage
3. Implement backup strategies
4. Use connection pooling
5. Set appropriate retention policies

## Future Enhancements

### Hybrid Mode (Planned)
- Start as in-memory, persist on demand
- Useful for debugging test failures
- "Snapshot" capability

### Edge Mode (Planned)
- Lightweight service for edge deployments
- Sync with central service
- Offline capability