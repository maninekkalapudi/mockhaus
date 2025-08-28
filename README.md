# Mockhaus

**Snowflake proxy with DuckDB backend - Complete SQL translation and data ingestion engine**

Mockhaus translates Snowflake SQL queries to DuckDB SQL and executes them locally, enabling cost-effective development and testing without connecting to actual Snowflake instances. Features complete data ingestion capabilities with stages, file formats, COPY INTO operations, and full session management.

## Current Status (v0.4.0 - Production Session Architecture)

### Core Features
- Complete SELECT query translation and execution
- WHERE clauses with numeric, string, and boolean filters
- ORDER BY, LIMIT, and complex query support
- Aggregate functions (COUNT, AVG, MAX, MIN, SUM)
- Date functions (CURRENT_DATE, SYSDATE)
- Window functions and CTEs
- Rich CLI with syntax highlighting and session management

### Data Ingestion
- **Stages**: CREATE/DROP STAGE with URL support (S3, GCS, Azure, local files)
- **File Formats**: CREATE/DROP FILE FORMAT with CSV, JSON, Parquet support
- **COPY INTO**: Full data loading from stages with named and inline formats
- **AST Parsing**: Robust parsing with sqlglot for reliable statement parsing

### Session Management
- **Multi-Session Architecture**: Isolated sessions with independent database state
- **Memory Sessions**: Fast in-memory sessions with configurable TTL
- **Persistent Sessions**: Local file-backed sessions that survive server restarts
- **Session API**: Full REST API for session lifecycle management
- **Automatic Cleanup**: Background cleanup of expired sessions with LRU eviction

### Server Architecture
- **HTTP API**: RESTful endpoints for query execution and session management
- **Query History**: Comprehensive tracking with search and analytics
- **Concurrent Sessions**: Support for multiple simultaneous isolated sessions
- **Thread Safety**: Safe concurrent access with proper locking mechanisms

## Quick Start

```bash
# Install dependencies
uv sync

# Start the HTTP server
uv run mockhaus serve --host localhost --port 8080

# Run REPL with automatic session management
uv run mockhaus repl --server-url http://localhost:8080
```

## Server Mode

### Starting the HTTP Server

```bash
# Start the server on default port 8080
uv run mockhaus serve

# Start on a custom port with session limits
uv run mockhaus serve --port 9000 --max-sessions 50

# Configure session TTL (default: 1 hour)
MOCKHAUS_SESSION_TTL=3600 uv run mockhaus serve
```

### Session Management API

The server provides comprehensive session management:

```bash
# Create a memory session
curl -X POST http://localhost:8080/api/v1/sessions \
  -H "Content-Type: application/json" \
  -d '{"type": "memory", "ttl_seconds": 1800}'

# Create a persistent session
curl -X POST http://localhost:8080/api/v1/sessions \
  -H "Content-Type: application/json" \
  -d '{
    "type": "persistent",
    "storage": {"type": "local", "path": "/tmp/my_session.db"}
  }'

# List all active sessions
curl http://localhost:8080/api/v1/sessions

# Get session information
curl http://localhost:8080/api/v1/sessions/{session_id}

# Execute query in a specific session
curl -X POST http://localhost:8080/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT * FROM sample_customers LIMIT 5",
    "session_id": "your-session-id"
  }'

# Terminate a session
curl -X DELETE http://localhost:8080/api/v1/sessions/{session_id}

# Cleanup expired sessions
curl -X POST http://localhost:8080/api/v1/sessions/cleanup
```

## REPL Client

The REPL is a session-aware client that connects to the Mockhaus HTTP server:

```bash
# Start server and REPL
uv run mockhaus serve &
uv run mockhaus repl

# REPL with custom session configuration
uv run mockhaus repl --session-type persistent --session-ttl 7200

# REPL with specific session ID
uv run mockhaus repl --session-id my-session-123

# REPL with persistent storage
uv run mockhaus repl --session-type persistent --persistent-path /tmp/my_session.db
```

### REPL Commands

- **SQL queries** - Execute against the current session
- `session` - Show current session information
- `sessions` - List all active sessions on the server
- `health` - Check server health and uptime
- `help` - Show available commands
- `quit` / `exit` / `q` - Exit and cleanup session

Example REPL session:
```
Mockhaus Interactive Client v0.4.0
Connected to server at http://localhost:8080
Session: abc123-session (memory, expires in 59m)

mockhaus> CREATE TABLE users (id INT, name VARCHAR, email VARCHAR)
Query executed successfully (no results)

mockhaus> INSERT INTO users VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com')
Query executed successfully (2 rows affected)

mockhaus> SELECT * FROM users WHERE name LIKE 'A%'
id | name  | email
---|-------|------------------
 1 | Alice | alice@example.com

1 row in 0.023s

mockhaus> session
Session ID: abc123-session
Type: memory
Created: 2025-08-29 10:30:15
TTL: 3600 seconds
Status: active

mockhaus> sessions
Active Sessions: 3
Total Sessions: 5
Usage: 60% of 100 max sessions
```

## Session Types

### Memory Sessions
Fast in-memory sessions ideal for development and testing:
- Isolated DuckDB database in memory
- Configurable TTL (time-to-live)
- Automatic cleanup when expired
- No persistence across server restarts

### Persistent Sessions  
File-backed sessions for data persistence:
- Local file storage with DuckDB database files
- Survive server restarts
- Configurable storage paths
- Automatic synchronization

## Query Examples

### Basic SQL Operations
```sql
-- Data Definition
CREATE TABLE customers (
    id INT,
    name VARCHAR(100),
    balance DECIMAL(10,2),
    signup_date DATE,
    is_active BOOLEAN
);

-- Data Manipulation
INSERT INTO customers VALUES 
(1, 'Alice Johnson', 1500.00, '2023-01-15', true),
(2, 'Bob Smith', 2300.50, '2023-02-20', true),
(3, 'Carol Davis', 750.25, '2023-03-10', false);

-- Complex Queries
SELECT 
    name,
    balance,
    CASE 
        WHEN balance > 2000 THEN 'Premium'
        WHEN balance > 1000 THEN 'Standard' 
        ELSE 'Basic'
    END as tier
FROM customers 
WHERE is_active = true
ORDER BY balance DESC;

-- Aggregation and Window Functions
SELECT 
    name,
    balance,
    AVG(balance) OVER () as avg_balance,
    RANK() OVER (ORDER BY balance DESC) as rank
FROM customers;
```

### Data Ingestion Workflow
```sql
-- 1. Create stages for data sources
CREATE STAGE s3_data URL = 's3://my-bucket/data/';
CREATE STAGE local_files URL = 'file:///path/to/data/';
CREATE STAGE user_stage;  -- User stage (@~)

-- 2. Define file formats
CREATE FILE FORMAT csv_standard 
TYPE = 'CSV' 
FIELD_DELIMITER = ',' 
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"';

CREATE FILE FORMAT pipe_delimited
TYPE = 'CSV'
FIELD_DELIMITER = '|'
ESCAPE_UNENCLOSED_FIELD = 'NONE';

CREATE FILE FORMAT json_format
TYPE = 'JSON'
STRIP_OUTER_ARRAY = TRUE;

-- 3. Load data with COPY INTO
COPY INTO customers 
FROM '@s3_data/customers.csv' 
FILE_FORMAT = 'csv_standard';

-- Inline format specification
COPY INTO orders 
FROM '@local_files/orders.csv'
FILE_FORMAT = (
    TYPE = 'CSV' 
    FIELD_DELIMITER = '|'
    SKIP_HEADER = 1
    COMPRESSION = 'GZIP'
);

-- Load JSON data
COPY INTO events 
FROM '@user_stage/events.json'
FILE_FORMAT = 'json_format';

-- Advanced options
COPY INTO large_dataset
FROM '@s3_data/data.parquet'
FILE_FORMAT = (TYPE = 'PARQUET')
ON_ERROR = 'CONTINUE'
SIZE_LIMIT = 100000;
```

## API Reference

### Query Execution
```
POST /api/v1/query
Content-Type: application/json

{
  "sql": "SELECT * FROM table_name",
  "session_id": "optional-session-id",
  "database": "optional-database-name"
}
```

### Session Management
```
POST /api/v1/sessions          # Create session
GET /api/v1/sessions           # List sessions  
GET /api/v1/sessions/{id}      # Get session info
DELETE /api/v1/sessions/{id}   # Terminate session
POST /api/v1/sessions/cleanup  # Cleanup expired sessions
```

### Health and Status
```
GET /api/v1/health             # Server health
GET /api/v1/                   # Server info
GET /docs                      # Interactive API docs
GET /redoc                     # Alternative API docs
```

## Configuration

### Environment Variables
- `MOCKHAUS_SERVER_URL` - Server URL for REPL client
- `MOCKHAUS_MAX_SESSIONS` - Maximum concurrent sessions (default: 100)
- `MOCKHAUS_SESSION_TTL` - Default session TTL in seconds (default: 3600)
- `MOCKHAUS_CLEANUP_INTERVAL` - Background cleanup interval (default: 300s)
- `MOCKHAUS_DEBUG` - Enable debug logging

### Server Options
```bash
uv run mockhaus serve --help

Options:
  --host TEXT          Host to bind to [default: localhost]
  --port INTEGER       Port to bind to [default: 8080]  
  --max-sessions INT   Maximum concurrent sessions [default: 100]
```

### REPL Options
```bash
uv run mockhaus repl --help

Options:
  --server-url TEXT        Mockhaus server URL
  --session-type TEXT      Session type: memory or persistent [default: memory]
  --session-id TEXT        Use specific session ID
  --session-ttl INTEGER    Session TTL in seconds [default: 3600]
  --persistent-path TEXT   Path for persistent session storage
```

## Development

### Running Tests
```bash
# Run all tests
make test

# Run specific test categories
uv run pytest tests/server/test_sessions.py -v      # Session management
uv run pytest tests/integration/ -v                # Integration tests
uv run pytest tests/unit/ -v                       # Unit tests

# Test specific functionality
uv run pytest tests/unit/snowflake/test_ingestion.py::TestDataIngestion::test_copy_into_with_named_format -v
```

### Code Quality
```bash
# Type checking
uv run mypy src/

# Code formatting
uv run ruff format src/ tests/

# Linting
uv run ruff check src/ tests/

# Run all quality checks
make lint
```

### Project Structure
```
src/mockhaus/
├── server/                    # HTTP server and API
│   ├── routes/               # API endpoints
│   ├── models/               # Data models
│   ├── storage/              # Session storage backends
│   └── concurrent_session_manager.py
├── snowflake/                # Snowflake SQL translation
│   ├── file_formats/         # File format handlers
│   ├── copy_into.py         # COPY INTO operations
│   └── translator.py        # SQL translation
├── repl/                     # Interactive REPL client
├── executor.py              # Query execution engine
└── query_history.py         # Query history tracking

tests/
├── server/                   # Server endpoint tests
├── integration/              # End-to-end tests  
└── unit/                     # Component unit tests
```

## Architecture

### Session Isolation
- Each session maintains its own DuckDB database instance
- Complete data isolation between concurrent sessions
- Session-specific query history and metadata
- Thread-safe concurrent access with proper locking

### Storage Backends
- **Memory**: Fast in-memory DuckDB databases
- **Local File**: Persistent local file storage
- **Extensible**: Plugin architecture for future storage backends (S3, etc.)

### Query Translation
- AST-based parsing using sqlglot for robust SQL analysis
- Comprehensive Snowflake to DuckDB function mapping
- Custom dialect extensions for Snowflake-specific features
- Detailed translation metadata and warnings

### Performance Features
- Connection pooling and session reuse
- Background cleanup of expired resources
- LRU eviction for session management
- Optimized query execution with DuckDB's columnar engine

## License

MIT License - see LICENSE file for details.