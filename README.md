# Mockhaus

**Snowflake proxy with DuckDB backend - Complete SQL translation and data ingestion engine**

Mockhaus translates Snowflake SQL queries to DuckDB SQL and executes them locally, enabling cost-effective development and testing without connecting to actual Snowflake instances. Now includes full data ingestion capabilities with stages, file formats, and COPY INTO operations.

## Current Status (v0.2.0 - Full Data Ingestion)

### Core Features
- Complete SELECT query translation and execution
- WHERE clauses with numeric, string, and boolean filters  
- ORDER BY and LIMIT support
- Aggregate functions (COUNT, AVG, MAX, MIN)
- Date functions (CURRENT_DATE)
- Rich CLI with syntax highlighting and management tools

### Data Ingestion (NEW)
- **Stages**: CREATE/DROP STAGE with URL support (S3, GCS, Azure, local files)
- **File Formats**: CREATE/DROP FILE FORMAT with CSV, JSON, Parquet support
- **COPY INTO**: Full data loading from stages with named and inline formats
- **AST Parsing**: Robust parsing with sqlglot (default) + regex fallback
- **CLI Management**: Stage and format management commands

### Technical
- AST-based parsing (default) with regex fallback for compatibility
- Configurable parsing methods via `use_ast_parser` parameter
- Comprehensive test suite (78+ test cases)
- In-memory and persistent DuckDB execution

## Quick Start

```bash
# Install dependencies
uv sync

# Set up sample data (creates in-memory database)
uv run mockhaus setup

# See sample queries
uv run mockhaus sample

# Run basic queries
uv run mockhaus query "SELECT customer_id, customer_name FROM sample_customers"
uv run mockhaus query "SELECT * FROM sample_customers WHERE account_balance > 1000" --verbose

# Use persistent database
uv run mockhaus setup -d my_data.db
uv run mockhaus query -d my_data.db "SELECT COUNT(*) FROM sample_customers"

# Start the HTTP server
uv run mockhaus serve

# Run tests
uv run pytest -v
```

## What Works Now

### Basic Queries
```sql
-- Basic SELECT with filtering
SELECT customer_name, account_balance 
FROM sample_customers 
WHERE account_balance > 1000 AND is_active = true
ORDER BY account_balance DESC;

-- Aggregation
SELECT COUNT(*) as total, AVG(account_balance) as avg_balance 
FROM sample_customers;

-- Date functions
SELECT customer_name, signup_date, CURRENT_DATE as today 
FROM sample_customers;
```

### Data Ingestion Examples

```sql
-- Create stages for different data sources
CREATE STAGE my_s3_stage URL = 's3://my-bucket/data/';
CREATE STAGE my_local_stage URL = 'file:///path/to/local/data/';
CREATE STAGE user_stage;  -- User stage (@~)

-- Create file formats
CREATE FILE FORMAT my_csv 
TYPE = 'CSV' 
FIELD_DELIMITER = ',' 
SKIP_HEADER = 1;

CREATE FILE FORMAT pipe_delimited
TYPE = 'CSV'
FIELD_DELIMITER = '|'
FIELD_OPTIONALLY_ENCLOSED_BY = '"';

-- Load data using COPY INTO with named format
COPY INTO customers 
FROM '@my_s3_stage/customers.csv' 
FILE_FORMAT = 'my_csv';

-- Load data with inline format specification
COPY INTO orders 
FROM '@my_local_stage/orders.csv'
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1);

-- Load from user stage
COPY INTO temp_data 
FROM '@~/upload/data.csv'
FILE_FORMAT = 'CSV_DEFAULT';
```

### CLI Management Commands

```bash
# Manage stages
uv run mockhaus stage list                    # List all stages
uv run mockhaus stage show my_s3_stage        # Show stage details
uv run mockhaus stage list -d my_data.db      # Use persistent database

# Manage file formats  
uv run mockhaus format list                   # List all formats
uv run mockhaus format show my_csv            # Show format details and DuckDB mapping
uv run mockhaus format show CSV_DEFAULT       # Show default format
```

## Configuration Options

### AST vs Regex Parsing

Mockhaus supports two parsing methods for data ingestion statements:

- **AST Parsing** (default): Uses sqlglot for robust parsing with better error handling
- **Regex Parsing** (legacy): Uses regular expressions for backward compatibility

```python
from mockhaus.executor import MockhausExecutor

# Default behavior (uses AST parsing)
executor = MockhausExecutor()

# Enable AST parsing explicitly  
executor = MockhausExecutor(use_ast_parser=True)

# Use legacy regex parsing
executor = MockhausExecutor(use_ast_parser=False)
```

**Benefits of AST parsing:**
- Handles quoted identifiers: `CREATE STAGE "My Stage With Spaces"`
- Better error messages and edge case handling
- More robust parsing of complex expressions
- Future-proof with sqlglot improvements

**Migration:** No changes required unless you specifically depend on regex parsing behavior.

## Development

```bash
# Run all tests
uv run pytest -v

# Run specific test categories
uv run pytest tests/test_snowflake_queries.py -v          # Basic query tests
uv run pytest tests/test_data_ingestion.py -v            # Data ingestion tests  
uv run pytest tests/test_ast_parser.py -v                # AST parser tests
uv run pytest tests/test_configurable_parsing.py -v      # AST vs regex comparison

# Run specific test
uv run pytest tests/test_data_ingestion.py::TestDataIngestion::test_copy_into_with_named_format -v

# Check types
uv run mypy src/

# Format code  
uv run ruff format src/ tests/

# Lint code
uv run ruff check src/ tests/
```

## Server and REPL

### Starting the HTTP Server

```bash
# Start the server on default port 8080
uv run mockhaus serve

# Start on a custom port
uv run mockhaus serve --port 9000

# Use a persistent database
uv run mockhaus serve -d my_data.db --port 8080
```

The server provides a REST API for executing Snowflake queries:

```bash
# Execute a query
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM sample_customers LIMIT 5"}'

# Check server health
curl http://localhost:8000/health
```

### Interactive REPL

The REPL is a client that connects to the Mockhaus HTTP server. You need to run both the server and client:

```bash
# Step 1: Start the Mockhaus server (in one terminal)
uv run mockhaus serve

# Step 2: Run the enhanced REPL client (in another terminal)
uv run python -m mockhaus.repl.enhanced_repl

# Or use the basic REPL if you prefer
uv run python -m mockhaus.repl.repl
```

You can also configure the server connection:
```bash
# Start server on a custom port
uv run mockhaus serve --port 9000

# Connect REPL to custom server URL
export MOCKHAUS_SERVER_URL=http://localhost:9000
uv run python -m mockhaus.repl.enhanced_repl
```

REPL features:
- Interactive SQL execution against the Mockhaus server
- Health monitoring with `health` command
- Formatted tabular output for query results
- Clear error messages for failed queries
- Connection testing on startup

Available commands:
- Any SQL query - Execute against the Mockhaus server
- `health` - Check server health and uptime
- `help` - Show available commands
- `quit` / `exit` / `q` - Exit the REPL
- `Ctrl+C` - Exit the REPL

Example REPL session:
```
🏠 Mockhaus Interactive Client
Type SQL queries, 'health' for server status, 'help' for commands, or 'quit' to exit
----------------------------------------------------------------------
✅ Connected to Mockhaus v0.3.0 at http://localhost:8080

mockhaus> SELECT * FROM sample_customers WHERE account_balance > 1000
customer_id | customer_name | account_balance | signup_date |    is_active
---------------------------------------------------------------------------
          1 |         Alice |          1500.0 |  2023-01-15 |         True
          2 |           Bob |          2300.0 |  2023-02-20 |         True

✅ 2 rows in 0.045s

mockhaus> CREATE STAGE my_stage URL = 's3://bucket/path/'
✅ Query executed successfully (no results)

mockhaus> health
✅ Server healthy - uptime: 5.2 minutes
```

## Architecture

```
src/mockhaus/
├── cli.py                          # Command-line interface
├── executor.py                     # Main execution engine
├── server.py                       # HTTP REST server
├── repl.py                         # Interactive REPL
├── snowflake/
│   ├── translator.py              # Query translation (SELECT, etc.)
│   ├── ingestion.py               # Data ingestion handler
│   ├── ast_parser.py              # AST-based parsing (sqlglot)
│   ├── stages.py                  # Stage management
│   ├── file_formats.py            # File format management
│   └── copy_into.py               # COPY INTO translation
```

See docs/AST_PARSING_PROGRESS.md for detailed implementation status and technical details.