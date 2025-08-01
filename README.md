# Mockhaus

**Snowflake proxy with DuckDB backend - SQL translation and execution engine for local development**

Mockhaus translates Snowflake SQL queries to DuckDB SQL and executes them locally, enabling cost-effective development and testing without connecting to actual Snowflake instances.

## Current Status (Milestone 0 - Complete)

- Basic SELECT query translation and execution  
- WHERE clauses with numeric, string, and boolean filters
- ORDER BY and LIMIT support
- Aggregate functions (COUNT, AVG, MAX, MIN)
- Date functions (CURRENT_DATE)
- Rich CLI with syntax highlighting
- Comprehensive test suite (13 test cases)
- In-memory DuckDB execution

## Quick Start

```bash
# Install dependencies
uv sync

# See sample queries
uv run mockhaus sample

# Run a basic query
uv run mockhaus query "SELECT customer_id, customer_name FROM sample_customers"

# See translation details
uv run mockhaus query "SELECT * FROM sample_customers WHERE account_balance > 1000" --verbose

# Run tests
uv run pytest tests/test_snowflake_queries.py -v
```

## What Works Now

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

## Development

```bash
# Run specific tests
uv run pytest tests/test_snowflake_queries.py::TestSnowflakeQueries::test_basic_select -v

# Check types
uv run mypy src/

# Format code  
uv run ruff format src/ tests/

# Lint code
uv run ruff check src/ tests/
```

See docs/MILESTONE_0_FINDINGS.md for detailed proof of concept results and next steps.