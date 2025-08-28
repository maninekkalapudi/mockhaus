# Custom SQLGlot Dialects Implementation Plan

## Overview
This plan outlines the implementation of custom SQLGlot dialects that extend existing dialects to add support for database-specific functions not yet supported in the main SQLGlot library. The initial implementation focuses on Snowflake's SYSDATE() function, with a structure designed for extensibility to other dialects.

## Motivation
- SQLGlot currently does not support some database-specific functions (e.g., Snowflake's SYSDATE())
- PR review and release cycles can take days or weeks
- Need immediate support for production use cases
- Ability to add database-specific functions as needed
- Support for multiple dialects in a maintainable structure

## Implementation Strategy

### 1. Project Structure
```
src/
├── sqlglot/
│   └── dialects/
│       ├── __init__.py
│       ├── base.py                    # Base classes for custom dialects
│       ├── custom_snowflake/
│       │   ├── __init__.py
│       │   ├── dialect.py             # Snowflake dialect extensions
│       │   ├── expressions.py         # Custom expressions (if needed)
│       │   └── functions.py           # Function definitions
│       ├── custom_postgres/           # Future: PostgreSQL extensions
│       │   ├── __init__.py
│       │   └── dialect.py
│       └── custom_oracle/             # Future: Oracle extensions
│           ├── __init__.py
│           └── dialect.py
├── tests/
│   ├── __init__.py
│   ├── sqlglot/
│   │   ├── __init__.py
│   │   ├── dialects/
│   │   │   ├── __init__.py
│   │   │   ├── test_custom_snowflake.py
│   │   │   ├── test_custom_postgres.py  # Future
│   │   │   └── test_custom_oracle.py    # Future
│   │   └── test_integration.py
│   └── test_helpers.py
├── examples/
│   ├── snowflake_usage.py
│   └── README.md
└── requirements.txt
```

### 2. Core Implementation

#### File: `src/sqlglot/dialects/base.py`
```python
"""Base classes for custom dialect extensions."""
from typing import Dict, Any
from sqlglot import exp
from sqlglot.dialects import Dialect


class CustomDialectMixin:
    """Base mixin for custom dialect functionality."""
    
    @classmethod
    def get_custom_functions(cls) -> Dict[str, Any]:
        """Override in subclasses to provide custom function mappings."""
        return {}
    
    @classmethod
    def get_custom_generators(cls) -> Dict[str, Any]:
        """Override in subclasses to provide custom SQL generators."""
        return {}
```

#### File: `src/sqlglot/dialects/custom_snowflake/expressions.py`
```python
"""Custom expressions for Snowflake dialect."""
from sqlglot import exp


class Sysdate(exp.Func):
    """Represents SYSDATE() function in Snowflake."""
    arg_types = {}  # No arguments
    is_var_len_args = False
```

#### File: `src/sqlglot/dialects/custom_snowflake/functions.py`
```python
"""Function definitions for Snowflake custom dialect."""
from sqlglot import exp
from sqlglot.parser import binary_range_parser
from .expressions import Sysdate


def _parse_sysdate(parser) -> exp.Expression:
    """Parse SYSDATE() function."""
    # Consume the opening parenthesis if present
    parser.match(parser.token_type.L_PAREN)
    # Consume the closing parenthesis if present
    parser.match(parser.token_type.R_PAREN)
    return parser.expression(Sysdate)


# Define custom function mappings
CUSTOM_FUNCTIONS = {
    "SYSDATE": _parse_sysdate,
    # Add more custom functions here as needed
}


# Define custom SQL generators
def sysdate_sql(self, expression: Sysdate) -> str:
    """Generate SQL for SYSDATE() function."""
    return "SYSDATE()"


CUSTOM_GENERATORS = {
    Sysdate: sysdate_sql,
}
```

#### File: `src/sqlglot/dialects/custom_snowflake/dialect.py`
```python
"""Custom Snowflake dialect extending SQLGlot's Snowflake dialect."""
from sqlglot.dialects.snowflake import Snowflake
from sqlglot import exp
from .functions import CUSTOM_FUNCTIONS, CUSTOM_GENERATORS
from .expressions import Sysdate


class CustomSnowflakeParser(Snowflake.Parser):
    """Extended Snowflake parser with custom functions."""
    
    # Extend the FUNCTIONS dictionary with custom functions
    FUNCTIONS = {
        **Snowflake.Parser.FUNCTIONS,
        **CUSTOM_FUNCTIONS,
    }


class CustomSnowflakeGenerator(Snowflake.Generator):
    """Extended Snowflake SQL generator with custom function support."""
    
    # Register custom SQL generators
    TRANSFORMS = {
        **Snowflake.Generator.TRANSFORMS,
        **CUSTOM_GENERATORS,
    }


class CustomSnowflake(Snowflake):
    """Custom Snowflake dialect with extended function support."""
    
    class Parser(CustomSnowflakeParser):
        pass
    
    class Generator(CustomSnowflakeGenerator):
        pass


# Convenience function for dialect registration
def get_dialect():
    """Return the custom Snowflake dialect instance."""
    return CustomSnowflake
```

#### File: `src/sqlglot/dialects/custom_snowflake/__init__.py`
```python
"""Custom Snowflake dialect package."""
from .dialect import CustomSnowflake, get_dialect
from .expressions import Sysdate

__all__ = ["CustomSnowflake", "get_dialect", "Sysdate"]
```

### 3. Additional Snowflake Functions to Consider

Based on common Snowflake-specific functions that may need custom support:
- `DATEADD()` with Snowflake-specific syntax
- `DATEDIFF()` with Snowflake-specific syntax
- `LISTAGG()` variations
- `FLATTEN()` for semi-structured data
- `PARSE_JSON()`
- `TO_VARIANT()`
- `ARRAY_AGG()`
- `OBJECT_AGG()`
- `GET_PATH()` for variant data
- `TRY_CAST()` for safe casting

### 4. Testing Strategy

#### File: `src/tests/sqlglot/dialects/test_custom_snowflake.py`
```python
"""Tests for custom Snowflake dialect."""
import pytest
from sqlglot import parse, transpile
from sqlglot.errors import ParseError
from sqlglot.dialects.custom_snowflake import CustomSnowflake, Sysdate


class TestCustomSnowflakeDialect:
    """Test suite for custom Snowflake dialect."""
    
    def test_sysdate_parsing(self):
        """Test that SYSDATE() is parsed correctly."""
        sql = "SELECT SYSDATE() AS current_time FROM table1"
        ast = parse(sql, dialect=CustomSnowflake)[0]
        
        # Find SYSDATE in the AST
        sysdate_nodes = list(ast.find_all(Sysdate))
        assert len(sysdate_nodes) == 1
        assert isinstance(sysdate_nodes[0], Sysdate)
    
    def test_sysdate_in_create_table(self):
        """Test SYSDATE() in CREATE TABLE statement."""
        sql = '''CREATE TABLE IF NOT EXISTS TABLE1 (
            "HASH" INTEGER,
            "NEW_COLUMN_2" INTEGER,
            "_META_LOADED_AT" TIMESTAMP_TZ DEFAULT SYSDATE() NOT NULL,
            "_META_SOURCE_FILE_NAME" STRING NOT NULL
        )'''
        
        result = transpile(sql, read=CustomSnowflake, write=CustomSnowflake)[0]
        assert "SYSDATE()" in result
        assert "DEFAULT SYSDATE()" in result
    
    def test_sysdate_generation(self):
        """Test that SYSDATE() is generated correctly."""
        sql = "SELECT SYSDATE() FROM table1"
        result = transpile(sql, read=CustomSnowflake, write=CustomSnowflake)[0]
        assert "SYSDATE()" in result
    
    def test_sysdate_without_parens(self):
        """Test handling of SYSDATE without parentheses."""
        # Some databases allow SYSDATE without parentheses
        sql = "SELECT SYSDATE FROM table1"
        # This should either parse with parens added or raise an error
        # depending on implementation choice
        try:
            result = transpile(sql, read=CustomSnowflake, write=CustomSnowflake)[0]
            # If it succeeds, check that it adds parentheses
            assert "SYSDATE()" in result
        except ParseError:
            # If strict mode, this is expected
            pass
    
    def test_multiple_sysdate_usage(self):
        """Test multiple SYSDATE() calls in a single query."""
        sql = """
        SELECT 
            SYSDATE() AS now,
            DATEADD('day', -7, SYSDATE()) AS week_ago,
            col1
        FROM table1
        WHERE created_at > SYSDATE() - 30
        """
        result = transpile(sql, read=CustomSnowflake, write=CustomSnowflake)[0]
        # Count occurrences of SYSDATE()
        assert result.count("SYSDATE()") >= 3
    
    def test_sysdate_in_where_clause(self):
        """Test SYSDATE() in WHERE clause."""
        sql = "SELECT * FROM orders WHERE order_date >= SYSDATE() - 7"
        result = transpile(sql, read=CustomSnowflake, write=CustomSnowflake)[0]
        assert "SYSDATE()" in result
        assert "WHERE" in result
    
    def test_regular_functions_still_work(self):
        """Ensure standard Snowflake functions still work."""
        sql = "SELECT CURRENT_TIMESTAMP(), CURRENT_DATE(), NOW() FROM table1"
        result = transpile(sql, read=CustomSnowflake, write=CustomSnowflake)[0]
        assert "CURRENT_TIMESTAMP()" in result
        assert "CURRENT_DATE()" in result
        assert "NOW()" in result
    
    def test_transpile_from_other_dialect(self):
        """Test transpiling from standard Snowflake to custom."""
        sql = "SELECT CURRENT_TIMESTAMP FROM table1"
        # Should work without issues
        result = transpile(sql, read="snowflake", write=CustomSnowflake)[0]
        assert "CURRENT_TIMESTAMP" in result

    def test_complex_query_with_sysdate(self):
        """Test SYSDATE() in a complex query."""
        sql = """
        WITH recent_data AS (
            SELECT *
            FROM events
            WHERE event_time >= SYSDATE() - INTERVAL '24 hours'
        )
        SELECT 
            user_id,
            COUNT(*) as event_count,
            MAX(event_time) as last_event,
            SYSDATE() as query_time
        FROM recent_data
        GROUP BY user_id
        """
        result = transpile(sql, read=CustomSnowflake, write=CustomSnowflake)[0]
        assert "SYSDATE()" in result
        assert result.count("SYSDATE()") >= 2
```

### 5. Usage Examples

#### File: `src/examples/snowflake_usage.py`
```python
"""Example usage of custom Snowflake dialect."""
from sqlglot import parse, transpile
from sqlglot.dialects.custom_snowflake import CustomSnowflake, Sysdate


def example_basic_usage():
    """Basic SYSDATE() usage example."""
    print("=== Basic SYSDATE() Usage ===")
    
    # Example 1: Simple SELECT with SYSDATE()
    sql = "SELECT SYSDATE() AS current_time, user_id FROM users"
    result = transpile(sql, read=CustomSnowflake, write=CustomSnowflake)
    print(f"Original: {sql}")
    print(f"Result: {result[0]}\n")
    
    # Example 2: SYSDATE() in WHERE clause
    sql = "SELECT * FROM orders WHERE created_at >= SYSDATE() - 7"
    result = transpile(sql, read=CustomSnowflake, write=CustomSnowflake)
    print(f"With WHERE: {result[0]}\n")


def example_create_table():
    """Example with CREATE TABLE using SYSDATE() as default."""
    print("=== CREATE TABLE with SYSDATE() Default ===")
    
    sql = '''
    CREATE TABLE IF NOT EXISTS user_actions (
        action_id INTEGER PRIMARY KEY,
        user_id INTEGER NOT NULL,
        action_type VARCHAR(50),
        created_at TIMESTAMP_TZ DEFAULT SYSDATE() NOT NULL,
        updated_at TIMESTAMP_TZ DEFAULT SYSDATE() NOT NULL
    )
    '''
    
    result = transpile(sql, read=CustomSnowflake, write=CustomSnowflake)
    print(f"Result: {result[0]}\n")


def example_ast_manipulation():
    """Example of working with AST."""
    print("=== AST Manipulation Example ===")
    
    sql = "SELECT SYSDATE() AS now, col1, col2 FROM table1"
    
    # Parse the SQL
    ast = parse(sql, dialect=CustomSnowflake)[0]
    
    # Find all SYSDATE nodes
    sysdate_nodes = list(ast.find_all(Sysdate))
    print(f"Found {len(sysdate_nodes)} SYSDATE() calls")
    
    # Generate SQL back
    generated = ast.sql(dialect=CustomSnowflake)
    print(f"Generated: {generated}\n")


def example_transpilation():
    """Example of transpiling between dialects."""
    print("=== Transpilation Example ===")
    
    # From standard Snowflake to custom
    standard_sql = "SELECT CURRENT_TIMESTAMP() FROM table1"
    result = transpile(standard_sql, read="snowflake", write=CustomSnowflake)
    print(f"Standard to Custom: {result[0]}")
    
    # Custom to standard (should preserve SYSDATE)
    custom_sql = "SELECT SYSDATE() FROM table1"
    try:
        # This might fail if standard Snowflake doesn't support SYSDATE
        result = transpile(custom_sql, read=CustomSnowflake, write="snowflake")
        print(f"Custom to Standard: {result[0]}")
    except Exception as e:
        print(f"Note: Standard Snowflake dialect doesn't support SYSDATE(): {e}")


def example_complex_query():
    """Example with complex query using SYSDATE()."""
    print("\n=== Complex Query Example ===")
    
    sql = """
    WITH daily_metrics AS (
        SELECT 
            DATE_TRUNC('day', event_time) AS event_day,
            COUNT(*) AS event_count,
            COUNT(DISTINCT user_id) AS unique_users
        FROM events
        WHERE event_time >= DATEADD('day', -30, SYSDATE())
          AND event_time < SYSDATE()
        GROUP BY 1
    )
    SELECT 
        event_day,
        event_count,
        unique_users,
        SYSDATE() AS report_generated_at
    FROM daily_metrics
    ORDER BY event_day DESC
    """
    
    result = transpile(sql, read=CustomSnowflake, write=CustomSnowflake)
    print(f"Complex query result:\n{result[0]}")


if __name__ == "__main__":
    example_basic_usage()
    example_create_table()
    example_ast_manipulation()
    example_transpilation()
    example_complex_query()
```

### 6. Integration with Existing Code

#### Simple Integration
```python
# Import the custom dialect
from sqlglot.dialects.custom_snowflake import CustomSnowflake

# Use it anywhere you would use a dialect
from sqlglot import transpile

# Parse and generate SQL
sql = "CREATE TABLE t1 (id INT, created_at TIMESTAMP DEFAULT SYSDATE())"
result = transpile(sql, read=CustomSnowflake, write=CustomSnowflake)
```

#### Integration Wrapper
```python
"""Wrapper for easier integration."""
from typing import List, Optional
from sqlglot import transpile as _transpile
from sqlglot.dialects.custom_snowflake import CustomSnowflake


def transpile_snowflake(
    sql: str,
    source_dialect: str = "snowflake",
    use_custom: bool = True,
    **kwargs
) -> List[str]:
    """
    Transpile SQL with optional custom Snowflake dialect.
    
    Args:
        sql: SQL string to transpile
        source_dialect: Source dialect (default: "snowflake")
        use_custom: Whether to use custom Snowflake dialect
        **kwargs: Additional arguments for transpile
    
    Returns:
        List of transpiled SQL strings
    """
    read_dialect = CustomSnowflake if use_custom and source_dialect == "snowflake" else source_dialect
    write_dialect = CustomSnowflake if use_custom else "snowflake"
    
    return _transpile(sql, read=read_dialect, write=write_dialect, **kwargs)
```

### 7. Adding New Functions

To add support for additional Snowflake functions, follow this pattern:

#### Step 1: Add Expression Class (if needed)
```python
# In src/sqlglot/dialects/custom_snowflake/expressions.py
class TryCast(exp.Cast):
    """Represents TRY_CAST function in Snowflake."""
    pass
```

#### Step 2: Add Parser Function
```python
# In src/sqlglot/dialects/custom_snowflake/functions.py
def _parse_try_cast(parser) -> exp.Expression:
    """Parse TRY_CAST function."""
    parser.match(parser.token_type.L_PAREN)
    expression = parser.parse_expression()
    parser.match(parser.token_type.AS)
    to_type = parser.parse_types()
    parser.match(parser.token_type.R_PAREN)
    return parser.expression(TryCast, this=expression, to=to_type)

# Add to CUSTOM_FUNCTIONS
CUSTOM_FUNCTIONS["TRY_CAST"] = _parse_try_cast
```

#### Step 3: Add SQL Generator
```python
# In src/sqlglot/dialects/custom_snowflake/functions.py
def try_cast_sql(self, expression: TryCast) -> str:
    """Generate SQL for TRY_CAST function."""
    expr_sql = self.sql(expression.this)
    type_sql = self.sql(expression.to)
    return f"TRY_CAST({expr_sql} AS {type_sql})"

# Add to CUSTOM_GENERATORS
CUSTOM_GENERATORS[TryCast] = try_cast_sql
```

### 8. Deployment and Installation

#### Local Development
```bash
# Add to your project's requirements.txt or pyproject.toml
# Assuming the custom dialects are in your project's src directory
# No additional installation needed, just ensure src is in PYTHONPATH
```

#### Package Structure for Distribution
```toml
# pyproject.toml
[project]
name = "custom-sqlglot-dialects"
version = "0.1.0"
dependencies = [
    "sqlglot>=20.0.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=7.0.0",
    "black>=23.0.0",
    "mypy>=1.0.0",
]
```

### 9. Maintenance and Best Practices

#### Version Compatibility
```python
# In src/sqlglot/dialects/__init__.py (or in a separate version check module)
import sqlglot
from packaging import version

SQLGLOT_VERSION = version.parse(sqlglot.__version__)
MIN_SUPPORTED_VERSION = version.parse("20.0.0")

if SQLGLOT_VERSION < MIN_SUPPORTED_VERSION:
    raise ImportError(
        f"custom sqlglot dialects require sqlglot>={MIN_SUPPORTED_VERSION}, "
        f"but found {SQLGLOT_VERSION}"
    )
```

#### Testing Strategy
- Unit tests for each custom function
- Integration tests with real Snowflake queries
- Regression tests when upgrading SQLGlot
- Performance tests for complex queries

#### Documentation Standards
- Document each custom function with examples
- Maintain compatibility matrix with SQLGlot versions
- Track differences from standard Snowflake dialect
- Include migration guides for new functions

### 10. Extensibility Guide

#### Adding Support for New Dialects
1. Create new directory: `src/sqlglot/dialects/custom_[dialect_name]/`
2. Follow the same structure as Snowflake implementation
3. Extend the base dialect from SQLGlot
4. Add comprehensive tests in `src/tests/sqlglot/dialects/`

#### Example: Adding PostgreSQL Custom Functions
```python
# src/sqlglot/dialects/custom_postgres/dialect.py
from sqlglot.dialects.postgres import Postgres
from .functions import CUSTOM_FUNCTIONS, CUSTOM_GENERATORS


class CustomPostgresParser(Postgres.Parser):
    FUNCTIONS = {
        **Postgres.Parser.FUNCTIONS,
        **CUSTOM_FUNCTIONS,
    }


class CustomPostgresGenerator(Postgres.Generator):
    TRANSFORMS = {
        **Postgres.Generator.TRANSFORMS,
        **CUSTOM_GENERATORS,
    }


class CustomPostgres(Postgres):
    class Parser(CustomPostgresParser):
        pass
    
    class Generator(CustomPostgresGenerator):
        pass
```

### 11. Contributing Back to SQLGlot

When contributing custom functions back to SQLGlot:

1. **Prepare the PR**:
   - Extract the function implementation
   - Add comprehensive tests
   - Update SQLGlot documentation
   - Follow SQLGlot's contribution guidelines

2. **PR Template**:
   ```markdown
   ## Description
   Adds support for Snowflake's SYSDATE() function.
   
   ## Implementation
   - Added SYSDATE to Snowflake parser functions
   - Added SQL generation for SYSDATE
   - Added comprehensive tests
   
   ## Testing
   - Tested with various Snowflake queries
   - All existing tests pass
   ```

3. **Migration Plan**:
   - Keep custom dialect until PR is merged and released
   - Test with beta releases
   - Gradually migrate to official support

## Next Steps

1. **Create the directory structure** under `src/custom_dialects/`
2. **Implement the base infrastructure** (base classes, utilities)
3. **Add Snowflake SYSDATE() support** with comprehensive tests
4. **Create integration tests** using your actual use case
5. **Document the implementation** and usage patterns
6. **Plan for additional functions** based on immediate needs

## Key Improvements in This Plan

1. **Proper SQLGlot Integration**: Uses FUNCTIONS and TRANSFORMS dictionaries correctly
2. **Extensible Structure**: Designed for multiple dialects from the start
3. **Dedicated Expression Classes**: Clean separation of concerns
4. **Comprehensive Testing**: Includes edge cases and integration tests
5. **Maintenance Focus**: Version compatibility and upgrade paths
6. **Clear Examples**: Shows exactly how to use the custom dialect
7. **Future-Proof**: Easy to add new functions and dialects