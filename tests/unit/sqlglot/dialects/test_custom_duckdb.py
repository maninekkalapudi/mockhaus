"""Tests for custom DuckDB dialect."""

import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent / "src"))

import sqlglot

from mockhaus.sqlglot.dialects import CustomDuckDB


class TestCustomDuckDBDialect:
    """Test suite for CustomDuckDB dialect."""

    def test_dialect_inheritance(self):
        """Test that CustomDuckDB properly inherits from DuckDB."""
        from sqlglot.dialects.duckdb import DuckDB

        assert issubclass(CustomDuckDB, DuckDB)

    def test_sysdate_transformation(self):
        """Test that SYSDATE is transformed to UTC timestamp in DuckDB."""
        from mockhaus.sqlglot.dialects import CustomSnowflake

        # Parse with Snowflake dialect
        sql = "SELECT SYSDATE() AS current_time"
        parsed = sqlglot.parse_one(sql, dialect=CustomSnowflake)

        # Transform to DuckDB
        duckdb_sql = parsed.sql(dialect=CustomDuckDB, pretty=True)

        # Should contain UTC transformation
        assert "CURRENT_TIMESTAMP AT TIME ZONE 'UTC'" in duckdb_sql

    def test_standard_duckdb_functions_work(self):
        """Test that standard DuckDB functions still work."""
        sql = "SELECT NOW(), CURRENT_DATE"
        parsed = sqlglot.parse_one(sql, dialect=CustomDuckDB)

        # Should parse without errors
        assert parsed is not None

    def test_custom_generator_handles_sysdate(self):
        """Test that the custom generator properly handles Sysdate expressions."""
        from mockhaus.sqlglot.dialects import Sysdate

        # Create a Sysdate expression manually
        sysdate_expr = Sysdate()

        # Generate SQL using CustomDuckDB
        dialect = CustomDuckDB()
        generator = dialect.generator_class()

        # This should transform to UTC timestamp
        result = generator.sql(sysdate_expr)
        assert "CURRENT_TIMESTAMP AT TIME ZONE 'UTC'" in result
