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

    def test_identifier_transformation_in_select(self):
        """Test that IDENTIFIER in SELECT is transformed to unquoted identifier."""
        from mockhaus.sqlglot.dialects import CustomSnowflake

        # Parse with Snowflake dialect (SELECT context uses IdentifierFunc)
        sql = "SELECT IDENTIFIER('col_name') FROM my_table"
        parsed = sqlglot.parse_one(sql, dialect=CustomSnowflake)

        # Transform to DuckDB
        duckdb_sql = parsed.sql(dialect=CustomDuckDB, pretty=True)

        # Should contain unquoted identifier instead of IDENTIFIER()
        assert "col_name" in duckdb_sql
        assert "IDENTIFIER(" not in duckdb_sql

    def test_identifier_transformation_in_create_table(self):
        """Test that IDENTIFIER in CREATE TABLE is transformed via Anonymous handler."""
        from mockhaus.sqlglot.dialects import CustomSnowflake

        # Parse with Snowflake dialect (table context uses Anonymous)
        sql = "CREATE TABLE IF NOT EXISTS IDENTIFIER('staging_table1') (id INT)"
        parsed = sqlglot.parse_one(sql, dialect=CustomSnowflake)

        # Transform to DuckDB
        duckdb_sql = parsed.sql(dialect=CustomDuckDB, pretty=True)

        # Should contain unquoted identifier instead of IDENTIFIER()
        assert "staging_table1" in duckdb_sql
        assert "IDENTIFIER(" not in duckdb_sql

    def test_identifier_transformation_variable(self):
        """Test that IDENTIFIER with variable remains as IDENTIFIER (correct behavior)."""
        from mockhaus.sqlglot.dialects import CustomSnowflake

        # Parse with Snowflake dialect
        sql = "SELECT * FROM IDENTIFIER($table_name)"
        parsed = sqlglot.parse_one(sql, dialect=CustomSnowflake)

        # Transform to DuckDB
        duckdb_sql = parsed.sql(dialect=CustomDuckDB, pretty=True)

        # Variables should remain as IDENTIFIER() since they can't be resolved statically
        # This will likely cause an error in DuckDB, which is appropriate behavior
        assert "$table_name" in duckdb_sql
        assert "IDENTIFIER(" in duckdb_sql  # Should keep IDENTIFIER for variables

    def test_custom_generator_handles_identifier(self):
        """Test that the custom generator properly handles IdentifierFunc expressions."""
        import sqlglot.expressions as exp

        from mockhaus.sqlglot.dialects import IdentifierFunc

        # Create an IdentifierFunc expression manually with string literal
        string_literal = exp.Literal.string("my_table")
        identifier_expr = IdentifierFunc(this=string_literal)

        # Generate SQL using CustomDuckDB
        dialect = CustomDuckDB()
        generator = dialect.generator_class()

        # This should transform to unquoted identifier
        result = generator.sql(identifier_expr)
        assert result == "my_table"
