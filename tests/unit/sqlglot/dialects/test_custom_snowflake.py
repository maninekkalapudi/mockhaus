"""Tests for custom Snowflake dialect."""

import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent / "src"))

import sqlglot

from mockhaus.sqlglot.dialects import CustomSnowflake, IdentifierFunc, Sysdate


class TestCustomSnowflakeDialect:
    """Test suite for CustomSnowflake dialect."""

    def test_sysdate_parsing(self):
        """Test that SYSDATE() is parsed correctly with custom dialect."""
        sql = "SELECT SYSDATE() AS current_time"
        parsed = sqlglot.parse_one(sql, dialect=CustomSnowflake)

        # Find SYSDATE in the AST
        sysdate_nodes = list(parsed.find_all(Sysdate))
        assert len(sysdate_nodes) == 1
        assert isinstance(sysdate_nodes[0], Sysdate)

    def test_sysdate_expression_type(self):
        """Test that Sysdate is properly defined as an expression."""
        assert issubclass(Sysdate, sqlglot.expressions.Expression)

    def test_dialect_inheritance(self):
        """Test that CustomSnowflake properly inherits from Snowflake."""
        from sqlglot.dialects.snowflake import Snowflake

        assert issubclass(CustomSnowflake, Snowflake)

    def test_custom_parser_registration(self):
        """Test that the custom parser includes SYSDATE function."""
        dialect = CustomSnowflake()
        parser = dialect.parser_class()

        # Verify SYSDATE is in the function tokens
        assert "SYSDATE" in parser.FUNCTIONS

    def test_identifier_parsing_in_select(self):
        """Test that IDENTIFIER() in SELECT is parsed as IdentifierFunc."""
        sql = "SELECT IDENTIFIER('col_name') FROM my_table"
        parsed = sqlglot.parse_one(sql, dialect=CustomSnowflake)

        # Find IDENTIFIER in the AST
        identifier_nodes = list(parsed.find_all(IdentifierFunc))
        assert len(identifier_nodes) == 1
        assert isinstance(identifier_nodes[0], IdentifierFunc)

    def test_identifier_parsing_in_create_table(self):
        """Test that IDENTIFIER() in CREATE TABLE is parsed as Anonymous (current behavior)."""
        sql = "CREATE TABLE IF NOT EXISTS IDENTIFIER('staging_table1') (id INT)"
        parsed = sqlglot.parse_one(sql, dialect=CustomSnowflake)

        # In table context, IDENTIFIER is parsed as Anonymous, not IdentifierFunc
        # This is expected due to different parsing paths for table names
        anonymous_nodes = [n for n in parsed.find_all(sqlglot.expressions.Anonymous) if n.this.upper() == "IDENTIFIER"]
        assert len(anonymous_nodes) == 1
        assert anonymous_nodes[0].this == "IDENTIFIER"

    def test_identifier_with_variable(self):
        """Test that IDENTIFIER() works with session variables."""
        sql = "SELECT * FROM IDENTIFIER($table_name)"
        parsed = sqlglot.parse_one(sql, dialect=CustomSnowflake)

        # Find IDENTIFIER in the AST
        identifier_nodes = list(parsed.find_all(IdentifierFunc))
        assert len(identifier_nodes) == 1
        assert isinstance(identifier_nodes[0], IdentifierFunc)

    def test_identifier_expression_type(self):
        """Test that IdentifierFunc is properly defined as an expression."""
        assert issubclass(IdentifierFunc, sqlglot.expressions.Expression)

    def test_identifier_parser_registration(self):
        """Test that the custom parser includes IDENTIFIER function."""
        dialect = CustomSnowflake()
        parser = dialect.parser_class()

        # Verify IDENTIFIER is in the function tokens
        assert "IDENTIFIER" in parser.FUNCTIONS

    def test_standard_snowflake_functions_still_work(self):
        """Test that standard Snowflake functions still work."""
        sql = "SELECT CURRENT_TIMESTAMP(), CURRENT_DATE()"
        parsed = sqlglot.parse_one(sql, dialect=CustomSnowflake)

        # Should parse without errors
        assert parsed is not None
        assert str(parsed).upper().find("CURRENT_TIMESTAMP") != -1
