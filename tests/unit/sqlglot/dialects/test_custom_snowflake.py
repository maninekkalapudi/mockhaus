"""Tests for custom Snowflake dialect."""

import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent / "src"))

import sqlglot

from mockhaus.sqlglot.dialects import CustomSnowflake, Sysdate


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

    def test_standard_snowflake_functions_still_work(self):
        """Test that standard Snowflake functions still work."""
        sql = "SELECT CURRENT_TIMESTAMP(), CURRENT_DATE()"
        parsed = sqlglot.parse_one(sql, dialect=CustomSnowflake)

        # Should parse without errors
        assert parsed is not None
        assert str(parsed).upper().find("CURRENT_TIMESTAMP") != -1
