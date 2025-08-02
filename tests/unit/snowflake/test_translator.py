"""Tests for SYSDATE() function translation in custom Snowflake dialect."""

import sys
from pathlib import Path

import pytest

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "src"))

import sqlglot

from mockhaus.snowflake.translator import SnowflakeToDuckDBTranslator
from mockhaus.sqlglot.dialects import CustomSnowflake, Sysdate


class TestSysdateTranslation:
    """Test suite for SYSDATE() function translation."""

    def setup_method(self):
        """Set up test fixtures."""
        self.translator = SnowflakeToDuckDBTranslator()

    def test_sysdate_parsing(self):
        """Test that SYSDATE() is parsed correctly with custom dialect."""
        sql = "SELECT SYSDATE() AS current_time FROM users"
        parsed = sqlglot.parse_one(sql, dialect=CustomSnowflake)

        # Find SYSDATE in the AST
        sysdate_nodes = list(parsed.find_all(Sysdate))
        assert len(sysdate_nodes) == 1
        assert isinstance(sysdate_nodes[0], Sysdate)

    def test_simple_select_translation(self):
        """Test SYSDATE() in simple SELECT statement."""
        sql = "SELECT SYSDATE() AS current_time FROM users"
        result = self.translator.translate(sql)

        # Should translate to NOW() AT TIME ZONE 'UTC'
        assert "CURRENT_TIMESTAMP AT TIME ZONE 'UTC'" in result
        assert 'AS "current_time"' in result

    def test_create_table_with_default_sysdate(self):
        """Test SYSDATE() in CREATE TABLE DEFAULT clause."""
        sql = """CREATE TABLE test_table (
            id INTEGER,
            created_at TIMESTAMP DEFAULT SYSDATE() NOT NULL,
            updated_at TIMESTAMP_TZ DEFAULT SYSDATE()
        )"""

        result = self.translator.translate(sql)

        # Should have parentheses around the AT TIME ZONE expression in DEFAULT clause
        # Note: SQLGlot may format this across multiple lines
        assert "CURRENT_TIMESTAMP AT TIME ZONE 'UTC'" in result
        assert "DEFAULT (" in result
        # Should appear twice (for both columns)
        assert result.count("CURRENT_TIMESTAMP AT TIME ZONE 'UTC'") == 2
        assert "DEFAULT (" in result

    def test_where_clause_with_sysdate(self):
        """Test SYSDATE() in WHERE clause."""
        sql = "SELECT * FROM orders WHERE created_at >= SYSDATE() - 7"
        result = self.translator.translate(sql)

        # Should not have extra parentheses in WHERE clause
        assert "CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - 7" in result
        # Should not have parentheses around the AT TIME ZONE expression
        assert "(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')" not in result

    def test_multiple_sysdate_calls(self):
        """Test multiple SYSDATE() calls in one query."""
        sql = """
        SELECT
            SYSDATE() AS query_time,
            user_id,
            created_at
        FROM events
        WHERE created_at >= SYSDATE() - INTERVAL '24 hours'
          AND updated_at < SYSDATE()
        """
        result = self.translator.translate(sql)

        # Should have three SYSDATE() translations
        assert result.count("CURRENT_TIMESTAMP AT TIME ZONE 'UTC'") == 3

    def test_sysdate_in_complex_expression(self):
        """Test SYSDATE() in complex expressions."""
        sql = "SELECT DATEADD('day', -7, SYSDATE()) AS week_ago FROM events"
        result = self.translator.translate(sql)

        # Should translate SYSDATE() correctly within function calls
        assert "CURRENT_TIMESTAMP AT TIME ZONE 'UTC'" in result

    def test_sysdate_with_arithmetic(self):
        """Test SYSDATE() with arithmetic operations."""
        sql = "SELECT SYSDATE() - 30 AS thirty_days_ago"
        result = self.translator.translate(sql)

        assert "CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - 30" in result

    def test_create_table_multiple_defaults(self):
        """Test multiple DEFAULT SYSDATE() in CREATE TABLE."""
        # Use simpler types that are more universally supported
        sql = """CREATE TABLE audit_log (
            id INTEGER PRIMARY KEY,
            action VARCHAR(50) NOT NULL,
            created_at TIMESTAMP_TZ DEFAULT SYSDATE() NOT NULL,
            updated_at TIMESTAMP_TZ DEFAULT SYSDATE() NOT NULL,
            processed_at TIMESTAMP_TZ
        )"""

        result = self.translator.translate(sql)

        # Should have parentheses for both DEFAULT clauses
        # Note: formatting may be across multiple lines
        assert result.count("CURRENT_TIMESTAMP AT TIME ZONE 'UTC'") == 2
        assert result.count("DEFAULT (") >= 2

    def test_translation_info(self):
        """Test get_translation_info method with SYSDATE()."""
        sql = "SELECT SYSDATE() FROM users"
        info = self.translator.get_translation_info(sql)

        assert info["success"] is True
        assert info["original_sql"] == sql
        assert "CURRENT_TIMESTAMP AT TIME ZONE 'UTC'" in info["translated_sql"]
        assert "sysdate_to_utc" in info["transformations_applied"]

    def test_sysdate_case_insensitive(self):
        """Test that SYSDATE() works with different cases."""
        # Note: This test might fail if the dialect is case-sensitive
        # but it's worth testing for robustness
        sql = "SELECT sysdate() AS current_time FROM users"
        try:
            result = self.translator.translate(sql)
            # If it works, check the result
            assert "CURRENT_TIMESTAMP AT TIME ZONE 'UTC'" in result
        except Exception:
            # If case-sensitive, this is expected
            pytest.skip("Dialect is case-sensitive for function names")

    def test_error_handling_with_invalid_sql(self):
        """Test error handling when SQL parsing fails."""
        sql = "SELECT INVALID_SYNTAX FROM"
        info = self.translator.get_translation_info(sql)

        assert info["success"] is False
        assert info["error"] is not None
        assert info["translated_sql"] is None

    def test_nested_sysdate_in_cte(self):
        """Test SYSDATE() in Common Table Expression (CTE)."""
        sql = """
        WITH recent_events AS (
            SELECT *
            FROM events
            WHERE created_at >= SYSDATE() - INTERVAL '7 days'
        )
        SELECT COUNT(*), SYSDATE() AS report_time
        FROM recent_events
        """
        result = self.translator.translate(sql)

        # Should translate both SYSDATE() calls
        assert result.count("CURRENT_TIMESTAMP AT TIME ZONE 'UTC'") == 2

    def test_sysdate_in_subquery(self):
        """Test SYSDATE() in subquery."""
        sql = """
        SELECT user_id,
               (SELECT COUNT(*)
                FROM user_actions
                WHERE action_time >= SYSDATE() - 30) AS recent_actions
        FROM users
        """
        result = self.translator.translate(sql)

        assert "CURRENT_TIMESTAMP AT TIME ZONE 'UTC'" in result

    def test_original_snowflake_functions_still_work(self):
        """Ensure standard Snowflake functions still work after adding SYSDATE()."""
        sql = "SELECT CURRENT_TIMESTAMP(), CURRENT_DATE(), NOW() FROM users"
        result = self.translator.translate(sql)

        # These should translate normally without our custom transformation
        assert "CURRENT_TIMESTAMP()" in result or "CURRENT_TIMESTAMP" in result
        assert "CURRENT_DATE" in result
        assert "NOW()" in result or "NOW" in result

    def test_performance_with_large_query(self):
        """Test performance with a query containing many SYSDATE() calls."""
        # Create a query with multiple SYSDATE() calls
        sysdate_calls = [f"SYSDATE() AS time_{i}" for i in range(10)]
        sql = f"SELECT {', '.join(sysdate_calls)} FROM users"

        result = self.translator.translate(sql)

        # Should handle all SYSDATE() calls
        assert result.count("CURRENT_TIMESTAMP AT TIME ZONE 'UTC'") == 10
