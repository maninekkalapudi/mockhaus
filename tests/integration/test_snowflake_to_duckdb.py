"""End-to-end integration tests for Snowflake to DuckDB translation."""

import pytest
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from mockhaus.snowflake.translator import SnowflakeToDuckDBTranslator


class TestSnowflakeToDuckDBIntegration:
    """Integration tests for the complete translation pipeline."""

    def setup_method(self):
        """Set up test fixtures."""
        self.translator = SnowflakeToDuckDBTranslator()

    def test_complete_translation_pipeline(self):
        """Test the complete translation from Snowflake to DuckDB."""
        snowflake_sql = """
        CREATE TABLE users (
            id INTEGER,
            name VARCHAR(100),
            created_at TIMESTAMP DEFAULT SYSDATE() NOT NULL,
            updated_at TIMESTAMP_TZ DEFAULT SYSDATE()
        )
        """

        result = self.translator.translate(snowflake_sql)

        # Should contain DuckDB-compatible syntax
        assert "CREATE TABLE" in result
        assert "CURRENT_TIMESTAMP AT TIME ZONE 'UTC'" in result
        assert result.count("CURRENT_TIMESTAMP AT TIME ZONE 'UTC'") == 2

    def test_complex_query_translation(self):
        """Test translation of complex queries with multiple features."""
        snowflake_sql = """
        WITH recent_orders AS (
            SELECT 
                customer_id,
                order_date,
                total_amount,
                SYSDATE() AS query_time
            FROM orders 
            WHERE order_date >= SYSDATE() - INTERVAL '30 days'
        )
        SELECT 
            customer_id,
            COUNT(*) as order_count,
            SUM(total_amount) as total_spent,
            AVG(total_amount) as avg_order_value,
            SYSDATE() AS report_generated_at
        FROM recent_orders
        GROUP BY customer_id
        HAVING COUNT(*) > 1
        ORDER BY total_spent DESC
        """

        result = self.translator.translate(snowflake_sql)

        # Check that all SYSDATE calls were translated
        assert result.count("CURRENT_TIMESTAMP AT TIME ZONE 'UTC'") == 3
        # Should maintain SQL structure
        assert "WITH recent_orders AS" in result
        assert "GROUP BY" in result
        assert "HAVING" in result
        assert "ORDER BY" in result

    def test_translation_info_comprehensive(self):
        """Test the translation info for complex queries."""
        snowflake_sql = """
        SELECT 
            user_id,
            login_time,
            SYSDATE() - login_time AS time_since_login
        FROM user_sessions 
        WHERE login_time >= SYSDATE() - 7
        """

        info = self.translator.get_translation_info(snowflake_sql)

        assert info["success"] is True
        assert info["original_sql"] == snowflake_sql
        assert "CURRENT_TIMESTAMP AT TIME ZONE 'UTC'" in info["translated_sql"]
        assert "sysdate_to_utc" in info["transformations_applied"]
        assert "dialect_translation" in info["transformations_applied"]
        assert info["source_dialect"] == "CustomSnowflake"
        assert info["target_dialect"] == "CustomDuckDB"

    def test_error_handling_integration(self):
        """Test error handling in the complete pipeline."""
        invalid_sql = "SELECT FROM WHERE"

        info = self.translator.get_translation_info(invalid_sql)

        assert info["success"] is False
        assert info["error"] is not None
        assert info["translated_sql"] is None
        assert info["original_sql"] == invalid_sql
