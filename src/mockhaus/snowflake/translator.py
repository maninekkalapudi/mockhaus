"""
This module provides the core SQL translation engine for converting Snowflake
SQL to DuckDB-compatible SQL.

It defines the `SnowflakeToDuckDBTranslator` class, which leverages the `sqlglot`
library and custom dialects to parse Snowflake SQL and generate the equivalent
SQL for DuckDB. This is responsible for handling standard DML and DDL but does
not handle complex data ingestion statements, which are managed by the
`SnowflakeIngestionHandler`.
"""

from typing import Any

import sqlglot

from ..sqlglot.dialects import CustomDuckDB, CustomSnowflake


class SnowflakeToDuckDBTranslator:
    """
    Translates Snowflake SQL queries to DuckDB-compatible SQL using `sqlglot`.

    This class is responsible for the core SQL translation, but does not handle
    data ingestion statements (see `SnowflakeIngestionHandler`).
    """

    def __init__(self) -> None:
        """Initialize the translator."""
        self.source_dialect = CustomSnowflake
        self.target_dialect = CustomDuckDB

    def translate(self, snowflake_sql: str) -> str:
        """
        Translate a Snowflake SQL query to DuckDB SQL.

        Args:
            snowflake_sql: The Snowflake SQL query to translate

        Returns:
            The translated DuckDB SQL query

        Raises:
            ValueError: If the SQL cannot be parsed or translated
        """
        try:
            # Parse the Snowflake SQL using custom dialect
            parsed = sqlglot.parse_one(snowflake_sql, dialect=self.source_dialect)

            # Generate DuckDB SQL using custom dialect
            # The dialects handle the SYSDATE transformation automatically
            return parsed.sql(dialect=self.target_dialect, pretty=True)

        except Exception as e:
            raise ValueError(f"Failed to translate SQL: {snowflake_sql}. Error: {str(e)}") from e

    def get_translation_info(self, snowflake_sql: str) -> dict[str, Any]:
        """
        Get detailed information about the translation process.

        Args:
            snowflake_sql: The Snowflake SQL query to analyze

        Returns:
            Dictionary containing translation details
        """
        try:
            parsed = sqlglot.parse_one(snowflake_sql, dialect=self.source_dialect)
            duckdb_sql = parsed.sql(dialect=self.target_dialect, pretty=True)

            return {
                "original_sql": snowflake_sql,
                "translated_sql": duckdb_sql,
                "source_dialect": self.source_dialect.__name__ if hasattr(self.source_dialect, "__name__") else str(self.source_dialect),
                "target_dialect": self.target_dialect.__name__ if hasattr(self.target_dialect, "__name__") else str(self.target_dialect),
                "success": True,
                "error": None,
                "transformations_applied": self._get_applied_transformations(snowflake_sql, duckdb_sql),
            }

        except Exception as e:
            return {
                "original_sql": snowflake_sql,
                "translated_sql": None,
                "source_dialect": self.source_dialect.__name__ if hasattr(self.source_dialect, "__name__") else str(self.source_dialect),
                "target_dialect": self.target_dialect.__name__ if hasattr(self.target_dialect, "__name__") else str(self.target_dialect),
                "success": False,
                "error": str(e),
                "transformations_applied": [],
            }

    def _get_applied_transformations(self, original_sql: str, translated_sql: str) -> list[str]:
        """Get a list of transformations that were applied."""
        transformations = []

        # Check if SYSDATE was transformed
        if "SYSDATE(" in original_sql.upper() and "CURRENT_TIMESTAMP AT TIME ZONE" in translated_sql.upper():
            transformations.append("sysdate_to_utc")

        # Check if there are other differences indicating transformation
        if original_sql.strip() != translated_sql.strip():
            transformations.append("dialect_translation")

        return transformations


def translate_snowflake_to_duckdb(sql: str) -> str:
    """
    Convenience function to translate Snowflake SQL to DuckDB SQL.

    Args:
        sql: The Snowflake SQL query to translate

    Returns:
        The translated DuckDB SQL query
    """
    translator = SnowflakeToDuckDBTranslator()
    return translator.translate(sql)
