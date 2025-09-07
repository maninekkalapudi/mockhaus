"""SQL translation engine for converting Snowflake SQL to DuckDB SQL."""

from typing import Any

import sqlglot
from sqlglot import expressions as exp


class SnowflakeToDuckDBTranslator:
    """
    Translates Snowflake SQL queries to DuckDB-compatible SQL using `sqlglot`.

    This class is responsible for the core SQL translation, but does not handle
    data ingestion statements (see `SnowflakeIngestionHandler`).
    """

    def __init__(self) -> None:
        """Initialize the translator."""
        self.source_dialect = "snowflake"
        self.target_dialect = "duckdb"

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
            # Parse the Snowflake SQL
            parsed = sqlglot.parse_one(snowflake_sql, dialect=self.source_dialect)

            # Apply custom transformations
            transformed = self._apply_custom_transformations(parsed)

            # Generate DuckDB SQL
            return transformed.sql(dialect=self.target_dialect, pretty=True)

        except Exception as e:
            raise ValueError(f"Failed to translate SQL: {snowflake_sql}. Error: {str(e)}") from e

    def _apply_custom_transformations(self, parsed_sql: exp.Expression) -> exp.Expression:
        """
        Apply custom transformations for Snowflake-specific patterns.

        Args:
            parsed_sql: The parsed SQL expression

        Returns:
            The transformed SQL expression
        """
        # Transform common Snowflake functions to DuckDB equivalents
        transformed = parsed_sql.transform(self._transform_functions)

        # Handle case sensitivity (Snowflake is case-insensitive by default)
        return self._handle_case_sensitivity(transformed)

    def _transform_functions(self, node: exp.Expression) -> exp.Expression:
        """Transform Snowflake-specific functions to DuckDB equivalents."""
        # For now, let's skip function transformations and just return the node as-is
        # We'll add function transformations later once we have basic translation working
        return node

    def _handle_case_sensitivity(self, parsed_sql: exp.Expression) -> exp.Expression:
        """
        Handle case sensitivity differences between Snowflake and DuckDB.

        Snowflake is case-insensitive by default, DuckDB is case-sensitive.
        """
        # For now, we'll leave identifiers as-is
        # In a full implementation, we might want to normalize to uppercase
        # or handle quoted identifiers differently
        return parsed_sql

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
            transformed = self._apply_custom_transformations(parsed)
            duckdb_sql = transformed.sql(dialect=self.target_dialect, pretty=True)

            return {
                "original_sql": snowflake_sql,
                "translated_sql": duckdb_sql,
                "source_dialect": self.source_dialect,
                "target_dialect": self.target_dialect,
                "success": True,
                "error": None,
                "transformations_applied": self._get_applied_transformations(parsed, transformed),
            }

        except Exception as e:
            return {
                "original_sql": snowflake_sql,
                "translated_sql": None,
                "source_dialect": self.source_dialect,
                "target_dialect": self.target_dialect,
                "success": False,
                "error": str(e),
                "transformations_applied": [],
            }

    def _get_applied_transformations(self, original: exp.Expression, transformed: exp.Expression) -> list[str]:
        """Get a list of transformations that were applied."""
        transformations = []

        # This is a simplified implementation
        # In practice, you'd want to track transformations more systematically
        if str(original) != str(transformed):
            transformations.append("function_mapping")

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
