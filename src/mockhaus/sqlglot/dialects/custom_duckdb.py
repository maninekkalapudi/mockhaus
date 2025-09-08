"""
This module defines a custom DuckDB dialect for `sqlglot`.

It extends the standard DuckDB dialect to provide custom SQL generation logic
for Snowflake-specific functions that have been parsed by the `CustomSnowflake`
dialect. This is where the translation from a Snowflake function call to its
DuckDB equivalent happens.
"""

from sqlglot import expressions as exp
from sqlglot.dialects.duckdb import DuckDB

from .expressions import IdentifierFunc, Sysdate


class CustomDuckDBGenerator(DuckDB.Generator):
    """
    An extended DuckDB SQL generator that knows how to translate custom expressions
    parsed from Snowflake SQL, such as `Sysdate` and `IdentifierFunc`.
    """

    def sysdate_sql(self, expression: Sysdate) -> str:
        """
        Generates the DuckDB SQL for Snowflake's `SYSDATE()` function.

        Snowflake's `SYSDATE()` returns the current timestamp in UTC. The DuckDB
        equivalent is `CURRENT_TIMESTAMP AT TIME ZONE 'UTC'`.

        Args:
            expression: The `Sysdate` expression node from the AST.

        Returns:
            The translated SQL string for DuckDB.
        """
        utc_expr = "CURRENT_TIMESTAMP AT TIME ZONE 'UTC'"
        # In contexts like CREATE TABLE DEFAULT, complex expressions need parentheses.
        if self._needs_parentheses_for_sysdate(expression):
            return f"({utc_expr})"
        return utc_expr

    def _needs_parentheses_for_sysdate(self, expression: Sysdate) -> bool:
        """
        Determines if the `SYSDATE` translation needs to be wrapped in parentheses.

        This is typically required when the function is used as a default value in
        a `CREATE TABLE` statement, as the `AT TIME ZONE` clause makes it a
        complex expression.

        Args:
            expression: The `Sysdate` expression node.

        Returns:
            True if parentheses are needed, False otherwise.
        """
        # Walk up the AST to see if the expression is inside a ColumnDef.
        current = expression.parent
        while current:
            if isinstance(current, exp.ColumnDef):
                return True
            current = current.parent
        return False

    def identifierfunc_sql(self, expression: IdentifierFunc) -> str:
        """
        Generates the DuckDB SQL for Snowflake's `IDENTIFIER()` function.

        For DuckDB, this translates `IDENTIFIER('my_table')` into a plain, unquoted
        identifier `my_table`.

        Args:
            expression: The `IdentifierFunc` expression node.

        Returns:
            The translated SQL string.
        """
        if isinstance(expression.this, exp.Literal) and expression.this.is_string:
            return str(exp.to_identifier(expression.this.this).sql(dialect=self.dialect))
        # Fallback for non-literals, which will likely error in DuckDB as intended.
        return self.function_fallback_sql(expression)

    # Register the custom transformations for the generator.
    TRANSFORMS = {
        **DuckDB.Generator.TRANSFORMS,
        Sysdate: sysdate_sql,
        IdentifierFunc: identifierfunc_sql,
    }


class CustomDuckDB(DuckDB):
    """
    A custom `sqlglot` dialect for DuckDB with extended generation capabilities.

    This dialect uses the `CustomDuckDBGenerator` to correctly translate custom
    Snowflake expressions into their DuckDB equivalents.
    """

    class Generator(CustomDuckDBGenerator):
        """The custom generator for this dialect."""

        pass