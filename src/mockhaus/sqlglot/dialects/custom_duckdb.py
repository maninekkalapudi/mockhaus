"""Custom DuckDB dialect for handling Snowflake-specific functions."""

from sqlglot import expressions as exp
from sqlglot.dialects.duckdb import DuckDB

from .expressions import IdentifierFunc, Sysdate


class CustomDuckDBGenerator(DuckDB.Generator):
    """Extended DuckDB SQL generator that knows how to handle Snowflake functions."""

    def sysdate_sql(self, expression: Sysdate) -> str:
        """
        Generate DuckDB SQL for Snowflake's SYSDATE() function.

        SYSDATE() in Snowflake returns current UTC timestamp.
        In DuckDB, we translate this to CURRENT_TIMESTAMP AT TIME ZONE 'UTC'.

        For CREATE TABLE DEFAULT clauses, we need to wrap complex expressions
        in parentheses for proper SQL syntax.
        """
        # Create the AT TIME ZONE expression
        utc_expr = "CURRENT_TIMESTAMP AT TIME ZONE 'UTC'"

        # Check if we need parentheses based on context
        if self._needs_parentheses_for_sysdate(expression):
            return f"({utc_expr})"

        return utc_expr

    def _needs_parentheses_for_sysdate(self, expression: Sysdate) -> bool:
        """
        Determine if SYSDATE translation needs parentheses.

        We need parentheses in CREATE TABLE DEFAULT clauses where
        complex expressions like 'AT TIME ZONE' need to be wrapped.
        """
        # Walk up the parent chain to check context
        current: exp.Expression = expression
        depth = 0
        max_depth = 10  # Prevent infinite loops

        while hasattr(current, "parent") and current.parent and depth < max_depth:
            current = current.parent
            depth += 1

            # Check if we're in a CREATE statement
            if isinstance(current, exp.Create):
                return True

            # Check if we're in a column definition with a default
            if isinstance(current, exp.ColumnDef):
                return True

        return False

    def identifierfunc_sql(self, expression: IdentifierFunc) -> str:
        """
        Generate DuckDB SQL for Snowflake's IDENTIFIER() function.

        For DuckDB, converts IDENTIFIER('literal') to a plain identifier.
        For non-literals, falls back to function call syntax (which may error).
        """
        # For DuckDB, convert IDENTIFIER('literal') to identifier if possible
        if isinstance(expression.this, exp.Literal) and expression.this.is_string:
            # Convert string literal to unquoted identifier
            return str(exp.to_identifier(expression.this.this).sql(dialect=self.dialect))

        # For non-literals (variables, expressions), fallback to function call
        # This may error in DuckDB, which is appropriate since IDENTIFIER()
        # is not natively supported
        return self.function_fallback_sql(expression)

    def anonymous_sql(self, expression: exp.Anonymous) -> str:
        """
        Handle Anonymous functions, including IDENTIFIER() that wasn't caught by parser.
        """
        # Handle IDENTIFIER() functions that ended up as Anonymous
        if expression.this.upper() == "IDENTIFIER":
            return self.identifierfunc_sql_from_anonymous(expression)

        # For all other anonymous functions, use default behavior
        return super().anonymous_sql(expression)

    def identifierfunc_sql_from_anonymous(self, expression: exp.Anonymous) -> str:
        """
        Convert Anonymous IDENTIFIER() to plain identifier for DuckDB.
        """
        # Get the first argument
        args = expression.expressions
        if args and len(args) >= 1:
            arg = args[0]
            # If it's a string literal, convert to unquoted identifier
            if isinstance(arg, exp.Literal) and arg.is_string:
                return str(exp.to_identifier(arg.this).sql(dialect=self.dialect))

        # Fallback for non-literals or no args
        return super().anonymous_sql(expression)

    # Register the custom transformation
    TRANSFORMS = {
        **DuckDB.Generator.TRANSFORMS,
        Sysdate: sysdate_sql,
        IdentifierFunc: identifierfunc_sql,
        exp.Anonymous: anonymous_sql,
    }


class CustomDuckDB(DuckDB):
    """
    Custom DuckDB dialect that can handle Snowflake-specific functions.

    This dialect extends DuckDB to understand how to generate appropriate
    DuckDB SQL for Snowflake functions like SYSDATE().
    """

    class Generator(CustomDuckDBGenerator):
        """Generator for custom DuckDB dialect."""

        pass


# Convenience function for dialect registration
def get_duckdb_dialect() -> type[CustomDuckDB]:
    """Return the custom DuckDB dialect class."""
    return CustomDuckDB
