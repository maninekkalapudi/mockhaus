"""Custom Snowflake dialect extending SQLGlot's Snowflake dialect."""

from typing import Any

from sqlglot import expressions as exp
from sqlglot.dialects.snowflake import Snowflake

from .expressions import IdentifierFunc, Sysdate


def _parse_sysdate(_: Any) -> exp.Expression:
    """
    Parse SYSDATE() function.

    SYSDATE() in Snowflake takes no arguments and returns current UTC timestamp.
    Args should be empty for SYSDATE().
    """
    return Sysdate()


def _build_identifier_func(args: list) -> exp.Expression:
    """
    Build IDENTIFIER() function expression.

    Args:
        args: List of arguments passed to IDENTIFIER()

    Returns:
        IdentifierFunc expression with the argument
    """
    from sqlglot.helper import seq_get

    return IdentifierFunc(this=seq_get(args, 0))


class CustomSnowflakeParser(Snowflake.Parser):
    """Extended Snowflake parser with custom functions."""

    # Extend the FUNCTIONS dictionary with custom functions
    FUNCTIONS = {
        **Snowflake.Parser.FUNCTIONS,
        "SYSDATE": _parse_sysdate,
        "IDENTIFIER": _build_identifier_func,
    }


class CustomSnowflakeGenerator(Snowflake.Generator):
    """Extended Snowflake SQL generator with custom function support."""

    def sysdate_sql(self, _: Sysdate) -> str:
        """Generate SQL for SYSDATE() function in Snowflake dialect."""
        return "SYSDATE()"

    def identifierfunc_sql(self, expression: IdentifierFunc) -> str:
        """Generate SQL for IDENTIFIER() function in Snowflake dialect."""
        # For Snowflake, always keep as IDENTIFIER() function
        return self.function_fallback_sql(expression)

    # Register custom SQL generators
    TRANSFORMS = {
        **Snowflake.Generator.TRANSFORMS,
        Sysdate: sysdate_sql,
        IdentifierFunc: identifierfunc_sql,
    }


class CustomSnowflake(Snowflake):
    """
    Custom Snowflake dialect with extended function support.

    This dialect adds support for Snowflake-specific functions that
    are not yet supported in the main SQLGlot library, starting with
    SYSDATE().
    """

    class Parser(CustomSnowflakeParser):
        """Parser for custom Snowflake dialect."""

        pass

    class Generator(CustomSnowflakeGenerator):
        """Generator for custom Snowflake dialect."""

        pass


# Convenience function for dialect registration
def get_dialect() -> type[CustomSnowflake]:
    """Return the custom Snowflake dialect class."""
    return CustomSnowflake
