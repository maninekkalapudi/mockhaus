"""
This module defines a custom Snowflake dialect for `sqlglot`.

It extends the standard Snowflake dialect to add parsing support for
Snowflake-specific functions that are not yet included in the base `sqlglot`
library. This allows these functions to be correctly represented in the
Abstract Syntax Tree (AST), which can then be translated by the
`CustomDuckDB` dialect.
"""

from typing import Any

from sqlglot import expressions as exp
from sqlglot.dialects.snowflake import Snowflake

from .expressions import IdentifierFunc, Sysdate


def _parse_sysdate(_: Any) -> exp.Expression:
    """
    A parser function for `sqlglot` to handle Snowflake's `SYSDATE()`.

    This function is registered with the parser to create a `Sysdate` expression
    node when it encounters the `SYSDATE` function call.

    Returns:
        A `Sysdate` expression node.
    """
    return Sysdate()


def _build_identifier_func(args: list) -> exp.Expression:
    """
    A builder function for `sqlglot` to handle Snowflake's `IDENTIFIER()`.

    This creates an `IdentifierFunc` expression node from the parsed arguments.

    Args:
        args: The list of arguments parsed from the function call.

    Returns:
        An `IdentifierFunc` expression node.
    """
    from sqlglot.helper import seq_get
    return IdentifierFunc(this=seq_get(args, 0))


class CustomSnowflakeParser(Snowflake.Parser):
    """
    An extended Snowflake parser that includes custom function definitions.
    """

    # Extend the base Snowflake parser's FUNCTIONS dictionary.
    FUNCTIONS = {
        **Snowflake.Parser.FUNCTIONS,
        "SYSDATE": _parse_sysdate,
        "IDENTIFIER": _build_identifier_func,
    }


class CustomSnowflakeGenerator(Snowflake.Generator):
    """
    An extended Snowflake SQL generator to handle the custom expressions.

    This ensures that if we parse and then regenerate Snowflake SQL, the custom
    functions are written back correctly.
    """

    def sysdate_sql(self, _: Sysdate) -> str:
        """Generates the SQL for the `SYSDATE()` function."""
        return "SYSDATE()"

    def identifierfunc_sql(self, expression: IdentifierFunc) -> str:
        """Generates the SQL for the `IDENTIFIER()` function."""
        return self.function_fallback_sql(expression)

    # Register the custom transformations for the generator.
    TRANSFORMS = {
        **Snowflake.Generator.TRANSFORMS,
        Sysdate: sysdate_sql,
        IdentifierFunc: identifierfunc_sql,
    }


class CustomSnowflake(Snowflake):
    """
    A custom `sqlglot` dialect for Snowflake with extended function support.

    This dialect integrates the custom parser and generator to provide full
    support for parsing and generating Snowflake-specific functions like
    `SYSDATE()` and `IDENTIFIER()`.
    """

    class Parser(CustomSnowflakeParser):
        """The custom parser for this dialect."""

        pass

    class Generator(CustomSnowflakeGenerator):
        """The custom generator for this dialect."""

        pass