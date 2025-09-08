"""
This module defines custom `sqlglot` expression nodes.

These classes represent Snowflake-specific SQL functions that are not part of
the standard `sqlglot` library. By defining these custom expression types, we
can parse these functions into a structured Abstract Syntax Tree (AST) and then
implement custom generation logic for them in our dialects.
"""

from sqlglot import expressions as exp


class Sysdate(exp.Func):
    """
    Represents Snowflake's `SYSDATE()` function in a `sqlglot` AST.

    Snowflake's `SYSDATE()` returns the current system timestamp in UTC. It is a
    nullary function (takes no arguments).
    """

    arg_types = {}  # No arguments.
    is_var_len_args = False


class IdentifierFunc(exp.Func):
    """
    Represents Snowflake's `IDENTIFIER()` function in a `sqlglot` AST.

    The `IDENTIFIER()` function allows for dynamic object references in SQL
    statements by taking a string literal or a variable as its argument.
    For example, `SELECT * FROM IDENTIFIER('my_table')`.
    """

    arg_types = {"this": True}  # Requires one argument.
    _sql_names = ["IDENTIFIER"]  # The SQL function name.
    is_var = True  # Indicates this can be used as an identifier.