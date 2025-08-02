"""Custom expressions for extended Snowflake dialect."""

from sqlglot import expressions as exp


class Sysdate(exp.Func):
    """
    Represents Snowflake's SYSDATE() function.

    SYSDATE() returns the current UTC timestamp in Snowflake.
    This function takes no arguments and returns a TIMESTAMP_TZ value.

    In Snowflake:
        - Returns current system timestamp in UTC
        - Type: TIMESTAMP_TZ
        - No arguments required
        - Commonly used in CREATE TABLE DEFAULT clauses
    """

    arg_types = {}  # No arguments
    is_var_len_args = False


class IdentifierFunc(exp.Func):
    """
    Represents Snowflake's IDENTIFIER() function.

    IDENTIFIER() allows using variables or string literals as dynamic object
    references (table names, column names, etc.) in DDL and DML statements.

    In Snowflake:
        - Takes a string literal, session variable, or bind variable
        - Returns an identifier that can be used for table/column names
        - Commonly used for dynamic SQL construction
        - Example: IDENTIFIER('my_table') or IDENTIFIER($table_var)

    In DuckDB translation:
        - The string content becomes an unquoted identifier
        - Example: IDENTIFIER('my_table') -> my_table
    """

    arg_types = {"this": True}  # Single required argument
    _sql_names = ["IDENTIFIER"]  # SQL function name
    is_var = True  # Indicates this can be used as an identifier
