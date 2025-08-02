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
