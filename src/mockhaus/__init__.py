"""Mockhaus - Snowflake proxy with DuckDB backend."""

__version__ = "0.1.0"

from .executor import MockhausExecutor, QueryResult
from .translator import SnowflakeToDuckDBTranslator, translate_snowflake_to_duckdb

__all__ = [
    "SnowflakeToDuckDBTranslator",
    "translate_snowflake_to_duckdb",
    "MockhausExecutor",
    "QueryResult",
]
