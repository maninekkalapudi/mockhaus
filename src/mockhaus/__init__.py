"""Mockhaus - Snowflake proxy with DuckDB backend."""

__version__ = "0.1.0"

from .executor import MockhausExecutor, QueryResult
from .snowflake import (
    CopyIntoTranslator,
    FileFormat,
    MockFileFormatManager,
    MockStageManager,
    SnowflakeToDuckDBTranslator,
    Stage,
    translate_snowflake_to_duckdb,
)

__all__ = [
    "SnowflakeToDuckDBTranslator",
    "translate_snowflake_to_duckdb",
    "MockhausExecutor",
    "QueryResult",
    "MockStageManager",
    "Stage",
    "MockFileFormatManager",
    "FileFormat",
    "CopyIntoTranslator",
]
