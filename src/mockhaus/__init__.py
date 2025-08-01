"""Mockhaus - Snowflake proxy with DuckDB backend."""

__version__ = "0.1.0"

from .copy_into import CopyIntoTranslator
from .executor import MockhausExecutor, QueryResult
from .file_formats import FileFormat, MockFileFormatManager
from .stages import MockStageManager, Stage
from .translator import SnowflakeToDuckDBTranslator, translate_snowflake_to_duckdb

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
