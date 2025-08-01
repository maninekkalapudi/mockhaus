"""Snowflake-specific functionality for Mockhaus."""

from .copy_into import CopyIntoTranslator
from .file_formats import FileFormat, MockFileFormatManager
from .ingestion import SnowflakeIngestionHandler
from .stages import MockStageManager, Stage
from .translator import SnowflakeToDuckDBTranslator, translate_snowflake_to_duckdb

__all__ = [
    "SnowflakeToDuckDBTranslator",
    "translate_snowflake_to_duckdb",
    "MockStageManager",
    "Stage",
    "MockFileFormatManager",
    "FileFormat",
    "CopyIntoTranslator",
    "SnowflakeIngestionHandler",
]
