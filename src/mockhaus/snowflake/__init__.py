"""
This package provides Snowflake-specific functionality for Mockhaus.

It includes modules for SQL translation, data ingestion (stages, file formats,
and COPY INTO operations), and database management. This `__init__.py` file
serves to mark `snowflake` as a Python package and makes key classes and
functions from its submodules directly accessible when `mockhaus.snowflake`
is imported.

The `__all__` variable explicitly defines the public API of this package,
controlling which names are exported when a wildcard import (`from mockhaus.snowflake import *`)
is used.
"""

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