"""Modular file format handling for Mockhaus."""

# Import the main classes for backward compatibility
# Import FileFormat from base module
from .base import FileFormat, FormatMappingResult, ValidationResult

# Import format handlers for registration
from .csv import CSVFormatHandler
from .json import JSONFormatHandler
from .manager import MockFileFormatManager
from .parquet import ParquetFormatHandler

__all__ = [
    "MockFileFormatManager",
    "FileFormat",
    "FormatMappingResult",
    "ValidationResult",
    "CSVFormatHandler",
    "JSONFormatHandler",
    "ParquetFormatHandler",
]
