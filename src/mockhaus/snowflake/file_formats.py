"""File format management for Mockhaus data ingestion.

This module provides backward compatibility by importing from the new modular system.
"""

# Import from new modular system for backward compatibility
from .file_formats import FileFormat, MockFileFormatManager

# Re-export for backward compatibility
__all__ = ["MockFileFormatManager", "FileFormat"]
