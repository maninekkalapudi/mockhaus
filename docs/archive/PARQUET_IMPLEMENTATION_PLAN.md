# PARQUET Format Implementation Plan

## Overview

This document outlines the implementation plan for adding comprehensive PARQUET format support to Mockhaus, along with refactoring the existing file format system for better modularity and extensibility.

## Current State Analysis

### Existing File Format System
- **Location**: `src/mockhaus/snowflake/file_formats.py`
- **Structure**: Monolithic `MockFileFormatManager` class with format-specific methods
- **Current PARQUET Support**: Minimal - only returns `{"FORMAT": "PARQUET"}`
- **Issues**:
  - All format logic in single file (263 lines)
  - Format-specific methods (`_map_csv_options`, `_map_json_options`, `_map_parquet_options`) tightly coupled
  - Hard to extend for new formats
  - PARQUET implementation is essentially empty (lines 210-214)

### Key Problems to Solve
1. **Monolithic Design**: All format logic in one class
2. **Limited PARQUET Support**: No option mapping
3. **Poor Extensibility**: Adding new formats requires modifying core class
4. **Inconsistent Mapping**: Each format has different mapping complexity

## Proposed Architecture

### Modular Format Handler Design

```
src/mockhaus/snowflake/file_formats/
├── __init__.py                 # Public API exports
├── base.py                     # Base classes and interfaces
├── manager.py                  # Refactored MockFileFormatManager
├── csv.py                      # CSV format handler
├── json.py                     # JSON format handler
├── parquet.py                  # PARQUET format handler (NEW)
└── utils.py                    # Shared utilities
```

### Class Hierarchy

```python
# Base format handler interface
class BaseFormatHandler(ABC):
    @abstractmethod
    def get_default_properties(self) -> dict[str, Any]: ...
    
    @abstractmethod
    def map_to_duckdb_options(self, properties: dict[str, Any]) -> FormatMappingResult: ...
    
    @abstractmethod
    def validate_properties(self, properties: dict[str, Any]) -> ValidationResult: ...

# Format mapping result with warnings
@dataclass
class FormatMappingResult:
    options: dict[str, Any]
    warnings: list[str] = field(default_factory=list)
    ignored_options: list[str] = field(default_factory=list)

# Specific format handlers
class CSVFormatHandler(BaseFormatHandler): ...
class JSONFormatHandler(BaseFormatHandler): ...
class ParquetFormatHandler(BaseFormatHandler): ...  # NEW
```

## Implementation Plan

### Phase 1: Create Base Architecture (1-2 days)

#### Task 1.1: Create Base Classes
**File**: `src/mockhaus/snowflake/file_formats/base.py`

```python
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

@dataclass
class FormatMappingResult:
    """Result of mapping Snowflake format properties to DuckDB options."""
    options: dict[str, Any]
    warnings: list[str] = field(default_factory=list)
    ignored_options: list[str] = field(default_factory=list)

@dataclass
class ValidationResult:
    """Result of validating format properties."""
    is_valid: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

class BaseFormatHandler(ABC):
    """Base class for all format handlers."""
    
    @property
    @abstractmethod
    def format_type(self) -> str:
        """Return the format type (e.g., 'CSV', 'PARQUET')."""
        pass
    
    @abstractmethod
    def get_default_properties(self) -> dict[str, Any]:
        """Get default properties for this format type."""
        pass
    
    @abstractmethod
    def map_to_duckdb_options(self, properties: dict[str, Any]) -> FormatMappingResult:
        """Map Snowflake format properties to DuckDB COPY options."""
        pass
    
    def validate_properties(self, properties: dict[str, Any]) -> ValidationResult:
        """Validate format properties. Default implementation returns valid."""
        return ValidationResult(is_valid=True)
    
    def _log_warnings(self, warnings: list[str]) -> None:
        """Log warnings using the existing logging system."""
        from mockhaus.logging import debug_log
        for warning in warnings:
            debug_log(f"{self.format_type} format warning: {warning}")
```

#### Task 1.2: Create Format Registry
**File**: `src/mockhaus/snowflake/file_formats/registry.py`

```python
from typing import Type, Dict
from .base import BaseFormatHandler

class FormatHandlerRegistry:
    """Registry for format handlers."""
    
    def __init__(self):
        self._handlers: Dict[str, Type[BaseFormatHandler]] = {}
    
    def register(self, format_type: str, handler_class: Type[BaseFormatHandler]) -> None:
        """Register a format handler."""
        self._handlers[format_type.upper()] = handler_class
    
    def get_handler(self, format_type: str) -> BaseFormatHandler:
        """Get handler instance for format type."""
        handler_class = self._handlers.get(format_type.upper())
        if not handler_class:
            raise ValueError(f"Unsupported format type: {format_type}")
        return handler_class()
    
    def get_supported_formats(self) -> list[str]:
        """Get list of supported format types."""
        return list(self._handlers.keys())

# Global registry instance
format_registry = FormatHandlerRegistry()
```

### Phase 2: Implement PARQUET Handler (2-3 days)

#### Task 2.1: Create PARQUET Format Handler
**File**: `src/mockhaus/snowflake/file_formats/parquet.py`

```python
from typing import Any
from .base import BaseFormatHandler, FormatMappingResult

class ParquetFormatHandler(BaseFormatHandler):
    """Handler for PARQUET format mappings."""
    
    @property
    def format_type(self) -> str:
        return "PARQUET"
    
    def get_default_properties(self) -> dict[str, Any]:
        """Get default PARQUET properties."""
        return {
            "compression": "AUTO",
            "binary_as_text": True,
            "null_if": [r"\N"],
            "trim_space": False,
        }
    
    def map_to_duckdb_options(self, properties: dict[str, Any]) -> FormatMappingResult:
        """Map PARQUET properties to DuckDB options."""
        options = {"FORMAT": "PARQUET"}
        warnings = []
        ignored_options = []
        
        # Handle compression mapping
        self._map_compression(properties, options, warnings)
        
        # Handle binary_as_text mapping
        self._map_binary_as_text(properties, options)
        
        # Handle unsupported options gracefully
        self._handle_unsupported_options(properties, warnings, ignored_options)
        
        # Log warnings
        if warnings:
            self._log_warnings(warnings)
        
        return FormatMappingResult(
            options=options,
            warnings=warnings,
            ignored_options=ignored_options
        )
    
    def _map_compression(self, props: dict[str, Any], options: dict[str, Any], warnings: list[str]) -> None:
        """Map compression settings."""
        compression = props.get("compression", props.get("COMPRESSION"))
        if not compression:
            return
            
        compression_mapping = {
            "AUTO": "snappy",      # Snowflake AUTO → DuckDB default
            "SNAPPY": "snappy",    # Direct mapping
            "NONE": "uncompressed", # Snowflake NONE → DuckDB uncompressed
            "GZIP": "gzip",        # Direct mapping
            "BROTLI": "brotli",    # Direct mapping
            "ZSTD": "zstd",        # Direct mapping
            "LZ4": "lz4",          # Direct mapping
        }
        
        compression_upper = compression.upper()
        if compression_upper == "LZO":
            # LZO not supported in DuckDB
            warnings.append("LZO compression not supported in DuckDB, using snappy instead")
            options["COMPRESSION"] = "snappy"
        elif compression_upper in compression_mapping:
            options["COMPRESSION"] = compression_mapping[compression_upper]
        else:
            warnings.append(f"Unknown compression type '{compression}', using snappy")
            options["COMPRESSION"] = "snappy"
    
    def _map_binary_as_text(self, props: dict[str, Any], options: dict[str, Any]) -> None:
        """Map BINARY_AS_TEXT to binary_as_string."""
        binary_as_text = props.get("BINARY_AS_TEXT", props.get("binary_as_text"))
        if binary_as_text is not None:
            # Convert to boolean if string
            if isinstance(binary_as_text, str):
                binary_as_text = binary_as_text.upper() in ('TRUE', '1', 'YES')
            options["binary_as_string"] = bool(binary_as_text)
    
    def _handle_unsupported_options(
        self, 
        props: dict[str, Any], 
        warnings: list[str], 
        ignored_options: list[str]
    ) -> None:
        """Handle unsupported options gracefully."""
        unsupported_options = {
            "NULL_IF": "null_if",
            "TRIM_SPACE": "trim_space"
        }
        
        for snowflake_opt, snake_case_opt in unsupported_options.items():
            if snowflake_opt in props or snake_case_opt in props:
                warnings.append(f"{snowflake_opt} not supported in DuckDB PARQUET format, ignoring option")
                ignored_options.append(snowflake_opt)
```

#### Task 2.2: Add Comprehensive Tests
**File**: `tests/unit/snowflake/test_parquet_handler.py`

```python
import pytest
from src.mockhaus.snowflake.file_formats.parquet import ParquetFormatHandler

class TestParquetFormatHandler:
    def setup_method(self):
        self.handler = ParquetFormatHandler()
    
    def test_format_type(self):
        assert self.handler.format_type == "PARQUET"
    
    def test_default_properties(self):
        defaults = self.handler.get_default_properties()
        assert defaults["compression"] == "AUTO"
        assert defaults["binary_as_text"] is True
    
    def test_compression_mapping(self):
        """Test all compression mappings."""
        test_cases = [
            ("AUTO", "snappy"),
            ("SNAPPY", "snappy"),
            ("NONE", "uncompressed"),
            ("GZIP", "gzip"),
            ("BROTLI", "brotli"),
            ("ZSTD", "zstd"),
            ("LZ4", "lz4"),
        ]
        
        for snowflake_compression, expected_duckdb in test_cases:
            props = {"COMPRESSION": snowflake_compression}
            result = self.handler.map_to_duckdb_options(props)
            
            assert result.options["FORMAT"] == "PARQUET"
            assert result.options["COMPRESSION"] == expected_duckdb
            assert len(result.warnings) == 0
    
    def test_lzo_compression_fallback(self):
        """Test LZO compression fallback."""
        props = {"COMPRESSION": "LZO"}
        result = self.handler.map_to_duckdb_options(props)
        
        assert result.options["COMPRESSION"] == "snappy"
        assert len(result.warnings) == 1
        assert "LZO compression not supported" in result.warnings[0]
    
    def test_binary_as_text_mapping(self):
        """Test BINARY_AS_TEXT mapping."""
        test_cases = [
            (True, True),
            (False, False),
            ("TRUE", True),
            ("FALSE", False),
            ("true", True),
            ("false", False),
        ]
        
        for input_value, expected in test_cases:
            props = {"BINARY_AS_TEXT": input_value}
            result = self.handler.map_to_duckdb_options(props)
            
            assert result.options["binary_as_string"] == expected
    
    def test_unsupported_options_handling(self):
        """Test unsupported options are handled gracefully."""
        props = {
            "COMPRESSION": "SNAPPY",
            "BINARY_AS_TEXT": True,
            "NULL_IF": ["", "NULL"],
            "TRIM_SPACE": False
        }
        result = self.handler.map_to_duckdb_options(props)
        
        # Supported options should be mapped
        assert result.options["COMPRESSION"] == "snappy"
        assert result.options["binary_as_string"] is True
        
        # Unsupported options should generate warnings
        assert len(result.warnings) == 2
        assert "NULL_IF not supported" in " ".join(result.warnings)
        assert "TRIM_SPACE not supported" in " ".join(result.warnings)
        
        # Ignored options should be tracked
        assert "NULL_IF" in result.ignored_options
        assert "TRIM_SPACE" in result.ignored_options
    
    def test_complex_inline_format(self):
        """Test complex inline format handling."""
        props = {
            "TYPE": "PARQUET",
            "COMPRESSION": "ZSTD", 
            "BINARY_AS_TEXT": "TRUE",
            "NULL_IF": ["\\N", ""],
            "TRIM_SPACE": "FALSE"
        }
        result = self.handler.map_to_duckdb_options(props)
        
        expected_options = {
            "FORMAT": "PARQUET",
            "COMPRESSION": "zstd",
            "binary_as_string": True
        }
        
        for key, value in expected_options.items():
            assert result.options[key] == value
        
        # Should have warnings for unsupported options
        assert len(result.warnings) >= 1
```

### Phase 3: Refactor Existing Format Manager (2-3 days)

#### Task 3.1: Extract Existing Format Handlers
**File**: `src/mockhaus/snowflake/file_formats/csv.py`

```python
from typing import Any
from .base import BaseFormatHandler, FormatMappingResult

class CSVFormatHandler(BaseFormatHandler):
    """Handler for CSV format mappings."""
    
    @property
    def format_type(self) -> str:
        return "CSV"
    
    def get_default_properties(self) -> dict[str, Any]:
        """Get default CSV properties."""
        return {
            "field_delimiter": ",",
            "record_delimiter": "\\n",
            "skip_header": 0,
            "field_optionally_enclosed_by": None,
            "null_if": [],
            "compression": "AUTO",
            "date_format": "AUTO",
            "time_format": "AUTO",
            "timestamp_format": "AUTO",
        }
    
    def map_to_duckdb_options(self, properties: dict[str, Any]) -> FormatMappingResult:
        """Map CSV properties to DuckDB options."""
        # Move existing _map_csv_options logic here
        # Return FormatMappingResult with warnings
        # ... (implementation moved from existing code)
```

**File**: `src/mockhaus/snowflake/file_formats/json.py`

```python
from typing import Any
from .base import BaseFormatHandler, FormatMappingResult

class JSONFormatHandler(BaseFormatHandler):
    """Handler for JSON format mappings."""
    
    @property
    def format_type(self) -> str:
        return "JSON"
    
    def get_default_properties(self) -> dict[str, Any]:
        """Get default JSON properties."""
        return {
            "compression": "AUTO",
            "date_format": "AUTO", 
            "time_format": "AUTO",
            "timestamp_format": "AUTO"
        }
    
    def map_to_duckdb_options(self, properties: dict[str, Any]) -> FormatMappingResult:
        """Map JSON properties to DuckDB options."""
        # Move existing _map_json_options logic here
        # ... (implementation moved from existing code)
```

#### Task 3.2: Refactor MockFileFormatManager
**File**: `src/mockhaus/snowflake/file_formats/manager.py`

```python
from typing import Any
import json
import duckdb
from .base import FileFormat, FormatMappingResult
from .registry import format_registry
from .csv import CSVFormatHandler
from .json import JSONFormatHandler
from .parquet import ParquetFormatHandler

# Register format handlers
format_registry.register("CSV", CSVFormatHandler)
format_registry.register("JSON", JSONFormatHandler)
format_registry.register("PARQUET", ParquetFormatHandler)

class MockFileFormatManager:
    """Refactored file format manager using modular handlers."""
    
    def __init__(self, connection: duckdb.DuckDBPyConnection) -> None:
        """Initialize file format manager with DuckDB connection."""
        self.connection = connection
        self._create_system_tables()
        self._create_default_formats()
    
    def map_to_duckdb_options(self, file_format: FileFormat) -> dict[str, Any]:
        """Map Snowflake file format properties to DuckDB COPY options."""
        try:
            handler = format_registry.get_handler(file_format.format_type)
            result = handler.map_to_duckdb_options(file_format.properties)
            
            # Store mapping result metadata for debugging
            if result.warnings or result.ignored_options:
                from mockhaus.logging import debug_log
                debug_log(f"Format mapping for {file_format.name}: "
                         f"warnings={result.warnings}, ignored={result.ignored_options}")
            
            return result.options
            
        except ValueError:
            # Fallback for unsupported formats
            return {"FORMAT": file_format.format_type}
    
    def create_format(self, name: str, format_type: str, properties: dict[str, Any] | None = None) -> FileFormat:
        """Create a new file format using format handlers."""
        if properties is None:
            properties = {}

        # Validate format type using registry
        try:
            handler = format_registry.get_handler(format_type)
        except ValueError as e:
            supported = format_registry.get_supported_formats()
            raise ValueError(f"Unsupported format type: {format_type}. Supported: {supported}") from e

        format_type = format_type.upper()

        # Get default properties from handler
        final_properties = handler.get_default_properties()
        final_properties.update(properties)

        # Validate properties
        validation_result = handler.validate_properties(final_properties)
        if not validation_result.is_valid:
            raise ValueError(f"Invalid properties: {validation_result.errors}")

        # Create format object
        file_format = FileFormat(name=name, format_type=format_type, properties=final_properties)

        # Store in system table
        self._store_format_metadata(file_format)

        return file_format
    
    # ... (rest of the methods remain the same: get_format, list_formats, etc.)
```

#### Task 3.3: Update Package Structure
**File**: `src/mockhaus/snowflake/file_formats/__init__.py`

```python
"""Modular file format handling for Mockhaus."""

# Import the main classes for backward compatibility
from .manager import MockFileFormatManager
from .base import FileFormat, FormatMappingResult, ValidationResult

# Import format handlers for registration
from .csv import CSVFormatHandler
from .json import JSONFormatHandler
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
```

### Phase 4: Integration & Testing (1-2 days)

#### Task 4.1: Update Imports
**Files to Update**:
- `src/mockhaus/snowflake/copy_into.py`
- `tests/unit/snowflake/test_ingestion.py`
- Any other files importing from `file_formats.py`

**Change**:
```python
# Old import
from .file_formats import MockFileFormatManager, FileFormat

# New import (backward compatible)
from .file_formats import MockFileFormatManager, FileFormat
```

#### Task 4.2: Integration Tests
**File**: `tests/integration/test_parquet_copy_into.py`

```python
def test_parquet_copy_into_with_compression():
    """Test end-to-end PARQUET COPY INTO with compression options."""
    # Test with named format
    # Test with inline format
    # Test with unsupported options
    # Verify warnings are logged but operation succeeds

def test_parquet_copy_into_binary_as_text():
    """Test BINARY_AS_TEXT option mapping."""
    # Test with BINARY_AS_TEXT=TRUE
    # Test with BINARY_AS_TEXT=FALSE
    # Verify correct DuckDB binary_as_string mapping

def test_parquet_copy_into_graceful_degradation():
    """Test graceful handling of unsupported options."""
    # Test with NULL_IF and TRIM_SPACE
    # Verify warnings logged
    # Verify operation continues successfully
```

## Migration Strategy

### Backward Compatibility
- **No Breaking Changes**: All existing APIs remain unchanged
- **Import Compatibility**: `from .file_formats import MockFileFormatManager` still works
- **Behavior Compatibility**: Existing CSV and JSON handling unchanged

### Migration Steps
1. **Phase 1**: Create new modular structure alongside existing code
2. **Phase 2**: Implement PARQUET handler and test thoroughly
3. **Phase 3**: Refactor existing handlers to use new structure
4. **Phase 4**: Remove old monolithic methods once everything is tested

### Rollback Plan
- Keep existing `file_formats.py` as `file_formats_legacy.py`
- If issues arise, can quickly revert imports
- Gradual migration allows for incremental testing

## Benefits of New Architecture

### Extensibility
- **Easy to Add New Formats**: Just create new handler class
- **Isolated Logic**: Each format handler is independent
- **Consistent Interface**: All handlers follow same pattern

### Maintainability
- **Single Responsibility**: Each handler manages one format
- **Testable**: Each handler can be tested in isolation
- **Clear Structure**: Easy to find format-specific logic

### Features
- **Rich Warning System**: Track warnings and ignored options
- **Validation Support**: Validate properties before use
- **Debugging Support**: Detailed logging and error reporting

## Success Criteria

### Phase 1 Success
- [ ] Base classes and registry implemented
- [ ] Architecture supports multiple format handlers
- [ ] Tests for base functionality pass

### Phase 2 Success  
- [ ] Full PARQUET option mapping implemented
- [ ] All compression types supported (except LZO with fallback)
- [ ] BINARY_AS_TEXT → binary_as_string mapping works
- [ ] Unsupported options logged gracefully
- [ ] Comprehensive test coverage (>95%)

### Phase 3 Success
- [ ] CSV and JSON handlers extracted and working
- [ ] MockFileFormatManager refactored to use handlers
- [ ] No regression in existing functionality
- [ ] All existing tests pass

### Phase 4 Success
- [ ] Integration tests pass
- [ ] End-to-end COPY INTO with PARQUET works
- [ ] Performance impact minimal (<5% overhead)
- [ ] Documentation updated

## Implementation Timeline

- **Phase 1** (Base Architecture): 1-2 days
- **Phase 2** (PARQUET Handler): 2-3 days  
- **Phase 3** (Refactoring): 2-3 days
- **Phase 4** (Integration): 1-2 days

**Total Estimate**: 6-10 days

## Future Extensions

### Additional Format Support
With the new architecture, adding support for other formats becomes straightforward:
- **AVRO**: Create `AvroFormatHandler`
- **ORC**: Create `OrcFormatHandler`
- **XML**: Create `XmlFormatHandler`

### Enhanced Features
- **Custom Validation Rules**: Per-format property validation
- **Performance Optimization**: Caching of mapping results
- **Advanced Logging**: Structured logging with metrics
- **Configuration**: External configuration for format mappings

This modular approach sets up Mockhaus for easy extension while providing comprehensive PARQUET support that gracefully handles all Snowflake compatibility scenarios.