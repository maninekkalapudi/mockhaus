# Extended CSV Format Implementation Plan

## Overview

This document outlines the implementation plan for adding comprehensive CSV format support to Mockhaus, building upon the existing modular file format system. The goal is to achieve maximum Snowflake compatibility while leveraging DuckDB's advanced CSV processing capabilities.

## Current State Analysis

### Existing CSV Implementation
- **Location**: `src/mockhaus/snowflake/file_formats/csv.py` 
- **Status**: Basic implementation with limited option mapping
- **Current Features**:
  - FIELD_DELIMITER → delimiter mapping
  - SKIP_HEADER → header mapping  
  - FIELD_OPTIONALLY_ENCLOSED_BY → quote mapping
  - NULL_IF → nullstr mapping (first value only)
  - DATE_FORMAT/TIMESTAMP_FORMAT → dateformat/timestampformat mapping

### Current Limitations
1. **Incomplete NULL_IF Support**: Only uses first value from list
2. **Missing Features**: EMPTY_FIELD_AS_NULL, TRIM_SPACE, PARSE_HEADER, RECORD_DELIMITER variants
3. **No Preprocessing**: Cannot handle complex transformations
4. **Limited Error Handling**: No graceful degradation for unsupported options
5. **No Multi-Value Support**: Cannot handle list-based options properly
6. **Missing Compression**: Limited compression type support

### Integration Points
- **CopyIntoTranslator**: Calls format manager for option mapping
- **MockFileFormatManager**: Manages format instances and mapping
- **Test Coverage**: Basic tests exist but need expansion for new features

## Proposed Architecture Enhancement

### Enhanced CSV Handler Design

```
src/mockhaus/snowflake/file_formats/
├── csv.py                      # Enhanced CSV handler (UPDATED)
├── preprocessing/              # New preprocessing module
│   ├── __init__.py
│   ├── null_normalizer.py      # Handle multiple NULL_IF values
│   ├── text_processor.py       # Handle TRIM_SPACE, EMPTY_FIELD_AS_NULL
│   └── header_extractor.py     # Handle PARSE_HEADER logic
└── utils.py                    # Shared utilities (UPDATED)
```

### Class Architecture Enhancement

```python
# Enhanced result with preprocessing requirements
@dataclass
class FormatMappingResult:
    options: dict[str, Any]
    warnings: list[str] = field(default_factory=list)
    ignored_options: list[str] = field(default_factory=list)
    preprocessing_steps: list[dict[str, Any]] = field(default_factory=list)  # NEW

# Preprocessing step definition
@dataclass
class PreprocessingStep:
    step_type: str  # 'null_normalization', 'trim_spaces', 'header_extraction'
    parameters: dict[str, Any]
    priority: int = 0  # Execution order
```

## Implementation Plan

### Phase 1: Enhanced CSV Handler Core (3-4 days)

#### Task 1.1: Enhance Base CSV Handler
**File**: `src/mockhaus/snowflake/file_formats/csv.py`

**Enhancements**:
1. **Multi-value NULL_IF Support**
   ```python
   def _map_null_handling(self, props: dict, options: dict, preprocessing: list) -> None:
       """Enhanced NULL handling with multiple values and empty field support."""
       null_if = props.get("NULL_IF", props.get("null_if", []))
       empty_as_null = props.get("EMPTY_FIELD_AS_NULL", props.get("empty_field_as_null", True))
       
       if null_if or empty_as_null:
           canonical_null = "__MOCKHAUS_NULL__"
           options["nullstr"] = canonical_null
           
           preprocessing.append({
               "type": "null_normalization",
               "null_if_list": null_if if isinstance(null_if, list) else [null_if],
               "empty_as_null": empty_as_null,
               "canonical_null": canonical_null,
               "priority": 1
           })
   ```

2. **RECORD_DELIMITER Support**
   ```python
   def _map_record_delimiter(self, props: dict, options: dict, warnings: list) -> None:
       """Map RECORD_DELIMITER to DuckDB new_line option."""
       record_delim = props.get("RECORD_DELIMITER", props.get("record_delimiter"))
       if record_delim:
           # Handle common escape sequences
           delimiter_mapping = {
               "\\n": "\n",
               "\\r\\n": "\r\n", 
               "\\r": "\r",
               "\n": "\n",     # Already unescaped
               "\r\n": "\r\n", # Already unescaped
               "\r": "\r"      # Already unescaped
           }
           
           mapped_delim = delimiter_mapping.get(record_delim)
           if mapped_delim:
               options["new_line"] = mapped_delim
           else:
               warnings.append(f"Unusual RECORD_DELIMITER '{record_delim}', using default")
   ```

3. **PARSE_HEADER Support**
   ```python
   def _map_header_options(self, props: dict, options: dict, preprocessing: list) -> None:
       """Handle SKIP_HEADER and PARSE_HEADER combination."""
       skip_header = props.get("SKIP_HEADER", props.get("skip_header", 0))
       parse_header = props.get("PARSE_HEADER", props.get("parse_header", False))
       
       if isinstance(skip_header, (int, str)):
           try:
               skip_count = int(skip_header)
               options["skip_rows"] = skip_count
           except (ValueError, TypeError):
               skip_count = 0
       
       if parse_header:
           options["header"] = True
           if skip_count > 0:
               # Need to extract headers from row after skipping
               preprocessing.append({
                   "type": "header_extraction",
                   "skip_rows": skip_count,
                   "priority": 0  # Must run first
               })
   ```

4. **Enhanced Compression Mapping**
   ```python
   def _map_compression(self, props: dict, options: dict, warnings: list) -> None:
       """Enhanced compression mapping with fallback handling."""
       compression = props.get("COMPRESSION", props.get("compression", "AUTO"))
       if not compression:
           return
           
       compression_mapping = {
           "AUTO": "auto",
           "NONE": "none", 
           "GZIP": "gzip",
           # Unsupported types map to auto with warning
           "BZ2": ("auto", "BZ2 compression not supported, using auto detection"),
           "BROTLI": ("auto", "BROTLI compression not supported, using auto detection"),
           "ZSTD": ("auto", "ZSTD compression not supported, using auto detection"),
           "DEFLATE": ("auto", "DEFLATE compression not supported, using auto detection"),
           "RAW_DEFLATE": ("auto", "RAW_DEFLATE compression not supported, using auto detection"),
       }
       
       compression_upper = compression.upper()
       mapping = compression_mapping.get(compression_upper)
       
       if isinstance(mapping, tuple):
           # Unsupported with fallback
           options["compression"] = mapping[0]
           warnings.append(mapping[1])
       elif mapping:
           # Direct mapping
           options["compression"] = mapping
       else:
           # Unknown compression
           warnings.append(f"Unknown compression type '{compression}', using auto detection")
           options["compression"] = "auto"
   ```

5. **Enhanced Encoding Mapping**
   ```python
   def _map_encoding(self, props: dict, options: dict, warnings: list) -> None:
       """Enhanced encoding mapping with extension support detection."""
       encoding = props.get("ENCODING", props.get("encoding", "UTF-8"))
       if not encoding:
           return
           
       # Native DuckDB encodings (no extension required)
       native_encodings = {
           "UTF-8": "UTF-8",
           "UTF-16": "UTF-16", 
           "UTF-16BE": "UTF-16",
           "UTF-16LE": "UTF-16",
           "ISO-8859-1": "Latin-1"
       }
       
       encoding_upper = encoding.upper()
       if encoding_upper in native_encodings:
           # Direct mapping for native encodings
           options["encoding"] = native_encodings[encoding_upper]
       else:
           # Extended encodings require DuckDB encodings extension
           # TODO: Check if encodings extension is available
           # For now, fallback to UTF-8 with warning
           warnings.append(f"Encoding '{encoding}' requires DuckDB encodings extension, using UTF-8 fallback")
           options["encoding"] = "UTF-8"
   ```

#### Task 1.2: Create Preprocessing Infrastructure
**File**: `src/mockhaus/snowflake/file_formats/preprocessing/__init__.py`

```python
"""Preprocessing infrastructure for complex format transformations."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, IO
import tempfile
from pathlib import Path

@dataclass
class PreprocessingStep:
    """Definition of a preprocessing step."""
    step_type: str
    parameters: dict[str, Any]
    priority: int = 0

class PreprocessingEngine:
    """Engine for executing preprocessing steps on CSV files."""
    
    def __init__(self):
        self._processors = {}
        self._register_default_processors()
    
    def register_processor(self, step_type: str, processor_class):
        """Register a preprocessing processor."""
        self._processors[step_type] = processor_class
    
    def preprocess_file(self, file_path: str, steps: list[PreprocessingStep]) -> str:
        """Apply preprocessing steps to a file, return path to processed file."""
        if not steps:
            return file_path
        
        # Sort steps by priority
        sorted_steps = sorted(steps, key=lambda s: s.priority)
        
        current_path = file_path
        temp_files = []
        
        try:
            for step in sorted_steps:
                processor_class = self._processors.get(step.step_type)
                if not processor_class:
                    continue  # Skip unknown processors
                
                processor = processor_class()
                new_path = processor.process(current_path, step.parameters)
                
                if new_path != current_path:
                    temp_files.append(new_path)
                    current_path = new_path
            
            return current_path
            
        except Exception:
            # Cleanup temp files on error
            for temp_file in temp_files:
                try:
                    Path(temp_file).unlink()
                except OSError:
                    pass
            raise
    
    def _register_default_processors(self):
        """Register default preprocessing processors."""
        from .null_normalizer import NullNormalizer
        from .text_processor import TextProcessor
        from .header_extractor import HeaderExtractor
        
        self.register_processor("null_normalization", NullNormalizer)
        self.register_processor("trim_spaces", TextProcessor) 
        self.register_processor("header_extraction", HeaderExtractor)

# Global preprocessing engine
preprocessing_engine = PreprocessingEngine()
```

#### Task 1.3: Implement Null Normalizer
**File**: `src/mockhaus/snowflake/file_formats/preprocessing/null_normalizer.py`

```python
"""NULL value normalization preprocessing."""

import csv
import tempfile
from pathlib import Path
from typing import Any

class NullNormalizer:
    """Preprocessor to normalize multiple NULL representations to a single canonical form."""
    
    def process(self, file_path: str, parameters: dict[str, Any]) -> str:
        """
        Normalize NULL values in CSV file.
        
        Parameters:
        - null_if_list: List of strings to treat as NULL
        - empty_as_null: Whether to treat empty fields as NULL
        - canonical_null: Canonical NULL string to use
        """
        null_if_list = parameters.get("null_if_list", [])
        empty_as_null = parameters.get("empty_as_null", True)
        canonical_null = parameters.get("canonical_null", "__MOCKHAUS_NULL__")
        
        if not null_if_list and not empty_as_null:
            return file_path  # No processing needed
        
        # Create null value set for fast lookup
        null_values = set(null_if_list) if null_if_list else set()
        
        # Create temporary output file
        temp_fd, temp_path = tempfile.mkstemp(suffix='.csv', prefix='mockhaus_null_')
        
        try:
            with open(file_path, 'r', encoding='utf-8', newline='') as infile:
                with open(temp_fd, 'w', encoding='utf-8', newline='') as outfile:
                    # Use csv.reader/writer to handle proper CSV parsing
                    reader = csv.reader(infile)
                    writer = csv.writer(outfile)
                    
                    for row in reader:
                        processed_row = []
                        for field in row:
                            if self._is_null_value(field, null_values, empty_as_null):
                                processed_row.append(canonical_null)
                            else:
                                processed_row.append(field)
                        writer.writerow(processed_row)
            
            return temp_path
            
        except Exception:
            # Cleanup on error
            try:
                Path(temp_path).unlink()
            except OSError:
                pass
            raise
    
    def _is_null_value(self, field: str, null_values: set, empty_as_null: bool) -> bool:
        """Determine if a field should be treated as NULL."""
        if empty_as_null and field == "":
            return True
        return field in null_values
```

#### Task 1.4: Implement Text Processor
**File**: `src/mockhaus/snowflake/file_formats/preprocessing/text_processor.py`

```python
"""Text processing (TRIM_SPACE, etc.) preprocessing."""

import csv
import tempfile
from pathlib import Path
from typing import Any

class TextProcessor:
    """Preprocessor for text transformations like TRIM_SPACE."""
    
    def process(self, file_path: str, parameters: dict[str, Any]) -> str:
        """
        Apply text processing transformations.
        
        Parameters:
        - trim_spaces: Whether to trim leading/trailing spaces
        - columns: Which columns to process ('all' or list of column indices)
        """
        trim_spaces = parameters.get("trim_spaces", False)
        target_columns = parameters.get("columns", "all")
        
        if not trim_spaces:
            return file_path  # No processing needed
        
        # Create temporary output file
        temp_fd, temp_path = tempfile.mkstemp(suffix='.csv', prefix='mockhaus_text_')
        
        try:
            with open(file_path, 'r', encoding='utf-8', newline='') as infile:
                with open(temp_fd, 'w', encoding='utf-8', newline='') as outfile:
                    reader = csv.reader(infile)
                    writer = csv.writer(outfile)
                    
                    for row_idx, row in enumerate(reader):
                        if row_idx == 0 and target_columns != "all":
                            # For first row, determine which columns to process
                            # This is a simplification - in practice we'd need header detection
                            pass
                        
                        processed_row = []
                        for col_idx, field in enumerate(row):
                            if self._should_process_column(col_idx, target_columns):
                                processed_row.append(field.strip() if trim_spaces else field)
                            else:
                                processed_row.append(field)
                        writer.writerow(processed_row)
            
            return temp_path
            
        except Exception:
            # Cleanup on error
            try:
                Path(temp_path).unlink()
            except OSError:
                pass
            raise
    
    def _should_process_column(self, col_idx: int, target_columns) -> bool:
        """Determine if a column should be processed."""
        if target_columns == "all":
            return True
        if isinstance(target_columns, list):
            return col_idx in target_columns
        return False
```

### Phase 2: Integration with Copy Into System (2-3 days)

#### Task 2.1: Update CopyIntoTranslator
**File**: `src/mockhaus/snowflake/copy_into.py`

**Enhancements**:
```python
class CopyIntoTranslator:
    def __init__(self, stage_manager, format_manager, use_ast_parser=False):
        # ... existing initialization
        self.preprocessing_engine = preprocessing_engine
    
    def _apply_file_format_with_preprocessing(self, duckdb_copy_sql: str, format_result: FormatMappingResult, file_path: str) -> str:
        """Apply file format with preprocessing if needed."""
        if not format_result.preprocessing_steps:
            # No preprocessing needed, use original logic
            return self._apply_file_format_options(duckdb_copy_sql, format_result.options)
        
        # Apply preprocessing to file
        processed_file_path = self.preprocessing_engine.preprocess_file(
            file_path, format_result.preprocessing_steps
        )
        
        # Update SQL to use processed file
        updated_sql = duckdb_copy_sql.replace(f"'{file_path}'", f"'{processed_file_path}'")
        
        # Apply format options (excluding preprocessing metadata)
        clean_options = {k: v for k, v in format_result.options.items() 
                        if not k.startswith('_')}
        
        return self._apply_file_format_options(updated_sql, clean_options)
```

#### Task 2.2: Add Preprocessing Cleanup
**File**: `src/mockhaus/snowflake/copy_into.py`

```python
class CopyIntoTranslator:
    def __init__(self):
        # ... existing init
        self._temp_files: list[str] = []  # Track temp files for cleanup
    
    def execute_copy_operation(self, sql: str, connection) -> dict:
        """Execute COPY operation with cleanup."""
        try:
            result = self._execute_copy_operation_impl(sql, connection)
            return result
        finally:
            # Cleanup temporary preprocessed files
            self._cleanup_temp_files()
    
    def _cleanup_temp_files(self):
        """Clean up temporary preprocessing files."""
        for temp_file in self._temp_files:
            try:
                Path(temp_file).unlink()
            except OSError:
                pass  # File already deleted or inaccessible
        self._temp_files.clear()
```

### Phase 3: Comprehensive Testing (2-3 days)

#### Task 3.1: Unit Tests for Enhanced Features
**File**: `tests/unit/snowflake/file_formats/test_csv_handler_enhanced.py`

```python
import pytest
import tempfile
import csv
from pathlib import Path
from src.mockhaus.snowflake.file_formats.csv import CSVFormatHandler

class TestEnhancedCSVHandler:
    def setup_method(self):
        self.handler = CSVFormatHandler()
    
    def test_multiple_null_if_handling(self):
        """Test handling of multiple NULL_IF values."""
        props = {
            "TYPE": "CSV",
            "NULL_IF": ["", "NULL", "N/A", "\\N"],
            "EMPTY_FIELD_AS_NULL": True
        }
        result = self.handler.map_to_duckdb_options(props)
        
        # Should use canonical NULL string
        assert result.options["nullstr"] == "__MOCKHAUS_NULL__"
        
        # Should require preprocessing
        assert len(result.preprocessing_steps) == 1
        step = result.preprocessing_steps[0]
        assert step["type"] == "null_normalization"
        assert step["null_if_list"] == ["", "NULL", "N/A", "\\N"]
        assert step["empty_as_null"] is True
    
    def test_record_delimiter_variants(self):
        """Test different RECORD_DELIMITER formats."""
        test_cases = [
            ("\\n", "\n"),
            ("\\r\\n", "\r\n"), 
            ("\\r", "\r"),
            ("\n", "\n"),      # Already unescaped
            ("\r\n", "\r\n"),  # Already unescaped
        ]
        
        for input_delim, expected_output in test_cases:
            props = {"RECORD_DELIMITER": input_delim}
            result = self.handler.map_to_duckdb_options(props)
            assert result.options["new_line"] == expected_output
    
    def test_parse_header_with_skip_header(self):
        """Test PARSE_HEADER combined with SKIP_HEADER."""
        props = {
            "SKIP_HEADER": 2,
            "PARSE_HEADER": True
        }
        result = self.handler.map_to_duckdb_options(props)
        
        assert result.options["skip_rows"] == 2
        assert result.options["header"] is True
        
        # Should require header extraction preprocessing
        assert len(result.preprocessing_steps) == 1
        step = result.preprocessing_steps[0]
        assert step["type"] == "header_extraction"
        assert step["skip_rows"] == 2
    
    def test_compression_fallback_handling(self):
        """Test fallback for unsupported compression types."""
        unsupported = ["BZ2", "BROTLI", "ZSTD", "DEFLATE", "RAW_DEFLATE"]
        
        for compression in unsupported:
            props = {"COMPRESSION": compression}
            result = self.handler.map_to_duckdb_options(props)
            
            assert result.options["compression"] == "auto"
            assert len(result.warnings) == 1
            assert compression in result.warnings[0]
    
    def test_encoding_mapping(self):
        """Test encoding mapping for native and extended encodings."""
        # Test native encodings (direct mapping)
        native_test_cases = [
            ("UTF-8", "UTF-8"),
            ("UTF-16", "UTF-16"),
            ("UTF-16BE", "UTF-16"),
            ("UTF-16LE", "UTF-16"),
            ("ISO-8859-1", "Latin-1"),
        ]
        
        for snowflake_encoding, expected_duckdb in native_test_cases:
            props = {"ENCODING": snowflake_encoding}
            result = self.handler.map_to_duckdb_options(props)
            assert result.options["encoding"] == expected_duckdb
            assert len(result.warnings) == 0  # No warnings for native encodings
        
        # Test extended encodings (require extension)
        extended_encodings = ["ISO-8859-2", "WINDOWS-1252", "BIG5", "EUC-JP"]
        
        for encoding in extended_encodings:
            props = {"ENCODING": encoding}
            result = self.handler.map_to_duckdb_options(props)
            
            # Should fallback to UTF-8 with warning
            assert result.options["encoding"] == "UTF-8"
            assert len(result.warnings) == 1
            assert "encodings extension" in result.warnings[0]
    
    def test_trim_space_preprocessing(self):
        """Test TRIM_SPACE preprocessing requirement."""
        props = {"TRIM_SPACE": True}
        result = self.handler.map_to_duckdb_options(props)
        
        # Should require text processing
        assert len(result.preprocessing_steps) == 1
        step = result.preprocessing_steps[0]
        assert step["type"] == "trim_spaces"
        assert step["columns"] == "all"
    
    def test_unsupported_options_handling(self):
        """Test graceful handling of unsupported options."""
        props = {
            "TYPE": "CSV",
            "FIELD_DELIMITER": ",",
            "BINARY_FORMAT": "HEX",              # Unsupported
            "SKIP_BLANK_LINES": True,            # Unsupported
            "VALIDATE_UTF8": False,              # Unsupported
            "ERROR_ON_COLUMN_COUNT_MISMATCH": False,  # Maps to ignore_errors
        }
        result = self.handler.map_to_duckdb_options(props)
        
        # Core options should be mapped
        assert result.options["delimiter"] == ","
        assert result.options["ignore_errors"] is True  # Mapped from ERROR_ON_COLUMN_COUNT_MISMATCH
        
        # Unsupported options should generate warnings
        warning_text = " ".join(result.warnings)
        assert "BINARY_FORMAT" in warning_text
        assert "SKIP_BLANK_LINES" in warning_text
        assert "VALIDATE_UTF8" in warning_text
        
        # Should track ignored options
        assert "BINARY_FORMAT" in result.ignored_options
        assert "SKIP_BLANK_LINES" in result.ignored_options
        assert "VALIDATE_UTF8" in result.ignored_options
```

#### Task 3.2: Preprocessing Tests
**File**: `tests/unit/snowflake/file_formats/test_preprocessing.py`

```python
import pytest
import tempfile
import csv
from pathlib import Path
from src.mockhaus.snowflake.file_formats.preprocessing import preprocessing_engine, PreprocessingStep

class TestPreprocessing:
    def test_null_normalization(self):
        """Test NULL value normalization preprocessing."""
        # Create test CSV with various NULL representations
        test_data = [
            ["id", "name", "email"],
            ["1", "Alice", "alice@test.com"],
            ["2", "", "bob@test.com"],      # Empty field
            ["3", "NULL", ""],              # NULL string and empty
            ["4", "N/A", "charlie@test.com"], # N/A as NULL
            ["5", "\\N", "david@test.com"],  # Escaped NULL
        ]
        
        # Write to temporary file
        temp_fd, temp_path = tempfile.mkstemp(suffix='.csv')
        with open(temp_fd, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(test_data)
        
        try:
            # Apply null normalization
            step = PreprocessingStep(
                step_type="null_normalization",
                parameters={
                    "null_if_list": ["", "NULL", "N/A", "\\N"],
                    "empty_as_null": True,
                    "canonical_null": "__NULL__"
                }
            )
            
            processed_path = preprocessing_engine.preprocess_file(temp_path, [step])
            
            # Read processed file and verify
            with open(processed_path, 'r') as f:
                reader = csv.reader(f)
                processed_data = list(reader)
            
            # Header should be unchanged
            assert processed_data[0] == ["id", "name", "email"]
            
            # Check NULL normalization
            assert processed_data[2][1] == "__NULL__"  # Empty field
            assert processed_data[2][2] == "__NULL__"  # Empty field
            assert processed_data[3][1] == "__NULL__"  # "NULL" string
            assert processed_data[4][1] == "__NULL__"  # "N/A" string
            assert processed_data[5][1] == "__NULL__"  # "\\N" string
            
            # Non-NULL values should be unchanged
            assert processed_data[1][1] == "Alice"
            assert processed_data[4][2] == "charlie@test.com"
            
        finally:
            # Cleanup
            Path(temp_path).unlink()
            if processed_path != temp_path:
                Path(processed_path).unlink()
    
    def test_text_processing_trim_spaces(self):
        """Test TRIM_SPACE text processing."""
        # Create test CSV with spaces
        test_data = [
            ["id", "name", "email"],
            ["1", "  Alice  ", " alice@test.com "],
            ["2", "Bob", "bob@test.com"],
            ["3", "  Charlie", "charlie@test.com  "],
        ]
        
        temp_fd, temp_path = tempfile.mkstemp(suffix='.csv')
        with open(temp_fd, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(test_data)
        
        try:
            step = PreprocessingStep(
                step_type="trim_spaces",
                parameters={
                    "trim_spaces": True,
                    "columns": "all"
                }
            )
            
            processed_path = preprocessing_engine.preprocess_file(temp_path, [step])
            
            with open(processed_path, 'r') as f:
                reader = csv.reader(f)
                processed_data = list(reader)
            
            # Check that spaces are trimmed
            assert processed_data[1][1] == "Alice"      # Trimmed from "  Alice  "
            assert processed_data[1][2] == "alice@test.com"  # Trimmed from " alice@test.com "
            assert processed_data[3][1] == "Charlie"    # Trimmed from "  Charlie"
            assert processed_data[3][2] == "charlie@test.com"  # Trimmed from "charlie@test.com  "
            
            # Already clean values unchanged
            assert processed_data[2][1] == "Bob"
            
        finally:
            Path(temp_path).unlink()
            if processed_path != temp_path:
                Path(processed_path).unlink()
```

#### Task 3.3: Integration Tests
**File**: `tests/integration/test_enhanced_csv_copy_into.py`

```python
def test_csv_copy_with_multiple_null_values():
    """Test COPY INTO with multiple NULL_IF values."""
    # Create test CSV with various NULL representations
    # Test end-to-end COPY INTO with complex NULL handling
    
def test_csv_copy_with_parse_header():
    """Test COPY INTO with PARSE_HEADER=TRUE."""
    # Create CSV with headers after comment lines
    # Test header extraction and column mapping
    
def test_csv_copy_with_trim_space():
    """Test COPY INTO with TRIM_SPACE=TRUE."""
    # Create CSV with spaces around values
    # Verify data is properly trimmed in database

def test_csv_copy_with_unsupported_compression():
    """Test COPY INTO with unsupported compression types."""
    # Test graceful fallback for BROTLI, ZSTD, etc.
    
def test_csv_copy_complex_scenario():
    """Test COPY INTO with multiple advanced features."""
    # Test combination of NULL_IF, TRIM_SPACE, PARSE_HEADER, etc.
```

### Phase 4: Documentation and Final Integration (1-2 days)

#### Task 4.1: Update Documentation
- Update CSV_FORMAT_MAPPING.md with implementation details
- Update COPY_INTO_GUIDE.md with new CSV examples
- Create troubleshooting guide for CSV issues

#### Task 4.2: Performance Testing
- Test preprocessing performance with large CSV files
- Benchmark memory usage during preprocessing
- Optimize preprocessing for common scenarios

#### Task 4.3: Error Handling Enhancement
- Improve error messages for preprocessing failures
- Add recovery mechanisms for partial preprocessing failures
- Enhance logging for debugging preprocessing issues

## Migration Strategy

### Backward Compatibility
- **No Breaking Changes**: All existing CSV functionality remains unchanged
- **Opt-in Enhancement**: New features only activate when corresponding Snowflake options are used
- **Graceful Degradation**: Unsupported options generate warnings but don't fail

### Rollout Plan
1. **Phase 1**: Deploy enhanced handler with backward compatibility
2. **Phase 2**: Enable preprocessing for new format options
3. **Phase 3**: Add comprehensive testing and monitoring
4. **Phase 4**: Optimize based on usage patterns

### Monitoring and Observability
- Track preprocessing step usage and performance
- Monitor temporary file cleanup
- Alert on preprocessing failures or unusual patterns
- Collect metrics on format option usage

## Success Criteria

### Phase 1 Success
- [ ] Enhanced CSV handler supports all new options
- [ ] Preprocessing infrastructure is fully functional
- [ ] NULL normalization and text processing work correctly
- [ ] All existing functionality remains unaffected
- [ ] Comprehensive unit test coverage for new features

### Phase 2 Success
- [ ] CopyIntoTranslator integrates preprocessing seamlessly
- [ ] Temporary file cleanup works reliably
- [ ] Performance impact is minimal (<10% overhead)
- [ ] Error handling and recovery mechanisms work properly

### Phase 3 Success
- [ ] All integration tests pass
- [ ] Complex CSV scenarios work end-to-end
- [ ] Preprocessing performance is acceptable for large files
- [ ] Memory usage is controlled during preprocessing

### Phase 4 Success
- [ ] Documentation is complete and accurate
- [ ] Performance benchmarks meet targets
- [ ] Error handling provides clear user feedback
- [ ] Production monitoring is in place

## Implementation Timeline

- **Phase 1** (Enhanced Core): 3-4 days
- **Phase 2** (Integration): 2-3 days  
- **Phase 3** (Testing): 2-3 days
- **Phase 4** (Documentation & Polish): 1-2 days

**Total Estimate**: 8-12 days

## Risk Mitigation

### Technical Risks
1. **Preprocessing Performance**: Monitor file processing time, implement streaming for large files
2. **Memory Usage**: Use streaming processing to avoid loading entire files in memory
3. **Temporary File Management**: Implement robust cleanup with error handling
4. **Backward Compatibility**: Extensive testing of existing functionality

### Operational Risks
1. **Storage Space**: Monitor temporary file disk usage, implement cleanup policies
2. **Concurrent Processing**: Test handling of multiple simultaneous COPY operations
3. **Error Recovery**: Ensure preprocessing failures don't leave orphaned files

## Future Extensions

### Additional Features (Post-Implementation)
1. **MULTI_LINE Support**: Complex parser for multi-line CSV fields
2. **Custom Encoding**: Support for non-UTF8 encodings
3. **Advanced Error Handling**: Use DuckDB's store_rejects for detailed error reporting
4. **Performance Optimizations**: Streaming preprocessing for very large files
5. **Schema Inference**: Enhanced column type detection and mapping

### Integration Opportunities
1. **Caching**: Cache preprocessing results for repeated operations
2. **Parallel Processing**: Multi-threaded preprocessing for large files
3. **Cloud Integration**: Direct preprocessing of cloud-stored files
4. **Monitoring Dashboard**: Real-time monitoring of CSV processing metrics

This comprehensive implementation plan provides a roadmap for delivering production-ready extended CSV support that maximizes Snowflake compatibility while leveraging DuckDB's advanced capabilities.