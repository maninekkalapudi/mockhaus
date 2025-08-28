# Snowflake to DuckDB CSV Format Options Mapping

## Overview

This document provides a comprehensive mapping between Snowflake CSV file format options and their DuckDB equivalents. This mapping is used to implement extended CSV support in Mockhaus COPY INTO operations with maximum compatibility.

## Current Implementation Status

**Current State**: Basic CSV support exists with partial option mapping
- Location: `src/mockhaus/snowflake/file_formats/csv.py`
- **Supported Options**: FIELD_DELIMITER, SKIP_HEADER, FIELD_OPTIONALLY_ENCLOSED_BY, NULL_IF (first value only), DATE_FORMAT, TIMESTAMP_FORMAT
- **Missing Options**: Many advanced CSV options are not yet implemented
- **Limitations**: 
  - NULL_IF only uses first value from list
  - No EMPTY_FIELD_AS_NULL support
  - No TRIM_SPACE support
  - Limited error handling options
  - No multi-line field support

## Comprehensive Snowflake CSV Format Options

Based on Snowflake documentation, the complete set of CSV format properties:

| Property | Type | Description | Default | Values |
|----------|------|-------------|---------|--------|
| TYPE | String | Must be 'CSV' for CSV file formats | N/A | 'CSV' |
| FIELD_DELIMITER | String | Character separating fields | ',' | Any single character |
| RECORD_DELIMITER | String | Character separating records | '\n' | '\n', '\r\n', '\r' |
| SKIP_HEADER | Integer | Number of header lines to skip | 0 | Non-negative integer |
| PARSE_HEADER | Boolean | Use first row headers for column names | FALSE | TRUE, FALSE |
| FIELD_OPTIONALLY_ENCLOSED_BY | String | Character to enclose fields | NONE | '"', "'", or NONE |
| ESCAPE | String | Character to escape other characters | NONE | Any single character or NONE |
| ESCAPE_UNENCLOSED_FIELD | String | Character to escape delimiters in unenclosed fields | '\' | Any single character |
| NULL_IF | List | List of strings to replace with NULL | ['\N'] | List of strings |
| EMPTY_FIELD_AS_NULL | Boolean | Treat empty fields as NULL | TRUE | TRUE, FALSE |
| DATE_FORMAT | String | Format string for dates | 'AUTO' | 'AUTO' or format string |
| TIME_FORMAT | String | Format string for times | 'AUTO' | 'AUTO' or format string |
| TIMESTAMP_FORMAT | String | Format string for timestamps | 'AUTO' | 'AUTO' or format string |
| BINARY_FORMAT | String | Format for binary data | 'HEX' | 'HEX', 'BASE64', 'UTF8' |
| TRIM_SPACE | Boolean | Remove leading/trailing whitespace | FALSE | TRUE, FALSE |
| SKIP_BLANK_LINES | Boolean | Skip blank lines | FALSE | TRUE, FALSE |
| MULTI_LINE | Boolean | Allow multiple lines in fields | FALSE | TRUE, FALSE |
| ERROR_ON_COLUMN_COUNT_MISMATCH | Boolean | Error on column count mismatch | TRUE | TRUE, FALSE |
| VALIDATE_UTF8 | Boolean | Validate UTF-8 encoding | TRUE | TRUE, FALSE |
| COMPRESSION | String | Compression algorithm | 'AUTO' | 'AUTO', 'GZIP', 'BZ2', 'BROTLI', 'ZSTD', 'DEFLATE', 'RAW_DEFLATE', 'NONE' |
| ENCODING | String | Character encoding of source files | 'UTF-8' | 'UTF-8', 'UTF-16', 'UTF-16BE', 'UTF-16LE', 'UTF-32', 'ISO-8859-1' to 'ISO-8859-9', 'WINDOWS-1250' to 'WINDOWS-1255', 'BIG5', 'EUC-JP', 'EUC-KR', 'GB18030', and others |
| FILE_EXTENSION | String | Extension for unloaded files | null | Any string |

## Comprehensive DuckDB CSV Options

DuckDB supports the following options for CSV format in COPY/READ statements:

| Option | Type | Description | Default | Values |
|--------|------|-------------|---------|--------|
| FORMAT | VARCHAR | Output format | N/A | 'CSV' |
| delimiter/delim/sep | VARCHAR | Column separator | ',' | Any string |
| new_line | VARCHAR | Line terminator | '\n' | '\n', '\r\n', '\r' |
| header | BOOLEAN | Whether first row contains column names | true | true, false |
| skip_rows/skip | BIGINT | Number of rows to skip at beginning | 0 | Non-negative integer |
| quote | VARCHAR | Quoting character | '"' | Any single character |
| escape | VARCHAR | Escape character | '"' | Any single character |
| nullstr | VARCHAR | String representing NULL values | '' | Any string |
| dateformat | VARCHAR | Date format string | '' | Format string |
| timestampformat | VARCHAR | Timestamp format string | '' | Format string |
| types/columns | STRUCT | Manual column type specification | {} | Struct of column types |
| all_varchar | BOOLEAN | Disable type detection, keep all as VARCHAR | false | true, false |
| auto_type_candidates | LIST | Types to consider for auto-detection | [standard types] | List of types |
| encoding | VARCHAR | Text encoding | 'UTF-8' | 'UTF-8', 'UTF-16', 'Latin-1' |
| max_line_size | BIGINT | Maximum line size in bytes | 2,097,152 | Positive integer |
| auto_detect | BOOLEAN | Control automatic CSV property detection | true | true, false |
| sample_size | BIGINT | Rows to sample for type detection | 20,480 | -1 for all, or positive integer |
| ignore_errors | BOOLEAN | Skip rows with errors instead of failing | false | true, false |
| store_rejects | BOOLEAN | Store faulty lines in temporary tables | false | true, false |
| rejects_table | VARCHAR | Name of table for faulty lines | 'reject_errors' | Table name |
| rejects_scan | VARCHAR | Name of table for scan info | 'reject_scans' | Table name |
| rejects_limit | BIGINT | Max faulty records to store | 0 | 0 for no limit, or positive integer |
| force_quote | LIST | List of columns to always quote | [] | List of column names |
| prefix | VARCHAR | String to prefix CSV file with | '' | Any string |
| suffix | VARCHAR | String to append as CSV file suffix | '' | Any string |
| union_by_name | BOOLEAN | Unify schemas across files | false | true, false |
| filename | BOOLEAN | Add filename column indicating source | false | true, false |
| compression | VARCHAR | Compression type | 'auto' | 'auto', 'none', 'gzip', 'zstd' |

## Detailed Mapping Analysis

### Direct Mappings (✅ Fully Compatible)

| Snowflake Property | DuckDB Option | Mapping Notes |
|-------------------|---------------|---------------|
| TYPE = 'CSV' | FORMAT = 'CSV' | Direct mapping |
| FIELD_DELIMITER | delimiter/delim/sep | Direct mapping |
| SKIP_HEADER | skip_rows/skip | Direct mapping |
| FIELD_OPTIONALLY_ENCLOSED_BY | quote | Direct mapping |
| ESCAPE | escape | Direct mapping |
| DATE_FORMAT | dateformat | Direct mapping |
| TIME_FORMAT | No direct equivalent | ⚠️ DuckDB only has dateformat and timestampformat |
| TIMESTAMP_FORMAT | timestampformat | Direct mapping |
| ENCODING | encoding | Direct mapping (with caveats) |

### Compression Mapping

| Snowflake Value | DuckDB Value | Status | Notes |
|-----------------|--------------|--------|-------|
| 'AUTO' | 'auto' | ✅ Map | Direct mapping |
| 'NONE' | 'none' | ✅ Map | Direct mapping |
| 'GZIP' | 'gzip' | ✅ Map | Direct mapping |
| 'BZ2', 'BROTLI', 'ZSTD', 'DEFLATE', 'RAW_DEFLATE' | N/A | ⚠️ Fallback | Not supported in DuckDB, fallback to 'auto' with warning |

### Encoding Mapping

| Snowflake Encoding | DuckDB Support | Status | Notes |
|-------------------|----------------|--------|-------|
| 'UTF-8' | 'UTF-8' | ✅ Direct | Native support in both systems |
| 'UTF-16', 'UTF-16BE', 'UTF-16LE' | 'UTF-16' | ✅ Direct | Native support in both systems |
| 'ISO-8859-1' | 'Latin-1' | ✅ Direct | Native support in both systems |
| 'ISO-8859-2' to 'ISO-8859-9' | Via extension | ⚠️ Extension | Requires DuckDB encodings extension |
| 'WINDOWS-1250' to 'WINDOWS-1255' | Via extension | ⚠️ Extension | Requires DuckDB encodings extension |
| 'BIG5', 'EUC-JP', 'EUC-KR', 'GB18030' | Via extension | ⚠️ Extension | Requires DuckDB encodings extension |

**Implementation Strategy for Encoding**:
- **Native Encodings**: Map directly for UTF-8, UTF-16, and Latin-1
- **Extended Encodings**: Check if DuckDB encodings extension is available, fallback to UTF-8 with warning if not
- **Unknown Encodings**: Fallback to UTF-8 with warning

### Complex Mappings (⚠️ Requires Special Handling)

| Snowflake Property | DuckDB Equivalent | Implementation Strategy |
|-------------------|-------------------|-------------------------|
| NULL_IF (list) | nullstr (single string) | **Multi-value Support**: Implement preprocessing to handle multiple NULL strings by replacing all variants with a single canonical NULL string |
| EMPTY_FIELD_AS_NULL | Custom logic | **Preprocessing**: Add logic to convert empty fields to explicit NULL string before DuckDB processing |
| RECORD_DELIMITER | new_line | **Format Translation**: Map common delimiters ('\n', '\r\n', '\r') directly, warn for unusual delimiters |
| PARSE_HEADER | header + custom logic | **Header Extraction**: When true, extract column names from first row and use DuckDB's header=true |
| TRIM_SPACE | Custom logic | **Post-processing**: Implement via DuckDB's TRIM function in generated SQL or preprocessing |
| MULTI_LINE | No direct equivalent | **File Preprocessing**: Requires complex parsing to handle multi-line fields, low priority |
| ENCODING (advanced) | encoding + extension | **Encoding Mapping**: Snowflake has 20+ built-in encodings, DuckDB has 3 native + 1000+ via encodings extension |

### Snowflake-Only Features (❌ No DuckDB Equivalent)

| Snowflake Property | DuckDB Equivalent | Implementation Strategy |
|-------------------|-------------------|-------------------------|
| ESCAPE_UNENCLOSED_FIELD | N/A | **Accept & Log**: Accept option, log as unsupported, use DuckDB's default escape behavior |
| BINARY_FORMAT | N/A | **Accept & Log**: Accept option, log as unsupported, DuckDB handles binary data automatically |
| SKIP_BLANK_LINES | N/A | **Accept & Log**: Accept option, log as unsupported, may add preprocessing in future |
| ERROR_ON_COLUMN_COUNT_MISMATCH | ignore_errors (inverse) | **Inverse Mapping**: Map to DuckDB's ignore_errors option (opposite behavior) |
| VALIDATE_UTF8 | N/A | **Accept & Log**: Accept option, log as handled automatically by DuckDB |
| FILE_EXTENSION | N/A | **Accept & Log**: Export-only feature, not relevant for COPY INTO |

### DuckDB-Only Features (➕ Additional Capabilities)

| DuckDB Option | Description | Implementation Strategy |
|---------------|-------------|-------------------------|
| all_varchar | Force all columns to VARCHAR | **Extension**: Could be added as Mockhaus extension |
| sample_size | Control auto-detection sampling | **Extension**: Could be added as Mockhaus extension |
| ignore_errors | Skip problematic rows | **Extension**: Could be added as Mockhaus extension for better error handling |
| store_rejects | Capture error details | **Extension**: Could be added as Mockhaus extension for debugging |
| max_line_size | Control maximum line length | **Extension**: Could be added as Mockhaus extension |
| union_by_name | Schema unification across files | **Extension**: Could be added as Mockhaus extension |
| filename | Add source file column | **Extension**: Could be added as Mockhaus extension |

## Implementation Plan

### Phase 1: Enhanced Core Mappings (Required)

1. **Improve NULL_IF handling**
   - Support multiple NULL string values via preprocessing
   - Convert all NULL variants to single canonical form

2. **Add EMPTY_FIELD_AS_NULL support**
   - Implement preprocessing to convert empty fields to NULL
   - Add configuration option to control behavior

3. **Enhance RECORD_DELIMITER mapping**
   - Support '\r\n' and '\r' delimiters
   - Add validation for unusual delimiters

4. **Add PARSE_HEADER support**
   - Extract column names from first row when enabled
   - Combine with SKIP_HEADER logic appropriately

5. **Add ENCODING support**
   - Map native encodings (UTF-8, UTF-16, Latin-1) directly
   - Check for DuckDB encodings extension availability
   - Implement fallback to UTF-8 for unsupported encodings

### Phase 2: Advanced Feature Support (Enhanced)

1. **Add TRIM_SPACE support**
   - Implement via SQL post-processing using TRIM functions
   - Add configuration to control per-column trimming

2. **Improve compression mapping**
   - Add fallback handling for unsupported compression types
   - Provide clear warnings for unsupported formats

3. **Add ERROR_ON_COLUMN_COUNT_MISMATCH mapping**
   - Map to DuckDB's ignore_errors option (inverse logic)
   - Provide validation feedback when enabled

### Phase 3: Advanced Features & Extensions (Future)

1. **Add DuckDB-specific extensions**
   - ignore_errors for better error handling
   - store_rejects for debugging
   - sample_size for large file optimization
   - union_by_name for multi-file scenarios

2. **Add MULTI_LINE support**
   - Implement complex multi-line field parsing
   - Requires significant preprocessing logic

## Example Usage Scenarios

### Snowflake CSV with Multiple Features
```sql
-- Complex Snowflake CSV format
COPY INTO my_table FROM '@stage/data.csv'
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_DELIMITER = '|'
    RECORD_DELIMITER = '\r\n'
    SKIP_HEADER = 1
    PARSE_HEADER = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    ESCAPE = '\'
    NULL_IF = ('', 'NULL', 'N/A', '\N')
    EMPTY_FIELD_AS_NULL = TRUE
    TRIM_SPACE = TRUE
    DATE_FORMAT = 'YYYY-MM-DD'
    TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'
    COMPRESSION = 'GZIP'
    ENCODING = 'ISO-8859-1'
)
```

### Expected DuckDB Translation
```sql
-- Preprocessed and translated to DuckDB
COPY my_table FROM 'resolved/path/data.csv' (
    FORMAT CSV,
    delimiter '|',
    new_line '\r\n',
    skip_rows 1,
    header true,
    quote '"',
    escape '\',
    nullstr '__MOCKHAUS_NULL__',  -- Canonical NULL after preprocessing
    dateformat 'YYYY-MM-DD',
    timestampformat 'YYYY-MM-DD HH24:MI:SS',
    compression 'gzip',
    encoding 'Latin-1'  -- Mapped from ISO-8859-1
)

-- Preprocessing applied:
-- 1. Convert all NULL_IF values ('', 'NULL', 'N/A', '\N') to '__MOCKHAUS_NULL__'
-- 2. Convert empty fields to '__MOCKHAUS_NULL__' (EMPTY_FIELD_AS_NULL = TRUE)
-- 3. Apply TRIM to all string fields in post-processing (TRIM_SPACE = TRUE)
-- 4. Extract column names from first row after skipping header (PARSE_HEADER = TRUE)

-- Warnings logged:
-- No warnings - all options successfully mapped or handled
```

### Unsupported Options Handling
```sql
-- Snowflake CSV with unsupported options
COPY INTO my_table FROM '@stage/legacy_data.csv'
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    BINARY_FORMAT = 'HEX'              -- Unsupported
    SKIP_BLANK_LINES = TRUE            -- Unsupported
    VALIDATE_UTF8 = FALSE              -- Unsupported
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE  -- Supported via ignore_errors
    COMPRESSION = 'BROTLI'             -- Unsupported compression
)
```

### Expected DuckDB Translation with Warnings
```sql
-- Translated with graceful degradation
COPY my_table FROM 'resolved/path/legacy_data.csv' (
    FORMAT CSV,
    delimiter ',',
    skip_rows 1,
    ignore_errors true,  -- Mapped from ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    compression 'auto'   -- Fallback from unsupported BROTLI
)

-- Warnings logged:
-- BINARY_FORMAT not supported in DuckDB CSV format, ignoring option
-- SKIP_BLANK_LINES not supported in DuckDB CSV format, ignoring option  
-- VALIDATE_UTF8 not needed in DuckDB (handled automatically), ignoring option
-- BROTLI compression not supported in DuckDB, using 'auto' instead
```

## Implementation Code Structure

### Enhanced CSV Handler

```python
# src/mockhaus/snowflake/file_formats/csv.py (enhanced)

class CSVFormatHandler(BaseFormatHandler):
    """Enhanced handler for CSV format mappings."""
    
    def map_to_duckdb_options(self, properties: dict[str, Any]) -> FormatMappingResult:
        """Map CSV properties to DuckDB options with full feature support."""
        options = {"FORMAT": "CSV"}
        warnings = []
        ignored_options = []
        preprocessing_required = []
        
        # Core field and record delimiters
        self._map_delimiters(properties, options, warnings)
        
        # Header handling (SKIP_HEADER + PARSE_HEADER)
        self._map_header_options(properties, options, preprocessing_required)
        
        # Quote and escape characters
        self._map_quote_escape(properties, options)
        
        # Advanced NULL handling (multiple NULL_IF + EMPTY_FIELD_AS_NULL)
        self._map_null_handling(properties, options, preprocessing_required)
        
        # Date/time formats
        self._map_datetime_formats(properties, options, warnings)
        
        # Text processing (TRIM_SPACE)
        self._map_text_processing(properties, preprocessing_required, warnings)
        
        # Compression mapping
        self._map_compression(properties, options, warnings)
        
        # Unsupported options (graceful handling)
        self._handle_unsupported_options(properties, warnings, ignored_options)
        
        # Advanced error handling
        self._map_error_handling(properties, options)
        
        # Store preprocessing requirements
        if preprocessing_required:
            options["_preprocessing"] = preprocessing_required
        
        return FormatMappingResult(
            options=options,
            warnings=warnings,
            ignored_options=ignored_options
        )
    
    def _map_null_handling(self, props: dict, options: dict, preprocessing: list) -> None:
        """Handle multiple NULL_IF values and EMPTY_FIELD_AS_NULL."""
        null_if = props.get("NULL_IF", props.get("null_if", []))
        empty_as_null = props.get("EMPTY_FIELD_AS_NULL", props.get("empty_field_as_null", True))
        
        if null_if or empty_as_null:
            # Use canonical NULL string for DuckDB
            canonical_null = "__MOCKHAUS_NULL__"
            options["nullstr"] = canonical_null
            
            # Add preprocessing requirement
            preprocessing.append({
                "type": "null_normalization",
                "null_if": null_if if isinstance(null_if, list) else [null_if],
                "empty_as_null": empty_as_null,
                "canonical_null": canonical_null
            })
    
    def _map_text_processing(self, props: dict, preprocessing: list, warnings: list) -> None:
        """Handle TRIM_SPACE via post-processing."""
        trim_space = props.get("TRIM_SPACE", props.get("trim_space", False))
        if trim_space:
            preprocessing.append({
                "type": "trim_spaces",
                "columns": "all"  # Apply to all string columns
            })
    
    # ... other mapping methods
```

## Testing Requirements

### Unit Tests for Enhanced Features

1. **Multi-value NULL_IF handling**
   ```python
   def test_csv_multiple_null_if_values(self):
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
       assert "_preprocessing" in result.options
       null_preprocessing = next(p for p in result.options["_preprocessing"] 
                                if p["type"] == "null_normalization")
       assert null_preprocessing["null_if"] == ["", "NULL", "N/A", "\\N"]
       assert null_preprocessing["empty_as_null"] is True
   ```

2. **PARSE_HEADER handling**
   ```python
   def test_csv_parse_header_with_skip_header(self):
       """Test PARSE_HEADER combined with SKIP_HEADER."""
       props = {
           "TYPE": "CSV",
           "SKIP_HEADER": 2,  # Skip 2 lines
           "PARSE_HEADER": True  # Then use next line as headers
       }
       result = self.handler.map_to_duckdb_options(props)
       
       assert result.options["skip_rows"] == 2
       assert result.options["header"] is True
       
       # Should require header extraction preprocessing
       assert "_preprocessing" in result.options
   ```

3. **RECORD_DELIMITER mapping**
   ```python
   def test_csv_record_delimiter_variants(self):
       """Test different record delimiter formats."""
       test_cases = [
           ("\\n", "\n"),      # Standard newline
           ("\\r\\n", "\r\n"), # Windows CRLF
           ("\\r", "\r"),      # Mac classic CR
       ]
       
       for snowflake_delim, expected_duckdb in test_cases:
           props = {"RECORD_DELIMITER": snowflake_delim}
           result = self.handler.map_to_duckdb_options(props)
           assert result.options["new_line"] == expected_duckdb
   ```

4. **Unsupported compression fallback**
   ```python
   def test_csv_unsupported_compression_fallback(self):
       """Test fallback for unsupported compression types."""
       unsupported_types = ["BZ2", "BROTLI", "ZSTD", "DEFLATE", "RAW_DEFLATE"]
       
       for compression_type in unsupported_types:
           props = {"COMPRESSION": compression_type}
           result = self.handler.map_to_duckdb_options(props)
           
           # Should fallback to auto
           assert result.options["compression"] == "auto"
           
           # Should have warning
           assert len(result.warnings) == 1
           assert compression_type in result.warnings[0]
           assert "not supported" in result.warnings[0]
   ```

### Integration Tests for Complete Workflows

1. **End-to-end CSV processing with preprocessing**
2. **Multi-file CSV ingestion with different formats**
3. **Error handling and recovery scenarios**
4. **Performance testing with large CSV files**

## Success Criteria

### Phase 1 Success
- [ ] Multiple NULL_IF values handled correctly via preprocessing
- [ ] EMPTY_FIELD_AS_NULL implemented and tested
- [ ] RECORD_DELIMITER supports common variants (\n, \r\n, \r)
- [ ] PARSE_HEADER extracts column names correctly
- [ ] All enhanced features have comprehensive test coverage

### Phase 2 Success
- [ ] TRIM_SPACE implemented via post-processing
- [ ] Compression fallback handles all unsupported types gracefully
- [ ] ERROR_ON_COLUMN_COUNT_MISMATCH maps to ignore_errors correctly
- [ ] All unsupported options logged with informative warnings
- [ ] No regression in existing CSV functionality

### Phase 3 Success
- [ ] DuckDB-specific extensions available as Mockhaus features
- [ ] MULTI_LINE support for complex CSV files
- [ ] Performance optimizations for large file processing
- [ ] Advanced error handling and debugging capabilities

## Documentation Updates Required

1. **Update COPY_INTO_GUIDE.md**
   - Add comprehensive CSV format support section
   - Document all supported/unsupported options with examples
   - Add troubleshooting guide for common CSV issues

2. **Update README.md**
   - Highlight enhanced CSV support as key feature
   - Add performance notes for large CSV files

3. **Create CSV_TROUBLESHOOTING.md**
   - Common CSV parsing issues and solutions
   - Guide for handling legacy CSV formats
   - Performance optimization tips

## Conclusion

This enhanced CSV implementation provides comprehensive Snowflake compatibility while leveraging DuckDB's superior CSV processing capabilities. Key benefits:

1. **Maximum Compatibility**: Supports virtually all Snowflake CSV options through direct mapping or preprocessing
2. **Graceful Degradation**: Unsupported features are handled gracefully with informative warnings
3. **Enhanced Capabilities**: DuckDB's advanced features (error handling, auto-detection) improve robustness
4. **Extensible Architecture**: Modular design allows easy addition of new features
5. **Performance Optimized**: Leverages DuckDB's efficient CSV parsing with smart preprocessing

The implementation strategy balances compatibility, performance, and maintainability while providing a clear upgrade path for future enhancements.