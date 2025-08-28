# Snowflake to DuckDB PARQUET Format Options Mapping

## Overview

This document provides a comprehensive mapping between Snowflake PARQUET file format options and their DuckDB equivalents. This mapping is used to implement full PARQUET support in Mockhaus COPY INTO operations.

## Current Implementation Status

**Current State**: Basic PARQUET support exists but with minimal option mapping
- Location: `src/mockhaus/snowflake/file_formats.py:210-214`
- Current implementation only returns `{"FORMAT": "PARQUET"}`
- All Snowflake PARQUET options are ignored

## Snowflake PARQUET Format Options

Based on Snowflake documentation, the supported PARQUET format properties are:

| Property       | Type    | Description                                    | Default | Values                                    |
|----------------|---------|------------------------------------------------|---------|-------------------------------------------|
| TYPE           | String  | Must be 'PARQUET' for Parquet file formats    | N/A     | 'PARQUET'                                |
| COMPRESSION    | String  | Compression algorithm                          | 'AUTO'  | 'AUTO', 'SNAPPY', 'NONE', 'GZIP', 'BROTLI', 'ZSTD', 'LZ4', 'LZO' |
| BINARY_AS_TEXT | Boolean | Treats BINARY fields as TEXT                   | TRUE    | TRUE, FALSE                              |
| NULL_IF        | List    | Strings to interpret as NULL                   | ['\N']  | List of strings                          |
| TRIM_SPACE     | Boolean | Trims leading/trailing spaces in string fields| FALSE   | TRUE, FALSE                              |

## DuckDB PARQUET Options

DuckDB supports the following options for PARQUET format in COPY statements:

| Option                | Type    | Description                                    | Default | Values                                    |
|-----------------------|---------|------------------------------------------------|---------|-------------------------------------------|
| FORMAT                | VARCHAR | Output format                                  | N/A     | 'PARQUET'                                |
| COMPRESSION           | VARCHAR | Compression type                               | 'snappy'| 'uncompressed', 'snappy', 'gzip', 'zstd', 'brotli', 'lz4', 'lz4_raw' |
| COMPRESSION_LEVEL     | BIGINT  | Compression level (zstd only)                  | 3       | 1-22                                     |
| ROW_GROUP_SIZE        | BIGINT  | Number of rows per row group                   | 122880  | Positive integer                         |
| ROW_GROUP_SIZE_BYTES  | BIGINT  | Target row group size in bytes                 | Auto    | Byte count (e.g., '2MB')                 |
| ROW_GROUPS_PER_FILE   | BIGINT  | Number of row groups per file                  | None    | Positive integer                         |
| PARQUET_VERSION       | VARCHAR | Parquet version                                | 'V1'    | 'V1', 'V2'                               |
| FIELD_IDS             | STRUCT  | Field ID per column                            | Empty   | Struct or 'auto'                         |

## Mapping Table

### Direct Mappings

| Snowflake Property | DuckDB Option | Mapping Notes |
|-------------------|---------------|---------------|
| TYPE = 'PARQUET'  | FORMAT = 'PARQUET' | Direct mapping |
| COMPRESSION       | COMPRESSION   | See compression mapping below |
| BINARY_AS_TEXT    | binary_as_string | Direct mapping |

### Compression Mapping

| Snowflake Value | DuckDB Value  | Status | Notes |
|-----------------|---------------|--------|-------|
| 'AUTO'          | 'snappy'      | ✅ Map | Use DuckDB default |
| 'SNAPPY'        | 'snappy'      | ✅ Map | Direct mapping |
| 'NONE'          | 'uncompressed'| ✅ Map | Direct mapping |
| 'GZIP'          | 'gzip'        | ✅ Map | Direct mapping |
| 'BROTLI'        | 'brotli'      | ✅ Map | Direct mapping |
| 'ZSTD'          | 'zstd'        | ✅ Map | Direct mapping |
| 'LZ4'           | 'lz4'         | ✅ Map | Direct mapping |
| 'LZO'           | N/A           | ⚠️ Fallback | Not supported in DuckDB, fallback to 'snappy' with warning |

### Unsupported Options (Accepted with Warnings)

| Snowflake Property | DuckDB Equivalent | Status | Implementation Strategy |
|-------------------|-------------------|--------|-------------------------|
| NULL_IF           | N/A               | ⚠️ Ignored | **Accept & Log**: Accept option, log as unsupported, continue without error. DuckDB handles NULL detection automatically. |
| TRIM_SPACE        | N/A               | ⚠️ Ignored | **Accept & Log**: Accept option, log as unsupported, continue without error. No equivalent in DuckDB COPY. |

### DuckDB-Only Options (Not in Snowflake)

| DuckDB Option        | Description | Implementation Strategy |
|----------------------|-------------|-------------------------|
| COMPRESSION_LEVEL    | zstd compression level | **Extension**: Could be added as Mockhaus extension |
| ROW_GROUP_SIZE       | Rows per row group | **Extension**: Could be added as Mockhaus extension |
| ROW_GROUP_SIZE_BYTES | Target row group size | **Extension**: Could be added as Mockhaus extension |
| ROW_GROUPS_PER_FILE  | Row groups per file | **Extension**: Could be added as Mockhaus extension |
| PARQUET_VERSION      | Parquet format version | **Extension**: Could be added as Mockhaus extension |
| FIELD_IDS            | Field ID mapping | **Extension**: Advanced feature, low priority |

## Implementation Plan

### Phase 1: Core Mapping (Required)

1. **Update `_map_parquet_options()` method** in `file_formats.py`
   - Map Snowflake COMPRESSION to DuckDB COMPRESSION
   - Handle 'AUTO' → 'snappy' mapping
   - Handle 'NONE' → 'uncompressed' mapping
   - Handle 'LZO' unsupported case (warning or error)

2. **Add PARQUET-specific tests**
   - Test all supported compression types
   - Test unsupported compression (LZO)
   - Test default behavior

### Phase 2: Unsupported Option Handling (Optional)

1. **Document limitations clearly**
   - NULL_IF: Document that DuckDB handles NULL detection automatically
   - TRIM_SPACE: Document that this is not supported

2. **Add graceful handling**
   - Log unsupported options as warnings
   - Continue processing without throwing errors
   - Provide informative log messages about what's being ignored

### Phase 3: Extensions (Future)

1. **Add DuckDB-specific extensions**
   - Allow setting ROW_GROUP_SIZE via Mockhaus extensions
   - Allow setting COMPRESSION_LEVEL for zstd
   - Allow setting PARQUET_VERSION

## Example Usage

### Snowflake COPY INTO with PARQUET format
```sql
-- Basic PARQUET
COPY INTO my_table FROM '@stage/data.parquet' 
FILE_FORMAT = (TYPE = 'PARQUET' COMPRESSION = 'SNAPPY')

-- With BINARY_AS_TEXT support and unsupported options
COPY INTO my_table FROM '@stage/data.parquet' 
FILE_FORMAT = (TYPE = 'PARQUET' COMPRESSION = 'SNAPPY' BINARY_AS_TEXT = TRUE NULL_IF = ('', 'NULL'))
```

### Expected DuckDB Translation
```sql
-- Basic PARQUET
COPY my_table FROM 'resolved/path/data.parquet' (FORMAT PARQUET, COMPRESSION 'snappy')

-- With BINARY_AS_TEXT support and unsupported options (accepted gracefully)
COPY my_table FROM 'resolved/path/data.parquet' (FORMAT PARQUET, COMPRESSION 'snappy', binary_as_string true)
-- Log: NULL_IF not supported in DuckDB PARQUET format, ignoring option
-- Log: TRIM_SPACE not supported in DuckDB PARQUET format, ignoring option
```

## Implementation Code Changes

### File: `src/mockhaus/snowflake/file_formats.py`

```python
def _map_parquet_options(self, props: dict[str, Any]) -> dict[str, Any]:
    """Map Parquet format properties to DuckDB options."""
    options = {"FORMAT": "PARQUET"}
    warnings = []
    
    # Handle compression
    compression = props.get("compression", props.get("COMPRESSION", "AUTO"))
    if compression:
        compression_mapping = {
            "AUTO": "snappy",
            "SNAPPY": "snappy", 
            "NONE": "uncompressed",
            "GZIP": "gzip",
            "BROTLI": "brotli",
            "ZSTD": "zstd",
            "LZ4": "lz4",
            "LZO": None  # Not supported
        }
        
        mapped_compression = compression_mapping.get(compression.upper())
        if mapped_compression is None:
            if compression.upper() == "LZO":
                warnings.append(f"LZO compression not supported in DuckDB, using snappy instead")
                options["COMPRESSION"] = "snappy"
            else:
                warnings.append(f"Unknown compression type: {compression}, using snappy")
                options["COMPRESSION"] = "snappy"
        else:
            options["COMPRESSION"] = mapped_compression
    
    # Handle BINARY_AS_TEXT (supported)
    binary_as_text = props.get("BINARY_AS_TEXT", props.get("binary_as_text"))
    if binary_as_text is not None:
        options["binary_as_string"] = binary_as_text
    
    # Handle unsupported options gracefully (accept but log)
    unsupported_options = {
        "NULL_IF": "null_if",
        "TRIM_SPACE": "trim_space"
    }
    
    for snowflake_opt, snake_case_opt in unsupported_options.items():
        if snowflake_opt in props or snake_case_opt in props:
            warnings.append(f"{snowflake_opt} not supported in DuckDB PARQUET format, ignoring option")
    
    # Store warnings for later use
    if warnings:
        options["_warnings"] = warnings
        
    return options
```

## Testing Requirements

### Unit Tests to Add

1. **Basic PARQUET mapping**
   ```python
   def test_parquet_compression_mapping(self):
       # Test all supported compression types
       # Test AUTO → snappy mapping
       # Test NONE → uncompressed mapping
   ```

2. **Unsupported compression handling**
   ```python
   def test_parquet_unsupported_compression(self):
       # Test LZO handling
       # Verify warning generation
   ```

3. **Unsupported options handling**
   ```python
   def test_parquet_unsupported_options(self):
       # Test BINARY_AS_TEXT warning
       # Test NULL_IF warning
       # Test TRIM_SPACE warning
   ```

4. **Integration tests**
   ```python
   def test_copy_into_parquet_end_to_end(self):
       # Test complete COPY INTO flow with PARQUET
   ```

## Documentation Updates Required

1. **Update COPY_INTO_GUIDE.md**
   - Add PARQUET format support section
   - Document supported/unsupported options
   - Add examples

2. **Update README.md**
   - Add PARQUET to supported formats list

## Conclusion

This mapping provides a foundation for full PARQUET support in Mockhaus. The main limitations are:

1. **No DuckDB equivalent** for BINARY_AS_TEXT, NULL_IF, and TRIM_SPACE
2. **LZO compression not supported** by DuckDB
3. **DuckDB has additional options** not available in Snowflake

The implementation should focus on the supported mappings while providing clear warnings for unsupported features.