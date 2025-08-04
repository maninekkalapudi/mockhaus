# PARQUET Test Fixtures

This directory contains real PARQUET test files used for integration testing of Mockhaus PARQUET format support.

## Test Files

The test files are generated using `generate_test_files.py` and include three datasets with various compression formats:

### Datasets

1. **`basic_*.parquet`** - Basic employee data with common data types
   - 5 rows, 6 columns: id, name, age, salary, is_active, department
   - Data types: INTEGER, VARCHAR, INTEGER, DOUBLE, BOOLEAN, VARCHAR

2. **`with_nulls_*.parquet`** - Employee data with NULL values for testing NULL handling
   - 5 rows, 5 columns: id, name, age, salary, is_active
   - Contains NULL values in various columns to test NULL handling

3. **`with_binary_*.parquet`** - Data with binary fields for testing BINARY_AS_TEXT option
   - 3 rows, 4 columns: id, name, binary_data, text_data
   - Contains binary data column to test `binary_as_string` mapping

### Compression Formats

Each dataset is available in the following compression formats:

- **`*_none.parquet`** - Uncompressed PARQUET files
- **`*_snappy.parquet`** - Snappy compression (DuckDB default)
- **`*_gzip.parquet`** - GZIP compression
- **`*_brotli.parquet`** - Brotli compression
- **`*_lz4.parquet`** - LZ4 compression
- **`*_zstd.parquet`** - ZSTD compression

## Usage in Tests

These files are used by `tests/integration/test_parquet_copy_into_real.py` to test:

- ✅ Real PARQUET file loading with actual data validation
- ✅ All supported compression formats
- ✅ NULL value handling
- ✅ BINARY_AS_TEXT option mapping
- ✅ Inline format specifications
- ✅ Graceful handling of unsupported options
- ✅ LZO compression fallback
- ✅ AUTO compression detection

## Regenerating Test Files

To regenerate the test files (e.g., after schema changes):

```bash
cd tests/fixtures/parquet
uv run python generate_test_files.py
```

This will recreate all 18 test files with the latest schema and data.

## File Sizes

The files are kept small for testing purposes:
- Basic files: ~5KB each
- With nulls files: ~4.5KB each  
- With binary files: ~3KB each

## Verification

Each integration test verifies:
1. **Data Loading Success** - Files load without errors
2. **Row Count Accuracy** - Correct number of rows loaded
3. **Data Integrity** - All values match expected data
4. **Type Preservation** - Data types correctly maintained
5. **Compression Handling** - Format-specific compression options work