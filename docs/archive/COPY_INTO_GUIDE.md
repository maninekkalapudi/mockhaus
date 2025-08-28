# Snowflake COPY INTO Emulation Guide

Mockhaus provides comprehensive emulation of Snowflake's COPY INTO functionality, including full support for stages, file formats, and all standard Snowflake syntax patterns.

## Overview

The COPY INTO emulation translates Snowflake data ingestion commands to equivalent DuckDB operations while maintaining complete compatibility with Snowflake syntax and semantics.

### Supported Features

- ✅ All stage types (USER, INTERNAL, EXTERNAL, TABLE)
- ✅ Named and inline file formats
- ✅ Complete stage reference syntax (@stage, @~/user, @%table)
- ✅ File pattern matching
- ✅ All COPY INTO options (ON_ERROR, FORCE, PURGE, etc.)
- ✅ AST-based parsing with regex fallback
- ✅ Schema validation and error handling

## Stage Management

### Stage Types and Resolution

Mockhaus maps Snowflake stages to local filesystem paths:

```bash
~/.mockhaus/
├── stages/     # Named stages (@my_stage)
├── user/       # User stage (@~/)
├── tables/     # Table stages (@%table_name)
└── external/   # External stages with URLs
    ├── s3/     # S3 URLs
    ├── gcs/    # Google Cloud Storage
    └── azure/  # Azure Blob Storage
```

### Stage Reference Patterns

| Snowflake Pattern | Mockhaus Resolution | Example |
|------------------|-------------------|---------|
| `@my_stage/file.csv` | `~/.mockhaus/stages/my_stage/file.csv` | Named stage |
| `@~/file.csv` | `~/.mockhaus/user/file.csv` | User stage |
| `@%my_table/file.csv` | `~/.mockhaus/tables/my_table/file.csv` | Table stage |
| `@ext_stage/file.csv` | Configured external path | External stage |

### Creating Stages

#### User Stage (Default)
```sql
CREATE STAGE my_user_stage;
-- Creates: ~/.mockhaus/stages/my_user_stage/
```

#### External Stage with File URL
```sql
CREATE STAGE local_data_stage 
URL = 'file:///path/to/data/';
-- Creates: /path/to/data/ (direct mapping)
```

#### External Stage with S3 URL
```sql
CREATE STAGE s3_stage 
URL = 's3://my-bucket/data/';
-- Creates: ~/.mockhaus/external/s3/my-bucket/data/
```

#### Stage with Properties
```sql
CREATE STAGE secure_stage 
URL = 's3://secure-bucket/data/'
CREDENTIALS = (AWS_KEY_ID = 'key' AWS_SECRET_KEY = 'secret');
-- Properties stored in metadata, path resolution unchanged
```

### Stage Operations

```sql
-- List all stages
SELECT * FROM mockhaus_stages;

-- Drop a stage
DROP STAGE my_stage;

-- Validate stage access
-- (Handled automatically during COPY INTO)
```

## File Format Management

### Supported Format Types

- **CSV** - Complete property mapping to DuckDB
- **JSON** - Basic format support
- **PARQUET** - Native DuckDB support
- **AVRO** - Format validation (limited DuckDB support)
- **ORC** - Format validation (limited DuckDB support)

### Default File Formats

Mockhaus creates these default formats automatically:

```sql
-- CSV_DEFAULT
CREATE FILE FORMAT CSV_DEFAULT 
TYPE = 'CSV'
FIELD_DELIMITER = ','
RECORD_DELIMITER = '\n'
SKIP_HEADER = 0
COMPRESSION = 'AUTO';

-- JSON_DEFAULT  
CREATE FILE FORMAT JSON_DEFAULT 
TYPE = 'JSON'
COMPRESSION = 'AUTO';

-- PARQUET_DEFAULT
CREATE FILE FORMAT PARQUET_DEFAULT 
TYPE = 'PARQUET'
COMPRESSION = 'AUTO';
```

### Custom File Formats

#### CSV with Custom Properties
```sql
CREATE FILE FORMAT pipe_delimited_csv
TYPE = 'CSV'
FIELD_DELIMITER = '|'
RECORD_DELIMITER = '\n'
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
NULL_IF = ('NULL', 'N/A', '')
DATE_FORMAT = 'YYYY-MM-DD'
TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS';
```

#### JSON Format
```sql
CREATE FILE FORMAT json_format
TYPE = 'JSON'
COMPRESSION = 'GZIP'
DATE_FORMAT = 'AUTO'
TIME_FORMAT = 'AUTO'
TIMESTAMP_FORMAT = 'AUTO';
```

### Property Mapping to DuckDB

| Snowflake Property | DuckDB Equivalent | Notes |
|-------------------|------------------|-------|
| `FIELD_DELIMITER` | `DELIMITER` | Direct mapping |
| `SKIP_HEADER` | `HEADER` | Converted to boolean |
| `FIELD_OPTIONALLY_ENCLOSED_BY` | `QUOTE` | Only `"` and `'` supported |
| `NULL_IF` | `NULL` | Uses first value in list |
| `DATE_FORMAT` | `DATEFORMAT` | When not 'AUTO' |
| `TIMESTAMP_FORMAT` | `TIMESTAMPFORMAT` | When not 'AUTO' |

## COPY INTO Syntax Support

### Basic Syntax

```sql
COPY INTO table_name 
FROM stage_reference 
[FILE_FORMAT = format_specification]
[OPTIONS];
```

### Complete Examples

#### Using Named File Format
```sql
-- Setup
CREATE TABLE customers (
    id INTEGER,
    name VARCHAR(100),
    email VARCHAR(255),
    created_date DATE
);

CREATE STAGE data_stage URL = 'file:///tmp/data/';

CREATE FILE FORMAT csv_with_header
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1;

-- Copy data
COPY INTO customers 
FROM '@data_stage/customers.csv' 
FILE_FORMAT = 'csv_with_header';
```

#### Using Inline Format
```sql
COPY INTO customers 
FROM '@data_stage/customers.csv' 
FILE_FORMAT = (
    TYPE = 'CSV' 
    FIELD_DELIMITER = ',' 
    SKIP_HEADER = 1
);
```

#### Using User Stage
```sql
-- First, place file in ~/.mockhaus/user/
COPY INTO customers 
FROM '@~/customers.csv' 
FILE_FORMAT = 'csv_with_header';
```

#### Using Table Stage
```sql
-- Uses ~/.mockhaus/tables/customers/
COPY INTO customers 
FROM '@%customers/backup.csv' 
FILE_FORMAT = 'csv_with_header';
```

### COPY INTO Options

#### Error Handling
```sql
COPY INTO customers 
FROM '@data_stage/customers.csv' 
FILE_FORMAT = 'csv_with_header'
ON_ERROR = 'CONTINUE';  -- Options: ABORT (default), CONTINUE, SKIP_FILE
```

#### Force and Purge
```sql
COPY INTO customers 
FROM '@data_stage/customers.csv' 
FILE_FORMAT = 'csv_with_header'
FORCE = TRUE        -- Reload files that were already loaded
PURGE = TRUE;       -- Delete files after successful load (emulated)
```

#### Pattern Matching
```sql
COPY INTO customers 
FROM '@data_stage/' 
PATTERN = '.*customers_[0-9]+\.csv'
FILE_FORMAT = 'csv_with_header';
```

#### Validation Mode
```sql
COPY INTO customers 
FROM '@data_stage/customers.csv' 
FILE_FORMAT = 'csv_with_header'
VALIDATION_MODE = 'RETURN_ERRORS';  -- Validate without loading
```

## Advanced Features

### File Pattern Matching

Mockhaus supports Snowflake-style pattern matching:

```sql
-- Load all CSV files
COPY INTO customers FROM '@data_stage/' 
PATTERN = '.*\.csv' 
FILE_FORMAT = 'csv_with_header';

-- Load files with date pattern
COPY INTO sales FROM '@data_stage/' 
PATTERN = 'sales_202[0-9]_[0-9]{2}\.csv'
FILE_FORMAT = 'csv_with_header';
```

### Multi-File Loading

```sql
-- Load multiple files matching pattern
COPY INTO logs FROM '@log_stage/' 
PATTERN = 'application_.*\.log'
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '|');
```

### Error Handling

Mockhaus defers validation to DuckDB, which provides clear error messages:

```sql
-- DuckDB automatically validates schema compatibility
COPY INTO customers FROM '@data_stage/data.csv'
FILE_FORMAT = 'csv_with_header';

-- DuckDB will throw clear errors for:
-- - Column count mismatches: "Expected 3 columns, got 4"
-- - Data type issues: "Could not convert 'abc' to INTEGER"
-- - Missing files: "No such file or directory"
-- - Format issues: "Invalid CSV format"
```

## Error Handling and Debugging

### Common Error Scenarios

#### File Not Found
```sql
COPY INTO customers FROM '@data_stage/missing.csv' FILE_FORMAT = 'csv_with_header';
-- Error: File not found: ~/.mockhaus/stages/data_stage/missing.csv
-- Check stage 'data_stage' and file path.
```

#### Format Not Found
```sql
COPY INTO customers FROM '@data_stage/customers.csv' FILE_FORMAT = 'missing_format';  
-- Error: File format 'missing_format' not found
```

#### Stage Not Found
```sql
COPY INTO customers FROM '@missing_stage/customers.csv' FILE_FORMAT = 'csv_with_header';
-- Error: Cannot resolve stage reference: @missing_stage/customers.csv
```

#### Schema Mismatch
```sql
-- If file has different columns than table
COPY INTO customers FROM '@data_stage/wrong_schema.csv' FILE_FORMAT = 'csv_with_header';
-- DuckDB Error: "Expected 3 columns, got 4"
-- DuckDB Error: "Could not convert string 'invalid_date' to DATE"
```

### Testing and Debugging

#### Component Validation
```sql
-- Test stage exists
SELECT * FROM mockhaus_stages WHERE name = 'my_stage';

-- Test file format exists
SELECT * FROM mockhaus_file_formats WHERE name = 'my_format';

-- File existence is checked during COPY INTO execution
-- DuckDB will throw "No such file or directory" if file missing
```

#### Validation Mode (Syntax Support)
```sql
-- Validation mode syntax is parsed but deferred to DuckDB
COPY INTO customers 
FROM '@data_stage/customers.csv' 
FILE_FORMAT = 'csv_with_header'
VALIDATION_MODE = 'RETURN_ERRORS';
-- DuckDB will validate the operation when executed
```

## Integration with Testing

### Test Data Setup

```python
# Python test setup example
import tempfile
from pathlib import Path

# Create test data directory
test_data_dir = Path(tempfile.mkdtemp())
csv_data = "id,name,email\n1,Alice,alice@test.com\n2,Bob,bob@test.com"
(test_data_dir / "test.csv").write_text(csv_data)

# Create external stage pointing to test data
executor.execute_snowflake_sql(f"CREATE STAGE test_stage URL = 'file://{test_data_dir}'")

# Use in COPY INTO
executor.execute_snowflake_sql("""
    COPY INTO customers 
    FROM '@test_stage/test.csv' 
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
""")
```

### CI/CD Integration

```yaml
# Example test data structure for CI
test_data/
├── stages/
│   ├── customer_data/
│   │   ├── customers.csv
│   │   └── customers.json
│   └── sales_data/
│       └── sales_2023.csv
└── formats/
    ├── csv_formats.sql
    └── json_formats.sql
```

## Transaction Handling

Mockhaus defers all transaction management to DuckDB's native capabilities:

- **Atomic Operations**: DuckDB COPY statements are inherently atomic
- **Error Rollback**: Failed COPY operations don't partially modify tables
- **Session Consistency**: Changes are immediately visible within the same connection
- **Isolation**: DuckDB handles transaction isolation automatically

```sql
-- DuckDB automatically handles transaction boundaries
COPY INTO customers FROM '@stage/file.csv' FILE_FORMAT = 'csv_format';
-- Either all rows are loaded or none are (on error)
```

For explicit transaction control in tests:
```sql
BEGIN TRANSACTION;
COPY INTO customers FROM '@stage/file1.csv' FILE_FORMAT = 'csv_format';
COPY INTO orders FROM '@stage/file2.csv' FILE_FORMAT = 'csv_format';
COMMIT; -- or ROLLBACK on error
```

## Performance Considerations

Since Mockhaus is designed for testing:

- **File Size**: Optimized for typical test data sizes (< 100MB)
- **Concurrency**: Single-threaded execution suitable for tests
- **Memory**: Uses DuckDB's efficient columnar storage
- **Validation**: Emphasizes correctness over speed
- **Transactions**: DuckDB's native ACID properties ensure data integrity

## Troubleshooting

### Debug Mode

Enable detailed logging to troubleshoot COPY INTO issues:

```python
# Enable AST parser debug mode
executor = MockhausExecutor(use_ast_parser=True)
executor.connect()

# Execute with detailed error reporting
result = executor.execute_snowflake_sql(copy_sql)
if not result.success:
    print(f"Error: {result.error}")
    print(f"Original SQL: {result.original_sql}")
    print(f"Translated SQL: {result.translated_sql}")
```

### Common Resolution Steps

1. **Verify stage exists and path is correct**
2. **Check file format definition and mapping**
3. **Ensure file exists at resolved path**
4. **Validate table schema compatibility**
5. **Test with simpler inline format first**

### File Path Resolution

To debug stage resolution:

```python
stage_manager = executor._ingestion_handler.stage_manager

# Test path resolution
resolved_path = stage_manager.resolve_stage_path("@my_stage/file.csv")
print(f"Resolved to: {resolved_path}")

# Check if accessible
accessible = stage_manager.validate_stage_access("@my_stage/file.csv")
print(f"Accessible: {accessible}")
```

## Focus Areas for COPY INTO Support

Mockhaus focuses on the areas where Snowflake emulation adds value:

### Core Responsibilities
- **Stage Resolution**: Converting `@stage/file.csv` to actual file paths
- **Format Translation**: Mapping Snowflake file format options to DuckDB equivalents  
- **Syntax Parsing**: Understanding all Snowflake COPY INTO syntax variations
- **Path Management**: Organizing stages in the local filesystem

### Deferred to DuckDB
- **Schema Validation**: DuckDB provides excellent error messages for mismatches
- **Data Type Conversion**: DuckDB handles type coercion and validation
- **Transaction Management**: DuckDB's ACID properties ensure data integrity
- **Performance**: DuckDB optimizes the actual data loading

## Extending Support

The modular design allows for easy extension:

- **New file formats**: Add to `FileFormatManager` and create DuckDB mappings
- **New stage types**: Extend `StageManager` with new URL patterns
- **Format mappings**: Add Snowflake→DuckDB option translations

See the source code in `src/mockhaus/snowflake/` for implementation details.