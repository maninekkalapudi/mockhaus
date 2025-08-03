# COPY Command Usage Guide

The COPY command in Mockhaus provides complete emulation of Snowflake's data ingestion functionality, allowing you to load data from files into tables using familiar Snowflake syntax.

## Quick Start

```sql
-- Create a table
CREATE TABLE customers (
    id INTEGER,
    name VARCHAR(100),
    email VARCHAR(255)
);

-- Load data from a CSV file
COPY INTO customers
FROM '@~/customers.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);
```

## Stage Types

Mockhaus supports all Snowflake stage types, mapping them to your local filesystem:

### User Stage (@~/)
Your personal stage for ad-hoc file uploads.
```sql
-- Maps to: ~/.mockhaus/user/
COPY INTO customers FROM '@~/data.csv' FILE_FORMAT = 'CSV_DEFAULT';
```

### Table Stage (@%table)
Automatic stage associated with each table.
```sql  
-- Maps to: ~/.mockhaus/tables/customers/
COPY INTO customers FROM '@%customers/backup.csv' FILE_FORMAT = 'CSV_DEFAULT';
```

### Named Stages (@stage_name)
Custom stages you create for specific purposes.
```sql
-- Create a named stage
CREATE STAGE data_imports URL = 'file:///tmp/data/';

-- Use the stage
COPY INTO customers FROM '@data_imports/customers.csv' FILE_FORMAT = 'CSV_DEFAULT';
```

### External Stages
Stages pointing to external locations (mapped locally for testing).
```sql
-- S3-style stage (maps to ~/.mockhaus/external/s3/bucket/path/)
CREATE STAGE s3_stage URL = 's3://my-bucket/data/';

-- Local file stage (maps directly to the path)
CREATE STAGE local_stage URL = 'file:///path/to/data/';
```

## File Formats

### Built-in Formats
Mockhaus provides default formats for common file types:
- `CSV_DEFAULT` - Standard comma-delimited CSV
- `JSON_DEFAULT` - Line-delimited JSON
- `PARQUET_DEFAULT` - Parquet files

### Custom CSV Formats
```sql
CREATE FILE FORMAT my_csv_format
TYPE = 'CSV'
FIELD_DELIMITER = '|'
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
NULL_IF = ('NULL', 'N/A', '');
```

### Inline Formats
Specify format options directly in the COPY command:
```sql
COPY INTO customers
FROM '@~/data.csv'
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
);
```

## Pattern Matching

Load multiple files matching a pattern:
```sql
-- Load all CSV files starting with "customers_"
COPY INTO customers
FROM '@data_stage/'
PATTERN = 'customers_*.csv'
FILE_FORMAT = 'CSV_DEFAULT';

-- Load files matching date pattern
COPY INTO sales
FROM '@data_stage/'
PATTERN = 'sales_202[0-9]_[0-9]{2}.csv'
FILE_FORMAT = 'CSV_DEFAULT';
```

## Complete Syntax

```sql
COPY INTO <table_name>
FROM '<stage_reference>[/path]'
[FILE_FORMAT = ('<format_name>' | (<inline_options>))]
[PATTERN = '<glob_pattern>']
[ON_ERROR = 'CONTINUE' | 'ABORT' | 'SKIP_FILE']
[FORCE = TRUE | FALSE]  
[PURGE = TRUE | FALSE]
[VALIDATION_MODE = 'RETURN_ERRORS']
```

## Common Examples

### Load CSV with Header
```sql
COPY INTO employees
FROM '@~/employees.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);
```

### Load Pipe-Delimited File
```sql
COPY INTO products  
FROM '@data_stage/products.txt'
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_DELIMITER = '|'
    SKIP_HEADER = 1
);
```

### Load JSON Lines File
```sql
COPY INTO events
FROM '@~/events.jsonl'
FILE_FORMAT = 'JSON_DEFAULT';
```

### Load with Error Handling
```sql
COPY INTO customers
FROM '@data_stage/customers.csv'
FILE_FORMAT = 'CSV_DEFAULT'
ON_ERROR = 'CONTINUE';
```

### Load Multiple Files
```sql
-- Load all customer files from 2023
COPY INTO customers_2023
FROM '@archive_stage/'
PATTERN = 'customers_2023_*.csv'
FILE_FORMAT = 'CSV_DEFAULT';
```

## File Preparation

### Setting Up Your Data

1. **User Stage Files**: Place files in `~/.mockhaus/user/`
   ```bash
   mkdir -p ~/.mockhaus/user
   cp my_data.csv ~/.mockhaus/user/
   ```

2. **Table Stage Files**: Place files in `~/.mockhaus/tables/[table_name]/`
   ```bash
   mkdir -p ~/.mockhaus/tables/customers
   cp customer_data.csv ~/.mockhaus/tables/customers/
   ```

3. **Named Stage Files**: Create stage pointing to your data directory
   ```sql
   CREATE STAGE my_data URL = 'file:///path/to/my/data/';
   ```

### Supported File Types
- **CSV**: Comma, pipe, tab, or custom delimited
- **JSON**: Line-delimited JSON (JSONL)
- **Parquet**: Columnar format files
- **Compression**: Gzip, Bzip2 (auto-detected)

## Error Handling

### Common Errors and Solutions

**File Not Found**
```
Error: File not found: ~/.mockhaus/user/data.csv
```
- Verify file exists at the expected path
- Check stage configuration
- Ensure file permissions allow reading

**Format Mismatch**
```  
Error: Expected 3 columns, got 4
```
- Verify CSV structure matches table schema
- Check if file has header row (use SKIP_HEADER = 1)
- Ensure delimiter matches file format

**Stage Not Found**
```
Error: Cannot resolve stage reference: @my_stage/file.csv
```
- Create the stage first: `CREATE STAGE my_stage`
- Verify stage name spelling
- Check if stage was dropped

### Validation Mode
Test your COPY command without loading data:
```sql
COPY INTO customers
FROM '@~/test_data.csv'
FILE_FORMAT = 'CSV_DEFAULT'
VALIDATION_MODE = 'RETURN_ERRORS';
```

## Performance Tips

### For Testing Workflows
- Use smaller sample files (< 100MB) for faster iteration
- Test with single files before using patterns
- Validate data format with a few rows first

### File Organization
```
~/.mockhaus/
├── user/              # Personal files (@~/)
├── stages/            # Named stages (@stage_name/)
├── tables/            # Table stages (@%table/)
└── external/          # External stage mappings
    ├── s3/           # S3 URLs
    ├── gcs/          # Google Cloud
    └── azure/        # Azure Blob
```

## Integration Examples

### CI/CD Pipeline
```sql
-- Load test data in CI
CREATE STAGE test_data URL = 'file:///github/workspace/test_data/';
COPY INTO test_customers FROM '@test_data/customers.csv' FILE_FORMAT = 'CSV_DEFAULT';
```

### Development Workflow
```sql
-- Quick data loading for development
COPY INTO dev_table FROM '@~/sample_data.csv' 
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 ON_ERROR = 'CONTINUE');
```

### Data Migration Testing
```sql
-- Test migration scripts
CREATE STAGE migration_stage URL = 's3://migration-bucket/data/';
COPY INTO target_table FROM '@migration_stage/' 
PATTERN = 'batch_*.csv' FILE_FORMAT = 'migration_csv_format';
```

## Differences from Snowflake

While Mockhaus provides comprehensive COPY INTO emulation, there are some differences optimized for testing:

### Simplified Areas
- **Transactions**: Handled automatically by DuckDB
- **Performance**: Optimized for test data sizes, not production scale  
- **Compression**: Auto-detected, manual specification not required
- **Encoding**: UTF-8 assumed, encoding options not needed

### Enhanced for Testing
- **Local File Access**: Direct file:// URL mapping
- **Flexible Stages**: Easy setup without cloud credentials
- **Clear Error Messages**: DuckDB provides excellent debugging info
- **Fast Iteration**: Immediate feedback for testing workflows

## Troubleshooting

### Debug Mode
Enable detailed logging for troubleshooting:
```python
executor = MockhausExecutor(use_ast_parser=True)
result = executor.execute_snowflake_sql(copy_sql)
if not result.success:
    print(f"Error: {result.error}")
    print(f"Translated SQL: {result.translated_sql}")
```

### Common Solutions
1. **Check file paths**: Use absolute paths when in doubt
2. **Verify permissions**: Ensure Mockhaus can read your files  
3. **Test formats**: Start with CSV_DEFAULT for simplicity
4. **Use patterns carefully**: Test pattern matching with simple globs first
5. **Check schemas**: Ensure table columns match file structure

## Next Steps

- See [docs/COPY_INTO_GUIDE.md](docs/COPY_INTO_GUIDE.md) for implementation details
- Check [tests/unit/snowflake/test_ingestion.py](tests/unit/snowflake/test_ingestion.py) for code examples
- Run integration tests to see comprehensive usage patterns

Happy data loading! 🚀