# AST Parsing Implementation Progress

## Overview
This document tracks the progress of refactoring Mockhaus to use sqlglot's AST parsing instead of regex-based parsing for Snowflake-specific SQL statements.

## Current Branch
- **Branch Name**: `snowflake-ast-parsing`
- **Created From**: `master` after merging data ingestion features

## Implementation Strategy
- Keep both regex and AST parsing implementations
- Make parsing method configurable via `use_ast_parser` parameter
- Default to regex parsing for backward compatibility
- AST parsing available as opt-in feature

## Progress Tracker

### ✅ Completed Tasks

1. **Create feature branch for data ingestion implementation** (high)
2. **Implement MockStageManager class for stage management** (high)
3. **Implement MockFileFormatManager class for file format handling** (high)
4. **Create system tables for stages and file formats** (high)
5. **Implement CREATE STAGE statement parsing and execution** (high)
6. **Implement CREATE FILE FORMAT statement parsing and execution** (high)
7. **Implement COPY INTO statement translation and execution** (high)
8. **Test basic data ingestion functionality with various stage types** (high)
9. **Create comprehensive test suite for data ingestion features** (medium)
10. **Refactor code to move Snowflake-specific logic to separate folder** (high)
11. **Create snowflake-ast-parsing branch for AST refactoring** (high)
12. **Implement AST parser for CREATE/DROP STAGE with configurable parsing** (high)
    - Created `src/mockhaus/snowflake/ast_parser.py`
    - Implemented `parse_create_stage()` and `parse_drop_stage()`
    - Added configurable parsing to `SnowflakeIngestionHandler`
    - Created comprehensive tests in `tests/test_ast_parser.py`
    - Created comparison tests in `tests/test_configurable_parsing.py`

13. **Implement AST parser for CREATE/DROP FILE FORMAT** (high)
    - Added `parse_create_file_format()` to ast_parser.py
    - Added `parse_drop_file_format()` to ast_parser.py
    - Updated ingestion handler to use configurable parsing for file formats
    - Added comprehensive tests for FILE FORMAT AST parsing
    - All existing tests continue to pass with both parsing methods

14. **Implement AST parser for COPY INTO** (high)
    - Added `parse_copy_into()` to ast_parser.py
    - Handles named format references: `FILE_FORMAT = 'format_name'`
    - Handles inline format specifications: `FILE_FORMAT = (TYPE = 'CSV' ...)`
    - Supports all major COPY INTO options (ON_ERROR, FORCE, PURGE, PATTERN)
    - Handles quoted stage references and user stage syntax (`@~/`)
    - Added comprehensive tests for COPY INTO AST parsing

### 🚧 In Progress Tasks

15. **Update documentation and examples** (low)
   - Update README with data ingestion examples
   - Document AST vs regex parsing options
   - Add migration guide for users

15. **Add CLI extensions for stage and format management** (medium)
    - ✅ Added `mockhaus stage list` command to list all stages
    - ✅ Added `mockhaus stage show <name>` command to show stage details with file listing
    - ✅ Added `mockhaus format list` command to list all file formats with key properties
    - ✅ Added `mockhaus format show <name>` command to show format details and DuckDB mappings
    - ✅ All commands support `--database` option for persistent storage
    - ✅ Rich formatting with tables, panels, and proper error handling

16. **Integrate COPY INTO AST parsing with copy translator** (medium)
    - ✅ Updated `CopyIntoTranslator` to support configurable parsing method
    - ✅ Added `_parse_copy_into_with_ast()` method using AST parser
    - ✅ Maintained backward compatibility with `_parse_copy_into_with_regex()` method
    - ✅ Updated `SnowflakeIngestionHandler` to pass AST parser preference to copy translator
    - ✅ All existing COPY INTO tests pass with both parsing methods
    - ✅ End-to-end testing verified with CLI: named formats and inline formats both work

### 📋 Pending Tasks

4. **Add performance benchmarks comparing AST vs regex parsing methods** (low)
   - Add performance benchmarks for all statement types
   - Document performance characteristics
   - Compare memory usage and execution time

## Technical Details

### Files Created/Modified

#### New Files
- `src/mockhaus/snowflake/ast_parser.py` - AST parsing implementation
- `tests/test_ast_parser.py` - Unit tests for AST parser
- `tests/test_configurable_parsing.py` - Comparison tests for both parsers

#### Modified Files
- `src/mockhaus/snowflake/ingestion.py` - Added configurable parsing for all statement types
- `src/mockhaus/executor.py` - Added `use_ast_parser` parameter
- `src/mockhaus/cli.py` - Added CLI extensions for stage and format management
- `src/mockhaus/snowflake/copy_into.py` - Integrated AST parsing with copy translator

### How to Use AST vs Regex Parsing

```python
# Default behavior (uses AST parsing)
executor = MockhausExecutor()

# Enable AST parsing explicitly
executor = MockhausExecutor(use_ast_parser=True)

# Use legacy regex parsing
executor = MockhausExecutor(use_ast_parser=False)
```

### AST Parser Advantages

1. **Handles quoted identifiers**: `CREATE STAGE "My Stage With Spaces"`
2. **More robust parsing**: Better error messages and edge case handling
3. **Easier to extend**: Adding new properties is straightforward
4. **Future-proof**: Can leverage sqlglot improvements

### Testing

All tests pass with both parsing methods:
- 34 AST parser unit tests (STAGE, FILE FORMAT, COPY INTO)
- 11 configurable parsing comparison tests
- 20 existing data ingestion tests
- 13 original query translation tests

Total: 78 tests passing

### CLI Extensions

New CLI commands for stage and format management:
```bash
# List stages and formats
mockhaus stage list [-d database]
mockhaus format list [-d database]

# Show detailed information
mockhaus stage show <stage_name> [-d database]
mockhaus format show <format_name> [-d database]
```

Features:
- Rich table formatting with color-coded output
- Shows key properties and creation timestamps
- File listing for stages (up to 10 files with count)
- DuckDB option mapping for formats
- Persistent database support

#### AST Parser Test Coverage
- **CREATE/DROP STAGE**: 10 tests (simple, with URL, quoted identifiers, IF EXISTS)
- **CREATE/DROP FILE FORMAT**: 14 tests (CSV options, JSON, Parquet, quoted identifiers)
- **COPY INTO**: 10 tests (named formats, inline formats, user stages, options)

## Breaking Changes

### v0.2.0 - AST Parsing Default
**AST parsing is now the default parsing method** (changed from regex parsing).

**For users who want to keep using regex parsing:**
```python
# Use this to maintain previous behavior
executor = MockhausExecutor(use_ast_parser=False)
```

**Benefits of AST parsing:**
- Better handling of quoted identifiers and special characters
- More robust parsing with better error messages
- Easier to extend and maintain

**Migration needed:** None, unless you have code that specifically depends on regex parsing behavior.

## Next Steps

1. **Performance benchmarks** (optional)
   - Add performance benchmarks comparing AST vs regex parsing methods
   - Document performance characteristics and memory usage
   - Identify any performance regressions or improvements

2. **Consider deprecating regex parsing** (future version)
   - After AST parsing has been stable as default for several releases
   - Provide migration period before removal
   - Remove regex parsing code to simplify maintenance

## Summary

The AST parsing implementation is now **complete** for all major Snowflake data ingestion statements:

✅ **CREATE/DROP STAGE** - Fully implemented and integrated
✅ **CREATE/DROP FILE FORMAT** - Fully implemented and integrated  
✅ **COPY INTO** - Fully implemented and integrated with copy translator
✅ **CLI Extensions** - Complete stage and format management commands

Additional features completed:
✅ **Configurable Parsing** - Both AST and regex parsing methods available
✅ **CLI Management Tools** - Rich command-line interface for stage and format management
✅ **End-to-End Integration** - All components work together seamlessly
✅ **Comprehensive Testing** - All functionality verified with extensive test coverage

All functionality maintains backward compatibility through configurable parsing, with comprehensive test coverage ensuring both parsing methods work correctly.

**🎉 As of v0.2.0, AST parsing is now the default method, providing users with better parsing capabilities and complete CLI management tools out of the box!**