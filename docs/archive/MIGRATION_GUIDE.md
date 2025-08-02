# Migration Guide

This guide covers important changes and breaking changes in Mockhaus releases.

## v0.2.0 - AST Parsing Default

### Breaking Change: AST Parsing is Now Default

**What Changed:** The default parsing method for Snowflake data ingestion statements has changed from regex-based parsing to AST-based parsing.

**Impact:** 
- Most users will experience improved parsing capabilities with no code changes required
- Users with code that specifically depends on regex parsing behavior may need to explicitly opt out

### Migration Steps

#### If you want to keep using regex parsing:
```python
# Before (automatic regex parsing)
executor = MockhausExecutor()

# After (explicit regex parsing)
executor = MockhausExecutor(use_ast_parser=False)
```

#### If you want to use AST parsing (new default):
```python
# Before (explicit AST parsing)
executor = MockhausExecutor(use_ast_parser=True)

# Now (automatic AST parsing - no change needed)
executor = MockhausExecutor()
```

### Benefits of AST Parsing (New Default)

1. **Better Quoted Identifier Support**
   ```sql
   -- These now work correctly by default:
   CREATE STAGE "My Stage With Spaces" URL = 's3://bucket/path'
   CREATE FILE FORMAT "Custom CSV Format" TYPE = 'CSV'
   ```

2. **More Robust Parsing**
   - Better error messages for invalid syntax
   - Handles edge cases more reliably
   - More consistent parsing behavior

3. **Enhanced Features**
   - Complex inline format specifications
   - Better handling of special characters
   - Improved support for nested properties

### When to Use Regex Parsing

You may want to explicitly use regex parsing (`use_ast_parser=False`) if:

- You have existing code that depends on specific regex parsing behavior
- You encounter any compatibility issues with AST parsing
- You're in a performance-critical scenario where parsing speed is crucial

### Testing Your Migration

Run your existing tests with the new default to ensure compatibility:

```python
import unittest
from mockhaus import MockhausExecutor

class TestMigration(unittest.TestCase):
    def test_existing_functionality(self):
        # This now uses AST parsing by default
        executor = MockhausExecutor()
        executor.connect()
        
        # Test your existing ingestion statements
        result = executor.execute_snowflake_sql("CREATE STAGE my_stage")
        self.assertTrue(result.success)
        
        # If any issues, try with regex parsing:
        # executor = MockhausExecutor(use_ast_parser=False)
```

### Rollback Plan

If you encounter issues with AST parsing, you can quickly rollback by explicitly setting `use_ast_parser=False`:

```python
# Temporary rollback to regex parsing
executor = MockhausExecutor(use_ast_parser=False)
```

### Getting Help

If you encounter any issues with the migration:

1. First try explicitly using regex parsing: `MockhausExecutor(use_ast_parser=False)`
2. Check the [AST Parsing Progress documentation](AST_PARSING_PROGRESS.md) for known limitations
3. Open an issue on GitHub with details about your specific use case

## Future Changes

### Planned for v0.3.0+
- Performance benchmarks for both parsing methods
- Potential deprecation warnings for regex parsing
- Additional AST parsing features

### Long-term (v1.0+)
- Possible removal of regex parsing (with appropriate migration period)
- AST parsing optimizations