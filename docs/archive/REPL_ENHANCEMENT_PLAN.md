# Mockhaus REPL Enhancement Plan

**Status**: Planned for Phase 2+  
**Current**: Basic readline with session persistence  
**Target**: Professional-grade CLI experience  
**Last Updated**: August 1, 2025

## Overview

This document outlines the plan to enhance the Mockhaus REPL client using modern terminal libraries to provide a professional database CLI experience comparable to DuckDB, PostgreSQL psql, and other industry-standard tools.

## Current State vs Target

### Current REPL Features
- ✅ Basic readline support (backspace, arrow keys)
- ✅ Session-based database persistence
- ✅ Multi-line SQL input
- ✅ Command history (terminal-based)
- ✅ Rich formatted output
- ✅ Dynamic prompt with database context

### Target Professional Features
- 🎯 **Persistent history** across sessions (saved to file)
- 🎯 **SQL auto-completion** with keywords and context-aware suggestions
- 🎯 **Syntax highlighting** for SQL input
- 🎯 **Advanced multi-line editing** with proper indentation
- 🎯 **History search** (Ctrl+R) and navigation
- 🎯 **Mouse support** for selection and editing
- 🎯 **Custom key bindings** for common operations
- 🎯 **Table/column completion** from database schema

## Technology Stack Analysis

### Selected Libraries

**Primary Choice: prompt_toolkit + rich**
- **prompt_toolkit**: Input handling, history, completion
- **rich**: Output formatting (already implemented)
- **Rationale**: Battle-tested by IPython, DuckDB Python bindings, psql-like tools

### Alternative Options Considered

| Library | Pros | Cons | Verdict |
|---------|------|------|---------|
| **prompt_toolkit** | Professional-grade, used by IPython/DuckDB, full-featured | Moderate learning curve | ✅ **Recommended** |
| **cmd2** | Full CLI framework, built-in features | Heavy, different architecture | ❌ Overkill |
| **rich only** | Already using, beautiful output | No advanced input features | ❌ Insufficient |
| **Rust/Node.js tools** | Performance, modern | Language change, complexity | ❌ Not worth it |

## Implementation Plan

### Phase 1: Core prompt_toolkit Integration
**Timeline**: 2-3 hours  
**Goal**: Replace basic input with professional terminal interface

#### Dependencies
```toml
# Add to pyproject.toml
dependencies = [
    # ... existing dependencies
    "prompt-toolkit>=3.0.0",
]
```

#### Core Implementation
```python
# client/enhanced_repl.py
from prompt_toolkit import prompt
from prompt_toolkit.history import FileHistory
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.shortcuts import CompleteStyle

class EnhancedMockhausClient(MockhausClient):
    def __init__(self, base_url: str = "http://localhost:8080"):
        super().__init__(base_url)
        
        # Setup persistent history
        self.history = FileHistory('.mockhaus_history')
        
        # SQL keywords for completion
        self.sql_completer = WordCompleter([
            'SELECT', 'FROM', 'WHERE', 'CREATE', 'DATABASE', 'TABLE',
            'INSERT', 'UPDATE', 'DELETE', 'DROP', 'USE', 'SHOW',
            'DATABASES', 'TABLES', 'INT', 'VARCHAR', 'DECIMAL',
            'PRIMARY', 'KEY', 'IF', 'NOT', 'EXISTS', 'COPY', 'INTO'
        ], ignore_case=True)
    
    def get_input(self, base_prompt: str) -> str:
        return prompt(
            base_prompt,
            history=self.history,
            completer=self.sql_completer,
            complete_style=CompleteStyle.READLINE_LIKE,
            multiline=True,
            complete_while_typing=False
        )
```

#### Features Delivered
- ✅ Persistent command history across sessions
- ✅ SQL keyword auto-completion
- ✅ Better multi-line editing
- ✅ History search with Ctrl+R
- ✅ Fallback to basic readline if prompt_toolkit unavailable

### Phase 2: Advanced Features
**Timeline**: 3-4 hours  
**Goal**: Context-aware completion and syntax highlighting

#### SQL Syntax Highlighting
```python
from prompt_toolkit.lexers import PygmentsLexer
from pygments.lexers.sql import SqlLexer

# Add syntax highlighting for SQL input
lexer = PygmentsLexer(SqlLexer)

def get_input(self, base_prompt: str) -> str:
    return prompt(
        base_prompt,
        history=self.history,
        completer=self.smart_completer,
        lexer=lexer,  # SQL syntax highlighting
        complete_style=CompleteStyle.READLINE_LIKE,
        multiline=True
    )
```

#### Context-Aware Completion
```python
class SmartSQLCompleter:
    def __init__(self, client):
        self.client = client
        self.base_keywords = [...]
        self.cached_tables = []
        self.cached_databases = []
    
    def get_completions(self, document, complete_event):
        # Complete database names after "USE"
        # Complete table names after "FROM"
        # Complete column names after "SELECT"
        # Cache and refresh database/table lists
```

#### Features Delivered
- ✅ SQL syntax highlighting as you type
- ✅ Context-aware auto-completion (database names, table names)
- ✅ Smart indentation for multi-line queries
- ✅ Cached schema information for fast completion

### Phase 3: Power User Features
**Timeline**: 2-3 hours  
**Goal**: Professional CLI power features

#### Custom Key Bindings
```python
from prompt_toolkit.key_binding import KeyBindings

bindings = KeyBindings()

@bindings.add('f5')
def show_databases(event):
    """F5: Quick show databases"""
    event.app.current_buffer.insert_text('SHOW DATABASES;')

@bindings.add('f6') 
def show_tables(event):
    """F6: Quick show tables"""
    event.app.current_buffer.insert_text("SELECT name FROM sqlite_master WHERE type='table';")

@bindings.add('c-l')
def clear_screen(event):
    """Ctrl+L: Clear screen"""
    event.app.renderer.clear()
```

#### Query History Management
```python
class QueryHistory:
    def __init__(self):
        self.history_file = '.mockhaus_query_history.json'
        self.queries = []
    
    def add_query(self, sql: str, success: bool, execution_time: float):
        """Add query to persistent history with metadata"""
        
    def search_history(self, pattern: str) -> List[str]:
        """Search previous queries by pattern"""
        
    def get_recent_queries(self, limit: int = 10) -> List[str]:
        """Get recent successful queries"""
```

#### Features Delivered
- ✅ Custom key bindings (F5=SHOW DATABASES, F6=SHOW TABLES, etc.)
- ✅ Query history with metadata (success/failure, timing)
- ✅ Advanced history search and filtering
- ✅ Mouse support for selection and editing
- ✅ Configurable key bindings

### Phase 4: Advanced Integration
**Timeline**: 3-4 hours  
**Goal**: Seamless integration with Mockhaus features

#### Database Schema Completion
```python
class SchemaAwareCompleter:
    def __init__(self, client):
        self.client = client
        self.schema_cache = {}
        self.last_refresh = 0
    
    async def refresh_schema(self):
        """Fetch current database schema for completion"""
        if self.client.current_database:
            # Get table names
            result = await self.client.query("SELECT name FROM sqlite_master WHERE type='table'")
            self.schema_cache['tables'] = [row['name'] for row in result['data']]
            
            # Get column names for each table
            for table in self.schema_cache['tables']:
                columns = await self.client.query(f"PRAGMA table_info({table})")
                self.schema_cache[f'columns_{table}'] = [col['name'] for col in columns['data']]
```

#### Configuration Management
```python
# ~/.mockhaus/config.yml
repl:
  history_file: '.mockhaus_history'
  max_history: 1000
  auto_completion: true
  syntax_highlighting: true
  key_bindings:
    show_databases: 'f5'
    show_tables: 'f6'
    clear_screen: 'ctrl+l'
  theme: 'monokai'
```

#### Features Delivered
- ✅ Real-time schema completion (table names, column names)
- ✅ Configurable settings file
- ✅ Multiple color themes
- ✅ Export/import command history
- ✅ Session management and restoration

## File Structure

```
client/
├── __init__.py
├── repl.py                    # Current basic REPL
├── enhanced_repl.py           # New prompt_toolkit REPL
├── completion/
│   ├── __init__.py
│   ├── sql_completer.py       # SQL keyword completion
│   ├── schema_completer.py    # Database schema completion
│   └── smart_completer.py     # Context-aware completion
├── history/
│   ├── __init__.py
│   ├── file_history.py        # Enhanced history management
│   └── query_history.py       # Query metadata tracking
└── config/
    ├── __init__.py
    ├── settings.py            # Configuration management
    └── keybindings.py         # Custom key binding definitions
```

## Migration Strategy

### Backward Compatibility
- Keep existing `client/repl.py` as fallback
- New enhanced REPL as `client/enhanced_repl.py`
- Auto-detect prompt_toolkit availability
- Graceful degradation to basic REPL

### User Migration
```python
# Automatic fallback system
try:
    from client.enhanced_repl import EnhancedMockhausClient as MockhausClient
    print("🚀 Enhanced REPL loaded (prompt_toolkit)")
except ImportError:
    from client.repl import MockhausClient
    print("📱 Basic REPL loaded (readline)")
```

### Installation Options
```bash
# Basic installation (current)
uv add requests

# Enhanced installation (future)
uv add requests prompt-toolkit pygments

# Or as extras
uv add "mockhaus[enhanced-repl]"
```

## Success Metrics

### User Experience Improvements
- **History Persistence**: Commands saved across sessions
- **Completion Speed**: Sub-100ms completion suggestions
- **Multi-line Editing**: Natural SQL editing experience
- **Error Reduction**: Fewer typos due to auto-completion

### Feature Parity with Professional Tools
- **DuckDB CLI**: Match core editing and history features
- **PostgreSQL psql**: Match completion and navigation
- **Modern CLIs**: Match visual polish and UX

### Performance Targets
- **Startup Time**: <500ms additional overhead
- **Completion Latency**: <100ms for keyword completion
- **Memory Usage**: <50MB additional for enhanced features
- **History File**: <10MB for 10K commands

## Dependencies and Requirements

### Required Dependencies
```toml
# Core enhancement
"prompt-toolkit>=3.0.0"

# Optional enhancements  
"pygments>=2.10.0"      # Syntax highlighting
"pyyaml>=6.0"          # Configuration files
```

### System Requirements
- **Python**: 3.8+ (prompt_toolkit requirement)
- **Terminal**: Any ANSI-compatible terminal
- **OS**: Cross-platform (Windows, macOS, Linux)

### Compatibility
- **Fallback**: Automatic fallback to basic REPL
- **SSH/Remote**: Works over SSH connections
- **CI/CD**: Detects non-interactive environments

## Risk Assessment

### Technical Risks
- **Dependency Management**: Additional dependencies increase complexity
- **Terminal Compatibility**: Some terminals may not support all features
- **Performance**: Enhanced features may impact startup time

### Mitigation Strategies
- **Graceful Degradation**: Automatic fallback to basic REPL
- **Feature Flags**: Allow disabling specific enhancements
- **Testing**: Comprehensive testing across terminal types
- **Documentation**: Clear installation and configuration guides

## Implementation Priority

### Phase 1: Essential (Must Have)
1. ✅ Basic prompt_toolkit integration
2. ✅ Persistent history
3. ✅ SQL keyword completion
4. ✅ Backward compatibility

### Phase 2: Professional (Should Have)
1. Context-aware completion
2. Syntax highlighting
3. Advanced multi-line editing
4. History search and navigation

### Phase 3: Power User (Nice to Have)
1. Custom key bindings
2. Schema-aware completion
3. Configuration management
4. Query history analytics

### Future Enhancements (Could Have)
1. Plugin system for custom completions
2. Integration with external SQL formatters
3. Visual query builder hints
4. Performance profiling integration

## Conclusion

The enhanced REPL will transform Mockhaus from a functional CLI tool into a professional-grade database interface that rivals established tools like DuckDB CLI and PostgreSQL psql. The phased approach ensures we can deliver value incrementally while maintaining backward compatibility.

**Key Benefits:**
- 🚀 **Professional UX**: Match expectations from modern database tools
- ⚡ **Productivity**: Faster query writing with completion and history
- 🛡️ **Reliability**: Fewer errors through auto-completion and validation
- 🎯 **Adoption**: Lower barrier to entry for new users

**Next Steps:**
1. Review and approve this enhancement plan
2. Begin Phase 1 implementation
3. Gather user feedback on enhanced features
4. Iterate and improve based on usage patterns

This enhancement will position Mockhaus as a best-in-class tool for Snowflake development and testing workflows.