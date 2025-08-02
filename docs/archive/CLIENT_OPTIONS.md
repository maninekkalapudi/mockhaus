# Mockhaus Client Options

**Status**: Phase 1 Complete  
**Last Updated**: August 1, 2025

## Overview

This document outlines various client options for interacting with the Mockhaus HTTP server. Mockhaus provides a REST API that can be accessed through multiple client interfaces, from simple command-line tools to full-featured interactive REPLs.

## Available Client Options

### Option 1: Python REPL Client (Implemented ✅)

**Location**: `client/repl.py`  
**Status**: ✅ Complete and ready to use

Interactive Python-based REPL that provides a SQL shell experience for Mockhaus.

#### Features
- **Interactive SQL execution** with formatted table output
- **Health monitoring** and server status checks
- **Error handling** with clear error messages
- **Connection testing** on startup
- **Command history** (via terminal)
- **Configurable server URL** via environment variables

#### Usage
```bash
# Start server
uv run mockhaus serve --host localhost --port 8080

# Install client dependency
uv add requests

# Run REPL client
python client/repl.py
```

#### Example Session
```
🏠 Mockhaus Interactive Client
mockhaus> SELECT * FROM sample_customers LIMIT 3
customer_id | customer_name | account_balance
-----------|--------------|-----------------
          1 |         Alice |          1500.0
          2 |           Bob |          2300.0
          3 |       Charlie |           850.0

✅ 3 rows in 0.045s

mockhaus> CREATE STAGE my_stage URL = 's3://bucket/data/'
✅ Query executed successfully

mockhaus> health
✅ Server healthy - uptime: 5.2 minutes
```

### Option 2: Rich Python Client (Planned)

**Location**: `client/rich_repl.py`  
**Status**: 📋 Planned for Phase 2+

Enhanced version with beautiful terminal formatting using the Rich library.

#### Additional Features (vs Option 1)
- **Syntax highlighting** for SQL queries
- **Rich table formatting** with colors and borders
- **Progress bars** for long-running queries
- **Better error displays** with panels and colors
- **Auto-completion** for SQL keywords
- **Query history browser**

#### Dependencies
```toml
rich = ">=13.0.0"
prompt-toolkit = ">=3.0.0"  # For auto-completion
```

### Option 3: Web Client (Planned)

**Location**: `client/web/`  
**Status**: 📋 Planned for Phase 3+

Browser-based SQL client with modern web interface.

#### Features
- **Web-based SQL editor** with syntax highlighting
- **Result visualization** with charts and graphs
- **Query history** with save/load functionality
- **Multiple database connections**
- **Export results** to CSV, JSON, Excel
- **Real-time query execution status**

#### Technology Stack
- Frontend: React/Vue.js + Monaco Editor
- Build: Vite/Webpack
- Styling: Tailwind CSS or Material-UI

### Option 4: Command-Line Tools

**Status**: ✅ Available now

Simple command-line tools for scripting and automation.

#### HTTPie
```bash
# Install
pip install httpie

# Create alias
alias mq='http POST localhost:8080/api/v1/query sql='

# Use
mq "SELECT COUNT(*) FROM sample_customers"
```

#### curl with alias
```bash
# Add to .bashrc/.zshrc
alias mockhaus-query='curl -s -X POST http://localhost:8080/api/v1/query -H "Content-Type: application/json" -d'

# Use
mockhaus-query '{"sql": "SELECT * FROM sample_customers LIMIT 5"}'
```

#### Custom bash script
```bash
#!/bin/bash
# mockhaus-cli.sh
SQL="$1"
curl -s -X POST http://localhost:8080/api/v1/query \
  -H "Content-Type: application/json" \
  -d "{\"sql\": \"$SQL\"}" | jq '.'
```

### Option 5: Programming Language Clients

#### Python Library
```python
# client/python_sdk.py
import requests
from typing import Optional, Dict, Any

class Mockhaus:
    def __init__(self, base_url: str = "http://localhost:8080"):
        self.base_url = base_url
        self.session = requests.Session()
    
    def query(self, sql: str, database: Optional[str] = None) -> Dict[str, Any]:
        response = self.session.post(
            f"{self.base_url}/api/v1/query",
            json={"sql": sql, "database": database}
        )
        response.raise_for_status()
        return response.json()
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.session.close()

# Usage
with Mockhaus() as client:
    result = client.query("SELECT COUNT(*) FROM sample_customers")
    print(result['data'])
```

#### JavaScript/Node.js Client
```javascript
// client/js/mockhaus.js
class MockhausClient {
    constructor(baseUrl = 'http://localhost:8080') {
        this.baseUrl = baseUrl;
    }
    
    async query(sql, database = null) {
        const response = await fetch(`${this.baseUrl}/api/v1/query`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ sql, database })
        });
        return response.json();
    }
    
    async health() {
        const response = await fetch(`${this.baseUrl}/api/v1/health`);
        return response.json();
    }
}

// Usage
const client = new MockhausClient();
const result = await client.query('SELECT * FROM sample_customers LIMIT 5');
console.table(result.data);
```

## Client Comparison Matrix

| Feature | Python REPL | Rich REPL | Web Client | HTTPie | Programming SDK |
|---------|-------------|-----------|------------|---------|-----------------|
| **Ease of Use** | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐ |
| **Interactive** | ✅ | ✅ | ✅ | ❌ | ❌ |
| **Formatting** | Basic | Rich | Rich | JSON | Raw |
| **Syntax Highlighting** | ❌ | ✅ | ✅ | ❌ | ❌ |
| **Auto-completion** | ❌ | ✅ | ✅ | ❌ | IDE-dependent |
| **Query History** | Terminal | Built-in | Built-in | Terminal | ❌ |
| **Scriptable** | ⭐⭐ | ⭐⭐ | ❌ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| **Dependencies** | requests | rich, prompt-toolkit | Browser | httpie | language-specific |
| **Installation** | Simple | Medium | None | Simple | Simple |

## Implementation Priority

### Phase 1 ✅ (Complete)
- [x] Basic Python REPL client
- [x] HTTPie/curl examples
- [x] Documentation

### Phase 2 (Optional)
- [ ] Rich Python REPL with enhanced formatting
- [ ] Auto-completion and syntax highlighting
- [ ] Query history persistence

### Phase 3 (Optional)
- [ ] Web-based client
- [ ] Programming language SDKs
- [ ] Advanced visualization features

## Usage Recommendations

### For Development & Testing
**Recommended**: Python REPL Client (`client/repl.py`)
- Interactive SQL development
- Quick testing and debugging
- Learning Snowflake SQL syntax

### For Automation & Scripts
**Recommended**: HTTPie or curl aliases
- CI/CD pipeline integration
- Automated testing scripts
- Batch query execution

### For Applications
**Recommended**: Programming language SDKs
- Embed Mockhaus in applications
- Programmatic query execution
- Error handling and retries

### For End Users
**Recommended**: Web Client (when available)
- User-friendly interface
- Visual query building
- Result visualization

## Getting Started

1. **Start Mockhaus server:**
   ```bash
   uv run mockhaus serve --host localhost --port 8080
   ```

2. **Choose your client:**
   ```bash
   # Python REPL (recommended)
   uv add requests
   python client/repl.py
   
   # HTTPie (for scripting)
   pip install httpie
   http POST localhost:8080/api/v1/query sql="SELECT 1"
   
   # curl (universal)
   curl -X POST http://localhost:8080/api/v1/query \
     -H "Content-Type: application/json" \
     -d '{"sql": "SELECT 1"}'
   ```

3. **Test the connection:**
   ```sql
   SELECT * FROM sample_customers LIMIT 5;
   ```

## Contributing

To add new client options:

1. Create client in appropriate subdirectory: `client/[language]/` or `client/[type]/`
2. Follow the existing API patterns from `client/repl.py`
3. Add documentation to this file
4. Add usage examples and tests
5. Update the comparison matrix

All clients should implement at minimum:
- Query execution (`POST /api/v1/query`)
- Health checks (`GET /api/v1/health`)
- Error handling
- Basic result formatting