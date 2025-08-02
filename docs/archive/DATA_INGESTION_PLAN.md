# Mockhaus Data Ingestion Mocking - High Level Plan

## 📋 **Executive Summary**

This plan outlines how to mock Snowflake's data ingestion pipeline (`COPY INTO`) by simulating stages, file formats, and the copy process using local file system operations and DuckDB's native data loading capabilities.

## 🎯 **Core Challenge**

Snowflake's data ingestion involves three key concepts that don't exist in DuckDB:
1. **Stages** - Named locations pointing to cloud storage or local directories
2. **File Formats** - Definitions for parsing CSV, JSON, Parquet, etc.
3. **COPY INTO** - Bulk data loading from stages using file formats

## 🏗️ **Proposed Architecture**

### **1. Stage Management System**

```
MockStageManager
├── Internal Stage (@%table_name/path) → Local temp directories
├── Named Stage (@my_stage/file.csv) → Local directories  
├── External Stage (@s3_bucket/path) → Mock S3-like local directories
└── User Stage (@~/file.csv) → User home directory simulation
```

**Implementation Approach:**
- **Stage Registry**: In-memory dictionary mapping stage names to local paths
- **Stage Types**: Support internal (`@%table`), named (`@stage`), and user (`@~`) stages
- **Path Translation**: Convert Snowflake stage paths to local file system paths
- **Auto-creation**: Automatically create local directories when stages are created

### **2. File Format Management System**

```
MockFileFormatManager
├── Built-in Formats (CSV, JSON, PARQUET, etc.)
├── Custom Formats (user-defined parsing rules)
├── Format Properties (delimiter, skip_header, date_format, etc.)
└── Format Validation (ensure compatibility with DuckDB)
```

**Implementation Approach:**
- **Format Registry**: Store file format definitions in memory/database
- **Property Mapping**: Map Snowflake format properties to DuckDB equivalents
- **Default Formats**: Pre-define common formats (CSV, JSON, PARQUET)
- **Validation**: Ensure format properties are compatible with DuckDB's capabilities

### **3. COPY INTO Translation Engine**

```
CopyIntoTranslator
├── Parse COPY INTO syntax
├── Resolve stage and file paths  
├── Apply file format rules
├── Generate DuckDB COPY/INSERT statements
└── Handle error reporting and validation
```

## 📊 **Detailed Implementation Plan**

### **Phase 1: Stage Infrastructure**

#### **1.1 Stage Creation (CREATE STAGE)**
```sql
-- Snowflake
CREATE STAGE my_csv_stage 
  URL = 's3://mybucket/data/' 
  CREDENTIALS = (AWS_KEY_ID='...' AWS_SECRET_KEY='...');

-- Mockhaus Translation
-- Creates local directory: ~/.mockhaus/stages/my_csv_stage/
-- Stores stage metadata in DuckDB system table
```

**Implementation:**
- Intercept `CREATE STAGE` statements
- Create corresponding local directory structure
- Store stage metadata in `mockhaus_stages` system table
- Support stage properties (URL, credentials, etc.) as metadata only

#### **1.2 Stage Types Mapping**
```
Snowflake Stage → Mockhaus Local Path
@my_stage/file.csv → ~/.mockhaus/stages/my_stage/file.csv
@%table_name/file.csv → ~/.mockhaus/tables/table_name/file.csv  
@~/file.csv → ~/.mockhaus/user/file.csv
@s3://bucket/file.csv → ~/.mockhaus/external/s3/bucket/file.csv
```

### **Phase 2: File Format System**

#### **2.1 File Format Creation (CREATE FILE FORMAT)**
```sql
-- Snowflake
CREATE FILE FORMAT my_csv_format
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  NULL_IF = ('NULL', 'null', '');

-- Mockhaus Storage
-- Store in mockhaus_file_formats table with properties
```

**Implementation:**
- Parse `CREATE FILE FORMAT` statements
- Store format definitions in `mockhaus_file_formats` system table
- Map Snowflake properties to DuckDB-compatible equivalents
- Validate property combinations

#### **2.2 Property Mapping**
```
Snowflake Property → DuckDB Equivalent
FIELD_DELIMITER → delim
SKIP_HEADER → header
NULL_IF → nullstr  
DATE_FORMAT → dateformat
TIMESTAMP_FORMAT → timestampformat
FIELD_OPTIONALLY_ENCLOSED_BY → quote
```

### **Phase 3: COPY INTO Translation**

#### **3.1 COPY Statement Parsing**
```sql
-- Snowflake
COPY INTO customers 
FROM @my_stage/customers.csv
FILE_FORMAT = (FORMAT_NAME = 'my_csv_format')
ON_ERROR = 'CONTINUE';

-- Translation Steps:
-- 1. Resolve @my_stage → ~/.mockhaus/stages/my_stage/
-- 2. Get format definition for 'my_csv_format' 
-- 3. Generate DuckDB COPY statement
-- 4. Handle error policy
```

#### **3.2 DuckDB COPY Generation**
```sql
-- Generated DuckDB
COPY customers FROM '~/.mockhaus/stages/my_stage/customers.csv'
(FORMAT CSV, DELIMITER ',', HEADER true, NULL 'NULL');
```

### **Phase 4: Error Handling & Validation**

#### **4.1 Error Policies**
```
Snowflake ON_ERROR → Mockhaus Behavior
'ABORT' → Fail on first error (default)
'CONTINUE' → Skip bad records, log errors
'SKIP_FILE' → Skip entire file on error
```

#### **4.2 Validation & Feedback**
- File existence validation
- Format compatibility checking  
- Schema mismatch detection
- Row count and error reporting

## 🛠️ **Implementation Components**

### **1. Core Classes**

```python
class MockStageManager:
    def create_stage(name, properties) -> Stage
    def resolve_stage_path(stage_ref) -> str
    def list_stage_files(stage_ref) -> List[str]
    def validate_stage_access(stage_ref) -> bool

class MockFileFormatManager:
    def create_format(name, properties) -> FileFormat
    def get_format(name) -> FileFormat
    def map_to_duckdb_options(format) -> Dict[str, str]

class CopyIntoTranslator:
    def translate_copy_statement(sql) -> str
    def resolve_dependencies(stage, format) -> CopyContext
    def generate_duckdb_copy(context) -> str
```

### **2. System Tables**

```sql
-- Stage metadata
CREATE TABLE mockhaus_stages (
    name VARCHAR,
    stage_type VARCHAR, -- 'INTERNAL', 'USER', 'EXTERNAL'
    url VARCHAR,
    local_path VARCHAR,
    properties JSON,
    created_at TIMESTAMP
);

-- File format metadata  
CREATE TABLE mockhaus_file_formats (
    name VARCHAR,
    format_type VARCHAR, -- 'CSV', 'JSON', 'PARQUET' 
    properties JSON,
    created_at TIMESTAMP
);

-- Copy operation history
CREATE TABLE mockhaus_copy_history (
    operation_id VARCHAR,
    table_name VARCHAR,
    stage_path VARCHAR,
    file_format VARCHAR,
    rows_loaded INTEGER,
    errors INTEGER,
    executed_at TIMESTAMP
);
```

### **3. Configuration Management**

```yaml
# ~/.mockhaus/config.yaml
stages:
  base_path: ~/.mockhaus/stages
  auto_create: true
  cleanup_on_exit: false

file_formats:  
  default_csv:
    delimiter: ","
    header: true
    null_values: ["NULL", "null", ""]
    
copy_operations:
  default_error_policy: "ABORT"
  max_file_size_mb: 100
  batch_size: 10000
```

## 📁 **Directory Structure**

```
~/.mockhaus/
├── stages/
│   ├── my_csv_stage/
│   │   ├── customers.csv
│   │   └── orders.csv
│   └── json_stage/
│       └── events.json
├── tables/
│   └── customers/  # Internal stage for @%customers
│       └── staging/
├── user/           # User stage @~
│   └── uploads/
├── external/       # External stages
│   ├── s3/
│   └── azure/
└── config.yaml
```

## 🧪 **Testing Strategy**

### **1. Unit Tests**
- Stage path resolution
- File format property mapping
- COPY statement parsing
- Error handling scenarios

### **2. Integration Tests**
- End-to-end COPY workflows
- Multi-file operations
- Format compatibility
- Error recovery

### **3. Compatibility Tests**
```sql
-- Test various Snowflake patterns
COPY INTO table1 FROM @stage/file.csv;
COPY INTO table2 FROM @%table2/data.json FILE_FORMAT = (TYPE = 'JSON');
COPY INTO table3 FROM '@~/upload.parquet' FILE_FORMAT = my_parquet_format;
```

## 🎯 **Success Criteria**

### **MVP (Minimal Viable Product)**
- ✅ CREATE STAGE support for named stages
- ✅ CREATE FILE FORMAT support for CSV
- ✅ Basic COPY INTO translation for single files
- ✅ Local file system stage mapping
- ✅ Error reporting for missing files/formats

### **Full Implementation**
- ✅ All stage types (internal, user, external)
- ✅ All major file formats (CSV, JSON, Parquet)
- ✅ Advanced COPY options (pattern matching, transformations)
- ✅ Comprehensive error handling
- ✅ Performance optimization for large files

## 🚨 **Known Limitations**

### **Will Not Support:**
- **Real cloud storage** (S3, Azure, GCS) - only local file simulation
- **Snowpipe streaming** - batch processing only
- **External functions** - no UDF support in COPY
- **Compression detection** - manual format specification required
- **Advanced security** - no real credential validation

### **Acceptable Compromises:**
- **Performance**: Local files vs. cloud storage will have different characteristics
- **Scale**: Limited by local disk space vs. cloud storage capacity  
- **Concurrency**: Single-threaded COPY vs. Snowflake's parallel loading
- **Monitoring**: Basic logging vs. Snowflake's detailed query history

## 📈 **Implementation Phases**

### **Phase 1 (Week 1)**: Foundation
- Stage management system
- Basic file format support (CSV only)
- Simple COPY INTO translation

### **Phase 2 (Week 2)**: Core Features  
- Multiple file formats (JSON, Parquet)
- Error handling and validation
- System table integration

### **Phase 3 (Week 3)**: Advanced Features
- Pattern matching in COPY statements
- Transformation support
- Performance optimization

### **Phase 4 (Week 4)**: Polish & Testing
- Comprehensive test suite
- Documentation and examples
- Integration with existing CLI

## 🔄 **Integration Points**

### **With Existing Mockhaus:**
- **Translator Extension**: Add DDL/COPY statement handling
- **Executor Enhancement**: Support stage and format operations  
- **CLI Commands**: Add stage management commands
- **Configuration**: Extend existing config system

### **Backward Compatibility:**
- Existing SELECT queries remain unchanged
- Current CREATE TABLE/INSERT functionality preserved
- No breaking changes to API or CLI

## 💡 **Example Usage Scenarios**

### **Scenario 1: Basic CSV Ingestion**
```sql
-- 1. Create a stage
CREATE STAGE my_data_stage;

-- 2. Create a file format  
CREATE FILE FORMAT csv_format
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1;

-- 3. Create target table
CREATE TABLE customers (
  id INTEGER,
  name VARCHAR(100),
  email VARCHAR(255)
);

-- 4. Copy data (assumes file exists at ~/.mockhaus/stages/my_data_stage/customers.csv)
COPY INTO customers 
FROM @my_data_stage/customers.csv
FILE_FORMAT = (FORMAT_NAME = 'csv_format');
```

### **Scenario 2: JSON Data Loading**
```sql
-- 1. Create JSON format
CREATE FILE FORMAT json_format TYPE = 'JSON';

-- 2. Load JSON data
COPY INTO events 
FROM @my_stage/events.json
FILE_FORMAT = (FORMAT_NAME = 'json_format');
```

### **Scenario 3: Internal Stage Usage**
```sql
-- Load data into table's internal stage
COPY INTO products 
FROM @%products/inventory.csv
FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1);
```

## 🔧 **CLI Extensions**

### **New Commands**
```bash
# Stage management
mockhaus stage create my_stage --path ./data
mockhaus stage list
mockhaus stage files my_stage

# File format management  
mockhaus format create csv_basic --type CSV --delimiter ","
mockhaus format list

# File operations
mockhaus file upload ./data.csv @my_stage/
mockhaus file list @my_stage/
```

## 📚 **Documentation Requirements**

### **User Documentation**
- Stage and file format concepts
- COPY INTO syntax guide
- Common ingestion patterns
- Troubleshooting guide

### **Developer Documentation**
- Architecture overview
- Extension points for new formats
- Testing framework
- Performance considerations

---

This approach provides a realistic simulation of Snowflake's data ingestion while leveraging DuckDB's excellent file loading capabilities and maintaining the simplicity that makes Mockhaus valuable for development and testing.