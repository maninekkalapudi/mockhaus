# Mockhaus System Design

## 1. Architecture Overview

Mockhaus is designed as a modular system that intercepts Snowflake SQL, translates it into DuckDB-compatible SQL, and executes it locally. This allows developers to test their Snowflake queries and data pipelines without incurring the cost and latency of connecting to a live Snowflake instance.

The architecture can be broken down into three main layers: the **Interface Layer**, the **Execution Layer**, and the **Snowflake Emulation Layer**.

```
+-----------------+
| Interface Layer |
| (CLI, Server)   |
+--------+--------+
         |
         v
+--------------------+
| Execution Layer    |
| (MockhausExecutor) |
+--------+-----------+
         |
         v
+-------------------------+
| Snowflake Emulation     |
| Layer                   |
| (Translator, Ingestion) |
+--------+----------------+
         |
         v
+-----------------+
| Backend         |
| (DuckDB)        |
+-----------------+
```

Flows:
This section illustrates the primary high-level data and control flows between the main architectural components. It provides a quick overview of how different parts of the system interact.

```
+-----------------+     +--------------------+     +-------------------------+     +-----------------+
| Interface Layer | --> | Execution Layer    | --> | Snowflake Emulation     | --> | Backend         |
| (CLI, Server)   |     | (MockhausExecutor) |     | Layer                   |     | (DuckDB)        |
+-----------------+     +--------------------+     | (Translator, Ingestion) |     +-----------------+
                                                   +-------------------------+
                                                                 |
                                                                 v
                                                     +------------------------------+
                                                     | Data Ingestion Components    |
                                                     | (ASTParser, CopyInto,        |
                                                     | StageManager, FormatManager) |
                                                     +------------------------------+
```

## 2. Component Descriptions

### 2.1. Interface Layer

*   **CLI (`cli.py`):** The primary interface for interacting with Mockhaus. It uses the `click` library to provide commands for executing queries, running the server, managing stages, and managing file formats.
*   **HTTP Server (`server/app.py`):** A FastAPI application that exposes Mockhaus functionality over a REST API. This allows Mockhaus to be used as a persistent service by teams or in CI/CD environments.

### 2.2. Execution Layer

*   **MockhausExecutor (`executor.py`):** The central orchestrator of the application. It receives a Snowflake SQL string and determines how to handle it.
    *   If the SQL is a standard DML/DDL statement, it passes it to the `SnowflakeToDuckDBTranslator`.
    *   If the SQL is a data ingestion command (e.g., `CREATE STAGE`, `COPY INTO`), it passes it to the `SnowflakeIngestionHandler`.
    *   It manages the connection to the DuckDB database, which can be either in-memory or a file on disk.

### 2.3. Snowflake Emulation Layer

This layer contains the logic for mimicking Snowflake's behavior.

*   **SQL Translator (`translator.py`):** Responsible for translating Snowflake SQL queries into DuckDB-compatible SQL. It uses the `sqlglot` library to parse the source SQL and generate the target SQL.
*   **Ingestion Handler (`ingestion.py`):** The entry point for all data ingestion operations. It identifies the type of ingestion statement and delegates to the appropriate component.
*   **AST Parser (`ast_parser.py`):** Uses `sqlglot` to parse Snowflake-specific DDL and DML, such as `CREATE STAGE` and `COPY INTO`. This provides a more robust and accurate way to deconstruct these commands than using regular expressions.
*   **Stage Manager (`stages.py`):** Simulates Snowflake stages. It maps stage names to local file system directories (under `~/.mockhaus/stages`) and stores metadata about the stages in a dedicated DuckDB table.
*   **File Format Manager (`file_formats.py`):** Simulates Snowflake file formats. It stores the properties of file formats (e.g., CSV delimiter, Parquet compression) in a DuckDB table and can map these properties to the equivalent options in DuckDB's `COPY` command.
*   **COPY INTO Translator (`copy_into.py`):** Handles the `COPY INTO` command. It parses the command, resolves the specified stage and file format, and generates a DuckDB `COPY` statement to load the data.

### 2.4. Backend

*   **DuckDB:** A fast, in-process analytical database. Mockhaus uses DuckDB as the execution engine for all queries and for storing metadata about stages and file formats.

## 3. Key Workflows

### 3.1. SELECT Query Execution

This workflow describes how a standard `SELECT` query is executed.

```
User
  |
  v
CLI
  | mockhaus query "SELECT * FROM my_table"
  v
Executor
  | execute_snowflake_sql("SELECT * FROM my_table")
  v
Translator
  | translate("SELECT * FROM my_table")
  v
Executor
  | "SELECT * FROM my_table"
  v
DuckDB
  | execute("SELECT * FROM my_table")
  v
Executor
  | Results
  v
CLI
  | QueryResult
  v
User
  | Display results
```

### 3.2. COPY INTO Data Ingestion

This workflow shows how data is loaded into a table from a stage using the `COPY INTO` command.

```
User
  |
  v
CLI
  | mockhaus query "COPY INTO my_table FROM @my_stage/data.csv"
  v
Executor
  | execute_snowflake_sql("COPY INTO my_table FROM @my_stage/data.csv")
  v
IngestionHandler
  | execute_ingestion_statement(...)
  v
CopyInto
  | execute_copy_operation(...)
  |
  | resolve_stage_path("@my_stage/data.csv")
  v
StageManager
  | /path/to/local/stage/data.csv
  v
CopyInto
  | get_format(...)
  v
FormatManager
  | FileFormat object
  v
CopyInto
  | execute("COPY my_table FROM '/path/to/local/stage/data.csv' (FORMAT CSV)")
  v
DuckDB
  | Rows loaded
  v
CopyInto
  | Result
  v
IngestionHandler
  | Result
  v
Executor
  | QueryResult
  v
CLI
  | Display rows loaded
```
