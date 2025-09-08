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

## 4. Detailed Component Guides

This section provides a more in-depth look at specific components and their roles within the Mockhaus architecture.

### 4.1. Server and Session Management

The HTTP server provides a persistent, multi-user interface to Mockhaus.

*   **Application (`server/app.py`):** The main FastAPI application file. It sets up the routes, middleware, and global state for the server.
*   **Session Management (`server/concurrent_session_manager.py`):** A key component for multi-tenancy. It manages active user sessions, ensuring that each user interacts with their own isolated DuckDB database and state. It handles session creation, expiration, and retrieval.
*   **Session Context (`server/session_context.py`):** A context manager that ensures each incoming API request is associated with the correct session. It uses a `ContextVar` to hold the current session ID, making it accessible throughout the request lifecycle without needing to pass it explicitly through function calls.
*   **Global State (`server/state.py`):** Holds the application-level state, primarily the `ConcurrentSessionManager` instance. This makes the session manager accessible to all API endpoints.

### 4.2. Database Management

*   **Database Manager (`snowflake/database_manager.py`):** This component abstracts the management of multiple databases within a single Mockhaus session. It handles `USE DATABASE` and `USE SCHEMA` commands, tracking the current database and schema for the session. All tables created are associated with the active database and schema, emulating Snowflake's namespace behavior.

### 4.3. Query History

*   **History (`query_history/history.py`):** Provides persistent storage for all queries executed through Mockhaus. It uses a dedicated table within the main DuckDB database to store the SQL text, execution status (success or failure), and a timestamp for each query. This is useful for debugging and auditing.

### 4.4. Interactive REPL

*   **Enhanced REPL (`repl/enhanced_repl.py`):** An interactive command-line interface for Mockhaus. It uses the `prompt_toolkit` library to provide features like syntax highlighting, autocompletion, and command history, offering a richer user experience than a simple input loop.

### 4.5. Custom SQLGlot Dialects

*   **Custom Dialects (`sqlglot/dialects/`):** Mockhaus extends `sqlglot`'s standard Snowflake and DuckDB dialects to handle SQL features and syntax that are not covered by the base library.
    *   `custom_snowflake.py`: Adds parsing for Snowflake-specific commands or functions.
    *   `custom_duckdb.py`: Defines how certain Snowflake features should be translated or generated in DuckDB-compatible SQL.
    *   `expressions.py`: Contains custom expression classes that are used in the translation process.