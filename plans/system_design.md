# System Design

This document outlines the architecture and design of Mockhaus, a Snowflake proxy that uses DuckDB as a backend.

## High-Level Architecture

The following diagram illustrates the high-level architecture of Mockhaus:

```mermaid
graph TD
    subgraph "User Interaction"
        A[REPL Client]
        B[HTTP API Client]
    end

    subgraph "Mockhaus Server"
        C[HTTP Server]
        D[Session Manager]
        E[Snowflake Translator]
        F[DuckDB Executor]
    end
    
    subgraph "Data Storage"
        G[In-Memory Database]
        H[Persistent Storage]
        I[Remote Storage]
    end

    A --> C
    B --> C
    C --> D
    D --> E
    E --> F
    F --> G
    F --> H
    F --> I
```

### Components

*   **REPL Client:** An interactive command-line interface for sending queries to the Mockhaus server.
*   **HTTP API Client:** Any client that can send HTTP requests to the Mockhaus server (e.g., `curl`, Python `requests`).
*   **HTTP Server:** A FastAPI server that exposes a REST API for session management and query execution.
*   **Session Manager:** Manages user sessions, including creating, deleting, and tracking active sessions. It supports both in-memory and persistent sessions.
*   **Snowflake Translator:** Translates Snowflake SQL queries to DuckDB SQL. It uses `sqlglot` for parsing and translation.
*   **DuckDB Executor:** Executes the translated DuckDB SQL queries using the DuckDB engine.
*   **In-Memory Database:** A DuckDB database that is stored in memory. Used for in-memory sessions.
*   **Persistent Storage:** A DuckDB database that is stored on disk. Used for persistent sessions.
*   **Remote Storage:** Remote storage for data ingestion (e.g., S3, GCS, Azure Blob Storage).

## Session Management

Mockhaus supports multiple user sessions, each with its own isolated DuckDB database instance. This allows users to work with different datasets and schemas without interfering with each other.

### Session Types

*   **In-Memory Sessions:** Sessions that are stored in memory. They are fast but not persistent. When the server is restarted, all in-memory sessions are lost.
*   **Persistent Sessions:** Sessions that are stored on disk. They are slower than in-memory sessions but are persistent across server restarts.

### Session Lifecycle

The following diagram illustrates the lifecycle of a session:

```mermaid
sequenceDiagram
    participant Client
    participant Server

    Client->>Server: Create Session (POST /api/v1/sessions)
    Server-->>Client: Session ID
    Client->>Server: Execute Query (POST /api/v1/query)
    Server-->>Client: Query Result
    Client->>Server: Delete Session (DELETE /api/v1/sessions/{session_id})
    Server-->>Client: Success
```

## Data Ingestion

Mockhaus supports data ingestion from remote storage using the `COPY INTO` command. The data ingestion workflow is as follows:

1.  **Create a Stage:** A stage is a reference to a remote storage location (e.g., S3 bucket, GCS bucket).
2.  **Create a File Format:** A file format describes the format of the data files (e.g., CSV, JSON, Parquet).
3.  **Copy Data:** The `COPY INTO` command copies data from the stage into a table.

The following diagram illustrates the data ingestion workflow:

```mermaid
graph TD
    subgraph "Data Ingestion"
        A[Create Stage]
        B[Create File Format]
        C[Copy Into]
    end

    A --> C
    B --> C
```

## Query Execution

The following diagram illustrates the sequence of events when a query is executed:

```mermaid
sequenceDiagram
    participant Client
    participant Server
    participant SnowflakeTranslator
    participant DuckDBExecutor

    Client->>Server: Execute Query (POST /api/v1/query)
    Server->>SnowflakeTranslator: Translate SQL
    SnowflakeTranslator-->>Server: Translated SQL
    Server->>DuckDBExecutor: Execute SQL
    DuckDBExecutor-->>Server: Query Result
    Server-->>Client: Query Result
```