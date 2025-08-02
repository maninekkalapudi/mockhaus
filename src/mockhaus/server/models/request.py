"""Request models for the HTTP API."""

from pydantic import BaseModel, Field


class QueryRequest(BaseModel):
    """Request model for SQL query execution."""

    sql: str = Field(..., description="Snowflake SQL query to execute", min_length=1)
    database: str | None = Field(None, description="Optional database file path")
    session_id: str | None = Field(None, description="Optional session ID for database context persistence")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {"sql": "SELECT * FROM sample_customers LIMIT 10", "database": None, "session_id": None},
                {"sql": "CREATE DATABASE my_project", "session_id": "abc123-def456"},
                {"sql": "CREATE TABLE employees (id INT, name VARCHAR(100))", "session_id": "abc123-def456"},
            ]
        }
    }
