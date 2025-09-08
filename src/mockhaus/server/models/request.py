"""
This module defines the Pydantic models for incoming API requests.

These models are used by FastAPI to validate and parse the JSON body of
incoming HTTP requests, ensuring that the data conforms to the expected structure
and types.
"""

from pydantic import BaseModel, Field


class QueryRequest(BaseModel):
    """
    Represents the request model for executing a SQL query.

    Attributes:
        sql: The Snowflake SQL query string to be executed.
        database: An optional path to a database file. This is typically unused
                  in favor of session-based databases.
        session_id: An optional ID for the session to execute the query against.
                    This allows for maintaining a persistent context (e.g.,
                    current database, created tables) across multiple requests.
    """

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
