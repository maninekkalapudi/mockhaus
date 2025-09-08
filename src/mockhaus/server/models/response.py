"""
This module defines the Pydantic models for API responses.

These models are used by FastAPI to serialize the output of API endpoints into
well-structured JSON responses. They ensure that clients receive consistent
and predictable data structures for both successful and error responses.
"""

from typing import Any

from pydantic import BaseModel, Field


class QueryResponse(BaseModel):
    """
    Represents the response model for a successful query execution.

    Attributes:
        success: A boolean indicating the success of the query (always True).
        data: The result set of the query, as a list of dictionaries.
        execution_time: The time taken to execute the query, in seconds.
        translated_sql: The DuckDB-compatible SQL that was actually executed.
        message: An optional informational message (e.g., for DDL statements).
        session_id: The ID of the session in which the query was executed.
        current_database: The name of the database active after the query.
    """

    success: bool = Field(True, description="Whether the query executed successfully")
    data: list[dict[str, Any]] | None = Field(None, description="Query result data")
    execution_time: float | None = Field(None, description="Query execution time in seconds")
    translated_sql: str | None = Field(None, description="Translated DuckDB SQL")
    message: str | None = Field(None, description="Success or info message")
    session_id: str | None = Field(None, description="Session ID for database context persistence")
    current_database: str | None = Field(None, description="Current database name")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "success": True,
                    "data": [
                        {"customer_id": 1, "customer_name": "Alice", "account_balance": 1500.0},
                        {"customer_id": 2, "customer_name": "Bob", "account_balance": 2300.0},
                    ],
                    "execution_time": 0.123,
                    "translated_sql": "SELECT customer_id, customer_name, account_balance FROM sample_customers",
                    "message": None,
                }
            ]
        }
    }


class HealthResponse(BaseModel):
    """
    Represents the response model for the server health check endpoint.

    Attributes:
        status: The health status of the server (e.g., 'healthy').
        version: The version number of the Mockhaus application.
        uptime: The uptime of the server in seconds.
    """

    status: str = Field(..., description="Server health status")
    version: str = Field(..., description="Mockhaus version")
    uptime: float | None = Field(None, description="Server uptime in seconds")

    model_config = {"json_schema_extra": {"examples": [{"status": "healthy", "version": "0.3.0", "uptime": 3600.5}]}}


class ErrorResponse(BaseModel):
    """
    Represents the standard response model for API errors.

    Attributes:
        success: A boolean indicating the success of the operation (always False).
        error: A high-level category for the error.
        detail: A detailed message describing the error.
    """

    success: bool = Field(False, description="Always false for error responses")
    error: str = Field(..., description="Error type or category")
    detail: str | None = Field(None, description="Detailed error message")

    model_config = {
        "json_schema_extra": {"examples": [{"success": False, "error": "SQL_EXECUTION_ERROR", "detail": "Table 'nonexistent_table' not found"}]}
    }
