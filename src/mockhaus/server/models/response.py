"""Response models for the HTTP API."""

from typing import Any

from pydantic import BaseModel, Field


class QueryResponse(BaseModel):
    """Response model for successful query execution."""

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
    """Response model for health check endpoint."""

    status: str = Field(..., description="Server health status")
    version: str = Field(..., description="Mockhaus version")
    uptime: float | None = Field(None, description="Server uptime in seconds")

    model_config = {"json_schema_extra": {"examples": [{"status": "healthy", "version": "0.3.0", "uptime": 3600.5}]}}


class ErrorResponse(BaseModel):
    """Response model for error responses."""

    success: bool = Field(False, description="Always false for error responses")
    error: str = Field(..., description="Error type or category")
    detail: str | None = Field(None, description="Detailed error message")

    model_config = {
        "json_schema_extra": {"examples": [{"success": False, "error": "SQL_EXECUTION_ERROR", "detail": "Table 'nonexistent_table' not found"}]}
    }
