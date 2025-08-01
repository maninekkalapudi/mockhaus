"""Request models for the HTTP API."""

from typing import Optional

from pydantic import BaseModel, Field


class QueryRequest(BaseModel):
    """Request model for SQL query execution."""

    sql: str = Field(..., description="Snowflake SQL query to execute", min_length=1)
    database: Optional[str] = Field(None, description="Optional database file path")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "sql": "SELECT * FROM sample_customers LIMIT 10",
                    "database": None
                },
                {
                    "sql": "CREATE STAGE my_stage URL = 's3://my-bucket/data/'",
                    "database": "my_data.db"
                }
            ]
        }
    }