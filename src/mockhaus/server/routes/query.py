"""Query execution endpoints."""

import time
from typing import Any

from fastapi import APIRouter, HTTPException

from ..models.request import QueryRequest
from ..models.response import ErrorResponse, QueryResponse
from ..session import session_manager
from ..state import get_server_executor

router = APIRouter(tags=["query"])


@router.post("/query", response_model=QueryResponse, responses={400: {"model": ErrorResponse}, 500: {"model": ErrorResponse}})
async def execute_query(request: QueryRequest) -> Any:
    """
    Execute Snowflake SQL query.

    Accepts any valid Snowflake SQL including:
    - SELECT queries
    - DDL statements (CREATE STAGE, CREATE FILE FORMAT, etc.)
    - DML statements (INSERT, UPDATE, DELETE)
    - Data ingestion (COPY INTO)

    Args:
        request: Query request containing SQL and optional database path

    Returns:
        Query execution results with data, execution time, and translated SQL

    Raises:
        HTTPException: 400 for SQL execution errors, 500 for server errors
    """
    start_time = time.time()

    try:
        session_id = request.session_id

        # Create session if none provided
        if session_id is None:
            session_id = session_manager.create_session()

        # Use persistent server executor
        with get_server_executor() as executor:
            result = executor.execute_snowflake_sql(request.sql)
            execution_time = time.time() - start_time

            if result.success:
                return QueryResponse(
                    success=True,
                    data=result.data,
                    execution_time=execution_time,
                    translated_sql=result.translated_sql,
                    message=None,
                    session_id=session_id,
                    current_database=executor._database_manager.current_database if executor._database_manager else None,
                )
            raise HTTPException(
                status_code=400, detail={"success": False, "error": "SQL_EXECUTION_ERROR", "detail": result.error, "session_id": session_id}
            )

    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "success": False,
                "error": "INTERNAL_SERVER_ERROR",
                "detail": f"Unexpected error: {str(e)}",
                "session_id": session_id if "session_id" in locals() else None,
            },
        ) from e
