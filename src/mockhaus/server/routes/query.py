"""
This module defines the primary query execution endpoint for the Mockhaus server.

It provides the `/query` route, which accepts Snowflake SQL queries, executes
_them within a session context, and returns the results. This is the main
entry point for all SQL-related interactions with the server.
"""

import time
from typing import Any

from fastapi import APIRouter, HTTPException

from ..models.request import QueryRequest
from ..models.response import ErrorResponse, QueryResponse

# Legacy session manager removed - now using ConcurrentSessionManager only
from ..state import server_state

router = APIRouter(tags=["query"])


@router.post("/query", response_model=QueryResponse, responses={400: {"model": ErrorResponse}, 500: {"model": ErrorResponse}})
async def execute_query(request: QueryRequest) -> Any:
    """
    Executes a Snowflake SQL query within a session context.

    This endpoint accepts a SQL query and an optional session ID. It uses the
    global `server_state` to get or create a session, then delegates the query
    execution to the session's context. This ensures that each query is handled
    in an isolated environment.

    The endpoint can handle various types of Snowflake SQL, including:
    - DQL (SELECT statements)
    - DDL (CREATE, ALTER, DROP statements for tables, stages, etc.)
    - DML (INSERT, UPDATE, DELETE statements)
    - Data ingestion commands (e.g., `COPY INTO`)

    Args:
        request: A `QueryRequest` object containing the SQL query and session ID.

    Returns:
        A `QueryResponse` object with the execution results, including data,
        execution time, and the translated SQL.

    Raises:
        HTTPException: A 400 error for SQL execution-related issues, or a 500
                       error for unexpected internal server errors.
    """
    start_time = time.time()

    try:
        session_id = request.session_id

        # Use session-based execution
        session_context = await server_state.get_or_create_session(session_id)

        # Execute query in session context
        result = await session_context.execute_sql(request.sql)
        execution_time = time.time() - start_time

        if result["success"]:
            return QueryResponse(
                success=True,
                data=result["data"],
                execution_time=execution_time,
                translated_sql=result["translated_sql"],
                message=None,
                session_id=result["session_id"],
                current_database=None,  # TODO: Add database tracking to session
            )
        # If the execution failed, raise an HTTPException with details.
        raise HTTPException(
            status_code=400,
            detail={
                "success": False,
                "error": "SQL_EXECUTION_ERROR",
                "detail": result.get("error", "Unknown error"),
                "session_id": result["session_id"],
            },
        )

    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        # Catch any other unexpected errors and return a 500 response.
        raise HTTPException(
            status_code=500,
            detail={
                "success": False,
                "error": "INTERNAL_SERVER_ERROR",
                "detail": f"Unexpected error: {str(e)}",
                "session_id": session_id if "session_id" in locals() else None,
            },
        ) from e
