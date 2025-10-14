"""
This module defines the FastAPI routes for emulating the Snowflake SQL REST API.

It includes endpoints for:
- Submitting a new SQL statement for execution.
- Checking the status and retrieving the results of a previously submitted statement.
- Canceling an in-progress statement.

These routes are designed to be compatible with the official Snowflake SQL API,
allowing existing Snowflake clients and tools to interact with Mockhaus as if it
were a genuine Snowflake instance. The handlers for these routes will orchestrate
the process of statement execution, status tracking, and result retrieval.
"""
import uuid
from datetime import datetime, timezone
from fastapi import APIRouter, HTTPException

from mockhaus.server.snowflake_api.models import (
    StatementRequest,
    StatementResponse,
    CancellationResponse,
    StatementStatus,
)
from .statement_manager import StatementManager

router = APIRouter()
statement_manager = StatementManager()


@router.post("/statements", response_model=StatementResponse)
async def submit_statement(request: StatementRequest) -> StatementResponse:
    """
    Handles the submission of a new SQL statement for asynchronous execution.

    Args:
        request: A Pydantic model containing the SQL statement and options.

    Returns:
        A StatementResponse object confirming the submission.
    """
    print(f"Received statement submission: {request.statement[:100]}...")
    response = statement_manager.submit_statement(request.statement)
    return response


@router.get("/statements/{statement_handle}", response_model=StatementResponse)
async def get_statement_status(statement_handle: str) -> StatementResponse:
    """
    Retrieves the status and results of a previously submitted statement.

    Args:
        statement_handle: The UUID handle of the statement to check.

    Returns:
        A StatementResponse object with the current status and results.
    """
    # IMPROVED: Added a print statement for debugging.
    print(f"Checking status for handle: {statement_handle}")
    response = statement_manager.get_statement_status(statement_handle)
    if not response:
        raise HTTPException(status_code=404, detail="Statement handle not found.")
    return response


@router.post(
    "/statements/{statement_handle}/cancel", response_model=CancellationResponse
)
async def cancel_statement(statement_handle: str) -> CancellationResponse:
    """
    Cancels an in-progress SQL statement.

    Args:
        statement_handle: The UUID handle of the statement to cancel.

    Returns:
        A CancellationResponse confirming the request was received.
    """

    print(f"Received cancellation request for handle: {statement_handle}")
    response = statement_manager.cancel_statement(statement_handle)
    if not response:
        raise HTTPException(status_code=404, detail="Statement handle not found.")
    return response