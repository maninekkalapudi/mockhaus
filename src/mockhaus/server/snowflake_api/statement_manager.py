import uuid
import asyncio
from datetime import datetime, timezone
from typing import Dict, Optional

from mockhaus.server.snowflake_api.models import (
    StatementResponse,
    StatementStatus,
    CancellationResponse,
)
from .async_executor import AsyncExecutor


class StatementManager:
    """
    Manages the lifecycle of Snowflake SQL statements in memory.

    This is a skeleton implementation for Phase 1.1 of the Snowflake API.
    It stores statement states in an in-memory dictionary.
    """

    def __init__(self):
        self._statements: Dict[str, StatementResponse] = {}
        self._async_executor = AsyncExecutor()

    def submit_statement(self, sql: str) -> StatementResponse:
        """
        Creates a new statement, stores it, and returns the response.
        Starts a background task to simulate execution.
        """
        statement_handle = str(uuid.uuid4())
        response = StatementResponse(
            statement_handle=statement_handle,
            status=StatementStatus.SUBMITTED,  # Initial status
            sqlState="00000",
            dateTime=datetime.now(timezone.utc).isoformat(),
            message=f"Statement submitted: {sql[:50]}...",
        )
        self._statements[statement_handle] = response
        
        # Start background task for execution
        asyncio.create_task(self._execute_statement_in_background(statement_handle, sql))
        
        return response

    async def _execute_statement_in_background(self, statement_handle: str, sql: str):
        """
        Executes the statement in the background and updates its status and results.
        """
        # Update status to RUNNING
        if statement_handle in self._statements:
            self._statements[statement_handle].status = StatementStatus.RUNNING
            self._statements[statement_handle].message = f"Statement running: {sql[:50]}..."
            print(f"Statement {statement_handle} status updated to RUNNING.")

        # Execute the SQL using AsyncExecutor
        execution_result = await self._async_executor.execute(statement_handle, sql)

        # Update status based on execution result
        if statement_handle in self._statements:
            current_response = self._statements[statement_handle]
            current_response.date_time = datetime.now(timezone.utc).isoformat()

            if execution_result["status"] == "SUCCEEDED":
                current_response.status = StatementStatus.SUCCEEDED
                current_response.message = f"Statement succeeded: {sql[:50]}..."
                if execution_result["results"]:
                    current_response.result_set = execution_result["results"]["resultSet"]
                    current_response.result_set_meta_data = execution_result["results"]["resultSetMetaData"]
                print(f"Statement {statement_handle} status updated to SUCCEEDED.")
            else:
                current_response.status = StatementStatus.FAILED
                current_response.message = execution_result["error_message"]
                current_response.error_code = execution_result["error_code"]
                print(f"Statement {statement_handle} status updated to FAILED: {execution_result["error_message"]}")

    def get_statement_status(self, handle: str) -> Optional[StatementResponse]:
        """
        Retrieves a statement by its handle.
        """
        return self._statements.get(handle)

    def cancel_statement(self, handle: str) -> Optional[CancellationResponse]:
        """
        (Skeleton) A placeholder for the cancellation logic.
        """
        if handle in self._statements:
            # In a real implementation, this would attempt to cancel the running statement.
            # For now, we just return a success response.
            return CancellationResponse(
                status="SUCCESS", message=f"Cancellation request for {handle} received."
            )
        return None
