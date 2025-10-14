import uuid
from datetime import datetime, timezone
from typing import Dict, Optional

from mockhaus.server.snowflake_api.models import (
    StatementResponse,
    StatementStatus,
    CancellationResponse,
)


class StatementManager:
    """
    Manages the lifecycle of Snowflake SQL statements in memory.

    This is a skeleton implementation for Phase 1.1 of the Snowflake API.
    It stores statement states in an in-memory dictionary.
    """

    def __init__(self):
        self._statements: Dict[str, StatementResponse] = {}

    def submit_statement(self, sql: str) -> StatementResponse:
        """
        Creates a new statement, stores it, and returns the response.
        """
        statement_handle = str(uuid.uuid4())
        response = StatementResponse(
            statementHandle=statement_handle,
            status=StatementStatus.SUBMITTED,  # Initial status
            sqlState="00000",
            dateTime=datetime.now(timezone.utc).isoformat(),
            message=f"Statement submitted: {sql[:50]}...",
        )
        self._statements[statement_handle] = response
        return response

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
