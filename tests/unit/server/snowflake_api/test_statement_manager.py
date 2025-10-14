import uuid
from mockhaus.server.snowflake_api.statement_manager import StatementManager
from mockhaus.server.snowflake_api.models import StatementStatus, CancellationResponse

def test_submit_statement():
    manager = StatementManager()
    sql_statement = "SELECT 1;"
    response = manager.submit_statement(sql_statement)

    assert response is not None
    assert uuid.UUID(response.statement_handle)  # Check if it's a valid UUID
    assert response.status == StatementStatus.SUBMITTED
    assert sql_statement[:50] in response.message
    assert manager.get_statement_status(response.statement_handle) == response

def test_get_statement_status_existing():
    manager = StatementManager()
    sql_statement = "SELECT 2;"
    submitted_response = manager.submit_statement(sql_statement)

    retrieved_response = manager.get_statement_status(submitted_response.statement_handle)
    assert retrieved_response == submitted_response

def test_get_statement_status_non_existent():
    manager = StatementManager()
    non_existent_handle = str(uuid.uuid4())
    response = manager.get_statement_status(non_existent_handle)
    assert response is None

def test_cancel_statement_existing():
    manager = StatementManager()
    sql_statement = "SELECT 3;"
    submitted_response = manager.submit_statement(sql_statement)

    cancellation_response = manager.cancel_statement(submitted_response.statement_handle)
    assert cancellation_response is not None
    assert cancellation_response.status == "SUCCESS"
    assert submitted_response.statement_handle in cancellation_response.message

def test_cancel_statement_non_existent():
    manager = StatementManager()
    non_existent_handle = str(uuid.uuid4())
    cancellation_response = manager.cancel_statement(non_existent_handle)
    assert cancellation_response is None
