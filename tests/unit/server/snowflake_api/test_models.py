"""
This module contains unit tests for the Pydantic models defined in
`src.mockhaus.server.snowflake_api.models`. These tests ensure that each model
correctly validates incoming data, handles required and optional fields,
and correctly maps field aliases.

The tests cover:
- Successful instantiation of models with valid data.
- Validation errors for missing required fields or incorrect data types.
- Correct assignment of values, especially for fields with aliases.
- Enum membership checks.
"""

import pytest
from pydantic import ValidationError
from src.mockhaus.server.snowflake_api.models import (
    StatementStatus,
    RowType,
    PartitionInfo,
    ResultSetMetadata,
    StatementRequest,
    StatementResponse,
    CancellationResponse,
)

def test_statement_status_enum():
    """Tests that the StatementStatus enum members have the correct string values."""
    assert StatementStatus.SUBMITTED == "SUBMITTED"
    assert StatementStatus.RUNNING == "RUNNING"
    assert StatementStatus.SUCCEEDED == "SUCCEEDED"
    assert StatementStatus.FAILED == "FAILED"
    assert StatementStatus.CANCELED == "CANCELED"

class TestRowType:
    """Tests for the RowType model."""
    def test_row_type_success(self):
        """Tests successful creation of a RowType model with valid data."""
        data = {
            "name": "col1",
            "database": "db1",
            "schema": "schema1",
            "table": "table1",
            "type": "text",
            "nullable": False,
        }
        model = RowType(**data)
        assert model.name == "col1"
        assert model.schema_ == "schema1"

    def test_row_type_missing_required(self):
        """Tests that a ValidationError is raised when required fields are missing."""
        with pytest.raises(ValidationError):
            RowType(name="col1")

class TestPartitionInfo:
    """Tests for the PartitionInfo model."""
    def test_partition_info_success(self):
        """Tests successful creation of a PartitionInfo model with valid data."""
        data = {"rowCount": 10, "uncompressedSize": 100, "compressedSize": 50}
        model = PartitionInfo(**data)
        assert model.row_count == 10
        assert model.uncompressed_size == 100
        assert model.compressed_size == 50

    def test_partition_info_invalid_type(self):
        """Tests that a ValidationError is raised for invalid data types."""
        with pytest.raises(ValidationError):
            PartitionInfo(rowCount="abc", uncompressedSize=100, compressedSize=50)

class TestResultSetMetadata:
    """Tests for the ResultSetMetadata model."""
    def test_result_set_metadata_success(self):
        """Tests successful creation of a ResultSetMetadata model."""
        data = {
            "numRows": 1,
            "format": "jsonv2",
            "rowType": [
                {
                    "name": "col1",
                    "database": "db1",
                    "schema": "schema1",
                    "table": "table1",
                    "type": "text",
                    "nullable": False,
                }
            ],
            "partitionInfo": [{"rowCount": 1, "uncompressedSize": 10, "compressedSize": 5}],
        }
        model = ResultSetMetadata(**data)
        assert model.num_rows == 1
        assert len(model.row_type) == 1
        assert len(model.partition_info) == 1

class TestStatementRequest:
    """Tests for the StatementRequest model."""
    def test_statement_request_success(self):
        """Tests successful creation of a StatementRequest model."""
        data = {"statement": "SELECT 1", "schema": "my_schema"}
        model = StatementRequest(**data)
        assert model.statement == "SELECT 1"
        assert model.schema_ == "my_schema"

    def test_statement_request_missing_statement(self):
        """Tests that a ValidationError is raised if 'statement' is missing."""
        with pytest.raises(ValidationError):
            StatementRequest(timeout=10)

class TestStatementResponse:
    """Tests for the StatementResponse model."""
    def test_statement_response_success(self):
        """Tests successful creation of a StatementResponse model."""
        data = {
            "statementHandle": "1234",
            "status": "SUCCEEDED",
            "sqlState": "00000",
            "dateTime": "2025-09-23T12:00:00Z",
        }
        model = StatementResponse(**data)
        assert model.statement_handle == "1234"
        assert model.status == StatementStatus.SUCCEEDED

class TestCancellationResponse:
    """Tests for the CancellationResponse model."""
    def test_cancellation_response_success(self):
        """Tests successful creation of a CancellationResponse model."""
        data = {"status": "CANCELED", "message": "Statement canceled."}
        model = CancellationResponse(**data)
        assert model.status == "CANCELED"
        assert model.message == "Statement canceled."