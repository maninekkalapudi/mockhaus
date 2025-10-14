from typing import Any, Dict, List, Optional

from mockhaus.server.snowflake_api.models import (
    ResultSetMetadata,
    RowType,
    PartitionInfo,
)

def map_duckdb_to_snowflake_results(
    data: List[Dict[str, Any]], columns: List[str]
) -> Dict[str, Any]:
    """
    Maps DuckDB query results (data and columns) to Snowflake's resultSet and resultSetMetaData format.

    Args:
        data: List of dictionaries representing rows.
        columns: List of column names.

    Returns:
        A dictionary containing 'resultSet' (list of dicts) and 'resultSetMetaData'.
    """
    # Map data to resultSet, converting column names to uppercase
    result_set: List[Dict[str, Any]] = []
    for row_dict in data:
        new_row_dict = {k.upper(): v for k, v in row_dict.items()}
        result_set.append(new_row_dict)

    # Generate resultSetMetaData
    row_types: List[RowType] = []
    for col_name in columns:
        # Simplified type mapping for now. In a real scenario, infer types from data.
        snowflake_type = "TEXT"  # Default to TEXT for simplicity
        # Add more sophisticated type inference here if needed

        row_types.append(
            RowType(
                name=col_name.upper(),
                database="MOCK_DB",  # Placeholder
                schema="PUBLIC",  # Placeholder
                table="MOCK_TABLE",  # Placeholder
                type=snowflake_type,
                nullable=True,  # Placeholder
                precision=0,
                scale=0,
            )
        )

    # Placeholder for partition info
    partition_info = [PartitionInfo(rowCount=len(data), uncompressedSize=0, compressedSize=0)]

    result_set_meta_data = ResultSetMetadata(
        numRows=len(data),
        format="jsonv2",
        rowType=row_types,
        partitionInfo=partition_info,
    )

    return {"resultSet": result_set, "resultSetMetaData": result_set_meta_data}
