#!/usr/bin/env python3
"""Generate test PARQUET files with different compression formats for integration testing."""

from pathlib import Path

import pandas as pd  # type: ignore[import-untyped]
import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]


def create_test_data():
    """Create test data for PARQUET files."""
    data = {
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "age": [25, 30, 35, 28, 32],
        "salary": [50000.0, 60000.0, 75000.0, 55000.0, 65000.0],
        "is_active": [True, True, False, True, True],
        "department": ["Engineering", "Sales", "Marketing", "Engineering", "Sales"],
    }
    return pd.DataFrame(data)


def create_test_data_with_nulls():
    """Create test data with NULL values for testing NULL handling."""
    data = {
        "id": [1, 2, None, 4, 5],
        "name": ["Alice", None, "Charlie", "Diana", ""],
        "age": [25, 30, None, 28, 32],
        "salary": [50000.0, None, 75000.0, 55000.0, 65000.0],
        "is_active": [True, None, False, True, None],
    }
    return pd.DataFrame(data)


def create_test_data_with_binary():
    """Create test data with binary fields for testing BINARY_AS_TEXT."""
    data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "binary_data": [b"binary1", b"binary2", b"binary3"],
        "text_data": ["text1", "text2", "text3"],
    }
    return pd.DataFrame(data)


def generate_parquet_files():
    """Generate PARQUET files with different compression formats."""
    output_dir = Path(__file__).parent

    # Test data sets
    datasets = {"basic": create_test_data(), "with_nulls": create_test_data_with_nulls(), "with_binary": create_test_data_with_binary()}

    # Compression formats supported by both PyArrow and DuckDB
    compressions = {
        "none": None,  # Uncompressed
        "snappy": "snappy",  # Snappy compression
        "gzip": "gzip",  # GZIP compression
        "brotli": "brotli",  # Brotli compression
        "lz4": "lz4",  # LZ4 compression (if available)
        "zstd": "zstd",  # ZSTD compression (if available)
    }

    generated_files = []

    for dataset_name, df in datasets.items():
        for comp_name, comp_type in compressions.items():
            try:
                filename = f"{dataset_name}_{comp_name}.parquet"
                filepath = output_dir / filename

                # Convert to PyArrow table for more control
                table = pa.Table.from_pandas(df)

                # Write PARQUET file with specified compression
                pq.write_table(
                    table,
                    filepath,
                    compression=comp_type,
                    use_dictionary=True,  # Enable dictionary encoding
                    row_group_size=3,  # Small row groups for testing
                    write_statistics=True,  # Include statistics
                )

                generated_files.append(filename)

                # Verify file was created successfully
                pq.ParquetFile(filepath)  # Just verify it's readable

            except Exception:
                # Generation failed - continue with other files
                pass

    return generated_files


def create_file_info():
    """Create a JSON file with information about the generated test files."""
    output_dir = Path(__file__).parent
    info_file = output_dir / "test_files_info.json"

    import json

    file_info = {
        "description": "Test PARQUET files for Mockhaus integration testing",
        "datasets": {
            "basic": {
                "description": "Basic test data with common data types",
                "columns": ["id", "name", "age", "salary", "is_active", "department"],
                "rows": 5,
            },
            "with_nulls": {
                "description": "Test data with NULL values to test NULL handling",
                "columns": ["id", "name", "age", "salary", "is_active"],
                "rows": 5,
                "notes": "Contains NULL values in various columns",
            },
            "with_binary": {
                "description": "Test data with binary fields for BINARY_AS_TEXT testing",
                "columns": ["id", "name", "binary_data", "text_data"],
                "rows": 3,
                "notes": "Contains binary data column for testing binary_as_string option",
            },
        },
        "compressions": {
            "none": "Uncompressed PARQUET files",
            "snappy": "Snappy compression (default in DuckDB)",
            "gzip": "GZIP compression",
            "brotli": "Brotli compression",
            "lz4": "LZ4 compression",
            "zstd": "ZSTD compression",
        },
    }

    with open(info_file, "w") as f:
        json.dump(file_info, f, indent=2)

    # File info created successfully


if __name__ == "__main__":
    # Generate test PARQUET files
    generated_files = generate_parquet_files()
    create_file_info()
    # Files generated successfully
