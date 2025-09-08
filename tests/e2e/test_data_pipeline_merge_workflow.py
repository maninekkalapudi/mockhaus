"""End-to-End test for data pipeline MERGE workflow.

This test validates the complete Mockhaus data pipeline workflow including:
- File format and stage creation
- Data ingestion via COPY INTO operations
- Complex MERGE operations with INSERT and UPDATE logic
- Data integrity validation throughout the process
"""

import sys
from pathlib import Path

import pytest

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from tests.e2e.conftest import DataValidator, E2EClient, SQLWorkflowExecutor


@pytest.mark.e2e
@pytest.mark.data_pipeline
class TestDataPipelineMergeWorkflow:
    """Test complete data pipeline workflow with MERGE operations."""

    def test_data_pipeline_merge_workflow(self, e2e_client: E2EClient, temp_stage_files) -> None:
        """
        Test the complete data pipeline MERGE workflow.

        This test executes a comprehensive workflow that includes:
        1. Creating file formats and stages
        2. Setting up staging and final tables
        3. Loading initial data via COPY INTO
        4. Executing first MERGE (all inserts)
        5. Loading incremental data
        6. Executing second MERGE (updates + inserts)
        7. Validating all data transformations
        """
        # Setup: Create session and prepare file paths
        session = e2e_client.create_session(type="memory", ttl_seconds=3600)
        session_id = session["session_id"]

        initial_file, incremental_file = temp_stage_files

        # Initialize utilities
        executor = SQLWorkflowExecutor(e2e_client, session_id)
        validator = DataValidator(e2e_client, session_id)

        print("\n=== Starting Data Pipeline MERGE Workflow Test ===")
        print(f"Session ID: {session_id}")
        print(f"Initial data file: {initial_file}")
        print(f"Incremental data file: {incremental_file}")

        try:
            # Phase 1: Setup file formats and stages
            temp_files_dir = initial_file.parent
            self._setup_file_format_and_stage(executor, temp_files_dir)

            # Phase 2: Create tables
            self._create_tables(executor)

            # Phase 3: Execute initial data pipeline
            self._execute_initial_data_pipeline(executor, validator, initial_file)

            # Phase 4: Execute incremental data pipeline
            self._execute_incremental_data_pipeline(executor, validator, incremental_file)

            # Phase 5: Final validation
            self._validate_final_results(validator)

            print("\n=== Data Pipeline MERGE Workflow Test PASSED ===")

        except Exception:
            # Print execution summary for debugging
            summary = executor.get_execution_summary()
            print("\n=== Test FAILED - Execution Summary ===")
            print(f"Total steps: {summary['total_steps']}")
            print(f"Successful steps: {summary['successful_steps']}")
            print(f"Failed steps: {summary['failed_steps']}")

            # Print last few steps for debugging
            if summary["execution_log"]:
                print("\nLast execution steps:")
                for step in summary["execution_log"][-3:]:
                    print(f"  {step.get('step_name', 'Unknown')}: {step.get('success', False)}")
                    if not step.get("success", False):
                        print(f"    Error: {step.get('error', 'Unknown error')}")

            raise

    def _setup_file_format_and_stage(self, executor: SQLWorkflowExecutor, temp_files_dir: Path) -> None:
        """Set up file format and stage for data ingestion."""
        print("\n--- Phase 1: Setting up file format and stage ---")

        # Create CSV file format
        format_sql = """
        CREATE FILE FORMAT csv_customer_format
        TYPE = 'CSV'
        FIELD_DELIMITER = ','
        SKIP_HEADER = 1
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        ENCODING = 'UTF-8'
        ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
        """

        result = executor.execute_sql_step(format_sql, "Create CSV file format")
        assert result["success"], f"Failed to create file format: {result.get('error')}"
        print("✓ CSV file format created successfully")

        # Create external stage pointing to temp directory
        stage_sql = f"""
        CREATE STAGE customer_data_stage
        URL = 'file://{temp_files_dir}'
        FILE_FORMAT = csv_customer_format
        """

        result = executor.execute_sql_step(stage_sql, "Create data stage")
        assert result["success"], f"Failed to create stage: {result.get('error')}"
        print(f"✓ External stage created successfully (pointing to {temp_files_dir})")

    def _create_tables(self, executor: SQLWorkflowExecutor) -> None:
        """Create staging and final tables."""
        print("\n--- Phase 2: Creating tables ---")

        # Create final target table
        final_table_sql = """
        CREATE TABLE customer_final (
            customer_id INTEGER PRIMARY KEY,
            name VARCHAR(100),
            email VARCHAR(100),
            signup_date DATE,
            status VARCHAR(20),
            last_updated TIMESTAMP,
            version INTEGER DEFAULT 1
        )
        """

        result = executor.execute_sql_step(final_table_sql, "Create final table")
        assert result["success"], f"Failed to create final table: {result.get('error')}"
        print("✓ Final table created successfully")

        # Create staging table
        staging_table_sql = """
        CREATE TABLE customer_staging (
            customer_id INTEGER,
            name VARCHAR(100),
            email VARCHAR(100),
            signup_date DATE,
            status VARCHAR(20),
            last_updated TIMESTAMP
        )
        """

        result = executor.execute_sql_step(staging_table_sql, "Create staging table")
        assert result["success"], f"Failed to create staging table: {result.get('error')}"
        print("✓ Staging table created successfully")

    def _execute_initial_data_pipeline(self, executor: SQLWorkflowExecutor, validator: DataValidator, initial_file: Path) -> None:  # noqa: ARG002
        """Execute the initial data load and merge."""
        print("\n--- Phase 3: Initial data pipeline ---")

        # Load initial data into staging
        copy_sql = """
        COPY INTO customer_staging
        FROM '@customer_data_stage/customer_initial_data.csv'
        FILE_FORMAT = csv_customer_format
        """

        result = executor.execute_sql_step(copy_sql, "Load initial data to staging")
        assert result["success"], f"Failed to load initial data: {result.get('error')}"
        print("✓ Initial data loaded to staging")

        # Validate staging data count
        staging_count = validator.get_record_count("customer_staging")
        assert staging_count == 3, f"Expected 3 records in staging, got {staging_count}"
        print(f"✓ Staging table has {staging_count} records")

        # Execute first MERGE operation
        merge_sql = """
        MERGE INTO customer_final AS target
        USING customer_staging AS source
        ON target.customer_id = source.customer_id
        WHEN MATCHED THEN
            UPDATE SET
                name = source.name,
                email = source.email,
                status = source.status,
                last_updated = source.last_updated,
                version = target.version + 1
        WHEN NOT MATCHED THEN
            INSERT (customer_id, name, email, signup_date, status, last_updated, version)
            VALUES (source.customer_id, source.name, source.email, source.signup_date, source.status, source.last_updated, 1)
        """

        result = executor.execute_sql_step(merge_sql, "Execute first MERGE")
        assert result["success"], f"Failed to execute first MERGE: {result.get('error')}"
        print("✓ First MERGE operation completed")

        # Validate final table after first merge
        final_count = validator.get_record_count("customer_final")
        assert final_count == 3, f"Expected 3 records in final table, got {final_count}"
        print(f"✓ Final table has {final_count} records after first MERGE")

        # Clear staging table
        clear_sql = "DELETE FROM customer_staging"
        result = executor.execute_sql_step(clear_sql, "Clear staging table")
        assert result["success"], f"Failed to clear staging table: {result.get('error')}"
        print("✓ Staging table cleared")

    def _execute_incremental_data_pipeline(self, executor: SQLWorkflowExecutor, validator: DataValidator, incremental_file: Path) -> None:  # noqa: ARG002
        """Execute the incremental data load and merge."""
        print("\n--- Phase 4: Incremental data pipeline ---")

        # Load incremental data into staging
        copy_sql = """
        COPY INTO customer_staging
        FROM '@customer_data_stage/customer_incremental_data.csv'
        FILE_FORMAT = csv_customer_format
        """

        result = executor.execute_sql_step(copy_sql, "Load incremental data to staging")
        assert result["success"], f"Failed to load incremental data: {result.get('error')}"
        print("✓ Incremental data loaded to staging")

        # Validate staging data count
        staging_count = validator.get_record_count("customer_staging")
        assert staging_count == 3, f"Expected 3 records in staging, got {staging_count}"
        print(f"✓ Staging table has {staging_count} records")

        # Execute second MERGE operation (updates + inserts)
        merge_sql = """
        MERGE INTO customer_final AS target
        USING customer_staging AS source
        ON target.customer_id = source.customer_id
        WHEN MATCHED THEN
            UPDATE SET
                name = source.name,
                email = source.email,
                status = source.status,
                last_updated = source.last_updated,
                version = target.version + 1
        WHEN NOT MATCHED THEN
            INSERT (customer_id, name, email, signup_date, status, last_updated, version)
            VALUES (source.customer_id, source.name, source.email, source.signup_date, source.status, source.last_updated, 1)
        """

        result = executor.execute_sql_step(merge_sql, "Execute second MERGE")
        assert result["success"], f"Failed to execute second MERGE: {result.get('error')}"
        print("✓ Second MERGE operation completed")

        # Validate final table after second merge
        final_count = validator.get_record_count("customer_final")
        assert final_count == 4, f"Expected 4 records in final table, got {final_count}"
        print(f"✓ Final table has {final_count} records after second MERGE")

    def _validate_final_results(self, validator: DataValidator) -> None:
        """Perform comprehensive validation of final results."""
        print("\n--- Phase 5: Final validation ---")

        # Get all final data for detailed validation
        final_data = validator.get_table_data("customer_final", "customer_id")

        # Validate total record count
        assert len(final_data) == 4, f"Expected 4 final records, got {len(final_data)}"
        print("✓ Final record count is correct (4 records)")

        # Create lookup for easier validation
        customers = {row["customer_id"]: row for row in final_data}

        # Validate customer 1001 (unchanged from initial load)
        customer_1001 = customers[1001]
        assert customer_1001["name"] == "John Doe"
        assert customer_1001["email"] == "john@example.com"
        assert customer_1001["status"] == "active"
        assert customer_1001["version"] == 1  # Should not be updated
        print("✓ Customer 1001 validation passed (unchanged)")

        # Validate customer 1002 (updated in incremental)
        customer_1002 = customers[1002]
        assert customer_1002["name"] == "Jane Smith Updated"
        assert customer_1002["email"] == "jane.smith@example.com"
        assert customer_1002["status"] == "active"
        assert customer_1002["version"] == 2  # Should be updated
        print("✓ Customer 1002 validation passed (updated)")

        # Validate customer 1003 (status updated from pending to active)
        customer_1003 = customers[1003]
        assert customer_1003["name"] == "Bob Johnson"
        assert customer_1003["email"] == "bob@example.com"
        assert customer_1003["status"] == "active"  # Changed from pending
        assert customer_1003["version"] == 2  # Should be updated
        print("✓ Customer 1003 validation passed (status updated)")

        # Validate customer 1004 (new insert)
        customer_1004 = customers[1004]
        assert customer_1004["name"] == "Alice Wilson"
        assert customer_1004["email"] == "alice@example.com"
        assert customer_1004["status"] == "active"
        assert customer_1004["version"] == 1  # New record
        print("✓ Customer 1004 validation passed (new insert)")

        # Validate version distribution
        version_1_count = sum(1 for c in final_data if c["version"] == 1)
        version_2_count = sum(1 for c in final_data if c["version"] == 2)

        assert version_1_count == 2, f"Expected 2 records with version 1, got {version_1_count}"
        assert version_2_count == 2, f"Expected 2 records with version 2, got {version_2_count}"
        print("✓ Version tracking validation passed")

        print("\n=== All validations passed successfully! ===")

    def test_session_isolation_during_workflow(self, e2e_client: E2EClient) -> None:
        """Test that workflow executions are properly isolated between sessions."""
        print("\n=== Testing Session Isolation ===")

        # Create two separate sessions
        session1 = e2e_client.create_session(type="memory")
        session2 = e2e_client.create_session(type="memory")

        session1_id = session1["session_id"]
        session2_id = session2["session_id"]

        # Create table in session 1
        create_table_sql = """
        CREATE TABLE isolation_test (
            id INTEGER PRIMARY KEY,
            session_name VARCHAR(50)
        )
        """

        result1 = e2e_client.execute_query(create_table_sql, session1_id)
        assert result1["success"], f"Failed to create table in session 1: {result1.get('error')}"

        # Insert data in session 1
        insert_sql = "INSERT INTO isolation_test VALUES (1, 'session1')"
        result1 = e2e_client.execute_query(insert_sql, session1_id)
        assert result1["success"], f"Failed to insert data in session 1: {result1.get('error')}"

        # Try to query the table from session 2 (should fail)
        result2 = e2e_client.execute_query("SELECT * FROM isolation_test", session2_id)
        assert not result2["success"], "Table should not exist in session 2"

        # Verify session 1 still has the data
        result1 = e2e_client.execute_query("SELECT * FROM isolation_test", session1_id)
        assert result1["success"], "Session 1 should still have access to its data"
        assert len(result1["data"]) == 1
        assert result1["data"][0]["session_name"] == "session1"

        print("✓ Session isolation working correctly")

    def test_error_handling_in_workflow(self, e2e_client: E2EClient) -> None:
        """Test error handling during workflow execution."""
        print("\n=== Testing Error Handling ===")

        session = e2e_client.create_session(type="memory")
        session_id = session["session_id"]

        executor = SQLWorkflowExecutor(e2e_client, session_id)

        # Try to create a table with invalid syntax
        invalid_sql = "CREATE TABLE invalid_table (id INVALID_TYPE)"
        result = executor.execute_sql_step(invalid_sql, "Invalid table creation")

        assert not result["success"], "Invalid SQL should fail"
        assert "error" in result
        print("✓ Error handling working correctly")

        # Verify session is still functional after error
        valid_sql = "SELECT 1 as test"
        result = executor.execute_sql_step(valid_sql, "Recovery test")

        assert result["success"], "Session should recover after error"
        print("✓ Session recovery after error working correctly")
