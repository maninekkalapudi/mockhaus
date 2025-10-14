# Changelog

All notable changes to this project will be documented in this file.

### 2025-10-14

- **Feature: Initial Snowflake API Skeleton (Issue #6)**
  - Created `src/mockhaus/server/snowflake_api/routes.py` with placeholder endpoints.
  - Integrated the new API router into `src/mockhaus/server/app.py`.
  - Added basic skeleton tests in `tests/unit/server/snowflake_api/test_routes.py`.
- **Feature: Implement AsyncExecutor for Background Statement Execution (Issue #9)**
  - Created `src/mockhaus/server/snowflake_api/async_executor.py` for simulating asynchronous tasks.
  - Modified `src/mockhaus/server/snowflake_api/statement_manager.py` to use `AsyncExecutor` for background statement execution, updating status from `SUBMITTED` to `RUNNING` then `SUCCEEDED`.
  - Updated unit tests in `tests/unit/server/snowflake_api/test_statement_manager.py` and `tests/unit/server/snowflake_api/test_routes.py` to reflect asynchronous behavior and ensure reliable testing.
- **Refactor: Update Pydantic Configuration**
  - Fixed a deprecation warning by refactoring Pydantic models to use `ConfigDict`.

### 2025-09-28

- **Feature: Add Pydantic Models for Snowflake API (PR #5)**
  - Created `src/mockhaus/server/snowflake_api/models.py` to define data structures for the Snowflake SQL API.
  - Added corresponding unit tests in `tests/unit/server/snowflake_api/test_models.py`.

### 2025-09-27

- **Fix: Windows Path Handling in Stages (PR #4)**
  - Corrected path handling for `file://` URLs in `CREATE STAGE` commands to resolve errors on Windows environments.
