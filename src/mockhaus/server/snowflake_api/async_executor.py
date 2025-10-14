import asyncio
import duckdb

from mockhaus.executor import MockhausExecutor, QueryResult
from .result_mapper import map_duckdb_to_snowflake_results


class AsyncExecutor:
    """
    Executes SQL queries asynchronously using MockhausExecutor and maps results.
    """

    def __init__(self):
        self._mockhaus_executor = MockhausExecutor()
        # Ensure connection is established for MockhausExecutor
        self._mockhaus_executor.connect()

    async def execute(self, task_id: str, sql: str) -> dict:
        """
        Executes a SQL query and returns mapped results or error information.
        """
        print(f"[AsyncExecutor] Task {task_id} started, executing SQL: {sql[:50]}...")
        try:
            query_result: QueryResult = self._mockhaus_executor.execute_snowflake_sql(sql)

            if query_result.success:
                mapped_results = None
                if query_result.data is not None and query_result.columns is not None:
                    mapped_results = map_duckdb_to_snowflake_results(query_result.data, query_result.columns)

                print(f"[AsyncExecutor] Task {task_id} finished successfully.")
                return {"status": "SUCCEEDED", "results": mapped_results}
            else:
                print(f"[AsyncExecutor] Task {task_id} failed: {query_result.error}")
                return {"status": "FAILED", "error_message": query_result.error, "error_code": "00001"}
        except Exception as e:
            print(f"[AsyncExecutor] Task {task_id} failed with unexpected error: {e}")
            return {"status": "FAILED", "error_message": str(e), "error_code": "00002"}
