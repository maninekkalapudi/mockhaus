import asyncio


class AsyncExecutor:
    """
    A simple executor to simulate asynchronous background tasks.
    """

    async def execute(self, task_id: str, duration: float = 0.001):
        """
        Simulates an asynchronous task by sleeping for a given duration.
        """
        print(f"[AsyncExecutor] Task {task_id} started, simulating work for {duration} seconds...")
        await asyncio.sleep(duration)
        print(f"[AsyncExecutor] Task {task_id} finished.")
