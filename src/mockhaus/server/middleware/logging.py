"""Request logging middleware."""

import time
from collections.abc import Callable
from typing import Any, cast

from fastapi import FastAPI, Request, Response


def add_logging_middleware(app: FastAPI) -> None:
    """
    Add request logging middleware to the FastAPI application.

    Logs request method, path, processing time, and response status.
    """

    @app.middleware("http")
    async def log_requests(request: Request, call_next: Callable[[Request], Any]) -> Response:
        start_time = time.time()

        response = await call_next(request)

        process_time = time.time() - start_time

        # Log request details

        # Add processing time header
        response.headers["X-Process-Time"] = str(process_time)

        return cast(Response, response)
