"""Request logging middleware."""

import time
from typing import Callable

from fastapi import FastAPI, Request, Response
from fastapi.routing import APIRoute


def add_logging_middleware(app: FastAPI) -> None:
    """
    Add request logging middleware to the FastAPI application.

    Logs request method, path, processing time, and response status.
    """

    @app.middleware("http")
    async def log_requests(request: Request, call_next: Callable) -> Response:
        start_time = time.time()

        response = await call_next(request)

        process_time = time.time() - start_time

        # Log request details
        print(f"{request.method} {request.url.path} - {response.status_code} - {process_time:.3f}s")

        # Add processing time header
        response.headers["X-Process-Time"] = str(process_time)

        return response
