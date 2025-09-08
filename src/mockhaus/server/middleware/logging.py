"""
This module provides a standard request logging middleware for FastAPI.

It adds a simple middleware to time each request and add the processing time
as a custom `X-Process-Time` header to the response. This is useful for basic
performance monitoring.
"""

import time
from collections.abc import Callable
from typing import Any, cast

from fastapi import FastAPI, Request, Response


def add_logging_middleware(app: FastAPI) -> None:
    """
    Adds a request logging middleware to the FastAPI application.

    This middleware calculates the processing time for each request and adds it
    as a custom `X-Process-Time` header to the response.

    Args:
        app: The `FastAPI` application instance.
    """

    @app.middleware("http")
    async def log_requests(request: Request, call_next: Callable[[Request], Any]) -> Response:
        """
        Middleware function to time requests and add a process time header.

        Args:
            request: The incoming `Request` object.
            call_next: The next middleware or endpoint in the processing chain.

        Returns:
            The `Response` object with the added process time header.
        """
        start_time = time.time()

        response = await call_next(request)

        process_time = time.time() - start_time

        # Log request details

        # Add processing time header
        response.headers["X-Process-Time"] = str(process_time)

        return cast(Response, response)
