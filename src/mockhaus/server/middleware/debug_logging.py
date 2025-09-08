"""This module provides a debug logging middleware for FastAPI.

When enabled, this middleware intercepts all incoming requests and outgoing
responses to log detailed information to stderr, including headers and bodies.
This is extremely useful for debugging client-server interactions during
development but should be disabled in production due to its performance overhead
and potential for leaking sensitive data in logs.
"""

import json
import sys
import time
from collections.abc import AsyncGenerator, Callable
from typing import Any, cast

from fastapi import FastAPI, Request, Response
from fastapi.responses import StreamingResponse


async def set_body(request: Request, body: bytes) -> None:
    """
    Attaches the request body to the request state for later access.

    This is a helper function to store the body after it has been read, so it
    doesn't need to be read again.

    Args:
        request: The FastAPI `Request` object.
        body: The raw request body as bytes.
    """
    request.state._body = body


async def get_body(request: Request) -> bytes:
    """
    Retrieves the request body, reading it from the stream if necessary.

    This function ensures the request body is read only once and then cached
    on the request state.

    Args:
        request: The FastAPI `Request` object.

    Returns:
        The raw request body as bytes.
    """
    if not hasattr(request, "_body"):
        body = await request.body()
        await set_body(request, body)
    return request._body  # type: ignore[attr-defined, no-any-return]


def add_debug_logging_middleware(app: FastAPI, debug: bool = True) -> None:
    """
    Adds a debug logging middleware to the FastAPI application.

    This middleware logs the full details of every request and response,
    including headers and body content. It is intended for development and
    troubleshooting purposes only.

    Args:
        app: The `FastAPI` application instance.
        debug: A boolean to enable or disable the middleware. Defaults to True.
    """
    if not debug:
        return

    @app.middleware("http")
    async def debug_log_requests(request: Request, call_next: Callable[[Request], Any]) -> Response:
        """Middleware function to log request and response details."""
        # Log request details
        sys.stderr.write(f"\n{'=' * 60}\n")
        sys.stderr.write(f"REQUEST: {request.method} {request.url.path}\n")
        sys.stderr.write(f"Headers: {dict(request.headers)}\n")
        # Get request body for POST requests
        if request.method == "POST":
            body = await get_body(request)
            if body:
                try:
                    body_json = json.loads(body)
                    sys.stderr.write(f"Body: {json.dumps(body_json, indent=2)}\n")
                except json.JSONDecodeError:
                    sys.stderr.write(f"Body (raw): {body.decode('utf-8', errors='ignore')}\n")
        # Process request
        start_time = time.time()
        response = await call_next(request)
        process_time = time.time() - start_time

        # Log response details
        sys.stderr.write(f"\nRESPONSE: {response.status_code}\n")
        sys.stderr.write(f"Process Time: {process_time:.3f}s\n")
        sys.stderr.write(f"Headers: {dict(response.headers)}\n")

        # For streaming responses, we need to capture the body
        if isinstance(response, StreamingResponse):
            # Collect the response body
            body_chunks: list[bytes] = []
            async for chunk in response.body_iterator:
                if isinstance(chunk, bytes):
                    body_chunks.append(chunk)
                elif isinstance(chunk, str):
                    body_chunks.append(chunk.encode("utf-8"))
                else:
                    # Handle memoryview or other types
                    body_chunks.append(bytes(chunk))
            # Create the body string
            response_body = b"".join(body_chunks).decode("utf-8", errors="ignore")
            # Log response
            sys.stderr.write(f"\nRESPONSE: {response.status_code}\n")
            sys.stderr.write(f"Process Time: {process_time:.3f}s\n")
            sys.stderr.write(f"Headers: {dict(response.headers)}\n")
            try:
                response_json = json.loads(response_body)
                sys.stderr.write(f"Body: {json.dumps(response_json, indent=2)}\n")
            except json.JSONDecodeError:
                sys.stderr.write(f"Body (raw): {response_body[:500]}...\n")  # First 500 chars

            sys.stderr.write(f"{'=' * 60}\n")

            # Return a new StreamingResponse with the same content
            async def generate() -> AsyncGenerator[bytes, None]:
                for chunk in body_chunks:
                    yield chunk

            return StreamingResponse(generate(), status_code=response.status_code, headers=dict(response.headers), media_type=response.media_type)

        # For regular responses
        sys.stderr.write(f"\nRESPONSE: {response.status_code}\n")
        sys.stderr.write(f"Process Time: {process_time:.3f}s\n")
        sys.stderr.write(f"{'=' * 60}\n")

        return cast(Response, response)
