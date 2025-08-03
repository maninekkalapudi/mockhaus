"""Debug logging middleware for detailed request/response logging."""

import json
import sys
import time
from collections.abc import AsyncGenerator, Callable
from typing import Any, cast

from fastapi import FastAPI, Request, Response
from fastapi.responses import StreamingResponse


async def set_body(request: Request, body: bytes) -> None:
    """Store body in request state for logging."""
    request._body = body  # type: ignore[attr-defined]


async def get_body(request: Request) -> bytes:
    """Get request body, reading it if necessary."""
    if not hasattr(request, "_body"):
        body = await request.body()
        await set_body(request, body)
    return request._body  # type: ignore[attr-defined, no-any-return]


def add_debug_logging_middleware(app: FastAPI, debug: bool = True) -> None:
    """
    Add debug logging middleware to log all requests and responses.

    Args:
        app: FastAPI application instance
        debug: Whether to enable debug logging
    """
    if not debug:
        return

    @app.middleware("http")
    async def debug_log_requests(request: Request, call_next: Callable[[Request], Any]) -> Response:
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
