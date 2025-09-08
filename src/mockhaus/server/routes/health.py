"""
This module defines the health check endpoint for the Mockhaus server.

It provides a simple way to verify that the server is running and responsive.
"""

import time
from typing import Any

from fastapi import APIRouter

from ..models.response import HealthResponse

router = APIRouter(tags=["health"])

# Track server start time for uptime calculation
_start_time = time.time()


@router.get("/health", response_model=HealthResponse)
async def health_check() -> Any:
    """
    Provides a health check endpoint for monitoring the server's status.

    Returns:
        A `HealthResponse` object containing the server's status, version,
        and uptime.
    """
    uptime = time.time() - _start_time

    return HealthResponse(status="healthy", version="0.3.0", uptime=uptime)
