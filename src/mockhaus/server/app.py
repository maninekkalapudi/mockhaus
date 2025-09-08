"""
This module sets up and configures the main FastAPI application for the Mockhaus server.

It initializes the FastAPI app, sets up the application lifecycle events (lifespan)
for managing server state, includes all the necessary API routers for different
endpoints (like query, health, and sessions), and configures middleware for
CORS, logging, and debugging.
"""

import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from ..banner import print_server_banner
from .middleware.cors import add_cors_middleware
from .middleware.debug_logging import add_debug_logging_middleware
from .middleware.logging import add_logging_middleware
from .routes import health, query, sessions
from .state import server_state


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Manages the application's lifecycle events.

    This context manager handles startup and shutdown logic. On startup, it
    initializes the server state and prints a startup banner. On shutdown, it
    gracefully cleans up resources managed by the server state, such as active sessions.
    """
    # Startup: initialize server state and print banner
    host = os.environ.get("MOCKHAUS_HOST", "0.0.0.0")
    port = int(os.environ.get("MOCKHAUS_PORT", "8080"))
    print_server_banner(host, port)
    yield
    # Shutdown: cleanup server state
    await server_state.shutdown()


# Create the main FastAPI application instance
app = FastAPI(
    title="Mockhaus Server",
    description="Snowflake proxy with DuckDB backend - HTTP API for SQL translation and execution",
    version="0.3.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# Add middleware to the application
# CORS middleware allows for cross-origin requests, essential for web clients
add_cors_middleware(app)
# Logging middleware records information about each incoming request
add_logging_middleware(app)

# Add debug logging if MOCKHAUS_DEBUG is set
debug_enabled = os.environ.get("MOCKHAUS_DEBUG", "").lower() in ("true", "1", "yes")
add_debug_logging_middleware(app, debug=debug_enabled)

# Include the API routers
# The query router handles all SQL execution endpoints
app.include_router(query.router, prefix="/api/v1")
# The health router provides a simple health check endpoint
app.include_router(health.router, prefix="/api/v1")
app.include_router(sessions.router, prefix="/api/v1")


@app.get("/")
async def root() -> dict[str, str]:
    """
    Provides a basic root endpoint with server information.

    Returns:
        A dictionary containing the server name, version, and documentation URL.
    """
    return {
        "name": "Mockhaus Server",
        "version": "0.3.0",
        "description": "Snowflake proxy with DuckDB backend",
        "docs_url": "/docs",
        "health_url": "/api/v1/health",
    }
