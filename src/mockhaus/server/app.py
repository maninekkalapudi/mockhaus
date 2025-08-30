"""FastAPI application setup for Mockhaus server."""

import os
import sys
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
    """Manage application lifecycle."""
    # Startup: initialize server state and print banner
    host = os.environ.get("MOCKHAUS_HOST", "0.0.0.0")
    port = int(os.environ.get("MOCKHAUS_PORT", "8080"))
    print_server_banner(host, port)
    yield
    # Shutdown: cleanup server state
    await server_state.shutdown()


# Create FastAPI application
app = FastAPI(
    title="Mockhaus Server",
    description="Snowflake proxy with DuckDB backend - HTTP API for SQL translation and execution",
    version="0.3.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# Add middleware
add_cors_middleware(app)
add_logging_middleware(app)

# Add debug logging if MOCKHAUS_DEBUG is set
debug_enabled = os.environ.get("MOCKHAUS_DEBUG", "").lower() in ("true", "1", "yes")
add_debug_logging_middleware(app, debug=debug_enabled)

# Include routers
app.include_router(query.router, prefix="/api/v1")
app.include_router(health.router, prefix="/api/v1")
app.include_router(sessions.router, prefix="/api/v1")


@app.get("/")
async def root() -> dict[str, str]:
    """Root endpoint with basic server information."""
    return {
        "name": "Mockhaus Server",
        "version": "0.3.0",
        "description": "Snowflake proxy with DuckDB backend",
        "docs_url": "/docs",
        "health_url": "/api/v1/health",
    }
