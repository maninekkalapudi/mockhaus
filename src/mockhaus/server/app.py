"""FastAPI application setup for Mockhaus server."""

from fastapi import FastAPI

from .middleware.cors import add_cors_middleware
from .middleware.logging import add_logging_middleware
from .routes import health, query

# Create FastAPI application
app = FastAPI(
    title="Mockhaus Server",
    description="Snowflake proxy with DuckDB backend - HTTP API for SQL translation and execution",
    version="0.3.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# Add middleware
add_cors_middleware(app)
add_logging_middleware(app)

# Include routers
app.include_router(query.router, prefix="/api/v1")
app.include_router(health.router, prefix="/api/v1")


@app.get("/")
async def root():
    """Root endpoint with basic server information."""
    return {
        "name": "Mockhaus Server",
        "version": "0.3.0",
        "description": "Snowflake proxy with DuckDB backend",
        "docs_url": "/docs",
        "health_url": "/api/v1/health",
    }
