"""
This module sets up the FastAPI application for the Mockhaus server.

It configures the main FastAPI app, adds essential middleware for CORS and
logging, and includes the API routers for different functionalities like
query execution and health checks.
"""

from fastapi import FastAPI

from .middleware.cors import add_cors_middleware
from .middleware.logging import add_logging_middleware
from .routes import health, query

# Create the main FastAPI application instance
app = FastAPI(
    title="Mockhaus Server",
    description="Snowflake proxy with DuckDB backend - HTTP API for SQL translation and execution",
    version="0.3.0",
    docs_url="/docs",  # Enable Swagger UI documentation
    redoc_url="/redoc",  # Enable ReDoc documentation
)

# Add middleware to the application
# CORS middleware allows for cross-origin requests, essential for web clients
add_cors_middleware(app)
# Logging middleware records information about each incoming request
add_logging_middleware(app)

# Include the API routers
# The query router handles all SQL execution endpoints
app.include_router(query.router, prefix="/api/v1")
# The health router provides a simple health check endpoint
app.include_router(health.router, prefix="/api/v1")


@app.get("/")
async def root():
    """Provides basic server information at the root endpoint."""
    return {
        "name": "Mockhaus Server",
        "version": "0.3.0",
        "description": "Snowflake proxy with DuckDB backend",
        "docs_url": "/docs",
        "health_url": "/api/v1/health"
    }
