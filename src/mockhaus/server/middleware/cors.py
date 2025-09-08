"""
This module configures the Cross-Origin Resource Sharing (CORS) middleware.

CORS is a security feature that controls how web pages in one domain can request
and interact with resources from a server in a different domain. This middleware
is essential for allowing web-based clients (e.g., a React or Vue frontend)
to communicate with the Mockhaus server.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware


def def add_cors_middleware(app: FastAPI) -> None:
    """
    Adds the CORS middleware to the FastAPI application.

    This middleware allows cross-origin requests, which is necessary for web-based
    clients hosted on different domains to access the API.

    Note:
        For production environments, the `allow_origins` list should be configured
        restrictively to only include the domains of trusted clients, rather than
        using the wildcard "*" which allows all origins.

    Args:
        app: The `FastAPI` application instance.
    """
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # In production, specify allowed origins
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=["*"],
    )
