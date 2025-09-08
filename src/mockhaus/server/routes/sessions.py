"""Session management endpoints."""

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ..models.session import SessionStorageConfig, SessionType
from ..state import server_state

router = APIRouter(tags=["sessions"])


class CreateSessionRequest(BaseModel):
    """Request model for creating a session."""

    session_id: str | None = None
    type: str = "memory"  # "memory" or "persistent"
    ttl_seconds: int | None = None
    storage: dict[str, Any] | None = None  # For persistent sessions


@router.post("/sessions", response_model=dict[str, Any])
async def create_session(request: CreateSessionRequest) -> dict[str, Any]:
    """
    Create a new session (memory or persistent).

    For persistent sessions, provide storage configuration:
    {
        "type": "persistent",
        "storage": {
            "type": "local",
            "path": "/path/to/database.db"
        }
    }
    """
    session_manager = await server_state.get_session_manager()

    try:
        # Parse session type
        session_type = SessionType.MEMORY if request.type == "memory" else SessionType.PERSISTENT

        # Create storage config if provided
        storage_config = None
        if session_type == SessionType.PERSISTENT:
            if not request.storage:
                raise ValueError("Persistent session requires storage configuration")

            storage_config = SessionStorageConfig(
                type=request.storage.get("type", "local"),
                path=request.storage.get("path", f"session_{request.session_id or 'new'}.db"),
                credentials=request.storage.get("credentials"),
                options=request.storage.get("options"),
            )

        # Create the session
        session_context = await session_manager.get_or_create_session(
            session_id=request.session_id, ttl_seconds=request.ttl_seconds, session_type=session_type, storage_config=storage_config
        )

        session_info = session_context.get_info()

        # Add storage info if persistent
        if session_type == SessionType.PERSISTENT and session_context.storage_backend:
            session_info["storage"] = session_context.storage_backend.get_info()

        return {"success": True, "session": session_info, "message": f"Created {session_type.value} session {session_context.session_id}"}

    except ValueError as e:
        raise HTTPException(status_code=400, detail={"error": "INVALID_REQUEST", "detail": str(e)}) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "SESSION_CREATION_ERROR", "detail": f"Failed to create session: {str(e)}"}) from e


@router.get("/sessions", response_model=dict[str, Any])
async def list_sessions() -> dict[str, Any]:
    """
    List all active sessions.

    Returns:
        Dictionary containing session information and statistics
    """
    session_manager = await server_state.get_session_manager()

    try:
        # Get all active sessions with detailed info
        sessions = await session_manager.list_sessions()
        stats = session_manager.get_stats()
        details = session_manager.get_session_details()

        return {
            "success": True,
            "sessions": sessions,
            "session_details": details,
            "statistics": stats,
            "limits": {
                "max_sessions": stats["max_sessions"],
                "active_sessions": stats["active_sessions"],
                "available_slots": stats["max_sessions"] - stats["active_sessions"],
                "usage_percentage": stats["usage_percentage"],
                "eviction_policy": stats["eviction_policy"],
            },
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "SESSION_LIST_ERROR", "detail": f"Failed to list sessions: {str(e)}"}) from e


@router.get("/sessions/{session_id}", response_model=dict[str, Any])
async def get_session_info(session_id: str) -> dict[str, Any]:
    """
    Get information about a specific session.

    Args:
        session_id: Session ID to retrieve information for

    Returns:
        Session information including status, creation time, and metadata
    """
    session_manager = await server_state.get_session_manager()

    try:
        session_context = await session_manager.get_session(session_id)

        if not session_context:
            raise HTTPException(status_code=404, detail={"error": "SESSION_NOT_FOUND", "detail": f"Session {session_id} not found or expired"})

        return {"success": True, "session": session_context.get_info()}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "SESSION_INFO_ERROR", "detail": f"Failed to get session info: {str(e)}"}) from e


@router.delete("/sessions/{session_id}", response_model=dict[str, Any])
async def terminate_session(session_id: str) -> dict[str, Any]:
    """
    Terminate and remove a specific session.

    Args:
        session_id: Session ID to terminate

    Returns:
        Success status and termination confirmation
    """
    session_manager = await server_state.get_session_manager()

    try:
        terminated = await session_manager.terminate_session(session_id)

        if not terminated:
            raise HTTPException(status_code=404, detail={"error": "SESSION_NOT_FOUND", "detail": f"Session {session_id} not found"})

        return {"success": True, "message": f"Session {session_id} terminated successfully"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "SESSION_TERMINATION_ERROR", "detail": f"Failed to terminate session: {str(e)}"}) from e


@router.post("/sessions/cleanup", response_model=dict[str, Any])
async def cleanup_expired_sessions() -> dict[str, Any]:
    """
    Force cleanup of expired sessions.

    Returns:
        Number of sessions that were cleaned up
    """
    session_manager = await server_state.get_session_manager()

    try:
        cleaned_count = await session_manager.cleanup_expired_sessions()

        return {"success": True, "message": f"Cleaned up {cleaned_count} expired sessions", "sessions_cleaned": cleaned_count}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "SESSION_CLEANUP_ERROR", "detail": f"Failed to cleanup sessions: {str(e)}"}) from e
