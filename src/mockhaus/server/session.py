"""Session management for database context persistence."""

import uuid
from typing import Any


class SessionManager:
    """Manages client sessions and their database contexts."""

    def __init__(self) -> None:
        """Initialize session manager."""
        self._sessions: dict[str, dict[str, Any]] = {}

    def create_session(self) -> str:
        """
        Create a new session.

        Returns:
            Session ID
        """
        session_id = str(uuid.uuid4())
        self._sessions[session_id] = {
            "current_database": None,
            "created_at": __import__("time").time(),
        }
        return session_id

    def get_session_database(self, session_id: str | None) -> str | None:
        """
        Get the current database for a session.

        Args:
            session_id: Session ID, or None for stateless requests

        Returns:
            Database path if set, None otherwise
        """
        if not session_id or session_id not in self._sessions:
            return None

        return self._sessions[session_id].get("current_database")

    def set_session_database(self, session_id: str | None, database_path: str | None) -> None:
        """
        Set the current database for a session.

        Args:
            session_id: Session ID
            database_path: Path to database file
        """
        if not session_id:
            return

        if session_id not in self._sessions:
            # Create session if it doesn't exist
            self._sessions[session_id] = {
                "created_at": __import__("time").time(),
            }

        self._sessions[session_id]["current_database"] = database_path

    def cleanup_old_sessions(self, max_age_seconds: int = 3600) -> None:
        """
        Clean up old sessions.

        Args:
            max_age_seconds: Maximum age of sessions in seconds
        """
        current_time = __import__("time").time()
        expired_sessions = [sid for sid, data in self._sessions.items() if current_time - data["created_at"] > max_age_seconds]

        for sid in expired_sessions:
            del self._sessions[sid]

    def get_session_count(self) -> int:
        """Get number of active sessions."""
        return len(self._sessions)


# Global session manager instance
session_manager = SessionManager()
