"""Simple logging setup for Mockhaus."""

import os
import sys
from typing import Any


def setup_debug_logging() -> bool:
    """Enable debug logging when MOCKHAUS_DEBUG is set."""
    if os.environ.get("MOCKHAUS_DEBUG", "").lower() in ("true", "1", "yes"):
        # Enable debug mode
        sys.stderr.write("[MOCKHAUS] Debug mode enabled\n")
        return True
    return False


def debug_log(message: str, **kwargs: Any) -> None:
    """Print debug message if debug mode is enabled."""
    if os.environ.get("MOCKHAUS_DEBUG", "").lower() in ("true", "1", "yes"):
        sys.stderr.write(f"[DEBUG] {message}\n")
        for key, value in kwargs.items():
            sys.stderr.write(f"  {key}: {value}\n")
