"""
This module provides a simple, environment-variable-based logging setup for Mockhaus.

It allows for enabling debug-level logging across the application by setting the
`MOCKHAUS_DEBUG` environment variable. This is a lightweight alternative to a full
logging configuration, suitable for development and troubleshooting.
"""

import os
import sys
from typing import Any


def setup_debug_logging() -> bool:
    """
    Enables debug logging if the MOCKHAUS_DEBUG environment variable is set.

    Checks for `MOCKHAUS_DEBUG` and, if it's set to a truthy value (e.g.,
    'true', '1', 'yes'), prints a confirmation message to stderr.

    Returns:
        True if debug mode is enabled, False otherwise.
    """
    if os.environ.get("MOCKHAUS_DEBUG", "").lower() in ("true", "1", "yes"):
        # Enable debug mode
        sys.stderr.write("[MOCKHAUS] Debug mode enabled\n")
        return True
    return False


def debug_log(message: str, **kwargs: Any) -> None:
    """
    Prints a debug message to stderr if debug mode is enabled.

    This function will only produce output if the `MOCKHAUS_DEBUG` environment
    variable is set to a truthy value.

    Args:
        message: The debug message to print.
        **kwargs: Additional key-value pairs to print for context.
    """
    if os.environ.get("MOCKHAUS_DEBUG", "").lower() in ("true", "1", "yes"):
        sys.stderr.write(f"[DEBUG] {message}\n")
        for key, value in kwargs.items():
            sys.stderr.write(f"  {key}: {value}\n")
