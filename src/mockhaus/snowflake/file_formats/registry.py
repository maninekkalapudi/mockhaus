"""
This module provides a central registry for file format handlers.

It defines the `FormatHandlerRegistry` class, which maintains a mapping between
format type names (e.g., 'CSV', 'JSON') and their corresponding handler classes.
This allows the `MockFileFormatManager` to be easily extended with new file
formats without modifying its core logic.
"""

from .base import BaseFormatHandler


class FormatHandlerRegistry:
    """Registry for format handlers."""

    def __init__(self) -> None:
        self._handlers: dict[str, type[BaseFormatHandler]] = {}

    def register(self, format_type: str, handler_class: type[BaseFormatHandler]) -> None:
        """
        Registers a new format handler class for a given format type.

        Args:
            format_type: The name of the format type (e.g., 'CSV').
            handler_class: The handler class that implements `BaseFormatHandler`.
        """
        self._handlers[format_type.upper()] = handler_class

    def get_handler(self, format_type: str) -> BaseFormatHandler:
        """
        Retrieves an instance of the handler for a given format type.

        Args:
            format_type: The name of the format type.

        Returns:
            An instance of the registered `BaseFormatHandler`.

        Raises:
            ValueError: If no handler is registered for the given format type.
        """
        handler_class = self._handlers.get(format_type.upper())
        if not handler_class:
            raise ValueError(f"Unsupported format type: {format_type}")
        return handler_class()

    def get_supported_formats(self) -> list[str]:
        """
        Returns a list of all registered (supported) format types.

        Returns:
            A list of format type names.
        """
        return list(self._handlers.keys())


# Global registry instance
format_registry = FormatHandlerRegistry()
