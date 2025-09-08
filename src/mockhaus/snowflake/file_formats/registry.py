"""Registry for format handlers."""

from .base import BaseFormatHandler


class FormatHandlerRegistry:
    """Registry for format handlers."""

    def __init__(self) -> None:
        self._handlers: dict[str, type[BaseFormatHandler]] = {}

    def register(self, format_type: str, handler_class: type[BaseFormatHandler]) -> None:
        """Register a format handler."""
        self._handlers[format_type.upper()] = handler_class

    def get_handler(self, format_type: str) -> BaseFormatHandler:
        """Get handler instance for format type."""
        handler_class = self._handlers.get(format_type.upper())
        if not handler_class:
            raise ValueError(f"Unsupported format type: {format_type}")
        return handler_class()

    def get_supported_formats(self) -> list[str]:
        """Get list of supported format types."""
        return list(self._handlers.keys())


# Global registry instance
format_registry = FormatHandlerRegistry()
