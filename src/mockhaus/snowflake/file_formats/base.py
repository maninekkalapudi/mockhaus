"""Base classes for file format handlers."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass
class FileFormat:
    """Represents a Snowflake file format in Mockhaus."""

    name: str
    format_type: str  # 'CSV', 'JSON', 'PARQUET', 'AVRO', 'ORC', 'XML'
    properties: dict[str, Any] = field(default_factory=dict)
    created_at: str | None = None


@dataclass
class FormatMappingResult:
    """Result of mapping Snowflake format properties to DuckDB options."""

    options: dict[str, Any]
    warnings: list[str] = field(default_factory=list)
    ignored_options: list[str] = field(default_factory=list)


@dataclass
class ValidationResult:
    """Result of validating format properties."""

    is_valid: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


class BaseFormatHandler(ABC):
    """Base class for all format handlers."""

    @property
    @abstractmethod
    def format_type(self) -> str:
        """Return the format type (e.g., 'CSV', 'PARQUET')."""
        pass

    @abstractmethod
    def get_default_properties(self) -> dict[str, Any]:
        """Get default properties for this format type."""
        pass

    @abstractmethod
    def map_to_duckdb_options(self, properties: dict[str, Any]) -> FormatMappingResult:
        """Map Snowflake format properties to DuckDB COPY options."""
        pass

    def validate_properties(self, properties: dict[str, Any]) -> ValidationResult:
        """Validate format properties. Default implementation returns valid."""
        del properties  # Unused parameter
        return ValidationResult(is_valid=True)

    def _log_warnings(self, warnings: list[str]) -> None:
        """Log warnings using debug logging."""
        if not warnings:
            return

        try:
            from mockhaus.logging import debug_log

            for warning in warnings:
                debug_log(f"{self.format_type} format warning: {warning}")
        except ImportError:
            # Fallback - silent if logging not available
            pass
