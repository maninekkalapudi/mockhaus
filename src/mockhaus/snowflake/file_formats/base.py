"""
This module defines the base classes and data models for file format handlers.

It provides the `BaseFormatHandler` abstract class, which defines the common
interface for all format-specific handlers (e.g., for CSV, JSON). It also
defines dataclasses like `FileFormat`, `FormatMappingResult`, and
`ValidationResult` to structure the data used in the format management system.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass
class FileFormat:
    """
    Represents a Snowflake file format and its properties within Mockhaus.

    Attributes:
        name: The unique name of the file format.
        format_type: The type of the format (e.g., 'CSV', 'JSON', 'PARQUET').
        properties: A dictionary of properties that define the format's behavior.
        created_at: The timestamp when the format was created.
    """

    name: str
    format_type: str  # 'CSV', 'JSON', 'PARQUET', 'AVRO', 'ORC', 'XML'
    properties: dict[str, Any] = field(default_factory=dict)
    created_at: str | None = None


@dataclass
class FormatMappingResult:
    """
    Represents the result of mapping Snowflake format properties to DuckDB options.

    Attributes:
        options: A dictionary of DuckDB-compatible `COPY` command options.
        warnings: A list of warnings generated during the mapping process.
        ignored_options: A list of Snowflake options that were ignored because
                         they have no equivalent in DuckDB.
    """

    options: dict[str, Any]
    warnings: list[str] = field(default_factory=list)
    ignored_options: list[str] = field(default_factory=list)


@dataclass
class ValidationResult:
    """
    Represents the result of validating a set of file format properties.

    Attributes:
        is_valid: A boolean indicating if the properties are valid.
        errors: A list of validation error messages.
        warnings: A list of validation warning messages.
    """

    is_valid: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


class BaseFormatHandler(ABC):
    """
    An abstract base class for all file format-specific handlers.

    This class defines the contract that all format handlers must follow, ensuring
    they can be used interchangeably by the `MockFileFormatManager`.
    """

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
        """
        Maps Snowflake format properties to DuckDB `COPY` command options.

        Args:
            properties: A dictionary of Snowflake file format properties.

        Returns:
            A `FormatMappingResult` containing the mapped DuckDB options.
        """
        pass

    def validate_properties(self, properties: dict[str, Any]) -> ValidationResult:
        """
        Validates a set of properties for this format type.

        The default implementation considers all properties valid. Subclasses should
        override this to implement specific validation logic.

        Args:
            properties: The properties to validate.

        Returns:
            A `ValidationResult` object.
        """
        del properties  # Unused in the default implementation.
        return ValidationResult(is_valid=True)

    def _log_warnings(self, warnings: list[str]) -> None:
        """Log warnings using debug logging."""
        if not warnings:
            return

        try:
            from mockhaus.my_logging import debug_log

            for warning in warnings:
                debug_log(f"{self.format_type} format warning: {warning}")
        except ImportError:
            # Fallback - silent if logging not available
            pass
