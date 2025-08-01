"""COPY INTO statement translation for Mockhaus data ingestion."""

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from .file_formats import FileFormat, MockFileFormatManager
from .stages import MockStageManager


@dataclass
class CopyIntoContext:
    """Context for COPY INTO operation."""

    table_name: str
    stage_reference: str
    file_path: str
    file_format: Optional[FileFormat] = None
    inline_format: Optional[str] = None
    on_error: str = "ABORT"
    force: bool = False
    purge: bool = False
    pattern: Optional[str] = None
    validation_mode: Optional[str] = None


class CopyIntoTranslator:
    """Translates Snowflake COPY INTO statements to DuckDB COPY statements."""

    def __init__(self, stage_manager: MockStageManager, format_manager: MockFileFormatManager) -> None:
        """Initialize COPY INTO translator."""
        self.stage_manager = stage_manager
        self.format_manager = format_manager

    def parse_copy_into_statement(self, sql: str) -> CopyIntoContext:
        """Parse a COPY INTO statement and extract components."""
        # Remove extra whitespace and normalize
        sql = re.sub(r'\s+', ' ', sql.strip())
        
        # Basic pattern for COPY INTO
        # COPY INTO table_name FROM stage_reference [FILE_FORMAT = ...] [OPTIONS]
        copy_pattern = r'COPY\s+INTO\s+(\w+)\s+FROM\s+([\'"]?@[^\'"\s]+[\'"]?)'
        
        match = re.search(copy_pattern, sql, re.IGNORECASE)
        if not match:
            raise ValueError(f"Invalid COPY INTO statement: {sql}")
        
        table_name = match.group(1)
        stage_reference = match.group(2).strip('\'"')
        
        context = CopyIntoContext(
            table_name=table_name,
            stage_reference=stage_reference,
            file_path=""  # Will be resolved later
        )
        
        # Extract file format specification
        self._parse_file_format(sql, context)
        
        # Extract other options
        self._parse_copy_options(sql, context)
        
        return context

    def _parse_file_format(self, sql: str, context: CopyIntoContext) -> None:
        """Parse file format specification from COPY INTO statement."""
        # Look for FILE_FORMAT = (FORMAT_NAME = 'name') or FILE_FORMAT = 'name'
        format_name_pattern = r'FILE_FORMAT\s*=\s*\(\s*FORMAT_NAME\s*=\s*[\'"](\w+)[\'"]\s*\)'
        format_direct_pattern = r'FILE_FORMAT\s*=\s*[\'"](\w+)[\'"]'
        
        # Look for inline format specification
        inline_pattern = r'FILE_FORMAT\s*=\s*\(([^)]+)\)'
        
        format_name_match = re.search(format_name_pattern, sql, re.IGNORECASE)
        format_direct_match = re.search(format_direct_pattern, sql, re.IGNORECASE)
        inline_match = re.search(inline_pattern, sql, re.IGNORECASE)
        
        if format_name_match:
            format_name = format_name_match.group(1)
            context.file_format = self.format_manager.get_format(format_name)
            if not context.file_format:
                raise ValueError(f"File format '{format_name}' not found")
        elif format_direct_match:
            format_name = format_direct_match.group(1)
            context.file_format = self.format_manager.get_format(format_name)
            if not context.file_format:
                raise ValueError(f"File format '{format_name}' not found")
        elif inline_match:
            inline_spec = inline_match.group(1)
            context.inline_format = inline_spec
            context.file_format = self.format_manager.create_temp_format_from_inline(inline_spec)

    def _parse_copy_options(self, sql: str, context: CopyIntoContext) -> None:
        """Parse COPY INTO options like ON_ERROR, FORCE, etc."""
        # ON_ERROR option
        on_error_pattern = r'ON_ERROR\s*=\s*[\'"](\w+)[\'"]'
        on_error_match = re.search(on_error_pattern, sql, re.IGNORECASE)
        if on_error_match:
            context.on_error = on_error_match.group(1).upper()
        
        # FORCE option
        if re.search(r'\bFORCE\s*=\s*TRUE\b', sql, re.IGNORECASE):
            context.force = True
        
        # PURGE option
        if re.search(r'\bPURGE\s*=\s*TRUE\b', sql, re.IGNORECASE):
            context.purge = True
        
        # PATTERN option
        pattern_match = re.search(r'PATTERN\s*=\s*[\'"]([^\'\"]+)[\'"]', sql, re.IGNORECASE)
        if pattern_match:
            context.pattern = pattern_match.group(1)
        
        # VALIDATION_MODE option
        validation_match = re.search(r'VALIDATION_MODE\s*=\s*[\'"](\w+)[\'"]', sql, re.IGNORECASE)
        if validation_match:
            context.validation_mode = validation_match.group(1).upper()

    def translate_copy_into(self, sql: str) -> str:
        """Translate a COPY INTO statement to DuckDB COPY statement."""
        # Parse the COPY INTO statement
        context = self.parse_copy_into_statement(sql)
        
        # Resolve file path from stage reference
        resolved_path = self.stage_manager.resolve_stage_path(context.stage_reference)
        if not resolved_path:
            raise ValueError(f"Cannot resolve stage reference: {context.stage_reference}")
        
        context.file_path = resolved_path
        
        # Check if file exists
        file_path = Path(context.file_path)
        if not file_path.exists():
            if context.pattern:
                # Handle pattern matching
                files = self._find_files_by_pattern(file_path.parent, context.pattern)
                if not files:
                    raise FileNotFoundError(f"No files found matching pattern '{context.pattern}' in {file_path.parent}")
                # For simplicity, use the first matching file
                context.file_path = str(files[0])
            else:
                raise FileNotFoundError(f"File not found: {context.file_path}")
        
        # Generate DuckDB COPY statement
        return self._generate_duckdb_copy(context)

    def _find_files_by_pattern(self, directory: Path, pattern: str) -> List[Path]:
        """Find files matching a pattern in directory."""
        if not directory.exists():
            return []
        
        # Convert SQL pattern to glob pattern (simplified)
        # In Snowflake, patterns use regex-like syntax
        # For now, we'll handle simple glob patterns
        glob_pattern = pattern.replace('%', '*')
        
        return list(directory.glob(glob_pattern))

    def _generate_duckdb_copy(self, context: CopyIntoContext) -> str:
        """Generate DuckDB COPY statement from context."""
        # Start with basic COPY statement
        copy_sql = f"COPY {context.table_name} FROM '{context.file_path}'"
        
        # Add format options
        if context.file_format:
            options = self.format_manager.map_to_duckdb_options(context.file_format)
            if options:
                option_parts = []
                for key, value in options.items():
                    if isinstance(value, bool):
                        option_parts.append(f"{key} {str(value).lower()}")
                    elif isinstance(value, str):
                        option_parts.append(f"{key} '{value}'")
                    else:
                        option_parts.append(f"{key} {value}")
                
                if option_parts:
                    copy_sql += f" ({', '.join(option_parts)})"
        
        return copy_sql

    def validate_copy_operation(self, context: CopyIntoContext) -> List[str]:
        """Validate COPY operation and return any warnings or errors."""
        warnings = []
        
        # Check if file exists
        if not Path(context.file_path).exists():
            warnings.append(f"File does not exist: {context.file_path}")
        
        # Check file format compatibility
        if context.file_format:
            file_ext = Path(context.file_path).suffix.lower()
            format_type = context.file_format.format_type.lower()
            
            expected_extensions = {
                'csv': ['.csv', '.txt'],
                'json': ['.json', '.jsonl'],
                'parquet': ['.parquet', '.pqt'],
                'avro': ['.avro'],
                'orc': ['.orc']
            }
            
            if format_type in expected_extensions:
                if file_ext not in expected_extensions[format_type]:
                    warnings.append(f"File extension '{file_ext}' may not match format type '{format_type}'")
        
        return warnings

    def execute_copy_operation(self, sql: str, connection) -> Dict[str, Any]:
        """Execute COPY INTO operation and return results."""
        try:
            # Translate to DuckDB COPY
            duckdb_sql = self.translate_copy_into(sql)
            
            # Execute the translated statement
            result = connection.execute(duckdb_sql)
            
            # Get row count (DuckDB COPY returns count)
            row_count = 0
            if result.description and len(result.description) > 0:
                rows = result.fetchall()
                if rows and len(rows) > 0:
                    row_count = rows[0][0] if isinstance(rows[0][0], int) else 0
            
            return {
                "success": True,
                "rows_loaded": row_count,
                "original_sql": sql,
                "translated_sql": duckdb_sql,
                "errors": []
            }
            
        except Exception as e:
            return {
                "success": False,
                "rows_loaded": 0,
                "original_sql": sql,
                "translated_sql": "",
                "errors": [str(e)]
            }