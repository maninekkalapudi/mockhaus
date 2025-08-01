"""AST-based parser for Snowflake-specific SQL statements."""

from typing import Any, Dict, Optional

import sqlglot
from sqlglot import expressions as exp


class SnowflakeASTParser:
    """Parser for Snowflake-specific SQL statements using sqlglot AST."""

    def __init__(self) -> None:
        """Initialize the AST parser."""
        self.dialect = "snowflake"

    def parse_create_stage(self, sql: str) -> Dict[str, Any]:
        """
        Parse CREATE STAGE statement using AST.
        
        Returns dict with:
        - stage_name: Name of the stage
        - stage_type: USER or EXTERNAL
        - url: URL if external stage
        - properties: Other properties
        - error: Error message if parsing failed
        """
        try:
            # Parse the SQL into AST
            ast = sqlglot.parse_one(sql, dialect=self.dialect)
            
            # Verify it's a CREATE STAGE statement
            if not isinstance(ast, exp.Create) or ast.args.get("kind") != "STAGE":
                return {"error": "Not a CREATE STAGE statement"}
            
            # Extract stage name
            stage_name = ast.args.get("this")
            if not stage_name:
                return {"error": "Stage name not found"}
            
            # Convert to string if it's an Identifier
            if hasattr(stage_name, "name"):
                stage_name = stage_name.name
            else:
                stage_name = str(stage_name)
            
            # Extract properties
            properties = {}
            url = None
            
            # Parse properties from WITH clause
            if ast.args.get("properties"):
                props = ast.args["properties"]
                if hasattr(props, "expressions"):
                    for prop in props.expressions:
                        if isinstance(prop, exp.Property):
                            key = str(prop.this).upper()
                            # Get value from args['value']
                            value_node = prop.args.get('value')
                            if value_node:
                                value = str(value_node).strip("'\"")
                                properties[key] = value
                                if key == "URL":
                                    url = value
            
            # Determine stage type based on URL
            stage_type = "EXTERNAL" if url else "USER"
            
            return {
                "stage_name": stage_name,
                "stage_type": stage_type,
                "url": url,
                "properties": properties,
                "error": None
            }
            
        except Exception as e:
            return {"error": f"Failed to parse CREATE STAGE: {str(e)}"}

    def parse_drop_stage(self, sql: str) -> Dict[str, Any]:
        """
        Parse DROP STAGE statement using AST.
        
        Returns dict with:
        - stage_name: Name of the stage to drop
        - if_exists: Whether IF EXISTS was specified
        - error: Error message if parsing failed
        """
        try:
            # Parse the SQL into AST
            ast = sqlglot.parse_one(sql, dialect=self.dialect)
            
            # Verify it's a DROP statement with STAGE kind
            if not isinstance(ast, exp.Drop):
                return {"error": "Not a DROP statement"}
            
            # Check if it's dropping a stage
            if ast.args.get("kind") != "STAGE":
                return {"error": "Not a DROP STAGE statement"}
            
            # Extract stage name
            stage_name = ast.args.get("this")
            if not stage_name:
                return {"error": "Stage name not found"}
            
            # Convert to string if it's an Identifier
            if hasattr(stage_name, "name"):
                stage_name = stage_name.name
            else:
                stage_name = str(stage_name)
            
            # Check for IF EXISTS
            if_exists = bool(ast.args.get("exists", False))
            
            return {
                "stage_name": stage_name,
                "if_exists": if_exists,
                "error": None
            }
            
        except Exception as e:
            return {"error": f"Failed to parse DROP STAGE: {str(e)}"}

    def parse_create_file_format(self, sql: str) -> Dict[str, Any]:
        """
        Parse CREATE FILE FORMAT statement using AST.
        
        Returns dict with:
        - format_name: Name of the file format
        - format_type: Type (CSV, JSON, PARQUET, etc.)
        - properties: Format properties/options
        - error: Error message if parsing failed
        """
        try:
            # Parse the SQL into AST
            ast = sqlglot.parse_one(sql, dialect=self.dialect)
            
            # Verify it's a CREATE FILE FORMAT statement
            if not isinstance(ast, exp.Create) or ast.args.get("kind") != "FILE FORMAT":
                return {"error": "Not a CREATE FILE FORMAT statement"}
            
            # Extract format name
            format_name = ast.args.get("this")
            if not format_name:
                return {"error": "File format name not found"}
            
            # Convert to string if it's an Identifier
            if hasattr(format_name, "name"):
                format_name = format_name.name
            else:
                format_name = str(format_name)
            
            # Extract properties
            properties = {}
            format_type = "CSV"  # Default
            
            # Parse properties from WITH clause
            if ast.args.get("properties"):
                props = ast.args["properties"]
                if hasattr(props, "expressions"):
                    for prop in props.expressions:
                        if isinstance(prop, exp.Property):
                            key = str(prop.this).upper()
                            # Get value from args['value']
                            value_node = prop.args.get('value')
                            if value_node:
                                # Get raw value without stripping quotes for certain properties
                                raw_value = str(value_node)
                                value = raw_value.strip("'\"")
                                
                                # Handle special properties
                                if key == "TYPE":
                                    format_type = value.upper()
                                elif key == "FIELD_DELIMITER":
                                    properties["field_delimiter"] = value
                                elif key == "SKIP_HEADER":
                                    try:
                                        properties["skip_header"] = int(value)
                                    except ValueError:
                                        properties["skip_header"] = 0
                                elif key == "FIELD_OPTIONALLY_ENCLOSED_BY":
                                    # For quote characters, we need to handle the literal value
                                    if raw_value.startswith("'") and raw_value.endswith("'"):
                                        # Extract content between single quotes
                                        quote_content = raw_value[1:-1]
                                        properties["field_optionally_enclosed_by"] = quote_content
                                    else:
                                        properties["field_optionally_enclosed_by"] = value
                                elif key == "NULL_IF":
                                    # Handle NULL_IF as list
                                    if isinstance(value_node, exp.Array):
                                        null_values = []
                                        for item in value_node.expressions:
                                            null_values.append(str(item).strip("'\""))
                                        properties["null_if"] = null_values
                                    else:
                                        properties["null_if"] = [value]
                                elif key == "RECORD_DELIMITER":
                                    properties["record_delimiter"] = value
                                elif key == "COMPRESSION":
                                    properties["compression"] = value.upper()
                                elif key == "DATE_FORMAT":
                                    properties["date_format"] = value
                                elif key == "TIME_FORMAT":
                                    properties["time_format"] = value
                                elif key == "TIMESTAMP_FORMAT":
                                    properties["timestamp_format"] = value
                                else:
                                    # Store other properties as-is
                                    properties[key.lower()] = value
            
            return {
                "format_name": format_name,
                "format_type": format_type,
                "properties": properties,
                "error": None
            }
            
        except Exception as e:
            return {"error": f"Failed to parse CREATE FILE FORMAT: {str(e)}"}

    def parse_drop_file_format(self, sql: str) -> Dict[str, Any]:
        """
        Parse DROP FILE FORMAT statement using AST.
        
        Returns dict with:
        - format_name: Name of the file format to drop
        - if_exists: Whether IF EXISTS was specified
        - error: Error message if parsing failed
        """
        try:
            # Parse the SQL into AST
            ast = sqlglot.parse_one(sql, dialect=self.dialect)
            
            # Verify it's a DROP statement with FILE FORMAT kind
            if not isinstance(ast, exp.Drop):
                return {"error": "Not a DROP statement"}
            
            # Check if it's dropping a file format
            if ast.args.get("kind") != "FILE FORMAT":
                return {"error": "Not a DROP FILE FORMAT statement"}
            
            # Extract format name
            format_name = ast.args.get("this")
            if not format_name:
                return {"error": "File format name not found"}
            
            # Convert to string if it's an Identifier
            if hasattr(format_name, "name"):
                format_name = format_name.name
            else:
                format_name = str(format_name)
            
            # Check for IF EXISTS
            if_exists = bool(ast.args.get("exists", False))
            
            return {
                "format_name": format_name,
                "if_exists": if_exists,
                "error": None
            }
            
        except Exception as e:
            return {"error": f"Failed to parse DROP FILE FORMAT: {str(e)}"}

    def parse_copy_into(self, sql: str) -> Dict[str, Any]:
        """
        Parse COPY INTO statement using AST.
        
        Returns dict with:
        - table_name: Target table name
        - stage_reference: Stage reference (e.g., '@stage/file')
        - file_format_name: Named file format (if used)
        - inline_format: Inline format specification (if used)
        - options: Other COPY options
        - error: Error message if parsing failed
        """
        try:
            # Parse the SQL into AST
            ast = sqlglot.parse_one(sql, dialect=self.dialect)
            
            # Handle COPY INTO statements (sqlglot may not have perfect support)
            # Let's check if it's parsed as a Command or other node type
            sql_upper = sql.upper().strip()
            if not sql_upper.startswith("COPY INTO"):
                return {"error": "Not a COPY INTO statement"}
            
            # For COPY INTO, we'll need to do some manual parsing since sqlglot
            # may not have full support for Snowflake's COPY INTO syntax
            return self._parse_copy_into_manual(sql)
            
        except Exception as e:
            # Fall back to manual parsing if AST parsing fails
            return self._parse_copy_into_manual(sql)

    def _parse_copy_into_manual(self, sql: str) -> Dict[str, Any]:
        """
        Manual parsing of COPY INTO statement with improved regex patterns.
        """
        try:
            import re
            
            # Normalize SQL
            sql = re.sub(r'\s+', ' ', sql.strip())
            
            # Extract table name and stage reference
            # Pattern: COPY INTO table_name FROM 'stage_reference'
            # Handle quoted stage names like '@"my stage"/file'
            copy_pattern = r'COPY\s+INTO\s+(\w+)\s+FROM\s+([\'"]?@(?:[^\'"\s]+|\"[^\"]*\")+(?:/[^\'"\s]*)?[\'"]?)'
            match = re.search(copy_pattern, sql, re.IGNORECASE)
            
            if not match:
                return {"error": "Invalid COPY INTO syntax: could not extract table and stage"}
            
            table_name = match.group(1)
            stage_reference = match.group(2).strip('\'"')
            
            result = {
                "table_name": table_name,
                "stage_reference": stage_reference,
                "file_format_name": None,
                "inline_format": None,
                "options": {},
                "error": None
            }
            
            # Extract file format specification
            self._parse_copy_file_format(sql, result)
            
            # Extract other options
            self._parse_copy_other_options(sql, result)
            
            return result
            
        except Exception as e:
            return {"error": f"Failed to parse COPY INTO: {str(e)}"}

    def _parse_copy_file_format(self, sql: str, result: Dict[str, Any]) -> None:
        """Parse file format specification from COPY INTO statement."""
        import re
        
        # Look for FILE_FORMAT = (FORMAT_NAME = 'name') or FILE_FORMAT = 'name'
        format_name_pattern = r'FILE_FORMAT\s*=\s*\(\s*FORMAT_NAME\s*=\s*[\'"](\w+)[\'"]\s*\)'
        format_direct_pattern = r'FILE_FORMAT\s*=\s*[\'"](\w+)[\'"]'
        
        # Look for inline format specification
        # FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)
        inline_pattern = r'FILE_FORMAT\s*=\s*\(([^)]+)\)'
        
        format_name_match = re.search(format_name_pattern, sql, re.IGNORECASE)
        format_direct_match = re.search(format_direct_pattern, sql, re.IGNORECASE)
        inline_match = re.search(inline_pattern, sql, re.IGNORECASE)
        
        if format_name_match:
            result["file_format_name"] = format_name_match.group(1)
        elif format_direct_match:
            result["file_format_name"] = format_direct_match.group(1)
        elif inline_match:
            inline_spec = inline_match.group(1)
            result["inline_format"] = inline_spec
            
            # Parse the inline format specification
            inline_options = {}
            
            # Extract TYPE
            type_match = re.search(r"TYPE\s*=\s*['\"](\w+)['\"]", inline_spec, re.IGNORECASE)
            if type_match:
                inline_options["TYPE"] = type_match.group(1).upper()
            
            # Extract common CSV options
            delimiter_match = re.search(r"FIELD_DELIMITER\s*=\s*['\"](.)['\"]", inline_spec, re.IGNORECASE)
            if delimiter_match:
                inline_options["field_delimiter"] = delimiter_match.group(1)
            
            header_match = re.search(r"SKIP_HEADER\s*=\s*(\d+)", inline_spec, re.IGNORECASE)
            if header_match:
                inline_options["skip_header"] = int(header_match.group(1))
            
            quote_match = re.search(r"FIELD_OPTIONALLY_ENCLOSED_BY\s*=\s*['\"](.)['\"]", inline_spec, re.IGNORECASE)
            if quote_match:
                inline_options["field_optionally_enclosed_by"] = quote_match.group(1)
            
            record_delimiter_match = re.search(r"RECORD_DELIMITER\s*=\s*['\"]([^'\"]+)['\"]", inline_spec, re.IGNORECASE)
            if record_delimiter_match:
                inline_options["record_delimiter"] = record_delimiter_match.group(1)
            
            compression_match = re.search(r"COMPRESSION\s*=\s*['\"](\w+)['\"]", inline_spec, re.IGNORECASE)
            if compression_match:
                inline_options["compression"] = compression_match.group(1).upper()
            
            result["inline_format_options"] = inline_options

    def _parse_copy_other_options(self, sql: str, result: Dict[str, Any]) -> None:
        """Parse other COPY INTO options like ON_ERROR, FORCE, etc."""
        import re
        
        options = {}
        
        # ON_ERROR option
        on_error_pattern = r'ON_ERROR\s*=\s*[\'"](\w+)[\'"]'
        on_error_match = re.search(on_error_pattern, sql, re.IGNORECASE)
        if on_error_match:
            options["on_error"] = on_error_match.group(1).upper()
        
        # FORCE option
        if re.search(r'\bFORCE\s*=\s*TRUE\b', sql, re.IGNORECASE):
            options["force"] = True
        
        # PURGE option
        if re.search(r'\bPURGE\s*=\s*TRUE\b', sql, re.IGNORECASE):
            options["purge"] = True
        
        # PATTERN option
        pattern_match = re.search(r'PATTERN\s*=\s*[\'"]([^\'\"]+)[\'"]', sql, re.IGNORECASE)
        if pattern_match:
            options["pattern"] = pattern_match.group(1)
        
        # VALIDATION_MODE option
        validation_match = re.search(r'VALIDATION_MODE\s*=\s*[\'"](\w+)[\'"]', sql, re.IGNORECASE)
        if validation_match:
            options["validation_mode"] = validation_match.group(1).upper()
        
        result["options"] = options