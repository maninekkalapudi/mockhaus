"""This module provides an AST-based parser for Snowflake-specific SQL statements.

It uses the `sqlglot` library to build an Abstract Syntax Tree (AST) from a
SQL string, allowing for robust and accurate parsing of complex statements
like `CREATE STAGE`, `CREATE FILE FORMAT`, and `COPY INTO`.

This approach is more reliable than using regular expressions, as it correctly
handles variations in syntax, whitespace, and quoting.
"""

from typing import Any

import sqlglot
from sqlglot import expressions as exp

from mockhaus.my_logging import debug_log


class SnowflakeASTParser:
    """
    A parser for Snowflake-specific SQL statements that uses `sqlglot` to
    generate and analyze an Abstract Syntax Tree (AST).

    This class provides methods to deconstruct complex DDL and DML statements
    into their constituent parts, which can then be used by other components
    to emulate Snowflake's behavior.
    """

    def __init__(self) -> None:
        """Initialize the AST parser."""
        self.dialect = "snowflake"

    def parse_create_stage(self, sql: str) -> dict[str, Any]:
        """
        Parse CREATE STAGE statement using AST.

        Args:
            sql: The SQL string to parse.

        Returns:
            A dictionary containing the parsed components of the statement,
            including the stage name, type, URL, and other properties.
            Returns an error message if parsing fails.
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
            stage_name = stage_name_node.name

            # Convert to string if it's an Identifier
            stage_name = stage_name.name if hasattr(stage_name, "name") else str(stage_name)

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
                            value_node = prop.args.get("value")
                            if value_node:
                                value = str(value_node).strip("'\"")
                                properties[key] = value
                                if key == "URL":
                                    url = value

            # Determine stage type based on URL
            stage_type = "EXTERNAL" if url else "USER"

            return {"stage_name": stage_name, "stage_type": stage_type, "url": url, "properties": properties, "error": None}

        except Exception as e:
            return {"error": f"Failed to parse CREATE STAGE: {str(e)}"}

    def parse_drop_stage(self, sql: str) -> dict[str, Any]:
        """
        Parses a `DROP STAGE` statement using an AST.

        Args:
            sql: The SQL string to parse.

        Returns:
            A dictionary containing the parsed stage name and whether `IF EXISTS`
            was specified. Returns an error message if parsing fails.
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
            stage_name = stage_name.name if hasattr(stage_name, "name") else str(stage_name)

            # Check for IF EXISTS
            if_exists = bool(ast.args.get("exists", False))

            return {"stage_name": stage_name, "if_exists": if_exists, "error": None}

        except Exception as e:
            return {"error": f"Failed to parse DROP STAGE: {str(e)}"}

    def parse_create_file_format(self, sql: str) -> dict[str, Any]:
        """
        Parse CREATE FILE FORMAT statement using AST.

        Args:
            sql: The SQL string to parse.

        Returns:
            A dictionary containing the format name, type, and properties.
            Returns an error message if parsing fails.
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
            format_name = format_name.name if hasattr(format_name, "name") else str(format_name)

            # Extract properties
            properties: dict[str, Any] = {}
            format_type = "CSV"  # Default

            # Parse properties from WITH clause
            if ast.args.get("properties"):
                props = ast.args["properties"]
                if hasattr(props, "expressions"):
                    for prop in props.expressions:
                        if isinstance(prop, exp.Property):
                            key = str(prop.this).upper()
                            # Get value from args['value']
                            value_node = prop.args.get("value")
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

            return {"format_name": format_name, "format_type": format_type, "properties": properties, "error": None}

        except Exception as e:
            return {"error": f"Failed to parse CREATE FILE FORMAT: {str(e)}"}

    def parse_drop_file_format(self, sql: str) -> dict[str, Any]:
        """
        Parse DROP FILE FORMAT statement using AST.

        Args:
            sql: The SQL string to parse.

        Returns:
            A dictionary containing the format name and whether `IF EXISTS` was
            specified. Returns an error message if parsing fails.
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
            format_name = format_name.name if hasattr(format_name, "name") else str(format_name)

            # Check for IF EXISTS
            if_exists = bool(ast.args.get("exists", False))

            return {"format_name": format_name, "if_exists": if_exists, "error": None}

        except Exception as e:
            return {"error": f"Failed to parse DROP FILE FORMAT: {str(e)}"}

    def parse_copy_into(self, sql: str) -> dict[str, Any]:
        """
        Parses a `COPY INTO` statement.

        This method first attempts to use `sqlglot` for parsing. If that fails
        (as `COPY INTO` syntax can be complex), it falls back to a manual, 
        regex-based parsing method.

        Args:
            sql: The SQL string to parse.

        Returns:
            A dictionary containing the parsed components, including the table
            name, stage reference, and file format details.
        """
        try:
            # Parse the SQL into AST
            ast = sqlglot.parse_one(sql, dialect=self.dialect)

            # Verify it's a COPY statement
            if not isinstance(ast, exp.Copy):
                return {"error": "Not a COPY INTO statement"}

            # Extract table name
            table = ast.this
            if not table:
                return {"error": "Target table not found"}

            # Build full table name
            table_parts = []
            if hasattr(table, "catalog") and table.catalog:
                table_parts.append(str(table.catalog))
            if hasattr(table, "db") and table.db:
                table_parts.append(str(table.db))
            table_parts.append(str(table.this))
            table_name = ".".join(table_parts)

            # Extract stage reference from files
            files = ast.args.get("files", [])
            if not files:
                return {"error": "No source files specified"}

            # Get the stage reference (it's stored as a Table with a Literal)
            stage_ref = files[0]
            stage_reference = stage_ref.this.this if hasattr(stage_ref, "this") and hasattr(stage_ref.this, "this") else str(stage_ref)

            # Extract file format and other parameters
            params = ast.args.get("params", [])
            file_format_name = None
            inline_format = None  # String representation of inline format
            inline_format_options = {}  # Parsed dictionary
            options = {}

            for param in params:
                if hasattr(param, "this"):
                    param_name = str(param.this).upper()

                    if param_name == "FILE_FORMAT":
                        # Check if it has an expression (named format)
                        if hasattr(param, "expression") and param.expression:
                            param_value = param.expression
                            if hasattr(param_value, "this"):
                                # Named format: FILE_FORMAT = 'format_name'
                                file_format_name = param_value.this
                        # Check if it has expressions (inline format)
                        elif hasattr(param, "expressions") and param.expressions:
                            # Inline format: FILE_FORMAT = (TYPE = 'CSV' ...)
                            inline_format_options = self._parse_inline_format_from_properties(param.expressions)
                            # Reconstruct the string format for backward compatibility
                            format_parts = []
                            for key, value in inline_format_options.items():
                                if key == "TYPE":
                                    format_parts.append(f"TYPE = '{value}'")
                                elif key == "field_delimiter":
                                    format_parts.append(f"FIELD_DELIMITER = '{value}'")
                                elif key == "skip_header":
                                    format_parts.append(f"SKIP_HEADER = {value}")
                                elif key == "field_optionally_enclosed_by":
                                    format_parts.append(f"FIELD_OPTIONALLY_ENCLOSED_BY = '{value}'")
                                elif key == "record_delimiter":
                                    format_parts.append(f"RECORD_DELIMITER = '{value}'")
                                else:
                                    format_parts.append(f"{key.upper()} = '{value}'")
                            inline_format = " ".join(format_parts)
                    elif hasattr(param, "expression") and param.expression:
                        param_value = param.expression
                        if param_name == "PATTERN":
                            options["pattern"] = param_value.this if hasattr(param_value, "this") else str(param_value)
                        elif param_name == "VALIDATION_MODE":
                            options["validation_mode"] = param_value.this if hasattr(param_value, "this") else str(param_value)
                        else:
                            # Store other options
                            options[param_name.lower()] = param_value.this if hasattr(param_value, "this") else str(param_value)

            return {
                "table_name": table_name,
                "stage_reference": stage_reference,
                "file_format_name": file_format_name,
                "inline_format": inline_format,
                "inline_format_options": inline_format_options,
                "options": options,
                "error": None,
            }

        except Exception as e:
            debug_log(f"AST parsing failed for COPY INTO: {e}, falling back to manual parsing")
            # Fall back to manual parsing if AST parsing fails
            return self._parse_copy_into_manual(sql)

    def _parse_inline_format_from_properties(self, properties: list[exp.Property]) -> dict[str, Any]:
        """Parse inline file format from property list."""
        inline_format: dict[str, Any] = {}

        for prop in properties:
            if isinstance(prop, exp.Property) and prop.this and "value" in prop.args:
                key = str(prop.this).upper()
                value_expr = prop.args["value"]

                # Extract the actual value
                value = value_expr.this if hasattr(value_expr, "this") else str(value_expr)

                # Store properties with original case for test compatibility
                if key == "TYPE":
                    inline_format["TYPE"] = value.upper() if isinstance(value, str) else str(value).upper()
                elif key == "FIELD_DELIMITER":
                    inline_format["field_delimiter"] = value
                elif key == "SKIP_HEADER":
                    try:
                        inline_format["skip_header"] = int(value)
                    except (ValueError, TypeError):
                        inline_format["skip_header"] = str(value)
                elif key == "FIELD_OPTIONALLY_ENCLOSED_BY":
                    inline_format["field_optionally_enclosed_by"] = value
                elif key == "RECORD_DELIMITER":
                    inline_format["record_delimiter"] = value
                elif key == "COMPRESSION":
                    inline_format["compression"] = value.upper() if isinstance(value, str) else str(value).upper()
                else:
                    inline_format[key] = value

        return inline_format

    def _parse_inline_format(self, format_expr: exp.Expression) -> dict[str, Any]:
        """Parse inline file format specification from AST."""
        inline_format: dict[str, Any] = {}

        # Handle parenthesized expressions like (TYPE = 'CSV', SKIP_HEADER = 1)
        if hasattr(format_expr, "expressions"):
            for expr in format_expr.expressions:
                if isinstance(expr, exp.EQ) and hasattr(expr, "this") and hasattr(expr, "expression"):
                    key = str(expr.this).upper()
                    value = expr.expression

                    # Extract the actual value
                    value = value.this if hasattr(value, "this") else str(value)

                    # Map common format properties
                    if key == "TYPE":
                        inline_format["type"] = value.upper() if isinstance(value, str) else str(value).upper()
                    elif key == "FIELD_DELIMITER":
                        inline_format["field_delimiter"] = value
                    elif key == "SKIP_HEADER":
                        try:
                            inline_format["skip_header"] = int(value)
                        except (ValueError, TypeError):
                            inline_format["skip_header"] = str(value)
                    elif key == "FIELD_OPTIONALLY_ENCLOSED_BY":
                        inline_format["field_optionally_enclosed_by"] = value
                    elif key == "RECORD_DELIMITER":
                        inline_format["record_delimiter"] = value
                    elif key == "COMPRESSION":
                        inline_format["compression"] = value.upper() if isinstance(value, str) else str(value).upper()
                    else:
                        inline_format[key.lower()] = value

        return inline_format

    def _parse_copy_into_manual(self, sql: str) -> dict[str, Any]:
        """
        A fallback manual parser for `COPY INTO` statements using regex.

        This is used if the primary AST-based parsing fails, providing more
        resilience for complex or non-standard `COPY INTO` syntax.

        Args:
            sql: The SQL string to parse.

        Returns:
            A dictionary containing the parsed components.
        """
        try:
            import re

            # Normalize SQL
            sql = re.sub(r"\s+", " ", sql.strip())

            # Extract table name and stage reference
            # Pattern: COPY INTO table_name FROM 'stage_reference'
            # Handle quoted stage names like '@"my stage"/file'
            copy_pattern = r'COPY\s+INTO\s+(\w+)\s+FROM\s+([\'"]?@(?:[^\'"\s]+|\"[^\"]*\")+(?:/[^\'"\s]*)?[\'"]?)'
            match = re.search(copy_pattern, sql, re.IGNORECASE)

            if not match:
                return {"error": "Invalid COPY INTO syntax: could not extract table and stage"}

            table_name = match.group(1)
            stage_reference = match.group(2).strip("'\"")

            result: dict[str, Any] = {
                "table_name": table_name,
                "stage_reference": stage_reference,
                "file_format_name": None,
                "inline_format": None,
                "options": {},
                "error": None,
            }

            # Extract file format specification
            self._parse_copy_file_format(sql, result)

            # Extract other options
            self._parse_copy_other_options(sql, result)

            return result

        except Exception as e:
            return {"error": f"Failed to parse COPY INTO: {str(e)}"}

    def _parse_copy_file_format(self, sql: str, result: dict[str, Any]) -> None:
        """Parse file format specification from COPY INTO statement."""
        import re

        # Look for FILE_FORMAT = (FORMAT_NAME = 'name') or FILE_FORMAT = 'name'
        format_name_pattern = r'FILE_FORMAT\s*=\s*\(\s*FORMAT_NAME\s*=\s*[\'"](\w+)[\'"]\s*\)'
        format_direct_pattern = r'FILE_FORMAT\s*=\s*[\'"](\w+)[\'"]'

        # Look for inline format specification
        # FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)
        inline_pattern = r"FILE_FORMAT\s*=\s*\(([^)]+)\)"

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

    def _parse_copy_other_options(self, sql: str, result: dict[str, Any]) -> None:
        """Parse other COPY INTO options like ON_ERROR, FORCE, etc."""
        import re

        options = {}

        # ON_ERROR option
        on_error_pattern = r'ON_ERROR\s*=\s*[\'"](\w+)[\'"]'
        on_error_match = re.search(on_error_pattern, sql, re.IGNORECASE)
        if on_error_match:
            options["on_error"] = on_error_match.group(1).upper()

        # FORCE option
        if re.search(r"\bFORCE\s*=\s*TRUE\b", sql, re.IGNORECASE):
            options["force"] = True

        # PURGE option
        if re.search(r"\bPURGE\s*=\s*TRUE\b", sql, re.IGNORECASE):
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
