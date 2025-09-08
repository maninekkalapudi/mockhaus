"""Expected translation results for test fixtures."""

# Expected DuckDB translations for the sample Snowflake queries

EXPECTED_TRANSLATIONS = {
    "simple_select": {
        "snowflake": "SELECT SYSDATE() AS current_time FROM users",
        "duckdb": 'SELECT CURRENT_TIMESTAMP AT TIME ZONE \'UTC\' AS "current_time" FROM "users"',
    },
    "create_table_with_defaults": {
        "snowflake": """CREATE TABLE audit_log (
            id INTEGER PRIMARY KEY,
            action VARCHAR(50) NOT NULL,
            created_at TIMESTAMP_TZ DEFAULT SYSDATE() NOT NULL,
            updated_at TIMESTAMP_TZ DEFAULT SYSDATE() NOT NULL
        )""",
        "duckdb_contains": ["CREATE TABLE", "CURRENT_TIMESTAMP AT TIME ZONE 'UTC'", "DEFAULT (", "PRIMARY KEY"],
    },
    "arithmetic_with_sysdate": {
        "snowflake": "SELECT SYSDATE() - 30 AS thirty_days_ago",
        "duckdb_contains": ["CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - 30"],
    },
}

# Common patterns that should appear in DuckDB translations
COMMON_PATTERNS = {
    "sysdate_replacement": "CURRENT_TIMESTAMP AT TIME ZONE 'UTC'",
    "quoted_identifiers": True,  # DuckDB typically quotes identifiers
    "case_preservation": True,  # Case should be preserved in translations
}
