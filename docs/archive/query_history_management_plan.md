# Query History Management Plan

## Overview
This document outlines future enhancements for query history management through REPL and REST API interfaces.

## REPL Slash Commands

### Proposed Commands
```
/history                    # Show recent query history
/history search <text>      # Search query history
/history clear              # Clear all history (with confirmation)
/history clear --before <date>  # Clear history before specific date
/history stats              # Show query statistics
/history export <format>    # Export history (json, csv)
/history disable            # Temporarily disable history recording
/history enable             # Re-enable history recording
```

### Implementation Notes
- Add slash command parser to enhanced REPL
- Commands should be clearly distinguished from SQL queries
- Consider adding `/h` as shorthand for `/history`
- Add confirmation prompts for destructive operations

## REST API Endpoints

### Proposed Endpoints
```
GET  /api/v1/history              # Get recent history with pagination
GET  /api/v1/history/:id          # Get specific query by ID
POST /api/v1/history/search       # Search with filters
GET  /api/v1/history/stats        # Get statistics
DELETE /api/v1/history            # Clear all history
DELETE /api/v1/history/before/:date  # Clear before date
POST /api/v1/history/export       # Export history
```

### Security Considerations
- Add authentication/authorization
- Consider rate limiting for history queries
- Option to disable history endpoints entirely
- Sanitize query text in responses (optional)

## Configuration Options

### Proposed Config
```yaml
history:
  enabled: true
  retention_days: 30
  max_size_mb: 1000
  
  # Privacy settings
  redact_literals: false
  exclude_patterns:
    - ".*password.*"
    - ".*secret.*"
  
  # API settings
  api:
    enabled: true
    require_auth: true
    max_results: 1000
  
  # REPL settings  
  repl:
    slash_commands: true
    auto_complete: true
```

## Future Enhancements

1. **Auto-cleanup**: Background process to enforce retention policies
2. **Query replay**: Ability to replay queries from history
3. **History analytics**: Built-in dashboards for query patterns
4. **Export templates**: Customizable export formats
5. **History sync**: Optional cloud backup/sync
6. **Team sharing**: Share query history with team members

## Implementation Priority

1. **Phase 1**: Basic slash commands in REPL
2. **Phase 2**: REST API endpoints
3. **Phase 3**: Configuration and privacy controls
4. **Phase 4**: Advanced features (analytics, sync, etc.)

## Notes
- Keep history management commands separate from SQL execution
- Ensure all operations are logged for audit purposes
- Consider performance impact of history queries on main operations
- Design with multi-user scenarios in mind for future