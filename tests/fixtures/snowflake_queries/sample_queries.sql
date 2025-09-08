-- Sample Snowflake queries for testing

-- Simple SELECT with SYSDATE
SELECT SYSDATE() AS current_time FROM users;

-- CREATE TABLE with DEFAULT SYSDATE
CREATE TABLE audit_log (
    id INTEGER PRIMARY KEY,
    action VARCHAR(50) NOT NULL,
    created_at TIMESTAMP_TZ DEFAULT SYSDATE() NOT NULL,
    updated_at TIMESTAMP_TZ DEFAULT SYSDATE() NOT NULL
);

-- Complex query with multiple SYSDATE calls
WITH recent_events AS (
    SELECT * 
    FROM events 
    WHERE created_at >= SYSDATE() - INTERVAL '7 days'
)
SELECT 
    COUNT(*) as event_count, 
    SYSDATE() AS report_time
FROM recent_events;

-- Query with arithmetic on SYSDATE
SELECT 
    user_id,
    login_time,
    SYSDATE() - login_time AS time_since_login
FROM user_sessions 
WHERE login_time >= SYSDATE() - 30;