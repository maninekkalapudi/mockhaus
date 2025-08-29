-- =============================================================================
-- Data Pipeline MERGE Workflow - End-to-End Test SQL Script
-- =============================================================================
-- This script tests the complete Mockhaus data pipeline workflow including:
-- 1. File format creation
-- 2. Stage creation  
-- 3. Table creation (staging and final)
-- 4. Initial data load via COPY INTO
-- 5. First MERGE operation (INSERT)
-- 6. Incremental data load
-- 7. Second MERGE operation (INSERT + UPDATE)
-- 8. Data validation queries
-- =============================================================================

-- Step 1: Create CSV file format for customer data
-- ================================================
CREATE FILE FORMAT csv_customer_format
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
NULL_IF = ('NULL', 'null', '')
ENCODING = 'UTF-8'
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- Step 2: Create internal stage for data files
-- ============================================
CREATE STAGE customer_data_stage
FILE_FORMAT = csv_customer_format;

-- Step 3: Create target tables
-- ============================

-- Final target table (represents production table)
CREATE TABLE customer_final (
    customer_id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    signup_date DATE,
    status VARCHAR(20),
    last_updated TIMESTAMP,
    version INTEGER DEFAULT 1
);

-- Staging table (for incoming data)
CREATE TABLE customer_staging (
    customer_id INTEGER,
    name VARCHAR(100),
    email VARCHAR(100),
    signup_date DATE,
    status VARCHAR(20),
    last_updated TIMESTAMP
);

-- Step 4: Load initial data batch into staging table
-- ==================================================
COPY INTO customer_staging
FROM '@customer_data_stage/customer_initial_data.csv'
FILE_FORMAT = csv_customer_format;

-- Step 5: Verify initial data load
-- ================================
SELECT 'Initial staging data loaded' as checkpoint, COUNT(*) as record_count 
FROM customer_staging;

-- Step 6: First MERGE operation - Initial data load
-- =================================================
MERGE INTO customer_final AS target
USING customer_staging AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN
    UPDATE SET
        name = source.name,
        email = source.email,
        status = source.status,
        last_updated = source.last_updated,
        version = target.version + 1
WHEN NOT MATCHED THEN
    INSERT (customer_id, name, email, signup_date, status, last_updated, version)
    VALUES (source.customer_id, source.name, source.email, source.signup_date, source.status, source.last_updated, 1);

-- Step 7: Verify first merge results
-- ==================================
SELECT 'After first merge' as checkpoint, COUNT(*) as record_count 
FROM customer_final;

-- Step 8: Clear staging table for next batch
-- ==========================================
DELETE FROM customer_staging;

-- Step 9: Load incremental data batch into staging table
-- =====================================================
COPY INTO customer_staging
FROM '@customer_data_stage/customer_incremental_data.csv'
FILE_FORMAT = csv_customer_format;

-- Step 10: Verify incremental data load
-- =====================================
SELECT 'Incremental staging data loaded' as checkpoint, COUNT(*) as record_count 
FROM customer_staging;

-- Step 11: Second MERGE operation - Incremental updates and inserts
-- =================================================================
MERGE INTO customer_final AS target
USING customer_staging AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN
    UPDATE SET
        name = source.name,
        email = source.email,
        status = source.status,
        last_updated = source.last_updated,
        version = target.version + 1
WHEN NOT MATCHED THEN
    INSERT (customer_id, name, email, signup_date, status, last_updated, version)
    VALUES (source.customer_id, source.name, source.email, source.signup_date, source.status, source.last_updated, 1);

-- Step 12: Final data validation
-- ==============================

-- Count total records in final table
SELECT 'Final record count' as validation_check, COUNT(*) as record_count 
FROM customer_final;

-- Verify version tracking (updated records should have version > 1)
SELECT 'Version tracking' as validation_check, 
       COUNT(CASE WHEN version = 1 THEN 1 END) as new_records,
       COUNT(CASE WHEN version > 1 THEN 1 END) as updated_records
FROM customer_final;

-- Show all final data for verification
SELECT 'Final data state' as validation_check, 
       customer_id, name, email, signup_date, status, last_updated, version
FROM customer_final
ORDER BY customer_id;

-- Verify specific business logic
-- ==============================

-- Check that customer 1002 was updated (should have version 2)
SELECT 'Customer 1002 update verification' as validation_check,
       customer_id, name, email, version
FROM customer_final 
WHERE customer_id = 1002;

-- Check that customer 1003 status was updated from 'pending' to 'active'  
SELECT 'Customer 1003 status update verification' as validation_check,
       customer_id, name, status, version
FROM customer_final
WHERE customer_id = 1003;

-- Check that new customer 1004 was inserted
SELECT 'Customer 1004 insertion verification' as validation_check,
       customer_id, name, email, version
FROM customer_final
WHERE customer_id = 1004;

-- Clean staging table for cleanup
DELETE FROM customer_staging;

-- Final checkpoint
SELECT 'Workflow completed successfully' as final_checkpoint, 
       'All MERGE operations completed' as status;