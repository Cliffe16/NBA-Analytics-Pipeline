-- Create a virtual warehouse for compute
CREATE WAREHOUSE NBA_WH
    WITH WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 300              -- Suspend after 5 minutes idle
    AUTO_RESUME = TRUE              -- Auto-resume when queried
    INITIALLY_SUSPENDED = TRUE;     -- Don't start it yet

-- Set it as default for this session
USE WAREHOUSE NBA_WH;

-- Verify warehouse
SHOW WAREHOUSES;
