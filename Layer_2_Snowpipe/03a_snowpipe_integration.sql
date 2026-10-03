-- ============================================================
-- Layer 3: Snowpipe (Part 1 - Infrastructure)
-- ============================================================
USE ROLE ACCOUNTADMIN;
USE DATABASE HAZARDWATCH_DB;
USE SCHEMA RAW_SCHEMA;

-- ------------------------------------------------------------
-- STEP 1: Create an External Stage
-- ------------------------------------------------------------
CREATE OR REPLACE STAGE hazardwatch_raw_stage
  URL = 'azure://hazardwatchraw.blob.core.windows.net/raw/'
  STORAGE_INTEGRATION = hazardwatch_azure_int
  FILE_FORMAT = (TYPE = PARQUET);

-- ------------------------------------------------------------
-- STEP 2: Create Landing Tables
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE landing_usgs (
  raw_file_name   STRING,
  raw_row_number  INT,
  raw_value       VARIANT,
  _loaded_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE landing_gdacs (
  raw_file_name   STRING,
  raw_row_number  INT,
  raw_value       VARIANT,
  _loaded_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE landing_firms (
  raw_file_name   STRING,
  raw_row_number  INT,
  raw_value       VARIANT,
  _loaded_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE landing_openaq (
  raw_file_name   STRING,
  raw_row_number  INT,
  raw_value       VARIANT,
  _loaded_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE landing_eonet (
  raw_file_name   STRING,
  raw_row_number  INT,
  raw_value       VARIANT,
  _loaded_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE landing_worldpop (
  raw_file_name   STRING,
  raw_row_number  INT,
  raw_value       VARIANT,
  _loaded_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ------------------------------------------------------------
-- STEP 3: Create Notification Integration
-- ------------------------------------------------------------
CREATE OR REPLACE NOTIFICATION INTEGRATION hazardwatch_eventgrid_int
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
  ENABLED = TRUE
  AZURE_STORAGE_QUEUE_PRIMARY_URI = 'https://hazardwatchraw.queue.core.windows.net/hazardwatch-refresh'
  AZURE_TENANT_ID = '${AZURE_TENANT_ID}';
