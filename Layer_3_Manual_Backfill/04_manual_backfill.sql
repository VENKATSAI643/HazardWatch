-- ============================================================
-- Layer 3: Manual Backfill
-- ============================================================
-- Use this script if you need to manually load historical data 
-- from your Azure storage container into the Snowflake landing tables.
-- This is necessary if the tables were created AFTER files were 
-- already dropped into Azure, or if Snowpipe failed to ingest them.

USE ROLE ACCOUNTADMIN;
USE DATABASE HAZARDWATCH_DB;
USE SCHEMA RAW_SCHEMA;

-- 0. Ensure the Stage exists (Requires Layer 1 Storage Integration)
CREATE STAGE IF NOT EXISTS hazardwatch_raw_stage
  URL = 'azure://hazardwatchraw.blob.core.windows.net/raw/'
  STORAGE_INTEGRATION = hazardwatch_azure_int
  FILE_FORMAT = (TYPE = PARQUET);

-- 1. Load USGS Earthquake data
COPY INTO landing_usgs (raw_file_name, raw_row_number, raw_value)
FROM (
  SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, $1
  FROM @hazardwatch_raw_stage/source=usgs/
)
FILE_FORMAT = (TYPE = PARQUET)
ON_ERROR = 'CONTINUE';

-- 2. Load GDACS Alert data
COPY INTO landing_gdacs (raw_file_name, raw_row_number, raw_value)
FROM (
  SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, $1
  FROM @hazardwatch_raw_stage/source=gdacs/
)
FILE_FORMAT = (TYPE = PARQUET)
ON_ERROR = 'CONTINUE';

-- 3. Load FIRMS Fire data
COPY INTO landing_firms (raw_file_name, raw_row_number, raw_value)
FROM (
  SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, $1
  FROM @hazardwatch_raw_stage/source=firms/
)
FILE_FORMAT = (TYPE = PARQUET)
ON_ERROR = 'CONTINUE';

-- 4. Load OpenAQ Air Quality data
COPY INTO landing_openaq (raw_file_name, raw_row_number, raw_value)
FROM (
  SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, $1
  FROM @hazardwatch_raw_stage/source=openaq/
)
FILE_FORMAT = (TYPE = PARQUET)
ON_ERROR = 'CONTINUE';

-- 5. Load EONET Event data
COPY INTO landing_eonet (raw_file_name, raw_row_number, raw_value)
FROM (
  SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, $1
  FROM @hazardwatch_raw_stage/source=eonet/
)
FILE_FORMAT = (TYPE = PARQUET)
ON_ERROR = 'CONTINUE';

-- 6. Load WorldPop static population data (Must be loaded manually)
COPY INTO landing_worldpop (raw_file_name, raw_row_number, raw_value)
FROM (
  SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, $1
  FROM @hazardwatch_raw_stage/source=worldpop/
)
FILE_FORMAT = (TYPE = PARQUET)
ON_ERROR = 'CONTINUE';
