-- ============================================================
-- Layer 3: Snowpipe (Part 2 - Pipes)
-- ============================================================
USE ROLE ACCOUNTADMIN;
USE DATABASE HAZARDWATCH_DB;
USE SCHEMA RAW_SCHEMA;

-- ------------------------------------------------------------
-- STEP 4: Create Snowpipes
-- (Binds the Stage to the Landing Tables with AUTO_INGEST=TRUE)
-- ------------------------------------------------------------

-- 1. USGS
CREATE OR REPLACE PIPE pipe_usgs
  AUTO_INGEST = TRUE
  INTEGRATION = 'HAZARDWATCH_EVENTGRID_INT'
  COMMENT = 'Loads USGS Parquet batches from raw/source=usgs/'
AS
COPY INTO landing_usgs (raw_file_name, raw_row_number, raw_value)
FROM (
  SELECT METADATA$FILENAME,
         METADATA$FILE_ROW_NUMBER,
         $1
  FROM @hazardwatch_raw_stage/source=usgs/
)
FILE_FORMAT = (TYPE = PARQUET)
ON_ERROR = 'CONTINUE';

-- 2. GDACS
CREATE OR REPLACE PIPE pipe_gdacs
  AUTO_INGEST = TRUE
  INTEGRATION = 'HAZARDWATCH_EVENTGRID_INT'
  COMMENT = 'Loads GDACS Parquet batches from raw/source=gdacs/'
AS
COPY INTO landing_gdacs (raw_file_name, raw_row_number, raw_value)
FROM (
  SELECT METADATA$FILENAME,
         METADATA$FILE_ROW_NUMBER,
         $1
  FROM @hazardwatch_raw_stage/source=gdacs/
)
FILE_FORMAT = (TYPE = PARQUET)
ON_ERROR = 'CONTINUE';

-- 3. FIRMS
CREATE OR REPLACE PIPE pipe_firms
  AUTO_INGEST = TRUE
  INTEGRATION = 'HAZARDWATCH_EVENTGRID_INT'
  COMMENT = 'Loads FIRMS Parquet batches from raw/source=firms/'
AS
COPY INTO landing_firms (raw_file_name, raw_row_number, raw_value)
FROM (
  SELECT METADATA$FILENAME,
         METADATA$FILE_ROW_NUMBER,
         $1
  FROM @hazardwatch_raw_stage/source=firms/
)
FILE_FORMAT = (TYPE = PARQUET)
ON_ERROR = 'CONTINUE';

-- 4. OpenAQ
CREATE OR REPLACE PIPE pipe_openaq
  AUTO_INGEST = TRUE
  INTEGRATION = 'HAZARDWATCH_EVENTGRID_INT'
  COMMENT = 'Loads OpenAQ Parquet batches from raw/source=openaq/'
AS
COPY INTO landing_openaq (raw_file_name, raw_row_number, raw_value)
FROM (
  SELECT METADATA$FILENAME,
         METADATA$FILE_ROW_NUMBER,
         $1
  FROM @hazardwatch_raw_stage/source=openaq/
)
FILE_FORMAT = (TYPE = PARQUET)
ON_ERROR = 'CONTINUE';

-- 5. EONET
CREATE OR REPLACE PIPE pipe_eonet
  AUTO_INGEST = TRUE
  INTEGRATION = 'HAZARDWATCH_EVENTGRID_INT'
  COMMENT = 'Loads EONET Parquet batches from raw/source=eonet/'
AS
COPY INTO landing_eonet (raw_file_name, raw_row_number, raw_value)
FROM (
  SELECT METADATA$FILENAME,
         METADATA$FILE_ROW_NUMBER,
         $1
  FROM @hazardwatch_raw_stage/source=eonet/
)
FILE_FORMAT = (TYPE = PARQUET)
ON_ERROR = 'CONTINUE';
