-- External stage for published output
CREATE OR REPLACE STAGE hazardwatch_pub_stage
  URL = 'azure://hazardwatchpub.blob.core.windows.net/published/'
  STORAGE_INTEGRATION = hazardwatch_azure_int
  FILE_FORMAT = (TYPE = PARQUET SNAPPY_COMPRESSION = TRUE);

-- Unload (run by Snowflake Task after each dbt build)
COPY INTO @hazardwatch_pub_stage/gold_event_exposure/
FROM HAZARDWATCH_DB.GOLD_SCHEMA.gold_event_exposure
OVERWRITE = TRUE HEADER = TRUE;

COPY INTO @hazardwatch_pub_stage/gold_daily_summary/
FROM HAZARDWATCH_DB.GOLD_SCHEMA.gold_daily_summary
OVERWRITE = TRUE HEADER = TRUE;
