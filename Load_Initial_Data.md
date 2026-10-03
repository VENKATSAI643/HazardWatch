# Step 3: Load Initial Data

## Why are the tables empty? (Layman's terms)
Because we just forcefully created the `landing_` tables from scratch in the last step, they are completely empty (0 rows).

Snowpipe is like a conveyor belt that only turns on when a *brand new* box (file) is dropped into Azure. It doesn't automatically reach back in time and grab all the boxes that were already sitting in Azure before the tables were created. 

Since the landing tables are empty, when `dbt` runs, it successfully reads 0 rows, transforms 0 rows, and creates Bronze, Silver, and Gold tables with 0 rows!

## The Solution
We need to manually tell Snowflake to grab all the historical data currently sitting in your Azure storage and dump it into our empty landing tables.

## Instructions
1. Go back to your **Snowflake Web Interface** (Worksheet).
2. Ensure your role is **`ACCOUNTADMIN`**.
3. Copy, paste, and **Run All** of the SQL below. This will manually copy the data from Azure into your tables.
4. Once you see the output saying files were loaded, go back to WSL and run `dbt run` one last time!

```sql
USE ROLE ACCOUNTADMIN;
USE DATABASE HAZARDWATCH_DB;
USE SCHEMA RAW_SCHEMA;

-- Load USGS Earthquake data
COPY INTO landing_usgs
FROM @hazardwatch_raw_stage/source=usgs/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = NONE;

-- Load GDACS Alert data
COPY INTO landing_gdacs
FROM @hazardwatch_raw_stage/source=gdacs/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = NONE;

-- Load FIRMS Fire data
COPY INTO landing_firms
FROM @hazardwatch_raw_stage/source=firms/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = NONE;

-- Load OpenAQ Air Quality data
COPY INTO landing_openaq
FROM @hazardwatch_raw_stage/source=openaq/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = NONE;

-- Load EONET Event data
COPY INTO landing_eonet
FROM @hazardwatch_raw_stage/source=eonet/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = NONE;

-- Load WorldPop static population data
COPY INTO landing_worldpop
FROM @hazardwatch_raw_stage/source=worldpop/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = NONE;
```
