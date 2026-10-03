# Fix: Missing Landing Tables

If you ran the previous fix and `worldpop` succeeded but the others still failed, it means **the other landing tables literally do not exist in your Snowflake account**. 

This usually happens if the `03a_snowpipe_integration.sql` script wasn't fully executed or if an error occurred during that step and the tables were never created. 

Let's force-create all the missing tables right now and apply the permissions again to be 100% sure they are there for `dbt`!

---

## Instructions

1. Go back to your **Snowflake Web Interface** (Worksheet).
2. Ensure your role is **`ACCOUNTADMIN`**.
3. Copy, paste, and **Run All** of the SQL below.
4. Try your `dbt run` command one last time!

```sql
USE ROLE ACCOUNTADMIN;
USE DATABASE HAZARDWATCH_DB;
USE SCHEMA RAW_SCHEMA;

-- 1. Force Create ALL Landing Tables
CREATE TABLE IF NOT EXISTS landing_usgs (
  raw_file_name   STRING,
  raw_row_number  INT,
  raw_value       VARIANT,
  _loaded_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS landing_gdacs (
  raw_file_name   STRING,
  raw_row_number  INT,
  raw_value       VARIANT,
  _loaded_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS landing_firms (
  raw_file_name   STRING,
  raw_row_number  INT,
  raw_value       VARIANT,
  _loaded_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS landing_openaq (
  raw_file_name   STRING,
  raw_row_number  INT,
  raw_value       VARIANT,
  _loaded_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS landing_eonet (
  raw_file_name   STRING,
  raw_row_number  INT,
  raw_value       VARIANT,
  _loaded_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS landing_worldpop (
  raw_file_name   STRING,
  raw_row_number  INT,
  raw_value       VARIANT,
  _loaded_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- 2. Grant Permissions to dbt
GRANT SELECT ON ALL TABLES IN SCHEMA HAZARDWATCH_DB.RAW_SCHEMA TO ROLE HAZARDWATCH_ADMIN;
GRANT SELECT ON FUTURE TABLES IN SCHEMA HAZARDWATCH_DB.RAW_SCHEMA TO ROLE HAZARDWATCH_ADMIN;
```
