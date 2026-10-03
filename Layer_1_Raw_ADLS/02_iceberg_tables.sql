-- ============================================================
-- Layer 1 & 2: Part 2 - Iceberg Tables
-- ============================================================
USE ROLE ACCOUNTADMIN;
USE DATABASE HAZARDWATCH_DB;
USE SCHEMA RAW_SCHEMA;

CREATE OR REPLACE ICEBERG TABLE raw_usgs_earthquakes (
  event_id STRING,
  source STRING,
  magnitude DOUBLE,
  place STRING,
  event_time TIMESTAMP,
  updated_at TIMESTAMP,
  latitude DOUBLE,
  longitude DOUBLE,
  depth_km DOUBLE,
  alert STRING,
  status STRING,
  raw_json STRING,
  _ingested_at TIMESTAMP
)
  EXTERNAL_VOLUME = 'hazardwatch_raw_vol'
  CATALOG         = 'SNOWFLAKE'
  BASE_LOCATION   = 'source=usgs/';

CREATE OR REPLACE ICEBERG TABLE raw_gdacs_alerts (
  event_id STRING,
  source STRING,
  event_type STRING,
  alert_level STRING,
  event_name STRING,
  event_time TIMESTAMP,
  latitude DOUBLE,
  longitude DOUBLE,
  country STRING,
  affected_population BIGINT,
  raw_json STRING,
  _ingested_at TIMESTAMP
)
  EXTERNAL_VOLUME = 'hazardwatch_raw_vol'
  CATALOG         = 'SNOWFLAKE'
  BASE_LOCATION   = 'source=gdacs/';

CREATE OR REPLACE ICEBERG TABLE raw_firms_fires (
  event_id STRING,
  source STRING,
  latitude DOUBLE,
  longitude DOUBLE,
  brightness DOUBLE,
  frp DOUBLE,
  confidence STRING,
  acq_date DATE,
  acq_time STRING,
  satellite STRING,
  daynight STRING,
  raw_json STRING,
  _ingested_at TIMESTAMP
)
  EXTERNAL_VOLUME = 'hazardwatch_raw_vol'
  CATALOG         = 'SNOWFLAKE'
  BASE_LOCATION   = 'source=firms/';

CREATE OR REPLACE ICEBERG TABLE raw_openaq_readings (
  location_id BIGINT,
  source STRING,
  city STRING,
  country STRING,
  parameter STRING,
  value DOUBLE,
  unit STRING,
  reading_time TIMESTAMP,
  latitude DOUBLE,
  longitude DOUBLE,
  raw_json STRING,
  _ingested_at TIMESTAMP
)
  EXTERNAL_VOLUME = 'hazardwatch_raw_vol'
  CATALOG         = 'SNOWFLAKE'
  BASE_LOCATION   = 'source=openaq/';

CREATE OR REPLACE ICEBERG TABLE raw_eonet_events (
  event_id STRING,
  source STRING,
  title STRING,
  category STRING,
  status STRING,
  latitude DOUBLE,
  longitude DOUBLE,
  event_date TIMESTAMP,
  raw_json STRING,
  _ingested_at TIMESTAMP
)
  EXTERNAL_VOLUME = 'hazardwatch_raw_vol'
  CATALOG         = 'SNOWFLAKE'
  BASE_LOCATION   = 'source=eonet/';
