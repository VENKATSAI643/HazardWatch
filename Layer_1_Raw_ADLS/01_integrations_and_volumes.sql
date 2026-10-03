-- ============================================================
-- Layer 1 & 2: Part 1 - Integrations and Volumes
-- ============================================================
USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS HAZARDWATCH_DB;
USE DATABASE HAZARDWATCH_DB;

CREATE SCHEMA IF NOT EXISTS RAW_SCHEMA;
USE SCHEMA RAW_SCHEMA;

CREATE OR REPLACE STORAGE INTEGRATION hazardwatch_azure_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = '${AZURE_TENANT_ID}'
  STORAGE_ALLOWED_LOCATIONS = (
    'azure://hazardwatchraw.blob.core.windows.net/raw/'
  );

CREATE OR REPLACE EXTERNAL VOLUME hazardwatch_raw_vol
  STORAGE_LOCATIONS = (
    (
      NAME             = 'hazardwatch-raw-eastus'
      STORAGE_PROVIDER = 'AZURE'
      STORAGE_BASE_URL = 'azure://hazardwatchraw.blob.core.windows.net/raw/'
      AZURE_TENANT_ID  = '${AZURE_TENANT_ID}'
    )
  );
