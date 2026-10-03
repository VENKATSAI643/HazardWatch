# Phase 1: Snowflake Infrastructure Setup

## What is the use of this? (Layman's Terms)
Before you can start moving data around, you need to build the physical "rooms" and "machinery" inside your Snowflake account. 
This script acts like a blueprint for a construction crew. When you run this script in Snowflake, it will automatically:

1. **Build a Security Guard (Resource Monitor):** It sets a strict budget so you never accidentally spend too much money. If the system uses too many credits (10 in this case), the guard forcefully shuts it down so you don't get a surprise bill.
2. **Build the Engine (Warehouse):** It creates the `HAZARDWATCH_WH` compute engine. We set it to 'X-Small' to keep costs incredibly low, and we tell it to automatically go to sleep after just 60 seconds of doing nothing.
3. **Build the Filing Cabinets (Database & Schemas):** It creates the `HAZARDWATCH_DB` database, and then creates the specific folders (schemas) inside it that our project needs: `RAW`, `BRONZE`, `SILVER`, `GOLD`, and `AUDIT`.
4. **Create Security Badges (Roles):** It creates different access levels. The `HAZARDWATCH_READER` badge only lets people look at the finished `GOLD` data (this is the badge we will give to Superset later), while the `HAZARDWATCH_ADMIN` badge can do everything.

## Instructions
*As per your project rules, I will not execute this against your Snowflake account automatically.*

1. Open a new Worksheet in your Snowflake web interface.
2. Make sure your role in the top right corner is set to `ACCOUNTADMIN`.
3. Copy and paste the SQL code below.
4. Replace `YOUR_USERNAME` at the very bottom with your actual Snowflake username.
5. Click **Run All**.

```sql
-- 1. Create the Resource Monitor (The Security Guard)
-- This ensures you never accidentally spend more than 10 credits a month.
CREATE OR REPLACE RESOURCE MONITOR hazardwatch_monitor
  WITH
    CREDIT_QUOTA = 10
    FREQUENCY    = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
      ON 50  PERCENT DO NOTIFY
      ON 80  PERCENT DO NOTIFY
      ON 100 PERCENT DO SUSPEND;

-- 2. Create the Warehouse (The Engine)
-- X-Small size, auto-suspends after 60 seconds to save money, and is tied to the monitor above.
CREATE OR REPLACE WAREHOUSE HAZARDWATCH_WH
  WITH WAREHOUSE_SIZE = 'XSMALL'
       AUTO_SUSPEND = 60
       AUTO_RESUME = TRUE
       RESOURCE_MONITOR = hazardwatch_monitor;

-- 3. Create the Database and Schemas (The Filing Cabinets)
CREATE OR REPLACE DATABASE HAZARDWATCH_DB;

CREATE OR REPLACE SCHEMA HAZARDWATCH_DB.RAW_SCHEMA;
CREATE OR REPLACE SCHEMA HAZARDWATCH_DB.BRONZE_SCHEMA;
CREATE OR REPLACE SCHEMA HAZARDWATCH_DB.SILVER_SCHEMA;
CREATE OR REPLACE SCHEMA HAZARDWATCH_DB.GOLD_SCHEMA;
CREATE OR REPLACE SCHEMA HAZARDWATCH_DB.AUDIT_SCHEMA;

-- 4. Create the Roles (The Security Badges)
CREATE OR REPLACE ROLE HAZARDWATCH_ADMIN;
CREATE OR REPLACE ROLE HAZARDWATCH_READER;
CREATE OR REPLACE ROLE HAZARDWATCH_INGEST;

-- Grant the Admin role access to everything
GRANT ALL PRIVILEGES ON DATABASE HAZARDWATCH_DB TO ROLE HAZARDWATCH_ADMIN;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE HAZARDWATCH_DB TO ROLE HAZARDWATCH_ADMIN;
GRANT USAGE, OPERATE ON WAREHOUSE HAZARDWATCH_WH TO ROLE HAZARDWATCH_ADMIN;

-- Grant the Reader role (Superset) access ONLY to the Gold Schema
GRANT USAGE ON DATABASE HAZARDWATCH_DB TO ROLE HAZARDWATCH_READER;
GRANT USAGE ON SCHEMA HAZARDWATCH_DB.GOLD_SCHEMA TO ROLE HAZARDWATCH_READER;
GRANT SELECT ON ALL TABLES IN SCHEMA HAZARDWATCH_DB.GOLD_SCHEMA TO ROLE HAZARDWATCH_READER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA HAZARDWATCH_DB.GOLD_SCHEMA TO ROLE HAZARDWATCH_READER;
GRANT USAGE ON WAREHOUSE HAZARDWATCH_WH TO ROLE HAZARDWATCH_READER;

-- Grant all roles to the all-powerful SYSADMIN so you can manage them in the UI easily
GRANT ROLE HAZARDWATCH_ADMIN TO ROLE SYSADMIN;
GRANT ROLE HAZARDWATCH_READER TO ROLE SYSADMIN;
GRANT ROLE HAZARDWATCH_INGEST TO ROLE SYSADMIN;

-- Finally, give YOUR user account the Admin badge
-- !!! CHANGE 'YOUR_USERNAME' TO YOUR ACTUAL USERNAME !!!
GRANT ROLE HAZARDWATCH_ADMIN TO USER YOUR_USERNAME;
```
