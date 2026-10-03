# Layer 8: Superset Dashboard (Concepts & Architecture)

## Core Concepts Used in this Layer

### 1. Principle of Least Privilege (RBAC)
When connecting a third-party BI tool (like Superset) to your data warehouse, you should never use an Admin account. 
We use a dedicated Role-Based Access Control (RBAC) strategy:
- We create a role called `HAZARDWATCH_READER`.
- We grant it `USAGE` on the database and `GOLD_SCHEMA`.
- We grant it `SELECT` on all tables in the `GOLD_SCHEMA`.
- It has absolutely zero access to the `RAW` or `SILVER` schemas, preventing accidental full-table scans of billions of rows.

### 2. Result Caching
Every time a query hits Snowflake, the Virtual Warehouse wakes up and consumes credits. 
Superset has a built-in Result Cache (usually backed by Redis). If 1,000 users open the World Map dashboard within 5 minutes, Superset only queries Snowflake exactly one time. The other 999 users are served the cached JSON payload directly from Superset's memory, completely bypassing Snowflake compute costs.

### 3. Aggressive Auto-Suspend
The Virtual Warehouse assigned to Superset (`HAZARDWATCH_WH`) is configured as `X-Small` with an auto-suspend of `60 seconds`. 
Because our queries run in milliseconds against the pre-aggregated Gold tables, the warehouse wakes up, serves the data, and immediately goes back to sleep 60 seconds later.
