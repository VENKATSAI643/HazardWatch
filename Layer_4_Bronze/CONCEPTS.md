# Layer 4: Bronze (Concepts & Architecture)

## The Medallion Architecture
The Medallion Architecture (Bronze, Silver, Gold) is a data design pattern used to logically organize data in a data lake or data warehouse, with the goal of incrementally and progressively improving the structure and quality of data.
* **Bronze (Raw):** The landing zone for raw data. The goal here is to simply apply a schema (types) and remove duplicates, without losing any historical records or meaning. 
* **Silver (Validated):** Data from various sources is merged, enriched, and cleaned into unified tables. 
* **Gold (Enriched):** Business-level aggregates, highly refined and optimized for BI/Dashboards.

## Core Concepts Used in this Layer

### 1. dbt (data build tool)
dbt is a transformation workflow that lets teams quickly and collaboratively deploy analytics code following software engineering best practices like modularity, portability, CI/CD, and documentation. In our setup, dbt manages all the transformations from the Bronze layer up to Gold by simply running `SELECT` statements, which dbt compiles into `CREATE TABLE` or `MERGE` statements in Snowflake.

### 2. Incremental Models
Notice the `{{ config(materialized = 'incremental') }}` block in the scripts. 
Instead of dropping and rebuilding a giant table with millions of records every 4 hours, an **incremental model** only processes the *new* records that have arrived since the last time the pipeline ran. It does this by checking the `_ingested_at` timestamp. This drastically reduces Snowflake compute costs and speeds up the pipeline.

### 3. Window Functions for Deduplication
When polling APIs via Azure Functions, network errors might cause the exact same event to be downloaded multiple times. 
We use Snowflake's Window Functions to deduplicate records on the fly:
```sql
ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY _ingested_at DESC)
```
This tells Snowflake to group the data by the unique `event_id`, sort them by the newest ingested time first, and assign them a row number. By keeping only `rn = 1`, we effortlessly drop all duplicates and keep the most up-to-date payload.

### 4. Semi-Structured Data parsing (VARIANT)
The Snowpipe layer loaded our data as a `VARIANT` column (a flexible JSON-like data type in Snowflake). In the Bronze layer, we extract the strongly typed columns directly from the JSON using Snowflake's colon notation:
```sql
raw_value:magnitude::FLOAT AS magnitude
```
This extracts the `magnitude` field from the JSON and casts it as a Float.
