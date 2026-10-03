# Layer 4: Bronze (Raw but Cleaned Data)

## What is this layer?
The Bronze layer is the first step where data actually lives as structured tables inside Snowflake. It takes the messy, raw data that was just dumped into Snowflake (via Snowpipe) and cleans it up just enough so that it is reliable to query, but without changing its core meaning.

## Why is it used? (Layman's Terminology)
Imagine you are receiving packages from five different delivery companies (USGS, GDACS, FIRMS, OpenAQ, EONET) every day.
- **The Raw Layer (Layer 1-3)** is like the loading dock where boxes are just thrown onto the floor as fast as possible. There might be duplicate boxes, the labels might be a bit messy, and it's hard to find what you need.
- **The Bronze Layer** is when you take those boxes from the loading dock, put them on proper shelves, remove any accidental duplicates, check that the tracking numbers are legible, and organize them by date. 

You still haven't opened the boxes to use the contents yet (that happens in the Silver and Gold layers), but you've made sure the boxes are organized and safe.

## What do these scripts do?
We are using a tool called **dbt (data build tool)** to do this cleanup automatically. The SQL scripts in the `hazardwatch_dbt/models/bronze/` folder do the following:
1. **Extract:** They read the raw JSON data that was ingested.
2. **Type-cast:** They pull out specific fields (like magnitude, latitude, longitude) and make sure Snowflake knows they are numbers, dates, or text.
3. **Deduplicate:** If the system accidentally downloaded the same earthquake twice, these scripts use a clever trick (`ROW_NUMBER() OVER (...)`) to keep only the most recently updated version of that event and discard the duplicate.
4. **Filter:** They remove any data that is obviously broken (like a latitude of 900 degrees, which is impossible since latitude only goes up to 90).

## How to use this?
Since dbt handles the creation of these tables in Snowflake automatically, you do not need to manually run these `CREATE TABLE` commands. When we run `dbt run`, dbt will connect to Snowflake, compile these `SELECT` statements, and build the cleaned Bronze tables for you.

To execute the pipeline, open your WSL terminal, navigate to the `hazardwatch_dbt` folder, and run:
```bash
# Standard incremental run (only processes new data)
dbt run

# Full refresh run (drops all tables and rebuilds them completely from scratch)
dbt run --full-refresh
```

> **Note on Full Refresh:** `dbt` uses Incremental Models to save time and money by only processing new data. If you ever need to manually backfill old data or reset the tables completely, use the `--full-refresh` flag so `dbt` knows to start from a blank slate!
