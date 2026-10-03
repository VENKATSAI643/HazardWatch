# Layer 3: Manual Backfill

## What is the use of this? (Layman's Terminology)
Usually, our Snowflake database is automatically fed by **Snowpipe**. Snowpipe acts like a conveyor belt that waits for *brand new* files to arrive in your Azure Storage. When a new file arrives, it instantly copies it into Snowflake.

However, Snowpipe is blind to the past! If there were already 100 files sitting in your Azure Storage *before* you built the Snowflake tables or started the Snowpipe, the pipe will ignore them completely. 

This `Layer 3_Manual_Backfill` step exists to solve that problem.

## When should you use this?
1. **First-time Setup:** When you first create your empty tables and need to dump all your historical, pre-existing Azure data into them.
2. **Missing Data:** If Snowpipe fails or gets stuck, and you need to manually force Snowflake to grab files it missed.
3. **Static Data:** For data like `WorldPop` (population density) that doesn't stream continuously and just needs to be loaded once.

## How to use it
You can either run this directly in the Snowflake Web Interface or execute it via your WSL terminal using the Snowflake CLI.

### Option 1: Via Terminal (Recommended)
Open your WSL terminal and run:
```bash
snow sql -f /mnt/e/Snowflake/HazardWatch/Layer_3_Manual_Backfill/04_manual_backfill.sql
```

### Option 2: Via Snowflake Web Interface
1. Open your **Snowflake Worksheet**.
2. Switch your role to **`ACCOUNTADMIN`**.
3. Copy the contents of `04_manual_backfill.sql` and click **Run All**.
4. Snowflake will manually scan the Azure folders and copy all the data it finds directly into your landing tables!
