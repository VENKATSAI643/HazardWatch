# Layer 3: Snowpipe (Automated High-Frequency Ingestion)

## What is Snowpipe?
Snowpipe is Snowflake's continuous data ingestion service. Instead of running batch `COPY INTO` commands every hour (and paying for an active Virtual Warehouse the whole time), Snowpipe listens for event notifications from your cloud provider (Azure Event Grid) and automatically ingests new files the exact second they land in your storage container.

Snowpipe uses "serverless" compute, meaning Snowflake automatically provisions and scales the compute required to ingest the files, and you are only billed per-second for the exact compute used during the micro-ingestion.

## Why are we using Snowpipe?
In Layer 1/2, we built **Iceberg Tables**, which allow us to query our raw Azure data directly. However, we discovered that Snowflake-managed Iceberg Tables do not support native auto-refresh. 

To achieve a true **near-real-time** architecture without manual intervention, we will use **Snowpipe** to automatically pull those raw Parquet files into internal Snowflake **Landing Tables** as VARIANT columns. 

This creates a highly robust "Dual-Path" architecture:
1. **Iceberg Tables (Layer 1/2):** Great for ad-hoc queries, manual data uploads, and massive historical scans without paying Snowflake storage costs.
2. **Snowpipe Landing Tables (Layer 3):** Great for automated, high-frequency, near-real-time streaming pipelines that feed our downstream `dbt` models.

## The Architecture of this Layer
1. **External Stage:** A pointer telling Snowflake where to look in Azure (using the same Storage Integration we created in Layer 1).
2. **Landing Tables:** 5 raw tables (one for each source) with a simple schema: File Name, Row Number, Raw JSON/Parquet (VARIANT), and Load Timestamp.
3. **Pipes:** 5 Snowpipes that bind the Stage to the Landing Tables.

*(Note: We will configure the Azure Event Grid triggers for the pipes in a later step!)*
