# Layer 7: Published & Mirror (Concepts & Architecture)

## Core Concepts Used in this Layer

### 1. Reverse ETL (Snowflake to ADLS)
While Snowpipe handles data ingestion, we use `COPY INTO <stage>` to handle data egress (exporting). By setting `OVERWRITE = TRUE`, Snowflake simply replaces the existing files in the Azure container with the freshest cut of the Gold table. The output format is Snappy-compressed Parquet, which is the gold standard for big data files.

### 2. Zero-Egress Storage (Cloudflare R2)
Cloud providers like AWS and Azure charge heavily for "data egress" (data leaving their network). For an open-source or public dataset, this can quickly become a massive financial liability. 
Cloudflare R2 is S3-compatible object storage that charges for storage space but has **$0 egress fees**. We use it as our public-facing CDN for the dataset.

### 3. Avoiding Iceberg Metadata Corruption on Mirroring
Notice in `rclone_mirror_job.sh` we explicitly use `--include "*.parquet"`. 
We are deliberately leaving the Iceberg metadata (`.json`, `.avro`) behind. 
Why? Because Iceberg metadata files hardcode the absolute URL of the data files (e.g., `azure://hazardwatchpub...`). If we mirrored the metadata files to R2, any user trying to read the Iceberg table from R2 would get an error as their engine would try to fetch the raw files from Azure, hitting permission blocks.
Instead, we only provide the raw Parquet files on R2, which allows public users to read them natively or build their own external tables using Hive-style partition inference.
