# Layer 1 & 2 — Raw ADLS & External Catalog Concepts

## What Is This Layer?

This layer builds the bridge between the raw files sitting in your Azure storage (from Layer 0) and your Snowflake database. It uses **Apache Iceberg**, which is an open table format. Instead of Snowflake copying all the data into its own expensive internal storage, we create "External Volumes" and "Iceberg Tables". This tells Snowflake: *"Hey, the data is over there in Azure. Go read it directly, and treat it like a normal table."*

## Why Do We Need It?

Historically, to query data in a data warehouse, you had to run a `COPY INTO` command to ingest the files. This meant paying for storage twice (once in Azure, once in Snowflake) and paying for the compute to copy it. By using Unmanaged Iceberg Tables (External Catalog), we get zero-copy data sharing. Even better, if someone manually drops a Parquet file into our Azure folder via a file explorer, Snowflake will automatically see it within seconds via an Azure Event Grid notification without us writing a single line of ingestion code.

## Real-World Analogy

Imagine a public library (Azure Data Lake) full of millions of books (Parquet files). 
In the old days, if you (Snowflake) wanted to read them, you had to hire a moving truck, copy every single page, and put them in your own private basement (Snowflake internal storage).

With Apache Iceberg, you just put a **Card Catalog** (Iceberg Metadata) in the library. When you want to query something, you look at the Card Catalog to find exactly which shelf and book has your answer, and you walk over and read just that one page directly. The books never leave the library.

## Key Technologies

- **Apache Iceberg:** An open-source table format for huge datasets. It keeps track of which files belong to which table and which files were added/deleted over time (metadata snapshots).
- **Parquet:** The actual physical files containing the data (which Layer 0 is currently writing).
- **Snowflake External Volume:** A secure configuration in Snowflake that stores the Azure credentials needed to read your ADLS storage account.
- **Azure Event Grid:** A notification system. When a new file lands in Azure, Event Grid shouts "New file!", and Snowflake hears it and instantly updates its Iceberg table.

## Key Terms Glossary

| Term | Plain English Meaning |
|---|---|
| **Zero-Copy** | Querying data directly where it lives without duplicating it. |
| **External Volume** | Snowflake's secure wrapper around an Azure Storage Account. |
| **Storage Integration** | The handshake between Snowflake and Azure that says "Snowflake is allowed to read this folder." |
| **Metadata** | Data about data. In Iceberg, it's tiny JSON/Avro files that say "There are 5 Parquet files, here are their names, and here are the minimum and maximum values inside them." |
| **Auto-Refresh** | Snowflake's ability to automatically listen to Azure Event Grid and update the table when a new file arrives. |

## How Data Flows Through This Layer

```
Azure Data Lake Storage (Raw Parquet Files from Layer 0)
        |
        +-- PyIceberg (in Layer 0) writes a metadata.json file
        |
        v
Snowflake External Volume (Looks at ADLS)
        |
        v
Snowflake Unmanaged Iceberg Table (Reads metadata.json)
        |
        v
Data is instantly queryable in Snowflake via SELECT *
```

## Connection to the Previous Layer

**Layer 0** is actively writing Parquet files and Iceberg metadata files into Azure Data Lake. This layer (Layer 1) relies entirely on those files existing. If Layer 0 stops, this layer simply shows no new data.

## Connection to the Next Layer

**Layer 3 (Snowpipe) & Layer 4 (Bronze):** While this Iceberg layer is perfect for manual files and immediate cataloging, our automated high-frequency data will flow through Snowpipe into Landing Tables, which are then processed into the Bronze layer by dbt. The Bronze layer will eventually consume the data we are exposing here.
