# Layer 0 — Data Sources & Polling — Concepts

## What Is This Layer?

Layer 0 is the **entry point** of the entire HazardWatch pipeline. Its only job is to go out to the internet, fetch hazard data from public APIs and datasets, and save it as files into Azure storage — without transforming, cleaning, or judging the data in any way. Think of it as the postman who collects the letters; the postman doesn't read or edit them, they just deliver raw mail.

This layer runs as **Azure Functions** — small pieces of code that wake up on a schedule (like an alarm clock), do their work, and go back to sleep. Each public data source gets its own dedicated function so they are completely independent.

---

## Why Do We Need It?

The seven public data sources (USGS, GDACS, EONET, FIRMS, OpenAQ, Open-Meteo, WorldPop) don't push data to us — we have to go and pull it on a schedule. If we don't collect and store the raw data ourselves, we lose it. Most APIs only give you the last N hours or days; they don't let you go back years. By landing raw data into Azure Data Lake Storage (ADLS), we:

1. Build our own permanent archive that we fully own
2. Decouple the API from the rest of the pipeline — if USGS changes their API format tomorrow, only this layer breaks
3. Allow any other tool (Snowflake, Spark, DuckDB, Power BI) to read the same raw files

---

## Real-World Analogy

Imagine you run a news clipping agency in the 1950s. You have seven employees, each assigned to one newspaper. Every morning they go to the newsstand, clip out the relevant articles, and file them in a folder in the archive room. They don't summarise, translate, or interpret anything — they just clip and file. The archive room is your Azure Data Lake. Later, editors (dbt models in Bronze/Silver) will read those clippings and turn them into meaningful reports.

Layer 0 = the clipping employees. ADLS = the archive room.

---

## Key Technologies

### Azure Functions
A "serverless" compute service on Azure. You write a Python function; Azure runs it for you on a schedule. You are not charged when it is not running. The `TimerTrigger` binding lets you set a cron schedule (e.g., every 5 minutes). There is no server for you to manage.

- **Why not a VM or container?** Functions are cheaper for short, infrequent tasks. A VM running 24/7 to poll an API every 5 minutes wastes ~99.9% of its time idle.

### Azure Function App
A container that groups multiple individual functions together. We have one Function App called `hazardwatch-func` that contains all seven pollers. They share the same settings, secrets, and Python environment.

### PyArrow
A Python library for creating Parquet files. We use it to convert the raw API response (JSON or CSV) into a typed, columnar Parquet file before saving it. Parquet is a binary format that is much smaller and faster to query than JSON or CSV.

### PyIceberg
A Python library for writing Iceberg table metadata. After writing the Parquet file to ADLS, PyIceberg adds a small metadata file (a few kilobytes) that tells Snowflake "a new file was added, here is its schema". This is what powers the External Catalog in Layer 2.

### Azure Data Lake Storage Gen2 (ADLS)
Microsoft's cloud file storage for big data. Think of it as a very large, very cheap hard drive in the cloud. Files are organized in a hierarchy: Storage Account > Container > Folders > Files. We use Hive-style partitioning (`source=usgs/date=2026-10-02/`) so that query engines can skip irrelevant folders.

### Hive Partitioning
A folder naming convention (`column=value/`) that lets query engines read only the folders they need. If you query `WHERE date = '2026-10-02'`, Snowflake will only open the `date=2026-10-02/` folder and skip everything else. This makes queries dramatically faster and cheaper.

---

## Key Terms Glossary

| Term | Plain English Meaning |
|---|---|
| **Timer Trigger** | The alarm clock that wakes up an Azure Function on a schedule |
| **Cron expression** | A compact way to write a schedule, e.g., `0 */5 * * * *` means "every 5 minutes" |
| **Parquet** | A compressed, columnar file format — like a spreadsheet optimised for computers, not humans |
| **Iceberg** | An open table format that adds a catalog (table of contents) on top of Parquet files |
| **ADLS Gen2** | Azure's big-data file storage. Gen2 means it supports hierarchical namespaces (real folders, not just key prefixes) |
| **Hive partition** | A folder named `column=value` so engines can skip irrelevant data |
| **Backoff** | Waiting progressively longer between retries: 5s, 10s, 20s, 40s... so you don't hammer a failing API |
| **Rate limit** | An API's rule limiting how many requests you can make per minute/hour/day |
| **MAP_KEY** | The API key required by NASA FIRMS to authenticate your requests |
| **GeoJSON** | A standard JSON format for geographic features (points, lines, polygons) |
| **VIIRS** | Visible Infrared Imaging Radiometer Suite — the satellite sensor NASA FIRMS uses to detect fires |
| **FRP** | Fire Radiative Power — how much heat a fire is emitting in megawatts |

---

## How Data Flows Through This Layer

```
Public API / Dataset
        |
        | HTTP GET (with authentication header or API key)
        v
Azure Function (poll_<source>)
        |
        | Parse JSON / CSV response
        | Flatten nested fields into a flat list of rows
        v
PyArrow Table (in memory, typed columns)
        |
        | Write to binary Parquet format
        v
ADLS Gen2: raw/source=<name>/date=<YYYY-MM-DD>/batch_<epoch>.parquet
        |
        | Commit Iceberg metadata (PyIceberg)
        v
ADLS Gen2: raw/source=<name>/metadata/ (Iceberg snapshot files)
```

Each function runs independently. If `poll_usgs` fails, `poll_gdacs` continues unaffected.

---

## Polling Schedules (Why These Intervals)

| Source | Interval | Reason |
|---|---|---|
| USGS | Every 5 min | Earthquakes can happen any second; 5-min lag is acceptable |
| GDACS | Every 15 min | Alerts update every ~15 min; faster polling gains nothing |
| EONET | Every 30 min | Discovery feed only, not safety-critical |
| FIRMS | Every 30 min | Satellite data is 3-hour delayed anyway |
| OpenAQ | Every 60 min | Tightest rate limit (2,000/hr) — hourly stays safe |
| Open-Meteo | On-demand | Only called when enriching a specific hazard event |
| WorldPop | One-time | Static population grid, updated annually |

---

## Connection to the Previous Layer

There is no previous layer. Layer 0 is the **source of truth**. Everything downstream depends on it. If this layer stops running, the entire pipeline goes stale.

---

## Connection to the Next Layer

**Layer 1 (Raw ADLS + Iceberg):** The Parquet files this layer writes become the permanent raw archive. Layer 1 defines the exact folder structure and Iceberg schema that this layer must write into.

**Layer 2 (External Catalog):** The Iceberg metadata commits made by PyIceberg in this layer are what allow Snowflake to auto-detect new files in Layer 2 — without any manual intervention.
