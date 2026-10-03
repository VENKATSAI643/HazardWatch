# HazardWatch — Low Level Design (LLD)

> **Version:** 1.0  
> **Date:** 2026-10-02  
> **Stack:** Azure Functions · ADLS Gen2 · Iceberg (PyIceberg) · Event Grid · Snowflake · dbt Core · Apache Superset  
> **Sources:** USGS · GDACS · EONET · NASA FIRMS · OpenAQ · Open-Meteo · WorldPop

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Folder & Naming Conventions](#2-folder--naming-conventions)
3. [Layer 0 — Data Sources & Polling](#3-layer-0--data-sources--polling)
4. [Layer 1 — Raw ADLS (Iceberg External Volume)](#4-layer-1--raw-adls-iceberg-external-volume)
5. [Layer 2 — External Catalog (Snowflake reads ADLS directly)](#5-layer-2--external-catalog-snowflake-reads-adls-directly)
6. [Layer 3 — Snowpipe (Automated high-frequency path)](#6-layer-3--snowpipe-automated-high-frequency-path)
7. [Layer 4 — Snowflake: Bronze](#7-layer-4--snowflake-bronze)
8. [Layer 5 — Snowflake: Silver](#8-layer-5--snowflake-silver)
9. [Layer 6 — Snowflake: Gold](#9-layer-6--snowflake-gold)
10. [Layer 7 — Published ADLS & R2 Mirror](#10-layer-7--published-adls--r2-mirror)
11. [Layer 8 — Superset Dashboard](#11-layer-8--superset-dashboard)
12. [Snowflake Objects Master List](#12-snowflake-objects-master-list)
13. [dbt Project Structure](#13-dbt-project-structure)
14. [Azure Function App Structure](#14-azure-function-app-structure)
15. [Security & Secrets](#15-security--secrets)
16. [Resource Monitor & Cost Guards](#16-resource-monitor--cost-guards)
17. [Error Handling & Retry Strategy](#17-error-handling--retry-strategy)
18. [Data Quality Checks](#18-data-quality-checks)
19. [Quick-Start Build Order](#19-quick-start-build-order)

---

## 1. System Overview

```
PUBLIC SOURCES
  USGS · GDACS · EONET · FIRMS · OpenAQ · Open-Meteo · WorldPop
          |
          v
AZURE FUNCTIONS (hazardwatch-func app)
  Timer Pollers (per-source cadence)
  Durable Backfills (fan-out, retries)
          |  writes Parquet + Iceberg commit
          v
RAW ADLS Gen2 (hazardwatchraw)
  container: raw/
  Iceberg External Volume  <-- manual uploads also land here
          |
          +--- AUTO REFRESH via Event Grid --------->  SNOWFLAKE EXTERNAL TABLE
          |                                              (no copy, zero cost)
          +--- BlobCreated --> Snowpipe ------------>  LANDING TABLES (auto ingest)
                                                               |
                                                         dbt Core job
                                                   Bronze -> Silver -> Gold
                                                               |
                                         +---------------------+
                                         v                     v
                                    SUPERSET            PUBLISHED ADLS
                                  (live queries)     COPY INTO -> R2 Mirror
                                                      Open dataset users
```

---

## 2. Folder & Naming Conventions

### ADLS Raw Container Layout

```
hazardwatchraw/
+-- raw/
    +-- source=usgs/
    |   +-- date=YYYY-MM-DD/
    |       +-- batch_<epoch>.parquet
    +-- source=gdacs/
    |   +-- date=YYYY-MM-DD/
    |       +-- batch_<epoch>.parquet
    +-- source=eonet/
    |   +-- date=YYYY-MM-DD/
    |       +-- batch_<epoch>.parquet
    +-- source=firms/
    |   +-- date=YYYY-MM-DD/
    |       +-- batch_<epoch>.parquet
    +-- source=openaq/
    |   +-- date=YYYY-MM-DD/
    |       +-- batch_<epoch>.parquet
    +-- source=openmeteo/
    |   +-- date=YYYY-MM-DD/
    |       +-- batch_<epoch>.parquet
    +-- source=worldpop/
        +-- year=2020/
            +-- worldpop_1km.parquet   <-- one-time download
```

> **Key rule:** Every folder is Hive-partitioned so Snowflake and Spark can partition-prune.  
> **Manual uploads:** Drop a file into any `source=<name>/date=<date>/` path and it will be auto-detected.

### Snowflake Database Layout

```
HAZARDWATCH_DB
+-- RAW_SCHEMA          <- External/Iceberg tables pointing to ADLS
+-- BRONZE_SCHEMA       <- dbt bronze models (typed, deduped landing)
+-- SILVER_SCHEMA       <- dbt silver models (unified hazard_event)
+-- GOLD_SCHEMA         <- dbt gold models (aggregated, exposure)
+-- AUDIT_SCHEMA        <- pipeline run logs, DQ results
```

---

## 3. Layer 0 — Data Sources & Polling

### 3.1 Polling Schedule

| Source | Azure Function name | Trigger | Endpoint | Output format |
|---|---|---|---|---|
| USGS earthquakes | `poll_usgs` | Timer 5 min | `earthquake.usgs.gov/fdsnws/event/1/query?updatedafter=...` | GeoJSON -> Parquet |
| GDACS alerts | `poll_gdacs` | Timer 15 min | `gdacs.org/gdacsapi/api/events/geteventlist/events4app` | JSON -> Parquet |
| NASA EONET | `poll_eonet` | Timer 30 min | `eonet.gsfc.nasa.gov/api/v3/events?status=open` | JSON -> Parquet |
| NASA FIRMS | `poll_firms` | Timer 30 min | `firms.modaps.eosdis.nasa.gov/api/area/csv/<KEY>/VIIRS_SNPP_NRT/<bbox>/1` | CSV -> Parquet |
| OpenAQ | `poll_openaq` | Timer 60 min | `api.openaq.org/v3/locations?...` paginated | JSON -> Parquet |
| Open-Meteo | `enrich_weather` | On-demand (per event) | `api.open-meteo.com/v1/forecast?...` | JSON -> Parquet |
| WorldPop | `download_worldpop` | One-time manual | `worldpop.org GeoTIFF` | GeoTIFF -> Parquet (H3 conversion) |

### 3.2 Azure Function: `poll_usgs` — Detailed Logic

```python
# functions/poll_usgs/__init__.py
import azure.functions as func
import httpx, pyarrow as pa, pyarrow.parquet as pq
from azure.storage.blob import BlobServiceClient
from pyiceberg.catalog import load_catalog
import datetime, os, time

ADLS_CONN = os.environ["ADLS_CONNECTION_STRING"]
UA = {"User-Agent": "hazardwatch-learning (your_email@example.com)"}

def main(mytimer: func.TimerRequest):
    now = datetime.datetime.utcnow()
    updated_after = (now - datetime.timedelta(minutes=6)).strftime("%Y-%m-%dT%H:%M:%S")
    date_str = now.strftime("%Y-%m-%d")
    epoch    = int(time.time())

    # 1. Fetch
    url = (
        f"https://earthquake.usgs.gov/fdsnws/event/1/query"
        f"?format=geojson&updatedafter={updated_after}&orderby=time"
    )
    r = httpx.get(url, headers=UA, timeout=30)
    r.raise_for_status()
    features = r.json()["features"]
    if not features:
        return  # nothing new

    # 2. Flatten to Arrow table
    rows = [{
        "event_id":    f["id"],
        "source":      "usgs",
        "magnitude":   f["properties"].get("mag"),
        "place":       f["properties"].get("place"),
        "event_time":  datetime.datetime.utcfromtimestamp(f["properties"]["time"] / 1000),
        "updated_at":  datetime.datetime.utcfromtimestamp(f["properties"]["updated"] / 1000),
        "latitude":    f["geometry"]["coordinates"][1],
        "longitude":   f["geometry"]["coordinates"][0],
        "depth_km":    f["geometry"]["coordinates"][2],
        "alert":       f["properties"].get("alert"),
        "status":      f["properties"].get("status"),
        "raw_json":    str(f),
        "_ingested_at": now,
    } for f in features]

    schema = pa.schema([
        pa.field("event_id",     pa.string()),
        pa.field("source",       pa.string()),
        pa.field("magnitude",    pa.float64()),
        pa.field("place",        pa.string()),
        pa.field("event_time",   pa.timestamp("us")),
        pa.field("updated_at",   pa.timestamp("us")),
        pa.field("latitude",     pa.float64()),
        pa.field("longitude",    pa.float64()),
        pa.field("depth_km",     pa.float64()),
        pa.field("alert",        pa.string()),
        pa.field("status",       pa.string()),
        pa.field("raw_json",     pa.string()),
        pa.field("_ingested_at", pa.timestamp("us")),
    ])
    arrow_table = pa.Table.from_pylist(rows, schema=schema)

    # 3. Write Parquet to ADLS
    blob_path = f"raw/source=usgs/date={date_str}/batch_{epoch}.parquet"
    buf = pa.BufferOutputStream()
    pq.write_table(arrow_table, buf)
    client = BlobServiceClient.from_connection_string(ADLS_CONN)
    client.get_blob_client("hazardwatchraw", blob_path) \
          .upload_blob(buf.getvalue().to_pybytes(), overwrite=True)

    # 4. Commit Iceberg metadata (so Snowflake External Table auto-refreshes)
    catalog = load_catalog("hazardwatch", **{"uri": os.environ["ICEBERG_CATALOG_URI"]})
    tbl = catalog.load_table("raw.usgs_earthquakes")
    tbl.append(arrow_table)
```

### 3.3 Rate Limit & Backoff Pattern (all pollers)

```python
# shared/retry.py
import time, functools, logging

def with_backoff(max_retries=5, base_wait=5):
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            wait = base_wait
            for attempt in range(max_retries):
                try:
                    return fn(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    logging.warning(f"Attempt {attempt+1} failed: {e}. Retrying in {wait}s")
                    time.sleep(wait)
                    wait = min(wait * 2, 300)  # cap at 5 minutes
        return wrapper
    return decorator
```

---

## 4. Layer 1 — Raw ADLS (Iceberg External Volume)

### What lives here

- **Data:** Parquet files, Hive-partitioned by `source` and `date`
- **Iceberg metadata:** `.avro` manifest files and `metadata.json` snapshots committed by PyIceberg
- **Lifecycle TTL:** 90-day delete policy on raw Parquet (keep metadata forever)

### Iceberg Table Schemas on ADLS

#### `raw.usgs_earthquakes`

| Column | Type | Notes |
|---|---|---|
| `event_id` | STRING | USGS unique ID |
| `source` | STRING | Always "usgs" |
| `magnitude` | DOUBLE | Richter magnitude |
| `place` | STRING | Human description |
| `event_time` | TIMESTAMP | UTC, from epoch ms |
| `updated_at` | TIMESTAMP | Last update UTC |
| `latitude` | DOUBLE | |
| `longitude` | DOUBLE | |
| `depth_km` | DOUBLE | |
| `alert` | STRING | green/yellow/orange/red |
| `status` | STRING | automatic/reviewed |
| `raw_json` | STRING | Full GeoJSON feature |
| `_ingested_at` | TIMESTAMP | When the function ran |

#### `raw.gdacs_alerts`

| Column | Type | Notes |
|---|---|---|
| `event_id` | STRING | GDACS event code |
| `source` | STRING | Always "gdacs" |
| `event_type` | STRING | EQ / TC / FL / VO / DR |
| `alert_level` | STRING | Green / Orange / Red |
| `event_name` | STRING | |
| `event_time` | TIMESTAMP | |
| `latitude` | DOUBLE | |
| `longitude` | DOUBLE | |
| `country` | STRING | ISO3 |
| `affected_population` | BIGINT | |
| `raw_json` | STRING | |
| `_ingested_at` | TIMESTAMP | |

#### `raw.firms_fires`

| Column | Type | Notes |
|---|---|---|
| `event_id` | STRING | `lat_lon_acq_date_acq_time` composite |
| `source` | STRING | Always "firms" |
| `latitude` | DOUBLE | |
| `longitude` | DOUBLE | |
| `brightness` | DOUBLE | VIIRS I-Band brightness temp (K) |
| `frp` | DOUBLE | Fire Radiative Power (MW) |
| `confidence` | STRING | low / nominal / high |
| `acq_date` | DATE | |
| `acq_time` | STRING | HHMM UTC |
| `satellite` | STRING | N (Suomi-NPP) / 1 (NOAA-20) |
| `daynight` | STRING | D / N |
| `raw_json` | STRING | |
| `_ingested_at` | TIMESTAMP | |

#### `raw.openaq_readings`

| Column | Type | Notes |
|---|---|---|
| `location_id` | BIGINT | OpenAQ location ID |
| `source` | STRING | Always "openaq" |
| `city` | STRING | |
| `country` | STRING | ISO2 |
| `parameter` | STRING | pm25 / pm10 / no2 / o3 / co |
| `value` | DOUBLE | |
| `unit` | STRING | µg/m³ |
| `reading_time` | TIMESTAMP | |
| `latitude` | DOUBLE | |
| `longitude` | DOUBLE | |
| `raw_json` | STRING | |
| `_ingested_at` | TIMESTAMP | |

#### `raw.eonet_events`

| Column | Type | Notes |
|---|---|---|
| `event_id` | STRING | EONET ID |
| `source` | STRING | Always "eonet" |
| `title` | STRING | |
| `category` | STRING | Wildfires / Volcanoes / etc. |
| `status` | STRING | open / closed |
| `latitude` | DOUBLE | Latest geometry point |
| `longitude` | DOUBLE | |
| `event_date` | TIMESTAMP | |
| `raw_json` | STRING | |
| `_ingested_at` | TIMESTAMP | |

---

## 5. Layer 2 — External Catalog (Snowflake reads ADLS directly)

> **Why this layer exists:** Any file dropped manually into Raw ADLS becomes immediately visible to Snowflake without Snowpipe or COPY INTO.

### 5.1 Snowflake Setup (run once, manually)

```sql
-- Step 1: Storage Integration (Azure service principal auth)
CREATE OR REPLACE STORAGE INTEGRATION hazardwatch_azure_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = '<your-azure-tenant-id>'
  STORAGE_ALLOWED_LOCATIONS = (
    'azure://hazardwatchraw.blob.core.windows.net/raw/',
    'azure://hazardwatchpub.blob.core.windows.net/published/'
  );
-- After this: DESC INTEGRATION hazardwatch_azure_int;
-- Copy AZURE_CONSENT_URL and grant Snowflake access in Azure portal.
```

```sql
-- Step 2: External Volume for Iceberg
CREATE OR REPLACE EXTERNAL VOLUME hazardwatch_raw_vol
  STORAGE_LOCATIONS = (
    (
      NAME             = 'hazardwatch-raw-eastus'
      STORAGE_PROVIDER = 'AZURE'
      STORAGE_BASE_URL = 'azure://hazardwatchraw.blob.core.windows.net/raw/'
      AZURE_TENANT_ID  = '<your-azure-tenant-id>'
    )
  );
```

```sql
-- Step 3: Iceberg Tables per source
USE DATABASE HAZARDWATCH_DB;
USE SCHEMA RAW_SCHEMA;

CREATE OR REPLACE ICEBERG TABLE raw_usgs_earthquakes
  EXTERNAL_VOLUME = 'hazardwatch_raw_vol'
  CATALOG         = 'SNOWFLAKE'
  BASE_LOCATION   = 'source=usgs/'
  AUTO_REFRESH    = TRUE;

CREATE OR REPLACE ICEBERG TABLE raw_gdacs_alerts
  EXTERNAL_VOLUME = 'hazardwatch_raw_vol'
  CATALOG         = 'SNOWFLAKE'
  BASE_LOCATION   = 'source=gdacs/'
  AUTO_REFRESH    = TRUE;

CREATE OR REPLACE ICEBERG TABLE raw_firms_fires
  EXTERNAL_VOLUME = 'hazardwatch_raw_vol'
  CATALOG         = 'SNOWFLAKE'
  BASE_LOCATION   = 'source=firms/'
  AUTO_REFRESH    = TRUE;

CREATE OR REPLACE ICEBERG TABLE raw_openaq_readings
  EXTERNAL_VOLUME = 'hazardwatch_raw_vol'
  CATALOG         = 'SNOWFLAKE'
  BASE_LOCATION   = 'source=openaq/'
  AUTO_REFRESH    = TRUE;

CREATE OR REPLACE ICEBERG TABLE raw_eonet_events
  EXTERNAL_VOLUME = 'hazardwatch_raw_vol'
  CATALOG         = 'SNOWFLAKE'
  BASE_LOCATION   = 'source=eonet/'
  AUTO_REFRESH    = TRUE;
```

### 5.2 Auto-Refresh via Event Grid

```sql
-- Snowflake notification integration for Event Grid
CREATE OR REPLACE NOTIFICATION INTEGRATION hazardwatch_eventgrid_int
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
  ENABLED = TRUE
  AZURE_STORAGE_QUEUE_PRIMARY_URI = 'https://<storage>.queue.core.windows.net/hazardwatch-refresh'
  AZURE_TENANT_ID = '<your-azure-tenant-id>';

ALTER ICEBERG TABLE raw_usgs_earthquakes SET AUTO_REFRESH = TRUE;
-- Repeat for all Iceberg tables
```

> **Manual upload flow (plain English):**
> 1. Open Azure Storage Explorer
> 2. Drag a Parquet file into `raw/source=usgs/date=2026-10-02/`
> 3. Event Grid fires within seconds
> 4. Snowflake refreshes `raw_usgs_earthquakes` within ~60 seconds
> 5. `SELECT COUNT(*) FROM HAZARDWATCH_DB.RAW_SCHEMA.raw_usgs_earthquakes` shows your new rows

---

## 6. Layer 3 — Snowpipe (Automated high-frequency path)

> Snowpipe handles high-volume automated poller output. External Table handles manual/ad-hoc files. Both feed Bronze.

### 6.1 External Stage

```sql
CREATE OR REPLACE STAGE hazardwatch_raw_stage
  URL = 'azure://hazardwatchraw.blob.core.windows.net/raw/'
  STORAGE_INTEGRATION = hazardwatch_azure_int
  FILE_FORMAT = (TYPE = PARQUET);
```

### 6.2 Landing Tables (Snowpipe target)

```sql
CREATE OR REPLACE TABLE HAZARDWATCH_DB.RAW_SCHEMA.landing_usgs (
  raw_file_name   STRING,
  raw_row_number  INT,
  raw_value       VARIANT,
  _loaded_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
-- Same pattern for: landing_gdacs, landing_firms, landing_openaq, landing_eonet
```

### 6.3 Snowpipe Definitions

```sql
CREATE OR REPLACE PIPE HAZARDWATCH_DB.RAW_SCHEMA.pipe_usgs
  AUTO_INGEST = TRUE
  COMMENT = 'Loads USGS Parquet batches from raw/source=usgs/'
AS
COPY INTO HAZARDWATCH_DB.RAW_SCHEMA.landing_usgs (raw_file_name, raw_row_number, raw_value)
FROM (
  SELECT METADATA$FILENAME,
         METADATA$FILE_ROW_NUMBER,
         $1
  FROM @hazardwatch_raw_stage/source=usgs/
)
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = NONE;
-- Repeat for gdacs, firms, openaq, eonet
```

---

## 7. Layer 4 — Snowflake: Bronze

> **Bronze = typed, deduped, validated copy of raw. Nothing deleted, nothing joined.**

### 7.1 dbt Model: `bronze_usgs_earthquakes`

```sql
-- models/bronze/bronze_usgs_earthquakes.sql
{{ config(
    materialized     = 'incremental',
    unique_key       = 'event_id',
    on_schema_change = 'append_new_columns',
    cluster_by       = ['event_date']
) }}

WITH source AS (
    SELECT
        raw_value:event_id::STRING          AS event_id,
        'usgs'                              AS source,
        raw_value:magnitude::FLOAT          AS magnitude,
        raw_value:place::STRING             AS place,
        raw_value:event_time::TIMESTAMP_NTZ AS event_time,
        raw_value:updated_at::TIMESTAMP_NTZ AS updated_at,
        raw_value:latitude::FLOAT           AS latitude,
        raw_value:longitude::FLOAT          AS longitude,
        raw_value:depth_km::FLOAT           AS depth_km,
        raw_value:alert::STRING             AS alert_level,
        raw_value:status::STRING            AS status,
        raw_value:raw_json::STRING          AS raw_json,
        raw_value:_ingested_at::TIMESTAMP_NTZ AS _ingested_at,
        DATE(raw_value:event_time::TIMESTAMP_NTZ) AS event_date
    FROM {{ source('raw', 'landing_usgs') }}
    {% if is_incremental() %}
    WHERE raw_value:_ingested_at::TIMESTAMP_NTZ >
          (SELECT MAX(_ingested_at) FROM {{ this }})
    {% endif %}
),

deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY event_id ORDER BY updated_at DESC
           ) AS rn
    FROM source
    WHERE event_id IS NOT NULL
      AND latitude  BETWEEN -90  AND 90
      AND longitude BETWEEN -180 AND 180
)

SELECT * EXCLUDE (rn) FROM deduped WHERE rn = 1
```

### 7.2 Bronze Schema Summary

| Table | Dedup key | Partitioned by |
|---|---|---|
| `bronze_usgs_earthquakes` | event_id | event_date |
| `bronze_gdacs_alerts` | event_id | event_date |
| `bronze_firms_fires` | event_id | acq_date |
| `bronze_openaq_readings` | location_id + parameter + reading_time | reading_date |
| `bronze_eonet_events` | event_id | event_date |
| `bronze_worldpop_h3` | h3_index_res7 | year |

---

## 8. Layer 5 — Snowflake: Silver

> **Silver = unified `hazard_event` table. All sources merged into one schema. Geometry added as H3 index.**

### 8.1 Unified Schema: `silver_hazard_event`

| Column | Type | Notes |
|---|---|---|
| `hazard_id` | STRING | `source || '_' || event_id` |
| `source` | STRING | usgs / gdacs / firms / openaq / eonet |
| `hazard_type` | STRING | earthquake / tropical_cyclone / flood / wildfire / volcano / air_quality |
| `severity_level` | STRING | low / medium / high / critical |
| `severity_score` | FLOAT | Normalized 0-100 |
| `event_time` | TIMESTAMP_NTZ | UTC |
| `event_date` | DATE | Partition column |
| `latitude` | FLOAT | |
| `longitude` | FLOAT | |
| `h3_index_res7` | STRING | H3 hex at ~5km resolution |
| `h3_index_res5` | STRING | H3 hex at ~90km resolution |
| `country_iso3` | STRING | Reverse-geocoded |
| `place_name` | STRING | Human readable |
| `affected_radius_km` | FLOAT | Estimated from magnitude/type/depth |
| `alert_level` | STRING | Source alert level |
| `status` | STRING | active / closed / reviewing |
| `extra` | VARIANT | Source-specific JSON fields |
| `_source_updated_at` | TIMESTAMP_NTZ | |
| `_dbt_updated_at` | TIMESTAMP_NTZ | CURRENT_TIMESTAMP() |

### 8.2 dbt Model: `silver_hazard_event` (USGS + GDACS shown)

```sql
-- models/silver/silver_hazard_event.sql
{{ config(
    materialized = 'incremental',
    unique_key   = 'hazard_id',
    cluster_by   = ['event_date', 'hazard_type']
) }}

WITH usgs AS (
    SELECT
        'usgs_' || event_id             AS hazard_id,
        'usgs'                          AS source,
        'earthquake'                    AS hazard_type,
        CASE
            WHEN magnitude >= 7.0 THEN 'critical'
            WHEN magnitude >= 6.0 THEN 'high'
            WHEN magnitude >= 4.5 THEN 'medium'
            ELSE 'low'
        END                             AS severity_level,
        LEAST(magnitude * 14.3, 100)   AS severity_score,
        event_time, event_date, latitude, longitude,
        H3_LATLNG_TO_CELL(latitude, longitude, 7)::STRING AS h3_index_res7,
        H3_LATLNG_TO_CELL(latitude, longitude, 5)::STRING AS h3_index_res5,
        NULL                            AS country_iso3,
        place                           AS place_name,
        CASE
            WHEN depth_km < 70  THEN magnitude * 15
            WHEN depth_km < 300 THEN magnitude * 8
            ELSE magnitude * 3
        END                             AS affected_radius_km,
        alert_level, status,
        OBJECT_CONSTRUCT('depth_km', depth_km, 'magnitude', magnitude) AS extra,
        updated_at                      AS _source_updated_at
    FROM {{ ref('bronze_usgs_earthquakes') }}
    {% if is_incremental() %}
    WHERE _ingested_at > (
        SELECT MAX(_dbt_updated_at) FROM {{ this }} WHERE source = 'usgs'
    )
    {% endif %}
),

gdacs AS (
    SELECT
        'gdacs_' || event_id            AS hazard_id,
        'gdacs'                         AS source,
        CASE event_type
            WHEN 'EQ' THEN 'earthquake'
            WHEN 'TC' THEN 'tropical_cyclone'
            WHEN 'FL' THEN 'flood'
            WHEN 'VO' THEN 'volcano'
            WHEN 'DR' THEN 'drought'
            ELSE 'unknown'
        END                             AS hazard_type,
        LOWER(alert_level)              AS severity_level,
        CASE alert_level
            WHEN 'Red'    THEN 90
            WHEN 'Orange' THEN 60
            WHEN 'Green'  THEN 20
            ELSE 5
        END                             AS severity_score,
        event_time,
        DATE(event_time)                AS event_date,
        latitude, longitude,
        H3_LATLNG_TO_CELL(latitude, longitude, 7)::STRING AS h3_index_res7,
        H3_LATLNG_TO_CELL(latitude, longitude, 5)::STRING AS h3_index_res5,
        country                         AS country_iso3,
        event_name                      AS place_name,
        NULL                            AS affected_radius_km,
        alert_level, 'active'           AS status,
        OBJECT_CONSTRUCT('affected_population', affected_population) AS extra,
        _ingested_at                    AS _source_updated_at
    FROM {{ ref('bronze_gdacs_alerts') }}
    {% if is_incremental() %}
    WHERE _ingested_at > (
        SELECT MAX(_dbt_updated_at) FROM {{ this }} WHERE source = 'gdacs'
    )
    {% endif %}
),

-- firms, openaq, eonet follow the same pattern...

unioned AS (
    SELECT *, CURRENT_TIMESTAMP() AS _dbt_updated_at FROM usgs
    UNION ALL
    SELECT *, CURRENT_TIMESTAMP() AS _dbt_updated_at FROM gdacs
)

SELECT * FROM unioned
```

---

## 9. Layer 6 — Snowflake: Gold

> **Gold = aggregated, enriched, analytics-ready. Joins hazard events with population grid.**

### 9.1 `gold_event_exposure`

```sql
-- models/gold/gold_event_exposure.sql
{{ config(materialized='table', cluster_by=['event_date', 'hazard_type']) }}

WITH events AS (
    SELECT * FROM {{ ref('silver_hazard_event') }}
    WHERE severity_level IN ('high', 'critical')
),

population AS (
    SELECT h3_index_res7, SUM(population) AS pop_in_cell
    FROM {{ ref('bronze_worldpop_h3') }}
    GROUP BY 1
),

-- H3 k-ring: get cells within affected radius
event_rings AS (
    SELECT
        e.hazard_id,
        e.h3_index_res7 AS center_h3,
        CEIL(e.affected_radius_km / 5.2) AS ring_count,   -- ~5.2km per res-7 cell
        H3_GRID_DISK(e.h3_index_res7, CEIL(e.affected_radius_km / 5.2)) AS neighbor_cells
    FROM events e
),

exposure AS (
    SELECT
        er.hazard_id,
        SUM(p.pop_in_cell) AS estimated_population_exposed
    FROM event_rings er,
         LATERAL FLATTEN(er.neighbor_cells) nc
    JOIN population p ON p.h3_index_res7 = nc.value::STRING
    GROUP BY er.hazard_id
)

SELECT
    e.*,
    COALESCE(ex.estimated_population_exposed, 0) AS estimated_population_exposed,
    CURRENT_TIMESTAMP() AS _gold_updated_at
FROM events e
LEFT JOIN exposure ex USING (hazard_id)
```

### 9.2 `gold_daily_summary`

```sql
-- models/gold/gold_daily_summary.sql
{{ config(materialized='table') }}

SELECT
    event_date,
    hazard_type,
    severity_level,
    country_iso3,
    COUNT(*)                             AS event_count,
    MAX(severity_score)                  AS max_severity,
    AVG(severity_score)                  AS avg_severity,
    SUM(estimated_population_exposed)    AS total_population_exposed,
    ARRAY_AGG(hazard_id)                 AS hazard_ids
FROM {{ ref('gold_event_exposure') }}
GROUP BY 1, 2, 3, 4
```

### 9.3 `gold_active_alerts` (Superset map source)

```sql
-- models/gold/gold_active_alerts.sql
{{ config(materialized='table') }}

SELECT *
FROM {{ ref('silver_hazard_event') }}
WHERE status = 'active'
  AND event_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY severity_score DESC
```

---

## 10. Layer 7 — Published ADLS & R2 Mirror

### 10.1 Unload Gold to Published ADLS

```sql
-- External stage for published output
CREATE OR REPLACE STAGE hazardwatch_pub_stage
  URL = 'azure://hazardwatchpub.blob.core.windows.net/published/'
  STORAGE_INTEGRATION = hazardwatch_azure_int
  FILE_FORMAT = (TYPE = PARQUET SNAPPY_COMPRESSION = TRUE);

-- Unload (run by Snowflake Task after each dbt build)
COPY INTO @hazardwatch_pub_stage/gold_event_exposure/
FROM HAZARDWATCH_DB.GOLD_SCHEMA.gold_event_exposure
OVERWRITE = TRUE HEADER = TRUE;

COPY INTO @hazardwatch_pub_stage/gold_daily_summary/
FROM HAZARDWATCH_DB.GOLD_SCHEMA.gold_daily_summary
OVERWRITE = TRUE HEADER = TRUE;
```

### 10.2 R2 Mirror (Azure Container Apps Job)

```bash
# rclone configured with R2 credentials via environment variables
rclone sync \
  "azure://hazardwatchpub.blob.core.windows.net/published/" \
  "r2:hazardwatch-open/" \
  --include "*.parquet" \
  --transfers 8 \
  --log-level INFO
```

> **Why Parquet-only to R2:** Iceberg metadata embeds absolute Azure paths.
> Copying metadata to R2 would break consumers. Mirror Parquet only — R2 consumers
> use Hive-style partition discovery.

---

## 11. Layer 8 — Superset Dashboard

### 11.1 Snowflake Connection

```
Host:      <account>.snowflakecomputing.com
Database:  HAZARDWATCH_DB
Schema:    GOLD_SCHEMA
Warehouse: HAZARDWATCH_WH  (X-Small, auto-suspend 60s)
Role:      HAZARDWATCH_READER
```

### 11.2 Key Datasets & Charts

| Dataset | Source table | Chart type |
|---|---|---|
| Active Alerts Map | `gold_active_alerts` | World Map (lat/lon, colored by severity) |
| Daily Event Count | `gold_daily_summary` | Bar Chart (by hazard_type, last 30 days) |
| Population Exposed | `gold_event_exposure` | Big Number (this week total) |
| Top Critical Events | `gold_event_exposure` | Table (sorted by population exposed) |

---

## 12. Snowflake Objects Master List

```
DATABASE: HAZARDWATCH_DB
|
+-- SCHEMA: RAW_SCHEMA
|   +-- ICEBERG TABLE: raw_usgs_earthquakes
|   +-- ICEBERG TABLE: raw_gdacs_alerts
|   +-- ICEBERG TABLE: raw_firms_fires
|   +-- ICEBERG TABLE: raw_openaq_readings
|   +-- ICEBERG TABLE: raw_eonet_events
|   +-- TABLE: landing_usgs         (Snowpipe target)
|   +-- TABLE: landing_gdacs
|   +-- TABLE: landing_firms
|   +-- TABLE: landing_openaq
|   +-- TABLE: landing_eonet
|   +-- PIPE: pipe_usgs
|   +-- PIPE: pipe_gdacs
|   +-- PIPE: pipe_firms
|   +-- PIPE: pipe_openaq
|   +-- PIPE: pipe_eonet
|
+-- SCHEMA: BRONZE_SCHEMA
|   +-- TABLE: bronze_usgs_earthquakes
|   +-- TABLE: bronze_gdacs_alerts
|   +-- TABLE: bronze_firms_fires
|   +-- TABLE: bronze_openaq_readings
|   +-- TABLE: bronze_eonet_events
|   +-- TABLE: bronze_worldpop_h3
|
+-- SCHEMA: SILVER_SCHEMA
|   +-- TABLE: silver_hazard_event
|
+-- SCHEMA: GOLD_SCHEMA
|   +-- TABLE: gold_event_exposure
|   +-- TABLE: gold_daily_summary
|   +-- TABLE: gold_active_alerts
|
+-- SCHEMA: AUDIT_SCHEMA
    +-- TABLE: pipeline_runs
    +-- TABLE: dq_results

WAREHOUSE:        HAZARDWATCH_WH          (X-Small, auto-suspend 60s)
RESOURCE MONITOR: hazardwatch_monitor     (10 credit cap, suspend at 100%)

INTEGRATIONS:
  hazardwatch_azure_int             (Storage integration)
  hazardwatch_eventgrid_int         (Notification for Iceberg auto-refresh)

VOLUMES:
  hazardwatch_raw_vol               (External Volume -> ADLS raw)

STAGES:
  hazardwatch_raw_stage             (Snowpipe source stage)
  hazardwatch_pub_stage             (Unload destination stage)

ROLES:
  HAZARDWATCH_ADMIN                 (Full access - dbt and setup scripts)
  HAZARDWATCH_READER                (SELECT on GOLD_SCHEMA - Superset)
  HAZARDWATCH_INGEST                (INSERT on landing tables - Snowpipe)
```

---

## 13. dbt Project Structure

```
hazardwatch_dbt/
+-- dbt_project.yml
+-- profiles.yml                    <- Snowflake connection (not committed)
+-- packages.yml                    <- dbt_utils, dbt_expectations
+-- models/
|   +-- sources.yml                 <- defines RAW_SCHEMA sources
|   +-- bronze/
|   |   +-- bronze_usgs_earthquakes.sql
|   |   +-- bronze_gdacs_alerts.sql
|   |   +-- bronze_firms_fires.sql
|   |   +-- bronze_openaq_readings.sql
|   |   +-- bronze_eonet_events.sql
|   |   +-- bronze_worldpop_h3.sql
|   +-- silver/
|   |   +-- silver_hazard_event.sql
|   +-- gold/
|       +-- gold_event_exposure.sql
|       +-- gold_daily_summary.sql
|       +-- gold_active_alerts.sql
+-- tests/
|   +-- assert_magnitude_range.sql
|   +-- assert_no_null_hazard_id.sql
|   +-- assert_lat_lon_valid.sql
+-- macros/
    +-- severity_score.sql
```

### `dbt_project.yml` key settings

```yaml
name: hazardwatch
version: '1.0.0'
profile: hazardwatch_snowflake

models:
  hazardwatch:
    bronze:
      +materialized: incremental
      +schema: BRONZE_SCHEMA
    silver:
      +materialized: incremental
      +schema: SILVER_SCHEMA
    gold:
      +materialized: table
      +schema: GOLD_SCHEMA

vars:
  incremental_lookback_hours: 6    # safety overlap for late-arriving data
```

### Snowflake Task (runs dbt every 4 hours)

```sql
CREATE OR REPLACE TASK hazardwatch_dbt_run
  WAREHOUSE = HAZARDWATCH_WH
  SCHEDULE  = 'USING CRON 0 */4 * * * UTC'
AS
  -- Calls Azure Container Apps Job via REST API
  -- Container job runs: dbt run --select bronze+ && dbt test
  SELECT SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(...);
```

---

## 14. Azure Function App Structure

```
hazardwatch-func/
+-- host.json
+-- local.settings.json             <- never commit (contains keys)
+-- requirements.txt
+-- poll_usgs/
|   +-- __init__.py
|   +-- function.json               <- timerTrigger, schedule: "0 */5 * * * *"
+-- poll_gdacs/
+-- poll_eonet/
+-- poll_firms/
+-- poll_openaq/
+-- enrich_weather/                 <- httpTrigger, called per event
+-- download_worldpop/              <- httpTrigger, one-time
+-- shared/
    +-- iceberg_writer.py
    +-- adls_writer.py
    +-- retry.py
```

### `requirements.txt`

```
azure-functions
azure-storage-blob
azure-identity
azure-keyvault-secrets
httpx
pyarrow
pyiceberg[azure]
h3
pandas
```

---

## 15. Security & Secrets

| Secret | Stored in | Accessed by |
|---|---|---|
| ADLS connection string | Azure Key Vault | Azure Functions (managed identity) |
| FIRMS MAP_KEY | Azure Key Vault | `poll_firms` |
| OpenAQ API key | Azure Key Vault | `poll_openaq` |
| Snowflake private key | Azure Key Vault | dbt Container Apps Job |
| Snowflake storage trust | Azure AD app registration | Snowflake (one-time setup) |

```python
# Accessing secrets (managed identity - no hardcoded credentials)
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

client = SecretClient(
    vault_url="https://hazardwatch-kv.vault.azure.net/",
    credential=DefaultAzureCredential()
)
firms_key = client.get_secret("firms-map-key").value
```

---

## 16. Resource Monitor & Cost Guards

```sql
-- Set from day one - before any other Snowflake work
CREATE OR REPLACE RESOURCE MONITOR hazardwatch_monitor
  WITH
    CREDIT_QUOTA = 10
    FREQUENCY    = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
      ON 50  PERCENT DO NOTIFY
      ON 80  PERCENT DO NOTIFY
      ON 100 PERCENT DO SUSPEND;

ALTER WAREHOUSE HAZARDWATCH_WH
  SET RESOURCE_MONITOR = hazardwatch_monitor;
```

| Guard | Setting | Reason |
|---|---|---|
| Warehouse auto-suspend | 60 seconds | Avoids idle billing |
| Warehouse size | X-Small | dbt submits SQL, not compute-heavy |
| dbt run frequency | Every 4 hours | Reduces warehouse wakeups |
| Superset result cache | Enabled | Dashboards don't re-run queries |
| Snowpipe batching | 1 file per 5-min window | Snowpipe charges per file |

---

## 17. Error Handling & Retry Strategy

### Azure Functions: exponential backoff

```python
# Applied to all HTTP calls with @with_backoff()
@with_backoff(max_retries=5, base_wait=5)
def fetch_usgs(updated_after):
    r = httpx.get(USGS_URL, params={"updatedafter": updated_after})
    r.raise_for_status()
    return r.json()
```

### Dead-letter: Snowflake Alert on pipeline failures

```sql
CREATE OR REPLACE ALERT hazardwatch_pipeline_failure_alert
  WAREHOUSE = HAZARDWATCH_WH
  SCHEDULE  = '15 MINUTES'
  IF (EXISTS (
    SELECT 1 FROM HAZARDWATCH_DB.AUDIT_SCHEMA.pipeline_runs
    WHERE status = 'ERROR'
      AND run_time > DATEADD('hour', -1, CURRENT_TIMESTAMP())
  ))
  THEN CALL SYSTEM$SEND_EMAIL(
    'hazardwatch_email_int',
    'your_email@example.com',
    'HazardWatch pipeline failure',
    'A poller failed in the last hour. Check AUDIT_SCHEMA.pipeline_runs.'
  );
```

---

## 18. Data Quality Checks

### dbt singular tests

```sql
-- tests/assert_magnitude_range.sql
SELECT * FROM {{ ref('bronze_usgs_earthquakes') }}
WHERE magnitude NOT BETWEEN -2 AND 10

-- tests/assert_no_null_hazard_id.sql
SELECT * FROM {{ ref('silver_hazard_event') }}
WHERE hazard_id IS NULL

-- tests/assert_lat_lon_valid.sql
SELECT * FROM {{ ref('silver_hazard_event') }}
WHERE latitude  NOT BETWEEN -90  AND 90
   OR longitude NOT BETWEEN -180 AND 180
```

### dbt schema tests (`schema.yml`)

```yaml
models:
  - name: silver_hazard_event
    columns:
      - name: hazard_id
        tests: [unique, not_null]
      - name: severity_level
        tests:
          - accepted_values:
              values: ['low', 'medium', 'high', 'critical']
      - name: event_date
        tests: [not_null]
      - name: latitude
        tests:
          - dbt_utils.accepted_range:
              min_value: -90
              max_value: 90
      - name: longitude
        tests:
          - dbt_utils.accepted_range:
              min_value: -180
              max_value: 180
```

---

## 19. Quick-Start Build Order

```
Phase 1 — Infrastructure (Day 1)
  [ ] Create Azure Storage accounts: hazardwatchraw, hazardwatchpub
  [ ] Create Snowflake trial account (Azure, same region as storage)
  [ ] Create HAZARDWATCH_DB, all schemas, warehouse
  [ ] Create resource monitor (FIRST THING before anything else)
  [ ] Create Storage Integration + External Volume
  [ ] Grant Snowflake service principal access in Azure AD portal

Phase 2 — First data flowing (Day 2)
  [ ] Write poll_usgs Azure Function locally
  [ ] Run locally, verify Parquet lands in ADLS
  [ ] Create raw_usgs_earthquakes Iceberg Table in Snowflake
  [ ] Query: SELECT COUNT(*) FROM HAZARDWATCH_DB.RAW_SCHEMA.raw_usgs_earthquakes
  [ ] Test manual upload: drop a file, verify it appears in Snowflake

Phase 3 — Bronze + Silver for USGS (Day 3)
  [ ] Set up dbt project with Snowflake profile
  [ ] Write bronze_usgs_earthquakes dbt model
  [ ] Write silver_hazard_event (USGS only)
  [ ] Run dbt, verify row counts match raw

Phase 4 — Add all sources (Days 4-6)
  [ ] Add GDACS, FIRMS, EONET, OpenAQ pollers
  [ ] Add bronze models for each source
  [ ] Add each source to silver_hazard_event UNION
  [ ] Run dbt tests across all bronze

Phase 5 — Gold + Superset (Day 7)
  [ ] Download WorldPop GeoTIFF, convert to H3 Parquet, load to bronze
  [ ] Build gold_event_exposure (H3 join)
  [ ] Build gold_daily_summary, gold_active_alerts
  [ ] Deploy Superset, connect to Snowflake GOLD_SCHEMA
  [ ] Build world map dashboard

Phase 6 — Hardening (Week 2)
  [ ] Set up Event Grid auto-refresh for Iceberg tables
  [ ] Set up Azure Key Vault for all secrets
  [ ] Set up pipeline failure alert in Snowflake
  [ ] Set up Snowflake Task to run dbt every 4 hours
  [ ] Set up rclone job to mirror to R2
```
