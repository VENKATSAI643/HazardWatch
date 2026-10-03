# ============================================================
# HazardWatch — Layer 0: Data Sources & Polling
# File:    poll_openaq/__init__.py
# Purpose: Fetch OpenAQ air quality sensor readings every hour
# Layer:   Layer 0 — Polling & Ingestion
#
# Azure Functions note:
#   The function app root (hazardwatch_func/) is always on sys.path
#   when running on Azure. Import shared.* directly — no path hacks needed.
# ============================================================
import datetime
import logging
import os
import time

import azure.functions as func
import httpx
import pyarrow as pa

from shared.adls_writer import write_parquet_to_adls
from shared.iceberg_writer import commit_to_iceberg
from shared.retry import with_backoff

logger = logging.getLogger(__name__)

SOURCE_NAME   = "openaq"
ICEBERG_TABLE = "raw.openaq_readings"
BASE_URL      = "https://api.openaq.org/v3/measurements"
USER_AGENT    = {"User-Agent": "hazardwatch-learning (replace_with_your_email@example.com)"}

# Rate limit: 60 requests/min, 2000 requests/hour per key
# We paginate to get all measurements — stay well under limits
MAX_PAGES   = 10
PAGE_SIZE   = 100

# Countries to poll — restrict scope to stay under rate limits
# India + neighbors. Add more ISO2 codes as needed.
TARGET_COUNTRIES = ["IN", "PK", "BD", "NP", "LK", "CN"]

SCHEMA = pa.schema([
    pa.field("location_id",   pa.int64()),
    pa.field("source",        pa.string()),
    pa.field("city",          pa.string()),
    pa.field("country",       pa.string()),
    pa.field("parameter",     pa.string()),
    pa.field("value",         pa.float64()),
    pa.field("unit",          pa.string()),
    pa.field("reading_time",  pa.timestamp("us", tz="UTC")),
    pa.field("latitude",      pa.float64()),
    pa.field("longitude",     pa.float64()),
    pa.field("raw_json",      pa.string()),
    pa.field("_ingested_at",  pa.timestamp("us", tz="UTC")),
])


def main(mytimer: func.TimerRequest) -> None:
    now = datetime.datetime.now(datetime.timezone.utc)
    if mytimer.past_due:
        logger.warning("[poll_openaq] Timer is past due")

    logger.info(f"[poll_openaq] Starting at {now.isoformat()}")

    api_key = os.environ["OPENAQ_API_KEY"]
    headers = {**USER_AGENT, "X-API-Key": api_key}

    all_rows = []
    # Fetch last 2 hours of measurements to ensure no gaps (hourly trigger has 1hr overlap)
    date_from = (now - datetime.timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")

    for country in TARGET_COUNTRIES:
        rows = _fetch_openaq_country(headers, country, date_from)
        all_rows.extend(rows)
        logger.info(f"[poll_openaq] {country}: {len(rows)} measurements")

    if not all_rows:
        logger.info("[poll_openaq] No measurements returned")
        return

    arrow_table = pa.Table.from_pylist(all_rows, schema=SCHEMA)

    date_str  = now.strftime("%Y-%m-%d")
    epoch     = int(time.time())
    blob_path = write_parquet_to_adls(arrow_table, SOURCE_NAME, date_str, epoch)
    commit_to_iceberg(arrow_table, ICEBERG_TABLE)

    logger.info(f"[poll_openaq] Done — {len(all_rows)} readings → {blob_path}")


@with_backoff(max_retries=3, base_wait=15)
def _fetch_openaq_country(headers: dict, country: str, date_from: str) -> list:
    """Fetch paginated measurements for a single country."""
    rows = []
    for page in range(1, MAX_PAGES + 1):
        params = {
            "country_id": country,
            "date_from":  date_from,
            "limit":      PAGE_SIZE,
            "page":       page,
            "order_by":   "datetime",
        }
        response = httpx.get(BASE_URL, params=params, headers=headers, timeout=30)
        response.raise_for_status()

        data      = response.json()
        results   = data.get("results", [])
        meta      = data.get("meta", {})

        for r in results:
            coord = r.get("coordinates", {}) or {}
            reading_raw = r.get("date", {}).get("utc") or r.get("date", {}).get("local")
            try:
                reading_time = datetime.datetime.fromisoformat(
                    reading_raw.replace("Z", "+00:00")
                ) if reading_raw else None
            except (ValueError, AttributeError):
                reading_time = None

            rows.append({
                "location_id":  r.get("location_id"),
                "source":       SOURCE_NAME,
                "city":         r.get("city", ""),
                "country":      country,
                "parameter":    r.get("parameter", ""),
                "value":        float(r.get("value", 0) or 0),
                "unit":         r.get("unit", ""),
                "reading_time": reading_time,
                "latitude":     float(coord.get("latitude", 0) or 0),
                "longitude":    float(coord.get("longitude", 0) or 0),
                "raw_json":     str(r),
                "_ingested_at": datetime.datetime.now(datetime.timezone.utc),
            })

        # Stop paginating if we have all results
        found = meta.get("found", 0)
        if not results or len(rows) >= found:
            break

        # Small pause between pages to stay within rate limits
        time.sleep(1)

    return rows
