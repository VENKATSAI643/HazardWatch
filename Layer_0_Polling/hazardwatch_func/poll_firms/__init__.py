# ============================================================
# HazardWatch — Layer 0: Data Sources & Polling
# File:    poll_firms/__init__.py
# Purpose: Fetch NASA FIRMS wildfire detections via bounding-box API
# Layer:   Layer 0 — Polling & Ingestion
#
# Azure Functions note:
#   The function app root (hazardwatch_func/) is always on sys.path
#   when running on Azure. Import shared.* directly — no path hacks needed.
# ============================================================
import csv
import datetime
import io
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

SOURCE_NAME   = "firms"
ICEBERG_TABLE = "raw.firms_fires"
# IMPORTANT: Use bounding-box area endpoint ONLY — country endpoints return errors
BASE_URL      = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"
USER_AGENT    = {"User-Agent": "hazardwatch-learning (replace_with_your_email@example.com)"}

SCHEMA = pa.schema([
    pa.field("event_id",     pa.string()),
    pa.field("source",       pa.string()),
    pa.field("latitude",     pa.float64()),
    pa.field("longitude",    pa.float64()),
    pa.field("brightness",   pa.float64()),
    pa.field("frp",          pa.float64()),
    pa.field("confidence",   pa.string()),
    pa.field("acq_date",     pa.date32()),
    pa.field("acq_time",     pa.string()),
    pa.field("satellite",    pa.string()),
    pa.field("daynight",     pa.string()),
    pa.field("raw_json",     pa.string()),
    pa.field("_ingested_at", pa.timestamp("us", tz="UTC")),
])


def main(mytimer: func.TimerRequest) -> None:
    now = datetime.datetime.now(datetime.timezone.utc)
    if mytimer.past_due:
        logger.warning("[poll_firms] Timer is past due")

    logger.info(f"[poll_firms] Starting at {now.isoformat()}")

    # Read config from environment
    map_key = os.environ["FIRMS_MAP_KEY"]
    bbox    = os.environ.get("FIRMS_BBOX", "68,6,98,37")   # Default: India region
    # Satellite product: VIIRS_SNPP_NRT — Near Real Time, global, 375m resolution
    product = "VIIRS_SNPP_NRT"
    days    = 1   # Fetch last 1 day (detections available ~3h after observation)

    rows_csv = _fetch_firms(map_key, product, bbox, days)
    if not rows_csv:
        logger.info("[poll_firms] No fire detections returned")
        return

    arrow_table = _csv_to_arrow(rows_csv, now)

    date_str  = now.strftime("%Y-%m-%d")
    epoch     = int(time.time())
    blob_path = write_parquet_to_adls(arrow_table, SOURCE_NAME, date_str, epoch)
    commit_to_iceberg(arrow_table, ICEBERG_TABLE)

    logger.info(f"[poll_firms] Done — {len(rows_csv)} detections → {blob_path}")


@with_backoff(max_retries=5, base_wait=15)
def _fetch_firms(map_key: str, product: str, bbox: str, days: int) -> list:
    """
    Fetch FIRMS CSV data for a bounding box.
    URL pattern: /api/area/csv/<MAP_KEY>/<product>/<bbox>/<days>
    Rate limit: 5,000 transactions per 10 minutes per MAP_KEY
    """
    url = f"{BASE_URL}/{map_key}/{product}/{bbox}/{days}"
    response = httpx.get(url, headers=USER_AGENT, timeout=60)

    # FIRMS returns a plain-text error (not HTTP status) for invalid keys
    if "Invalid API key" in response.text or "invalid" in response.text.lower()[:50]:
        raise ValueError(f"FIRMS API rejected request: {response.text[:200]}")

    response.raise_for_status()

    # Parse CSV
    reader = csv.DictReader(io.StringIO(response.text))
    return list(reader)


def _csv_to_arrow(rows: list, ingested_at: datetime.datetime) -> pa.Table:
    records = []
    for row in rows:
        lat  = float(row.get("latitude",  0) or 0)
        lon  = float(row.get("longitude", 0) or 0)
        acq_date_str = row.get("acq_date", "")
        acq_time_str = row.get("acq_time", "")

        # Composite unique ID: lat_lon_date_time
        event_id = f"{acq_date_str}_{acq_time_str}_{lat}_{lon}"

        try:
            acq_date = datetime.date.fromisoformat(acq_date_str) if acq_date_str else None
        except ValueError:
            acq_date = None

        records.append({
            "event_id":     event_id,
            "source":       SOURCE_NAME,
            "latitude":     lat,
            "longitude":    lon,
            "brightness":   float(row.get("bright_ti4", 0) or 0),
            "frp":          float(row.get("frp", 0) or 0),
            "confidence":   row.get("confidence", ""),
            "acq_date":     acq_date,
            "acq_time":     acq_time_str,
            "satellite":    row.get("satellite", ""),
            "daynight":     row.get("daynight", ""),
            "raw_json":     str(row),
            "_ingested_at": ingested_at,
        })

    return pa.Table.from_pylist(records, schema=SCHEMA)
