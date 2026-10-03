# ============================================================
# HazardWatch — Layer 0: Data Sources & Polling
# File:    poll_eonet/__init__.py
# Purpose: Fetch NASA EONET open natural events every 30 minutes
# Layer:   Layer 0 — Polling & Ingestion
#
# Azure Functions note:
#   The function app root (hazardwatch_func/) is always on sys.path
#   when running on Azure. Import shared.* directly — no path hacks needed.
# ============================================================
import datetime
import logging
import time

import azure.functions as func
import httpx
import pyarrow as pa

from shared.adls_writer import write_parquet_to_adls
from shared.iceberg_writer import commit_to_iceberg
from shared.retry import with_backoff

logger = logging.getLogger(__name__)

SOURCE_NAME   = "eonet"
ICEBERG_TABLE = "raw.eonet_events"
BASE_URL      = "https://eonet.gsfc.nasa.gov/api/v3/events"
USER_AGENT    = {"User-Agent": "hazardwatch-learning (replace_with_your_email@example.com)"}

SCHEMA = pa.schema([
    pa.field("event_id",     pa.string()),
    pa.field("source",       pa.string()),
    pa.field("title",        pa.string()),
    pa.field("category",     pa.string()),
    pa.field("status",       pa.string()),
    pa.field("latitude",     pa.float64()),
    pa.field("longitude",    pa.float64()),
    pa.field("event_date",   pa.timestamp("us", tz="UTC")),
    pa.field("raw_json",     pa.string()),
    pa.field("_ingested_at", pa.timestamp("us", tz="UTC")),
])


def main(mytimer: func.TimerRequest) -> None:
    now = datetime.datetime.now(datetime.timezone.utc)
    if mytimer.past_due:
        logger.warning("[poll_eonet] Timer is past due")

    logger.info(f"[poll_eonet] Starting at {now.isoformat()}")

    events = _fetch_eonet()
    if not events:
        logger.info("[poll_eonet] No open events returned")
        return

    arrow_table = _flatten_to_arrow(events, now)

    date_str  = now.strftime("%Y-%m-%d")
    epoch     = int(time.time())
    blob_path = write_parquet_to_adls(arrow_table, SOURCE_NAME, date_str, epoch)
    commit_to_iceberg(arrow_table, ICEBERG_TABLE)

    logger.info(f"[poll_eonet] Done — {len(events)} events → {blob_path}")


@with_backoff(max_retries=5, base_wait=10)
def _fetch_eonet() -> list:
    """Fetch all currently open EONET events."""
    params = {
        "status": "open",
        "limit":  500,   # generous limit; EONET typically has <100 open events
    }
    response = httpx.get(BASE_URL, params=params, headers=USER_AGENT, timeout=30)
    response.raise_for_status()
    return response.json().get("events", [])


def _flatten_to_arrow(events: list, ingested_at: datetime.datetime) -> pa.Table:
    rows = []
    for event in events:
        # Get the most recent geometry point
        geometries = event.get("geometry", [])
        latest_geom = geometries[-1] if geometries else {}
        coords = latest_geom.get("coordinates", [None, None])
        lat = coords[1] if isinstance(coords, list) and len(coords) > 1 else None
        lon = coords[0] if isinstance(coords, list) else None

        # Event date from latest geometry
        raw_date = latest_geom.get("date")
        try:
            event_date = datetime.datetime.fromisoformat(
                raw_date.replace("Z", "+00:00")
            ) if raw_date else None
        except (ValueError, AttributeError):
            event_date = None

        # Category — take first one
        categories = event.get("categories", [])
        category = categories[0].get("title", "") if categories else ""

        rows.append({
            "event_id":     event.get("id", ""),
            "source":       SOURCE_NAME,
            "title":        event.get("title", ""),
            "category":     category,
            "status":       event.get("status", ""),
            "latitude":     lat,
            "longitude":    lon,
            "event_date":   event_date,
            "raw_json":     str(event),
            "_ingested_at": ingested_at,
        })

    return pa.Table.from_pylist(rows, schema=SCHEMA)
