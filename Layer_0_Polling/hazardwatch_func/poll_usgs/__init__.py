# ============================================================
# HazardWatch — Layer 0: Data Sources & Polling
# File:    poll_usgs/__init__.py
# Purpose: Fetch USGS earthquake events (global) on a 5-min timer
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

# ── Constants ──────────────────────────────────────────────
SOURCE_NAME   = "usgs"
ICEBERG_TABLE = "raw.usgs_earthquakes"
BASE_URL      = "https://earthquake.usgs.gov/fdsnws/event/1/query"
USER_AGENT    = {"User-Agent": "hazardwatch-learning (replace_with_your_email@example.com)"}

# Pyarrow schema — must match Layer 1 Iceberg table definition exactly
SCHEMA = pa.schema([
    pa.field("event_id",     pa.string()),
    pa.field("source",       pa.string()),
    pa.field("magnitude",    pa.float64()),
    pa.field("place",        pa.string()),
    pa.field("event_time",   pa.timestamp("us", tz="UTC")),
    pa.field("updated_at",   pa.timestamp("us", tz="UTC")),
    pa.field("latitude",     pa.float64()),
    pa.field("longitude",    pa.float64()),
    pa.field("depth_km",     pa.float64()),
    pa.field("alert",        pa.string()),
    pa.field("status",       pa.string()),
    pa.field("raw_json",     pa.string()),
    pa.field("_ingested_at", pa.timestamp("us", tz="UTC")),
])


def main(mytimer: func.TimerRequest) -> None:
    """
    Main entry point — called by Azure Functions timer trigger every 5 minutes.
    Uses updatedafter so we only fetch events changed since the last run.
    """
    now = datetime.datetime.now(datetime.timezone.utc)

    if mytimer.past_due:
        logger.warning("[poll_usgs] Timer is past due — running immediately")

    logger.info(f"[poll_usgs] Starting at {now.isoformat()}")

    # Fetch events updated in the last 6 minutes (1-min overlap prevents gaps)
    updated_after = (now - datetime.timedelta(minutes=6)).strftime("%Y-%m-%dT%H:%M:%S")

    features = _fetch_usgs(updated_after)
    if not features:
        logger.info("[poll_usgs] No new events — nothing to write")
        return

    arrow_table = _flatten_to_arrow(features, now)

    date_str = now.strftime("%Y-%m-%d")
    epoch    = int(time.time())
    blob_path = write_parquet_to_adls(arrow_table, SOURCE_NAME, date_str, epoch)

    commit_to_iceberg(arrow_table, ICEBERG_TABLE)

    logger.info(f"[poll_usgs] Done — {len(features)} events → {blob_path}")


@with_backoff(max_retries=5, base_wait=5)
def _fetch_usgs(updated_after: str) -> list:
    """Fetch earthquake GeoJSON features from USGS FDSN API."""
    params = {
        "format":       "geojson",
        "updatedafter": updated_after,
        "orderby":      "time",
        "limit":        20000,      # FDSN hard cap; should never be hit in 6 mins
    }
    response = httpx.get(BASE_URL, params=params, headers=USER_AGENT, timeout=30)

    if response.status_code == 429:
        retry_after = int(response.headers.get("Retry-After", 60))
        raise RuntimeError(f"Rate limited — retry after {retry_after}s")

    response.raise_for_status()
    return response.json().get("features", [])


def _flatten_to_arrow(features: list, ingested_at: datetime.datetime) -> pa.Table:
    """Convert GeoJSON feature list into a typed PyArrow table."""
    rows = []
    for f in features:
        props = f.get("properties", {})
        coords = f.get("geometry", {}).get("coordinates", [None, None, None])

        # Convert epoch milliseconds to datetime
        event_ms   = props.get("time")
        updated_ms = props.get("updated")
        event_time   = datetime.datetime.fromtimestamp(event_ms / 1000, tz=datetime.timezone.utc) if event_ms else None
        updated_at   = datetime.datetime.fromtimestamp(updated_ms / 1000, tz=datetime.timezone.utc) if updated_ms else None

        rows.append({
            "event_id":     f.get("id"),
            "source":       SOURCE_NAME,
            "magnitude":    props.get("mag"),
            "place":        props.get("place"),
            "event_time":   event_time,
            "updated_at":   updated_at,
            "latitude":     coords[1] if len(coords) > 1 else None,
            "longitude":    coords[0] if len(coords) > 0 else None,
            "depth_km":     coords[2] if len(coords) > 2 else None,
            "alert":        props.get("alert"),
            "status":       props.get("status"),
            "raw_json":     str(f),
            "_ingested_at": ingested_at,
        })

    return pa.Table.from_pylist(rows, schema=SCHEMA)
