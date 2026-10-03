# ============================================================
# HazardWatch — Layer 0: Data Sources & Polling
# File:    poll_gdacs/__init__.py
# Purpose: Fetch GDACS multi-hazard alerts (global) every 15 minutes
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

SOURCE_NAME   = "gdacs"
ICEBERG_TABLE = "raw.gdacs_alerts"
# events4app returns the most recent 100 events from the last 4 days — ideal for incremental poll
BASE_URL      = "https://www.gdacs.org/gdacsapi/api/events/geteventlist/events4app"
USER_AGENT    = {"User-Agent": "hazardwatch-learning (replace_with_your_email@example.com)"}

SCHEMA = pa.schema([
    pa.field("event_id",            pa.string()),
    pa.field("source",              pa.string()),
    pa.field("event_type",          pa.string()),
    pa.field("alert_level",         pa.string()),
    pa.field("event_name",          pa.string()),
    pa.field("event_time",          pa.timestamp("us", tz="UTC")),
    pa.field("latitude",            pa.float64()),
    pa.field("longitude",           pa.float64()),
    pa.field("country",             pa.string()),
    pa.field("affected_population", pa.int64()),
    pa.field("raw_json",            pa.string()),
    pa.field("_ingested_at",        pa.timestamp("us", tz="UTC")),
])


def main(mytimer: func.TimerRequest) -> None:
    now = datetime.datetime.now(datetime.timezone.utc)
    if mytimer.past_due:
        logger.warning("[poll_gdacs] Timer is past due")

    logger.info(f"[poll_gdacs] Starting at {now.isoformat()}")

    features = _fetch_gdacs()
    if not features:
        logger.info("[poll_gdacs] No events returned")
        return

    arrow_table = _flatten_to_arrow(features, now)

    date_str  = now.strftime("%Y-%m-%d")
    epoch     = int(time.time())
    blob_path = write_parquet_to_adls(arrow_table, SOURCE_NAME, date_str, epoch)
    commit_to_iceberg(arrow_table, ICEBERG_TABLE)

    logger.info(f"[poll_gdacs] Done — {len(features)} events → {blob_path}")


@with_backoff(max_retries=5, base_wait=10)
def _fetch_gdacs() -> list:
    """Fetch GDACS events4app endpoint — returns last 100 events from past 4 days."""
    response = httpx.get(BASE_URL, headers=USER_AGENT, timeout=30)
    response.raise_for_status()
    data = response.json()
    return data.get("features", [])


def _flatten_to_arrow(features: list, ingested_at: datetime.datetime) -> pa.Table:
    rows = []
    for f in features:
        props = f.get("properties", {})
        geom  = f.get("geometry", {})
        coords = geom.get("coordinates", [None, None]) if geom else [None, None]

        # GDACS returns ISO date strings
        raw_date = props.get("fromdate") or props.get("todate")
        try:
            event_time = datetime.datetime.fromisoformat(
                raw_date.replace("Z", "+00:00")
            ) if raw_date else None
        except (ValueError, AttributeError):
            event_time = None

        rows.append({
            "event_id":            str(props.get("eventid", "")),
            "source":              SOURCE_NAME,
            "event_type":          props.get("eventtype"),
            "alert_level":         props.get("alertlevel"),
            "event_name":          props.get("eventname"),
            "event_time":          event_time,
            "latitude":            coords[1] if isinstance(coords, list) and len(coords) > 1 else None,
            "longitude":           coords[0] if isinstance(coords, list) else None,
            "country":             props.get("iso3"),
            "affected_population": int(props.get("population", 0) or 0),
            "raw_json":            str(f),
            "_ingested_at":        ingested_at,
        })

    return pa.Table.from_pylist(rows, schema=SCHEMA)
