# ============================================================
# HazardWatch — Layer 0: Data Sources & Polling
# File:    enrich_weather/__init__.py
# Purpose: On-demand weather enrichment for a hazard event lat/lon
# Layer:   Layer 0 — Polling & Ingestion
#
# Azure Functions note:
#   The function app root (hazardwatch_func/) is always on sys.path
#   when running on Azure. Import shared.* directly — no path hacks needed.
# ============================================================
import datetime
import json
import logging
import os
import time

import azure.functions as func
import httpx
import pyarrow as pa

from shared.adls_writer import write_parquet_to_adls
from shared.retry import with_backoff

logger = logging.getLogger(__name__)

SOURCE_NAME = "openmeteo"
BASE_URL    = "https://api.open-meteo.com/v1/forecast"
USER_AGENT  = {"User-Agent": "hazardwatch-learning (replace_with_your_email@example.com)"}

# Daily limit: <10,000 calls. Rate limit: 600/min, 5,000/hr
# Only call this for severity >= "high" events — see gold layer trigger

SCHEMA = pa.schema([
    pa.field("hazard_id",          pa.string()),
    pa.field("source",             pa.string()),
    pa.field("latitude",           pa.float64()),
    pa.field("longitude",          pa.float64()),
    pa.field("temperature_2m",     pa.float64()),
    pa.field("relative_humidity",  pa.float64()),
    pa.field("wind_speed_10m",     pa.float64()),
    pa.field("wind_direction_10m", pa.float64()),
    pa.field("precipitation",      pa.float64()),
    pa.field("weather_code",       pa.int32()),
    pa.field("fetched_at",         pa.timestamp("us", tz="UTC")),
    pa.field("_ingested_at",       pa.timestamp("us", tz="UTC")),
])


def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP-triggered weather enrichment.

    Expected JSON body:
    {
        "hazard_id": "usgs_us6000abc",
        "latitude": 35.6,
        "longitude": 139.5
    }

    Called by the dbt Gold post-hook or a Snowflake Task via REST.
    Returns 200 OK on success.
    """
    now = datetime.datetime.now(datetime.timezone.utc)

    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON body", status_code=400)

    hazard_id = body.get("hazard_id")
    latitude  = body.get("latitude")
    longitude = body.get("longitude")

    if not all([hazard_id, latitude is not None, longitude is not None]):
        return func.HttpResponse(
            "Missing required fields: hazard_id, latitude, longitude",
            status_code=400
        )

    weather = _fetch_weather(float(latitude), float(longitude))
    if not weather:
        return func.HttpResponse("Weather fetch returned no data", status_code=502)

    current = weather.get("current", {})
    row = {
        "hazard_id":          hazard_id,
        "source":             SOURCE_NAME,
        "latitude":           float(latitude),
        "longitude":          float(longitude),
        "temperature_2m":     current.get("temperature_2m"),
        "relative_humidity":  current.get("relative_humidity_2m"),
        "wind_speed_10m":     current.get("wind_speed_10m"),
        "wind_direction_10m": current.get("wind_direction_10m"),
        "precipitation":      current.get("precipitation"),
        "weather_code":       current.get("weather_code"),
        "fetched_at":         now,
        "_ingested_at":       now,
    }

    arrow_table = pa.Table.from_pylist([row], schema=SCHEMA)
    date_str    = now.strftime("%Y-%m-%d")
    epoch       = int(time.time())
    blob_path   = write_parquet_to_adls(arrow_table, SOURCE_NAME, date_str, epoch)

    logger.info(f"[enrich_weather] Enriched {hazard_id} → {blob_path}")
    return func.HttpResponse(
        json.dumps({"status": "ok", "blob_path": blob_path}),
        mimetype="application/json",
        status_code=200,
    )


@with_backoff(max_retries=3, base_wait=5)
def _fetch_weather(lat: float, lon: float) -> dict:
    """Fetch current weather from Open-Meteo (no API key needed)."""
    params = {
        "latitude":  lat,
        "longitude": lon,
        "current": ",".join([
            "temperature_2m",
            "relative_humidity_2m",
            "wind_speed_10m",
            "wind_direction_10m",
            "precipitation",
            "weather_code",
        ]),
        "timezone": "UTC",
    }
    response = httpx.get(BASE_URL, params=params, headers=USER_AGENT, timeout=20)
    response.raise_for_status()
    return response.json()
