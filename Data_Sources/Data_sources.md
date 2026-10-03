I'll look up the global replacements and the actual rate and update limits for each source.# Global source set and their limits

NWS is US-only, so I've dropped it. **NASA EONET** and **GDACS** cover the same need worldwide. I also found two things that change my earlier advice, noted below.

## Limits per source

**1. USGS earthquakes (global, no key)**
- The summary feeds refresh every minute.
- For custom queries, the FDSN service rejects any request over 20,000 events, and it has an `updatedafter` parameter that suits incremental polling.
- I found no published request-rate limit, so poll politely (my suggestion below).

**2. NASA FIRMS wildfires (global, free key)**
- Each MAP_KEY gets 5,000 transactions per 10 minutes, and larger requests, such as 7 days, can count as several.
- A single request can cover 1 to 5 days.
- Detections are available within about 3 hours of the satellite observation. Polling faster than every 15 to 30 minutes gains you nothing.
- A whole-world VIIRS query can return 30,000 to 100,000+ records per day, so decide whether to filter by region or confidence.
- **Correction to my last message:** I mentioned a country-level endpoint. One integration reports that the country endpoints are dead and return an invalid-call error. Use the bounding-box area endpoint only.

**3. GDACS multi-hazard alerts (global, no key)**
- The search API returns at most 100 records per call, and you page through the rest with a `pagenumber` parameter.
- The `events4app` endpoint returns the most recent 100 events from the last 4 days, which is ideal for a frequent incremental poll.
- I didn't find a published rate limit, so treat it as unknown. Fetch per-event polygons only for new or changed events.

**4. NASA EONET natural events (global, no key)**
- The v3 API has event and GeoJSON endpoints with filters for status, category, source, days, date range, and limit.
- Treat it as a discovery feed for visualization and general information, not a safety-of-life source. Your README should say the same.
- I didn't find a published rate limit.

**5. OpenAQ air quality (global, free key)**
- The limit is 60 requests per minute and 2,000 per hour, scoped to your API key.
- Attribution to the data source is required.
- This is the tightest limit in the project. Thousands of sensors can't be polled individually, so limit it to your chosen countries and poll by location.

**6. Open-Meteo weather context (global, no key)**
- The free tier allows fewer than 10,000 calls per day, 5,000 per hour, and 600 per minute, for non-commercial use only.
- Limits are applied per IP address.
- Attribution is required (CC BY 4.0). Only enrich events above a severity threshold to stay well under the daily cap.

**7. WorldPop population grid (one-time download)**
- The global 1km mosaics are GeoTIFFs under CC BY 4.0. There's no polling, so no rate limit matters. It's a single download followed by H3 conversion.

## Suggested polling plan

| Source | Poll every | Approx. calls/day | Headroom |
|---|---|---|---|
| USGS (`updatedafter`) | 5 min | ~290 | Unknown limit, so stay polite |
| GDACS `events4app` | 15 min | ~100 + fan-out | Unknown limit |
| EONET open events | 30 min | ~50 | Unknown limit |
| FIRMS (world or bbox, 1 day) | 30 min | ~50 | Far below 5,000 per 10 min |
| OpenAQ (chosen countries) | 1 hour | cap ~1,000/hr | Under 2,000/hr |
| Open-Meteo | on demand | under ~2,000 | Under 10,000/day |

These intervals are my recommendation, not documented requirements. Add exponential backoff on any 429 response and respect the rate-limit headers where provided.

## Updated probe script

```python
import requests
UA = {"User-Agent": "hazardwatch-learning (your_email@example.com)"}

r = requests.get("https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson", headers=UA)
print("USGS", r.status_code, len(r.json()["features"]))

r = requests.get("https://www.gdacs.org/gdacsapi/api/events/geteventlist/events4app", headers=UA)
print("GDACS", r.status_code, len(r.json().get("features", [])))

r = requests.get("https://eonet.gsfc.nasa.gov/api/v3/events?status=open&limit=20", headers=UA)
print("EONET", r.status_code, len(r.json()["events"]))

r = requests.get("https://api.openaq.org/v3/locations?limit=5", headers={"X-API-Key": "YOUR_KEY"})
print("OpenAQ", r.status_code, r.headers.get("x-ratelimit-remaining"))

# FIRMS: check your key and remaining transactions, then pull a small India-area box
KEY = "YOUR_MAP_KEY"
r = requests.get(f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{KEY}/VIIRS_SNPP_NRT/68,6,98,37/1")
print("FIRMS", r.status_code, len(r.text.splitlines()) - 1, "rows")
```

Source IDs and the response shape of GDACS may differ slightly from what I assumed, so check the printed output. Share it if anything fails.

Once these all return data, the next step is deciding your region scope. Do you want the whole world, or India and its neighboring countries with the world as a later extension? That choice sets the FIRMS bounding box, the OpenAQ country list, and which WorldPop files you download.