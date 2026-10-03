# Apache Superset Configuration Guide

Follow these steps to connect Superset to the HazardWatch Snowflake database and build the final dashboard.

## 1. Database Connection
In Superset, go to **Settings > Database Connections** and add a new Snowflake connection using the SQLAlchemy URI format:
```
snowflake://<username>:<password>@<account_identifier>/HAZARDWATCH_DB/GOLD_SCHEMA?warehouse=HAZARDWATCH_WH&role=HAZARDWATCH_READER
```
*(Note: `<account_identifier>` is your Snowflake Account Identifier, usually formatted as `orgname-account_name`. You do not need to include `.snowflakecomputing.com`.)*
*(Ensure you have installed the `snowflake-sqlalchemy` driver on your Superset instance).*

## 2. Datasets
Sync the following tables from the `GOLD_SCHEMA` as Datasets in Superset:
1. `gold_active_alerts`
2. `gold_daily_summary`
3. `gold_event_exposure`

## 3. Chart Configurations

### Chart A: Active Alerts Map
- **Dataset:** `gold_active_alerts`
- **Visualization Type:** deck.gl Scatterplot (or Mapbox)
- **Longitude & Latitude:** mapped to the `longitude` and `latitude` columns.
- **Point Radius:** `severity_score` (Multiplier: 100)
- **Point Color:** Categorical based on `severity_level` (Critical = Red, High = Orange, Medium = Yellow, Low = Green).

### Chart B: Daily Event Count
- **Dataset:** `gold_daily_summary`
- **Visualization Type:** Bar Chart
- **Time Column:** `event_date` (Time Grain: Day)
- **Metrics:** `SUM(event_count)`
- **Group by:** `hazard_type` (Stacked Bar)

### Chart C: Population Exposed
- **Dataset:** `gold_event_exposure`
- **Visualization Type:** Big Number
- **Time Range:** Last 7 days
- **Metric:** `SUM(estimated_population_exposed)`

### Chart D: Top Critical Events
- **Dataset:** `gold_event_exposure`
- **Visualization Type:** Table
- **Columns:** `event_date`, `hazard_type`, `place_name`, `severity_score`, `estimated_population_exposed`
- **Sort By:** `estimated_population_exposed` DESC
- **Row Limit:** 10
