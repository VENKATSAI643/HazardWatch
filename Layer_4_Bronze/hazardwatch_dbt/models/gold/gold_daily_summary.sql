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
