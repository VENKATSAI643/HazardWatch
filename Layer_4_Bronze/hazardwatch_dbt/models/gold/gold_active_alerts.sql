-- models/gold/gold_active_alerts.sql
{{ config(materialized='table') }}

SELECT *
FROM {{ ref('silver_hazard_event') }}
WHERE status = 'active'
  AND event_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY severity_score DESC
