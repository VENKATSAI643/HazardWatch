-- models/gold/gold_event_exposure.sql
{{ config(materialized='table', cluster_by=['event_date', 'hazard_type']) }}

WITH events AS (
    SELECT * FROM {{ ref('silver_hazard_event') }}
    WHERE severity_level IN ('high', 'critical')
),

population AS (
    SELECT h3_index_res7, SUM(population) AS pop_in_cell
    FROM {{ ref('bronze_worldpop_h3') }}
    GROUP BY 1
),

-- H3 k-ring: get cells within affected radius
event_rings AS (
    SELECT
        e.hazard_id,
        e.h3_index_res7 AS center_h3,
        CEIL(e.affected_radius_km / 5.2) AS ring_count,   -- ~5.2km per res-7 cell
        H3_GRID_DISK(e.h3_index_res7, CEIL(e.affected_radius_km / 5.2)) AS neighbor_cells
    FROM events e
),

exposure AS (
    SELECT
        er.hazard_id,
        SUM(p.pop_in_cell) AS estimated_population_exposed
    FROM event_rings er,
         LATERAL FLATTEN(er.neighbor_cells) nc
    JOIN population p ON p.h3_index_res7 = nc.value::STRING
    GROUP BY er.hazard_id
)

SELECT
    e.*,
    COALESCE(ex.estimated_population_exposed, 0) AS estimated_population_exposed,
    CURRENT_TIMESTAMP() AS _gold_updated_at
FROM events e
LEFT JOIN exposure ex USING (hazard_id)
