-- models/silver/silver_hazard_event.sql
{{ config(
    materialized = 'incremental',
    unique_key   = 'hazard_id',
    cluster_by   = ['event_date', 'hazard_type']
) }}

WITH usgs AS (
    SELECT
        'usgs_' || event_id             AS hazard_id,
        'usgs'                          AS source,
        'earthquake'                    AS hazard_type,
        CASE
            WHEN magnitude >= 7.0 THEN 'critical'
            WHEN magnitude >= 6.0 THEN 'high'
            WHEN magnitude >= 4.5 THEN 'medium'
            ELSE 'low'
        END                             AS severity_level,
        LEAST(magnitude * 14.3, 100)   AS severity_score,
        event_time, event_date, latitude, longitude,
        H3_LATLNG_TO_CELL(latitude, longitude, 7)::STRING AS h3_index_res7,
        H3_LATLNG_TO_CELL(latitude, longitude, 5)::STRING AS h3_index_res5,
        NULL                            AS country_iso3,
        place                           AS place_name,
        CASE
            WHEN depth_km < 70  THEN magnitude * 15
            WHEN depth_km < 300 THEN magnitude * 8
            ELSE magnitude * 3
        END                             AS affected_radius_km,
        alert_level, status,
        OBJECT_CONSTRUCT('depth_km', depth_km, 'magnitude', magnitude) AS extra,
        updated_at                      AS _source_updated_at
    FROM {{ ref('bronze_usgs_earthquakes') }}
    {% if is_incremental() %}
    WHERE _ingested_at > (
        SELECT MAX(_dbt_updated_at) FROM {{ this }} WHERE source = 'usgs'
    )
    {% endif %}
),

gdacs AS (
    SELECT
        'gdacs_' || event_id            AS hazard_id,
        'gdacs'                         AS source,
        CASE event_type
            WHEN 'EQ' THEN 'earthquake'
            WHEN 'TC' THEN 'tropical_cyclone'
            WHEN 'FL' THEN 'flood'
            WHEN 'VO' THEN 'volcano'
            WHEN 'DR' THEN 'drought'
            ELSE 'unknown'
        END                             AS hazard_type,
        LOWER(alert_level)              AS severity_level,
        CASE alert_level
            WHEN 'Red'    THEN 90
            WHEN 'Orange' THEN 60
            WHEN 'Green'  THEN 20
            ELSE 5
        END                             AS severity_score,
        event_time,
        DATE(event_time)                AS event_date,
        latitude, longitude,
        H3_LATLNG_TO_CELL(latitude, longitude, 7)::STRING AS h3_index_res7,
        H3_LATLNG_TO_CELL(latitude, longitude, 5)::STRING AS h3_index_res5,
        country                         AS country_iso3,
        event_name                      AS place_name,
        NULL                            AS affected_radius_km,
        alert_level, 'active'           AS status,
        OBJECT_CONSTRUCT('affected_population', affected_population) AS extra,
        _ingested_at                    AS _source_updated_at
    FROM {{ ref('bronze_gdacs_alerts') }}
    {% if is_incremental() %}
    WHERE _ingested_at > (
        SELECT MAX(_dbt_updated_at) FROM {{ this }} WHERE source = 'gdacs'
    )
    {% endif %}
),

firms AS (
    SELECT
        'firms_' || event_id            AS hazard_id,
        'firms'                         AS source,
        'wildfire'                      AS hazard_type,
        CASE
            WHEN confidence = 'high' AND frp > 100 THEN 'critical'
            WHEN confidence = 'high' THEN 'high'
            WHEN confidence = 'nominal' THEN 'medium'
            ELSE 'low'
        END                             AS severity_level,
        LEAST(frp, 100)                 AS severity_score,
        TO_TIMESTAMP(TO_VARCHAR(acq_date, 'YYYY-MM-DD') || ' ' || LEFT(LPAD(acq_time::VARCHAR, 4, '0'), 2) || ':' || RIGHT(LPAD(acq_time::VARCHAR, 4, '0'), 2), 'YYYY-MM-DD HH24:MI') AS event_time,
        acq_date                        AS event_date,
        latitude, longitude,
        H3_LATLNG_TO_CELL(latitude, longitude, 7)::STRING AS h3_index_res7,
        H3_LATLNG_TO_CELL(latitude, longitude, 5)::STRING AS h3_index_res5,
        NULL                            AS country_iso3,
        NULL                            AS place_name,
        1.0                             AS affected_radius_km,
        confidence                      AS alert_level,
        'active'                        AS status,
        OBJECT_CONSTRUCT('brightness', brightness, 'frp', frp, 'satellite', satellite) AS extra,
        _ingested_at                    AS _source_updated_at
    FROM {{ ref('bronze_firms_fires') }}
    {% if is_incremental() %}
    WHERE _ingested_at > (
        SELECT MAX(_dbt_updated_at) FROM {{ this }} WHERE source = 'firms'
    )
    {% endif %}
),

openaq AS (
    SELECT
        'openaq_' || location_id || '_' || parameter || '_' || TO_CHAR(reading_time, 'YYYYMMDDHH24MISS') AS hazard_id,
        'openaq'                        AS source,
        'air_quality'                   AS hazard_type,
        CASE
            WHEN parameter = 'pm25' AND value > 250 THEN 'critical'
            WHEN parameter = 'pm25' AND value > 150 THEN 'high'
            WHEN parameter = 'pm25' AND value > 50 THEN 'medium'
            ELSE 'low'
        END                             AS severity_level,
        LEAST(value / 3, 100)           AS severity_score,
        reading_time                    AS event_time,
        DATE(reading_time)              AS event_date,
        latitude, longitude,
        H3_LATLNG_TO_CELL(latitude, longitude, 7)::STRING AS h3_index_res7,
        H3_LATLNG_TO_CELL(latitude, longitude, 5)::STRING AS h3_index_res5,
        country                         AS country_iso3,
        city                            AS place_name,
        10.0                            AS affected_radius_km,
        NULL                            AS alert_level,
        'active'                        AS status,
        OBJECT_CONSTRUCT('parameter', parameter, 'value', value, 'unit', unit) AS extra,
        _ingested_at                    AS _source_updated_at
    FROM {{ ref('bronze_openaq_readings') }}
    {% if is_incremental() %}
    WHERE _ingested_at > (
        SELECT MAX(_dbt_updated_at) FROM {{ this }} WHERE source = 'openaq'
    )
    {% endif %}
),

eonet AS (
    SELECT
        'eonet_' || event_id            AS hazard_id,
        'eonet'                         AS source,
        LOWER(category)                 AS hazard_type,
        'medium'                        AS severity_level,
        50.0                            AS severity_score,
        event_timestamp                 AS event_time,
        event_date                      AS event_date,
        latitude, longitude,
        H3_LATLNG_TO_CELL(latitude, longitude, 7)::STRING AS h3_index_res7,
        H3_LATLNG_TO_CELL(latitude, longitude, 5)::STRING AS h3_index_res5,
        NULL                            AS country_iso3,
        title                           AS place_name,
        20.0                            AS affected_radius_km,
        NULL                            AS alert_level,
        status                          AS status,
        OBJECT_CONSTRUCT('title', title, 'category', category) AS extra,
        _ingested_at                    AS _source_updated_at
    FROM {{ ref('bronze_eonet_events') }}
    {% if is_incremental() %}
    WHERE _ingested_at > (
        SELECT MAX(_dbt_updated_at) FROM {{ this }} WHERE source = 'eonet'
    )
    {% endif %}
),

unioned AS (
    SELECT *, CURRENT_TIMESTAMP() AS _dbt_updated_at FROM usgs
    UNION ALL
    SELECT *, CURRENT_TIMESTAMP() AS _dbt_updated_at FROM gdacs
    UNION ALL
    SELECT *, CURRENT_TIMESTAMP() AS _dbt_updated_at FROM firms
    UNION ALL
    SELECT *, CURRENT_TIMESTAMP() AS _dbt_updated_at FROM openaq
    UNION ALL
    SELECT *, CURRENT_TIMESTAMP() AS _dbt_updated_at FROM eonet
)

SELECT * FROM unioned
