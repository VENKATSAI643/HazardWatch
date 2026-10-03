{{ config(
    materialized     = 'incremental',
    unique_key       = ['location_id', 'parameter', 'reading_time'],
    on_schema_change = 'append_new_columns',
    cluster_by       = ['reading_date']
) }}

WITH source AS (
    SELECT
        raw_value:location_id::BIGINT           AS location_id,
        'openaq'                                AS source,
        raw_value:city::STRING                  AS city,
        raw_value:country::STRING               AS country,
        raw_value:parameter::STRING             AS parameter,
        raw_value:value::FLOAT                  AS value,
        raw_value:unit::STRING                  AS unit,
        raw_value:reading_time::TIMESTAMP_NTZ   AS reading_time,
        raw_value:latitude::FLOAT               AS latitude,
        raw_value:longitude::FLOAT              AS longitude,
        raw_value:raw_json::STRING              AS raw_json,
        raw_value:_ingested_at::TIMESTAMP_NTZ   AS _ingested_at,
        DATE(raw_value:reading_time::TIMESTAMP_NTZ) AS reading_date
    FROM {{ source('raw', 'landing_openaq') }}
    {% if is_incremental() %}
    WHERE raw_value:_ingested_at::TIMESTAMP_NTZ >
          (SELECT MAX(_ingested_at) FROM {{ this }})
    {% endif %}
),

deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY location_id, parameter, reading_time ORDER BY _ingested_at DESC
           ) AS rn
    FROM source
    WHERE location_id IS NOT NULL
      AND latitude  BETWEEN -90  AND 90
      AND longitude BETWEEN -180 AND 180
)

SELECT * EXCLUDE (rn) FROM deduped WHERE rn = 1
