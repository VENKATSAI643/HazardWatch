{{ config(
    materialized     = 'incremental',
    unique_key       = 'event_id',
    on_schema_change = 'append_new_columns',
    cluster_by       = ['event_date']
) }}

WITH source AS (
    SELECT
        raw_value:event_id::STRING              AS event_id,
        'eonet'                                 AS source,
        raw_value:title::STRING                 AS title,
        raw_value:category::STRING              AS category,
        raw_value:status::STRING                AS status,
        raw_value:latitude::FLOAT               AS latitude,
        raw_value:longitude::FLOAT              AS longitude,
        raw_value:event_date::TIMESTAMP_NTZ     AS event_timestamp,
        raw_value:raw_json::STRING              AS raw_json,
        raw_value:_ingested_at::TIMESTAMP_NTZ   AS _ingested_at,
        DATE(raw_value:event_date::TIMESTAMP_NTZ) AS event_date
    FROM {{ source('raw', 'landing_eonet') }}
    {% if is_incremental() %}
    WHERE raw_value:_ingested_at::TIMESTAMP_NTZ >
          (SELECT MAX(_ingested_at) FROM {{ this }})
    {% endif %}
),

deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY event_id ORDER BY _ingested_at DESC
           ) AS rn
    FROM source
    WHERE event_id IS NOT NULL
      AND latitude  BETWEEN -90  AND 90
      AND longitude BETWEEN -180 AND 180
)

SELECT * EXCLUDE (rn) FROM deduped WHERE rn = 1
