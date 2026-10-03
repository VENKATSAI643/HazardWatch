{{ config(materialized='table') }}

SELECT
    raw_value:h3_index_res7::STRING AS h3_index_res7,
    raw_value:population::FLOAT     AS population,
    raw_value:year::INT             AS year
FROM {{ source('raw', 'landing_worldpop') }}
