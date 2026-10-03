# Layer 5: Silver (Concepts & Architecture)

## Core Concepts Used in this Layer

### 1. Unified Schema (Conformed Dimensions)
In data engineering, a "unified schema" (or conformed dimension) means designing one standard table structure that multiple different data sources must fit into. In this layer, we force USGS, GDACS, FIRMS, OpenAQ, and EONET to map to `silver_hazard_event`. 
This requires:
- Generating a synthetic globally unique `hazard_id` (e.g., `usgs_12345`).
- Normalizing disparate metrics (magnitudes, AQI values) into a standard `severity_score` (0-100).

### 2. Semi-Structured Overflow (`VARIANT` columns)
When mapping multiple sources to a unified schema, you often have fields that only exist in one source (e.g., earthquakes have `depth_km`, but wildfires do not). 
Instead of adding 50 columns to our unified table where 90% of them are `NULL`, we use Snowflake's `VARIANT` column to store these unique fields in a JSON object called `extra`. 
```sql
OBJECT_CONSTRUCT('depth_km', depth_km, 'magnitude', magnitude) AS extra
```

### 3. Spatial Indexing (Uber H3)
Joining geospatial points (lat/lon) to polygons (like population maps) is computationally expensive.
To solve this, we use the **H3 Hexagonal Hierarchical Spatial Index** (created by Uber). By converting `latitude` and `longitude` into an H3 string (e.g., `87283472bffffff`), spatial intersections become simple, hyper-fast string matches.
```sql
H3_LATLNG_TO_CELL(latitude, longitude, 7)::STRING AS h3_index_res7
```

### 4. Unions
The SQL `UNION ALL` operator is used heavily here to vertically stack the results of the 5 separate CTEs (Common Table Expressions) into one single continuous table.
