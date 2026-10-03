# Layer 6: Gold (Concepts & Architecture)

## Core Concepts Used in this Layer

### 1. Pre-Aggregation (Materialized as Tables)
Unlike Bronze and Silver which use `materialized = 'incremental'` to process delta updates, Gold models use `materialized = 'table'`. This means the table is completely dropped and rebuilt on every dbt run. Because Gold tables are typically highly aggregated summaries (thousands of rows instead of millions), this is very fast. It ensures the downstream BI tool (Superset) always hits a pristine, pre-computed cache.

### 2. Spatial Joins via H3 k-rings
Joining a point (hazard) to a massive grid (WorldPop) using traditional ST_CONTAINS or ST_DISTANCE is extremely slow.
Instead, we use Uber's H3 Spatial Indexing system:
1. We take the hazard's center H3 cell.
2. We use `H3_GRID_DISK()` to find all neighboring H3 cells within the hazard's affected radius. This generates an array of neighbor cell IDs.
3. We `FLATTEN` that array into rows.
4. We do a simple, lightning-fast STRING join against the `bronze_worldpop_h3` table (which is already indexed by H3 strings) and `SUM(population)`.

This completely avoids math-heavy geospatial functions during the join.

### 3. Analytics Ready (Wide Tables)
Gold tables are typically "denormalized" (wide tables). We do not want the BI tool to have to execute `JOIN` statements. The data should be ready to be plotted directly onto a chart.
