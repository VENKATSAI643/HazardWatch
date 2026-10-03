# ============================================================
# HazardWatch — Layer 0: Data Sources & Polling
# File:    shared/iceberg_writer.py
# Purpose: Commit PyArrow table to an Iceberg table on ADLS
# Layer:   Layer 0 — Polling & Ingestion
# ============================================================
import os
import logging

import pyarrow as pa

logger = logging.getLogger(__name__)


def commit_to_iceberg(arrow_table: pa.Table, table_name: str) -> None:
    """
    Append a PyArrow table to an Iceberg table.

    This writes Iceberg snapshot metadata (a few KB) alongside the
    Parquet data already written to ADLS. Snowflake's External Volume
    reads this metadata to auto-refresh its Iceberg Table view.

    Args:
        arrow_table: Data that was already written to ADLS as Parquet.
        table_name:  Fully qualified Iceberg table name, e.g. "raw.usgs_earthquakes".

    Environment variables required:
        ICEBERG_CATALOG_URI  -- URI for the Iceberg catalog backend.
                                Use "sqlite:///tmp/hazardwatch.db" for local dev.
                                Use an Azure-backed REST catalog for production.

    Note:
        If the Iceberg catalog is unreachable, we log a warning but do NOT
        raise — the Parquet file is already safely written to ADLS. Snowflake
        can also manually refresh the External Table via ALTER ICEBERG TABLE.
    """
    try:
        from pyiceberg.catalog import load_catalog

        catalog_uri = os.environ.get("ICEBERG_CATALOG_URI", "sqlite:///tmp/hazardwatch.db")
        catalog = load_catalog("hazardwatch", uri=catalog_uri)

        tbl = catalog.load_table(table_name)
        tbl.append(arrow_table)

        logger.info(
            f"[iceberg_writer] Committed {len(arrow_table)} rows to Iceberg table '{table_name}'"
        )

    except Exception as exc:
        # Non-fatal: Parquet is already in ADLS; Iceberg metadata will be rebuilt
        # on next successful run or via manual ALTER ICEBERG TABLE ... REFRESH
        logger.warning(
            f"[iceberg_writer] Failed to commit to Iceberg table '{table_name}': {exc}. "
            f"Parquet file is safe — skipping Iceberg commit."
        )
