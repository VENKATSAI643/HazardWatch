# ============================================================
# HazardWatch — Layer 0: Data Sources & Polling
# File:    shared/adls_writer.py
# Purpose: Write a PyArrow table as Parquet to ADLS Gen2
# Layer:   Layer 0 — Polling & Ingestion
# ============================================================
import io
import os
import logging

import pyarrow as pa
import pyarrow.parquet as pq
from azure.storage.blob import BlobServiceClient

logger = logging.getLogger(__name__)


def write_parquet_to_adls(
    arrow_table: pa.Table,
    source_name: str,
    date_str: str,
    epoch: int,
) -> str:
    """
    Write a PyArrow table as a Parquet file to ADLS Gen2.

    Path pattern:
        raw/source=<source_name>/date=<date_str>/batch_<epoch>.parquet

    Args:
        arrow_table: The data to write.
        source_name: The data source identifier (e.g., "usgs", "gdacs").
        date_str:    The partition date string in YYYY-MM-DD format.
        epoch:       Unix timestamp used to make the filename unique.

    Returns:
        The full blob path that was written.

    Environment variables required:
        ADLS_CONNECTION_STRING  -- Full connection string for the storage account
        ADLS_CONTAINER_NAME     -- Container name (default: "raw")
    """
    conn_str = os.environ["ADLS_CONNECTION_STRING"]
    container = os.environ.get("ADLS_CONTAINER_NAME", "raw")

    blob_path = f"source={source_name}/date={date_str}/batch_{epoch}.parquet"

    # Serialize the Arrow table to Parquet bytes in memory
    buf = pa.BufferOutputStream()
    pq.write_table(
        arrow_table,
        buf,
        compression="snappy",           # Good balance of size and speed
        row_group_size=50_000,          # Keeps row groups manageable
    )
    parquet_bytes = buf.getvalue().to_pybytes()

    # Upload to ADLS
    blob_client = BlobServiceClient.from_connection_string(conn_str) \
                                   .get_blob_client(container, blob_path)
    blob_client.upload_blob(parquet_bytes, overwrite=True)

    file_size_kb = len(parquet_bytes) / 1024
    logger.info(
        f"[adls_writer] Wrote {len(arrow_table)} rows to "
        f"{container}/{blob_path} ({file_size_kb:.1f} KB)"
    )
    return blob_path
