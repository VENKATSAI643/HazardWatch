#!/bin/bash
# R2 Mirror script (Run as an Azure Container Apps Job)

echo "Starting rclone sync from Azure ADLS to Cloudflare R2..."

# rclone is configured via environment variables (RCLONE_CONFIG_...)
rclone sync \
  "azure://hazardwatchpub.blob.core.windows.net/published/" \
  "r2:hazardwatch-open/" \
  --include "*.parquet" \
  --transfers 8 \
  --log-level INFO

echo "Sync complete."
