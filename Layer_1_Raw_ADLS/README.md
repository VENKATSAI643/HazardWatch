# Layer 1 & 2 — Raw ADLS & External Catalog Deployment Guide

> **Layer:** 1 & 2 of 8  
> **What it deploys:** Snowflake Database, Storage Integration, External Volume, and 5 Iceberg Tables  
> **Estimated time:** 15–30 minutes  
> **Cost:** ~$0/month (Snowflake compute is only billed when you run queries against these tables)

---

## Overview

This guide walks you through connecting your Snowflake account to the Azure Data Lake Storage account you created in Layer 0. By executing the provided SQL script in Snowflake, you will establish a secure "Zero-Copy" connection using Unmanaged Iceberg Tables. When the Azure Functions drop new files into ADLS, they will instantly appear in these Snowflake tables.

---

## Prerequisites

Before starting this layer, you need:

- [ ] Completion of **Layer 0** (Azure Storage Account must exist).
- [ ] Your Azure **Tenant ID** (Found in Azure Portal > Microsoft Entra ID > Overview).
- [ ] A **Snowflake Account** with `ACCOUNTADMIN` privileges.

---

## Step 1 — Create the Integrations & Volumes

You must create the Storage Integration and External Volume FIRST so that Snowflake generates an App Identity for you to authorize in Azure. If you try to create the tables before this, it will crash.

> [!CAUTION]
> **Do not run Part 1 more than once!**
> Running `01_integrations_and_volumes.sql` will execute a `CREATE OR REPLACE` command. If you re-run this *after* you have set up your Azure permissions (in Step 3), Snowflake will delete your old identity and generate a new one, breaking your Azure connection! If you encounter errors in Step 4, only re-run Step 4.

1. Make sure your `.env` file in the root folder contains your `AZURE_TENANT_ID`.
2. Open your WSL terminal and run these commands to execute **Part 1**:

```bash
# 1. Load the variables
export $(grep -v '^#' /mnt/e/Snowflake/HazardWatch/.env | xargs)

# 2. Inject variables and run Part 1
envsubst < /mnt/e/Snowflake/HazardWatch/Layer_1_Raw_ADLS/01_integrations_and_volumes.sql > /tmp/01_run.sql
snow sql -f /tmp/01_run.sql
```

---

## Step 2 — Get the Azure Consent URL

You just told Snowflake to talk to Azure, but now you need to authorize it.

1. Run this command in your terminal to ask Snowflake for the authorization URL:
   ```bash
   snow sql -q "DESC INTEGRATION hazardwatch_azure_int;"
   ```
2. In the output, find the property `AZURE_CONSENT_URL`. Copy that URL, paste it into a web browser, and click **Accept** to grant Snowflake permission to read your Azure account.
3. Also note the `AZURE_MULTI_TENANT_APP_NAME` in the output — you will need this in Step 3.

---

## Step 3 — Grant Snowflake Access in Azure

You just told Snowflake to talk to Azure, but now you need to tell Azure it is allowed to listen to Snowflake.

1. Open the **Azure Portal**.
2. Go to your Storage Account (`hazardwatchraw`).
3. Click **Access control (IAM)** on the left menu.
4. Click **+ Add** -> **Add role assignment**.
5. Select the **Storage Blob Data Contributor** role (Snowflake needs Contributor, not Reader, so it can write the Iceberg metadata) and click Next.
6. For "Assign access to", choose **User, group, or service principal**.
7. Click **+ Select members**. Paste the `AZURE_MULTI_TENANT_APP_NAME` you copied in Step 2. Select it.
8. Click **Review + assign**.

---

## Step 4 — Create the Iceberg Tables

Now that Azure has granted Snowflake permission to read the files, you can safely create the Iceberg tables!

Run this in your terminal to execute **Part 2**:

```bash
envsubst < /mnt/e/Snowflake/HazardWatch/Layer_1_Raw_ADLS/02_iceberg_tables.sql > /tmp/02_run.sql
snow sql -f /tmp/02_run.sql
```

---

## Step 5 — Refreshing the Data

Because we are using **Snowflake-managed Iceberg Tables** (where Snowflake actively generates and stores the metadata for us in Azure), Snowflake currently does not support `AUTO_REFRESH = TRUE` directly from Event Grid for this specific table type.

Instead, when new raw Parquet files land in Azure, Snowflake must be explicitly told to scan the folder and update the metadata.

You can do this at any time by running:
```sql
ALTER ICEBERG TABLE raw_usgs_earthquakes REFRESH;
```

*(Don't worry about automating this manually — in Layer 3, we will set up a Snowflake Task or Snowpipe to handle this on a schedule!)*

---

## Verify It Works

To verify the connection was successful, let's query the data!

1. In your Snowflake Worksheet, run this query:
   ```sql
   SELECT * FROM HAZARDWATCH_DB.RAW_SCHEMA.raw_usgs_earthquakes LIMIT 10;
   ```
2. If it returns rows of earthquake data, the connection is successful!

---

## Troubleshooting

| Problem | Likely Cause | Fix |
|---|---|---|
| `Cannot access external storage` error on SELECT | IAM Role Assignment missing | Double-check Azure Portal (Step 3). Ensure the role is **Storage Blob Data Reader** (not just "Reader"). It can take 5 minutes for Azure IAM to update. |
| Tables are empty | Layer 0 is not running | Check your Azure Function App from Layer 0 to ensure it is actually downloading data into the `raw/` folder. |
| `Unsupported file format` | Incorrect path | Make sure the `BASE_LOCATION` in the SQL matches the folder names in ADLS (e.g., `source=usgs/`). |

---

## Handoff to Next Layer

When this layer is working correctly, the following will be true:
- ✅ You can run `SELECT * FROM raw_usgs_earthquakes` in Snowflake and see data.
- ✅ You did not have to use `COPY INTO` or run a warehouse to load the data!

**Next:** We move to Layer 3 (Snowpipe), where we will set up automated ingestion pipelines for ultra-high-frequency data streams.
