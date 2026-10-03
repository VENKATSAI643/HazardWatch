# Layer 3 — Snowpipe & Landing Tables

> **Layer:** 3 of 8  
> **What it deploys:** 1 External Stage, 5 Landing Tables, 5 Snowpipes  
> **Estimated time:** 5–10 minutes  
> **Cost:** Serverless compute (~$0.01 per minute of active ingestion, extremely cheap)

---

## Overview

In Layer 1, we built **Iceberg Tables** so you could directly query your Parquet files in Azure without moving them. However, Snowflake-managed Iceberg Tables do not support true real-time `AUTO_REFRESH`. 

To ensure our downstream dashboards are updated the *exact second* an earthquake hits, we will use **Snowpipe**. Snowpipe will automatically listen to Azure, pick up the incoming Parquet files, and dump their raw contents into **Landing Tables** using a `VARIANT` column.

From there, our `dbt` models (Layer 4) will pick up the data from the Landing Tables.

---

## Prerequisites

Before running the script, you must create the Storage Queue in Azure that Snowflake will listen to:
1. Open the Azure Portal and go to your **`hazardwatchraw`** storage account.
2. On the left menu, scroll down to **Data storage** and click **Queues**.
3. Click **+ Queue** at the top.
4. Name it **`hazardwatch-refresh`** and click OK.

- [ ] The `hazardwatch-refresh` queue must exist in Azure.
- [ ] Completion of **Layer 1** (The `hazardwatch_azure_int` Storage Integration must exist).
- [ ] Your `.env` file in the root directory must have `AZURE_TENANT_ID`.

---

## Step 1 — Create the Stage, Tables, and Integration

Run this command in your WSL terminal to dynamically inject your Azure variables (while safely preserving Snowflake's metadata variables). This will create the Notification Integration and the Landing Tables.

```bash
# 1. Load the variables
export $(grep -v '^#' /mnt/e/Snowflake/HazardWatch/.env | xargs)

# 2. Inject ONLY the Tenant ID and run Part 1
envsubst '${AZURE_TENANT_ID}' < /mnt/e/Snowflake/HazardWatch/Layer_3_Snowpipe/03a_snowpipe_integration.sql > /tmp/03a_run.sql
snow sql -f /tmp/03a_run.sql
```

---

## Step 2 — Grant Queue Access in Azure

The script just created a **Notification Integration** so Snowpipe can automatically listen to Azure Event Grid for new files. You must authorize this identity so it can read the Event Grid Queue! If you skip this, Snowflake will block the next step!

> [!WARNING]
> Snowflake usually generates a **BRAND NEW** App Identity for Notification Integrations! Do not assume it is the same app you authorized in Layer 1. You must get the new name and consent to it.

1. Run this command to get the new App Name:
   ```bash
   snow sql -q "DESC NOTIFICATION INTEGRATION hazardwatch_eventgrid_int;"
   ```
2. Look at the `AZURE_MULTI_TENANT_APP_NAME`. It is likely completely different than Layer 1!
3. Copy the `AZURE_CONSENT_URL` into your browser and accept it on behalf of your organization.
4. Go to the Azure Portal -> your `hazardwatchraw` storage account -> **Access control (IAM)** -> **Add role assignment**.
5. Select the **Storage Queue Data Contributor** role (this is different than before!).
6. Search for the exact `AZURE_MULTI_TENANT_APP_NAME` and assign it.

Wait 60 seconds for the permission to propagate through Azure!

---

## Step 3 — Create the Snowpipes

Now that Snowflake is authorized to read the queue, we can safely create the pipes!

```bash
snow sql -f /mnt/e/Snowflake/HazardWatch/Layer_3_Snowpipe/03b_snowpipe_pipes.sql
```

---

## Step 4 — Verify the Pipes

Run this command to make sure your 5 pipes were created successfully:

```bash
snow sql -q "SHOW PIPES IN SCHEMA HAZARDWATCH_DB.RAW_SCHEMA;"
```

You should see `PIPE_USGS`, `PIPE_GDACS`, `PIPE_FIRMS`, `PIPE_OPENAQ`, and `PIPE_EONET` with `notification_channel` properties attached!

*(Note: In a production environment, you would copy those `notification_channel` IDs and configure an Azure Event Grid subscription to trigger them).*
