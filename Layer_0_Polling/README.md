# Layer 0 — Data Sources & Polling — Deployment Guide

> **Layer:** 0 of 8  
> **What it deploys:** Azure Function App with 6 timer-triggered pollers + 1 on-demand enrichment function  
> **Estimated time:** 2–3 hours for first-time setup  
> **Cost:** ~$0/month on Azure Functions Consumption plan within free tier (first 1M executions/month free)

---

## Overview

This guide walks you through deploying the HazardWatch polling layer using the **Azure Portal UI**. By the end, you will have seven Azure Functions running on schedules, fetching hazard data from public APIs and saving Parquet files into Azure Data Lake Storage. No servers to manage — Azure handles all the infrastructure.

---

## Prerequisites

Before starting Layer 0, you need:

- [ ] An active **Azure subscription** (free trial is fine).
- [ ] **VS Code** with the **Azure Functions Extension** installed (we will use this to publish the code instead of the command line).
- [ ] A **NASA FIRMS MAP_KEY** (free) — register at https://firms.modaps.eosdis.nasa.gov/api/map_key/
- [ ] An **OpenAQ API key** (free) — register at https://api.openaq.org/register
- [ ] **No Snowflake account needed yet** — that comes in Layer 2.

> **Important:** Layer 1 (ADLS storage account creation) is handled in Step 2 below.

---

## Step 1 — Create the Azure Resource Group

A Resource Group is a logical container for all your Azure resources. Everything for HazardWatch should live in one group so you can manage or delete everything at once.

**In Azure Portal:**
1. Go to https://portal.azure.com
2. Search for **"Resource groups"** in the top search bar and click it.
3. Click **+ Create**.
4. Fill in:
   - **Subscription:** Select your subscription.
   - **Resource group name:** `hazardwatch-rg`
   - **Region:** Choose the region closest to you (e.g., `East US 2`, `West Europe`). **Write this down — every other resource must be in the same region.**
5. Click **Review + Create**, then click **Create**.

---

## Step 2 — Create the Azure Storage Account (ADLS Gen2)

This is the raw data lake where all Parquet files will be saved. 

**In Azure Portal:**
1. Search for **"Storage accounts"** in the top search bar and click it.
2. Click **+ Create**.
3. On the **Basics** tab, fill in:
   - **Resource group:** Select `hazardwatch-rg`
   - **Storage account name:** `hazardwatchraw` *(Must be globally unique — if taken, try `hazardwatchraw2026`)*
   - **Region:** Same region as your resource group!
   - **Performance:** Standard
   - **Redundancy:** LRS (Locally-redundant storage — cheapest)
4. Click the **Advanced** tab at the top.
5. Check the box for **Enable hierarchical namespace** *(This is critical — it turns normal Blob storage into ADLS Gen2 with real folders).*
6. Click **Review + Create**, then **Create**.
7. Wait for deployment to finish, then click **Go to resource**.

**Create the 'raw' container:**
1. On your new storage account's left menu, click **Containers** (under Data storage).
2. Click **+ Container**.
3. Name it `raw` and click **Create**.

**Copy your Connection String:**
1. On the left menu, click **Access keys** (under Security + networking).
2. Click **Show** next to Connection string (under key1).
3. Copy this string and paste it in a notepad — you need it for Step 3.

---

## Step 3 — Create Azure Key Vault

This is where all secrets (API keys, connection strings) are stored securely. 

**In Azure Portal:**
1. Search for **"Key vaults"** and click it.
2. Click **+ Create**.
3. Fill in:
   - **Resource group:** `hazardwatch-rg`
   - **Key vault name:** `hazardwatch-kv` *(Must be globally unique — try adding numbers if taken)*
   - **Region:** Same region!
   - **Pricing tier:** Standard
4. Click **Review + Create**, then **Create**.
5. Wait for deployment, then click **Go to resource**.

**Add your secrets to the Vault:**
1. On the left menu of your Key Vault, click **Secrets**.
2. Click **+ Generate/Import**.
3. Add these three secrets one by one (repeat the process for each):

| Name | Value |
|---|---|
| `adls-connection-string` | *Paste the Connection String you copied in Step 2* |
| `firms-map-key` | *Your NASA FIRMS MAP_KEY* |
| `openaq-api-key` | *Your OpenAQ API key* |

---

## Step 4 — Create the Azure Function App

The Function App is the container that runs all your polling Python scripts.

**In Azure Portal:**
1. Search for **"Function App"** and click it.
2. Click **+ Create** -> **Consumption** (if it asks for a plan type, choose Consumption/Serverless).
3. On the **Basics** tab, fill in:
   - **Resource group:** `hazardwatch-rg`
   - **Function App name:** `hazardwatch-func` *(Globally unique)*
   - **Runtime stack:** Python
   - **Version:** 3.11
   - **Region:** Same region!
   - **Operating System:** Linux
4. Click **Review + Create**, then **Create**.
5. Wait for deployment, then click **Go to resource**.

**Enable Managed Identity (So the Function can read the Key Vault):**
1. On the left menu of your Function App, scroll down to **Identity** (under Settings).
2. Under the **System assigned** tab, set Status to **On** and click **Save**.
3. Click **Yes** to confirm. 
4. A **Object (principal) ID** will appear. Copy it.

**Grant the Function App access to the Key Vault:**
1. Go back to your **Key Vault** (`hazardwatch-kv`).
2. Click **Access control (IAM)** on the left menu.
3. Click **+ Add** -> **Add role assignment**.
4. Search for and select the role: **Key Vault Secrets User**, then click Next.
5. Assign access to: **Managed identity**.
6. Click **+ Select members**, choose "Function App", and select your `hazardwatch-func`. 
7. Click **Select**, then **Review + assign**.

---

## Step 5 — Configure App Settings

These settings tell your Python code where to look for the secrets in the Key Vault.

**In Azure Portal:**
1. Go to your **Function App** (`hazardwatch-func`).
2. On the left menu, click **Environment variables** (under Settings).
3. Under the **App settings** tab, click **+ Add** for each of these:

| Name | Value |
|---|---|
| `ADLS_CONNECTION_STRING` | `@Microsoft.KeyVault(VaultName=hazardwatch-kv;SecretName=adls-connection-string)` |
| `FIRMS_MAP_KEY` | `@Microsoft.KeyVault(VaultName=hazardwatch-kv;SecretName=firms-map-key)` |
| `OPENAQ_API_KEY` | `@Microsoft.KeyVault(VaultName=hazardwatch-kv;SecretName=openaq-api-key)` |
| `ADLS_ACCOUNT_NAME` | `hazardwatchraw` *(Match your actual storage name)* |
| `ADLS_CONTAINER_NAME` | `raw` |
| `FIRMS_BBOX` | `68,6,98,37` *(India bounding box — change if you want global)* |

4. After adding all of them, click **Apply** at the bottom, then **Confirm**.

---

## Step 6 — Deploy the Code via WSL Terminal

The most reliable way to push your code to Azure is via the WSL terminal (bypassing potential VS Code UI bugs). Because newer versions of Ubuntu often lack official Microsoft package support on day one, these instructions include the standard bypasses.

**1. Install Azure Tools in WSL**
Run this in your WSL terminal to fix security keys and force the installation from the stable `jammy` repo:

```bash
# Fix missing Microsoft GPG key
sudo gpg --keyserver keyserver.ubuntu.com --recv-keys EE4D7792F748182B
sudo gpg --export EE4D7792F748182B | sudo tee /etc/apt/trusted.gpg.d/microsoft-azure.gpg > /dev/null

# Force the stable 'jammy' repository (bypasses missing packages on newer Ubuntus)
sudo sh -c 'echo "deb [arch=amd64] https://packages.microsoft.com/repos/microsoft-ubuntu-jammy-prod jammy main" > /etc/apt/sources.list.d/dotnetdev.list'

# Install the Core Tools
sudo apt-get update
sudo apt-get install azure-functions-core-tools-4 -y
```

**2. Publish the Code**
Navigate to the code folder, log into Azure, bypass a known .NET Linux bug, and publish!

```bash
cd /mnt/e/Snowflake/HazardWatch/Layer_0_Polling/hazardwatch_func

# Log into Azure via your browser
az login --use-device-code

# Bypass .NET libicu crash on Linux
export DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1

# Push the code!
func azure functionapp publish hazardwatch-func --python
```

---

## Verify It Works

### Check 1 — See if the Functions are running

1. Go to your Function App in the Azure Portal.
2. Click **Overview**. You should see the 7 functions listed (`poll_usgs`, `poll_gdacs`, etc.) at the bottom.
3. Click on **poll_usgs**, then click **Monitor** on the left menu.
4. You should see a list of Successful runs happening every 5 minutes.

### Check 2 — See if Files are landing in ADLS

1. Go to your Storage Account (`hazardwatchraw`).
2. Click **Storage browser** on the left menu.
3. Click **Blob containers**, then **raw**.
4. You should see folders starting to appear (e.g., `source=usgs`).
5. Click into `source=usgs` -> `date=...` -> you will see your `.parquet` files!

---

## Troubleshooting

| Problem | Likely Cause | Fix |
|---|---|---|
| Function shows "KeyVault reference not resolved" (Red X next to setting) | Managed identity not granted Key Vault access | Re-check Step 4 — IAM role assignment. Ensure it is "Key Vault Secrets User". |
| Storage container empty | Connection string wrong | Copy the Connection String again from Storage -> Access Keys and update the Key Vault secret. |
| USGS returns 0 results | No events recently | This is normal — wait 5-10 mins and check again. |
| FIRMS returns Invalid API key | MAP_KEY not active yet | New FIRMS keys can take 1–2 hours to activate. |
| Deployment fails in VS Code | Missing Python | Ensure you have selected a Python interpreter in VS Code before deploying. |

---

## Handoff to Next Layer

When this layer is working correctly, the following will be true:
- ✅ `raw/source=usgs/date=<today>/` has Parquet files inside it.
- ✅ `raw/source=gdacs/date=<today>/` has Parquet files inside it.

**Next:** We move to Layer 1, where we will configure the Iceberg metadata so Snowflake can read these Parquet files automatically.
