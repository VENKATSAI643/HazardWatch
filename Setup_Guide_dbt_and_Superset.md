# Setup Guide: dbt & Apache Superset

## 1. How is dbt set up to run with Snowflake?

### What is the use of this? (Layman's Terms)
For dbt to do its magic and clean your data, it needs two things:
1. **The Keys:** It needs to know your Snowflake username, password, and which warehouse to use.
2. **An Alarm Clock:** We don't want a human to manually click "run" every 4 hours. We need an automated schedule.

### How it actually works:

**A. The `profiles.yml` file (The Keys)**
On the machine running dbt, there is a hidden file called `profiles.yml`. This file contains the secure connection details. 
*Note: We never save this file in the project folder so that passwords don't accidentally leak to GitHub.*
```yaml
# Example ~/.dbt/profiles.yml
hazardwatch_snowflake:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <your-snowflake-account>
      user: <your-username>
      password: <your-password>
      role: HAZARDWATCH_ADMIN
      database: HAZARDWATCH_DB
      warehouse: HAZARDWATCH_WH
      schema: BRONZE_SCHEMA
      threads: 4
```

**B. The Snowflake Task (The Alarm Clock)**
In our architecture, we schedule a **Snowflake Task** to run every 4 hours. 
When this task triggers, it sends an alert to Microsoft Azure. Azure then spins up a tiny, cheap server (Azure Container App) which runs the command `dbt run`. Once dbt finishes building the Bronze, Silver, and Gold tables, the Azure server shuts down to save money.

---

### How to run dbt locally (Manual Setup)

If you are setting this up for the first time on your own machine (e.g., in a WSL Python environment), follow these steps:

**1. Install dbt-snowflake**
Inside your Python virtual environment, install the dbt-snowflake adapter:
```bash
pip install dbt-snowflake
```

**2. Set Up Your profiles.yml**
Create the hidden `.dbt` folder and the profile file:
```bash
mkdir -p ~/.dbt
nano ~/.dbt/profiles.yml
```
Paste the YAML configuration from step **A** above, filling in your actual `<your-snowflake-account>`, `<your-username>`, and `<your-password>`. Save and exit.

**3. Run the dbt project**
Navigate to the dbt project folder, install dependencies, and run the models:
```bash
# Go to the dbt project directory
cd /mnt/e/Snowflake/HazardWatch/Layer_4_Bronze/hazardwatch_dbt

# Install packages (like dbt-utils)
dbt deps

# Test the connection to Snowflake
dbt debug

# Build the Bronze, Silver, and Gold tables!
dbt run

# (Optional) Run data quality tests
dbt test
```

---

## 2. How to install and run Apache Superset

### What is the use of this? (Layman's Terms)
Apache Superset is a massive piece of software. If you tried to install it directly on your computer, it would ask you to install dozens of specific versions of Python, databases, and web servers, which could easily break your computer's current setup.

To avoid this, we use a tool called **Docker**. 
Docker puts Superset into an isolated digital box (called a container). This box already has everything Superset needs to run perfectly. You just download the box, turn it on, and view the dashboard in your web browser.

### Step-by-Step Installation Guide

**Step 1: Ensure Docker is running in WSL**
- Since you already have Docker installed via WSL, simply open your preferred WSL Linux distribution terminal (e.g., Ubuntu).
- Run `docker --version` and `docker-compose --version` to verify the engine is running.

**Step 2: Download Superset**
Inside your WSL terminal, run the following commands to download the official Superset "box" to a directory inside your WSL environment (for better file performance):
```bash
git clone https://github.com/apache/superset.git
cd superset
```

**Step 3: Start Superset**
Inside that folder, tell Docker to start the application in the background (this will take 5-10 minutes the very first time as it downloads the necessary files):
```bash
docker compose -f docker-compose-non-dev.yml pull
docker compose -f docker-compose-non-dev.yml up -d
```

**Step 4: Log In**
- Since we used the `-d` (detach) flag, it will start up in the background and return control of your terminal to you!
- Wait a minute or two for the web server to finish booting up, then open your web browser.
- Go to: `http://localhost:8088`
- **Username:** `admin`
- **Password:** `admin`

**Step 5: Connect to Snowflake**
By default, the Docker version of Superset might not know how to talk to Snowflake. You need to permanently install the Snowflake "translator" driver into your Docker image.
1. In your WSL terminal (make sure you are inside the `superset` folder), add the driver to the requirements file:
```bash
echo "snowflake-sqlalchemy" >> ./docker/requirements-local.txt
```
2. Rebuild the Superset container with the driver baked in (this takes a minute):
```bash
docker compose -f docker-compose-non-dev.yml build --force-rm
```
3. Restart the environment to apply the changes:
```bash
docker compose -f docker-compose-non-dev.yml up -d
```
4. You can now follow the `08_superset_setup.md` guide to plug in your Snowflake credentials!
