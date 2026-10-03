Snowflake is a good choice, and it makes the design more standard. This diagram replaces the DuckDB version.

## What changes with Snowflake

- **Event Grid and Snowpipe come back.** They load raw files into Snowflake landing tables. Snowpipe charges per file, so batch your poller output rather than writing many tiny files every 5 minutes. A scheduled `COPY INTO` task is a cheaper alternative.
- **dbt runs against Snowflake.** The Container Apps job only submits SQL, so it needs very little compute. The warehouse does the work, so use X-Small with a short auto-suspend.
- **Superset connects to Snowflake directly.** It no longer needs DuckDB. Every dashboard query wakes the warehouse, so enable result caching and keep dashboards light.
- **Gold has to be exported to ADLS.** dbt builds tables inside Snowflake, so the published Parquet comes from an unload step (`COPY INTO` an external stage), which runs after each dbt build. That's what feeds the R2 mirror.

## Iceberg with Snowflake

This is where Iceberg fits more naturally. Snowflake can create Iceberg tables directly on an Azure external volume, so the gold tables could be Iceberg without a separate conversion job. The catch for the mirror is unchanged: Iceberg metadata contains absolute paths, so mirror Parquet only, or accept that the Iceberg tables live on Azure.

## Cost and risk to plan for

- **The trial lasts 30 days.** After that, Snowflake charges credits. Check current trial terms, since they can change. For a long-running public project, this is the main budget risk.
- **Choose the region carefully.** Pick Azure and the same region as your storage account when you create the Snowflake account. It can't be changed later, and it avoids cross-region transfer costs.
- **Set resource monitors** with a credit limit and suspend action from day one.
- **Cut cost with schedule choices.** Run dbt every few hours instead of hourly, and use incremental models for silver and gold.

If you want, I can write the Snowflake setup next: the storage integration, external stage, Snowpipe definitions and resource monitor.