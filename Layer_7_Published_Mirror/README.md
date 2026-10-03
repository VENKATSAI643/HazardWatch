# Layer 7: Published & Mirror (Open Data)

## What is this layer?
This layer is the distribution center. It takes the polished Gold data we just built in Snowflake and exports it back out into cloud file folders so that the public and external systems can download it.

## Why is it used? (Layman's Terminology)
Snowflake is incredibly powerful, but it charges us money every time someone runs a query. If we gave 1,000 university researchers direct access to our Snowflake database to study the hazards, our budget would run out in a day!

Instead, we use a concept called **Data Publishing**. 
After we finish building the Gold tables, we export them as standard, highly-compressed files (called Parquet files) back to Azure. 

Then, we run a small script to copy those exact files over to **Cloudflare R2**. 
Cloudflare R2 is a special storage system that does not charge any "Egress Fees" (the fee a cloud provider usually charges when someone downloads a file over the internet). This means researchers, hobbyists, and the public can download our hazard data thousands of times for free, without costing us a dime.

## What do these scripts do?
1. **`07_published_stage_and_unload.sql`**: This tells Snowflake to take the `gold_event_exposure` and `gold_daily_summary` tables and "Unload" them as files onto our Azure storage drive.
2. **`rclone_mirror_job.sh`**: This is a small computer program that runs automatically, connects to our Azure drive, connects to our free Cloudflare R2 drive, and copies the newest files over so the public can access them.
