# Layer 6: Gold (Business Intelligence & Dashboards)

## What is this layer?
The Gold layer is the absolute top of the pyramid. It is the highly-polished dataset designed specifically for dashboards, charts, and maps. 

## Why is it used? (Layman's Terminology)
In the Silver layer, we translated everything into one big "Hazard Event" list. 
But if a dashboard asks, "How many people are currently trapped in the path of the wildfire?", the Silver layer doesn't know. It only knows where the fire is, not who lives there.

The Gold layer solves this by doing the heavy lifting of **Joining and Aggregating**.
It takes our unified hazards and matches them up against a massive global population map (WorldPop) to answer business questions instantly. 

We build it as a "Table" (meaning it recalculates on a schedule) so that when a human opens the Superset Dashboard, it loads instantly rather than forcing them to wait 30 seconds for the database to calculate population sizes on the fly.

## What do these scripts do?
- **`gold_event_exposure`**: This takes the high-severity events from Silver, draws an imaginary circle around them (using their radius), and counts up all the humans living inside that circle using the WorldPop grid.
- **`gold_daily_summary`**: This script creates a simple tally table. "On October 3rd, there were 4 earthquakes, 2 wildfires, and 12,000 people exposed." This makes bar charts incredibly fast.
- **`gold_active_alerts`**: This is a simple cut of the data specifically tuned for the World Map visualization, showing only active events from the last 7 days sorted by severity.
