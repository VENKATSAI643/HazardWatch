# Layer 5: Silver (Unified & Translated Data)

## What is this layer?
The Silver layer takes all the different tables from the Bronze layer and mashes them together into one single, giant table called `silver_hazard_event`. 

## Why is it used? (Layman's Terminology)
Imagine every delivery company (USGS, GDACS, FIRMS) uses a different language to describe how urgent a package is. 
- USGS uses "Magnitude" (e.g., 6.5).
- GDACS uses "Colors" (e.g., Red, Orange).
- FIRMS (wildfires) uses "Fire Radiative Power" (e.g., 150 MW).

If an analyst wants to find "all severe events happening today," they would have to learn 5 different grading systems and search 5 different tables. 

**The Silver Layer is the translator.**
It reads all 5 Bronze tables, translates everyone's unique grading system into a standardized `severity_level` (Low, Medium, High, Critical) and a standardized `severity_score` (0 to 100). It also assigns a standard "Hazard Type" (earthquake, flood, wildfire, etc.).

Now, analysts only need to look at **one single table** to see every hazard in the world, cleanly categorized.

## What do these scripts do?
The dbt script for this layer does three main things:
1. **Translate & Normalize:** It maps API-specific jargon to standard English.
2. **Merge (Union):** It stacks the 5 translated tables on top of each other into one giant dataset.
3. **Spatial Indexing:** It takes the latitude and longitude and translates them into an "H3 Hexagon" (a clever way of chopping the Earth up into standard-sized honeycombs), which makes it incredibly fast to search for nearby populations later.
