# 🚕 I Analyzed 25M NYC Taxi Trips to Audit NYC's New Congestion Toll — Here's What I Found

*How $15.6M in "ghost trips" reveals cracks in NYC's congestion pricing system*

---

On January 5, 2025, New York City made history by implementing America's first congestion pricing zone. The $9 toll for entering Manhattan south of 60th Street was supposed to reduce traffic and raise billions for transit improvements.

But is the system actually working? And where is money falling through the cracks?

I built a data pipeline using Python, DuckDB, and the NYC TLC trip record data to find out. Here's what 25 million taxi trips revealed.

## The Big Numbers

**$20.2 Million** — Total congestion surcharges collected in Q1 2025

**77.14%** — Compliance rate (meaning 23% of zone-entering trips had NO surcharge recorded)

**587,000+** — Fraudulent "ghost trips" detected

**$15.6 Million** — Suspicious fares from ghost trips alone

## The Ghost Trip Problem 👻

Using DuckDB, I flagged three types of impossible trips:

1. **Teleporters** — 3-minute trips with $150 fares
2. **Impossible Physics** — Average speeds exceeding 65 MPH in Manhattan traffic
3. **Stationary Rides** — Zero distance traveled, but money charged

The results were alarming: **Vendor #2** alone accounted for 60% of all ghost trips, with $10.6 million in suspicious fares.

## The Border Effect 📍

Are passengers getting dropped off just OUTSIDE the zone to avoid the toll?

The data suggests yes. Drop-offs increased significantly in Upper Manhattan zones immediately north of 60th Street — classic toll avoidance behavior.

## Does Rain Affect Taxi Demand? 🌧️

I merged weather data from the Open-Meteo API with daily trip counts. The correlation was nearly zero (0.05), proving New Yorkers will take taxis regardless of weather.

**May 2025** was the wettest month, yet trip counts barely budged.

## The Tech Stack

- **DuckDB** — Blazing fast SQL on parquet files (~500MB processed in seconds)
- **Streamlit** — Interactive 4-tab dashboard
- **Folium** — Geospatial mapping of the "border effect"
- **Plotly** — Velocity heatmaps and correlation plots
- **Beautiful Soup** — Automated web scraping of TLC data portal

## Key Recommendations

1. Investigate Vendors #1 and #2 for systematic fraud
2. Target locations 77, 67, and 259 with 100% missing surcharges
3. Implement real-time anomaly detection for impossible trips
4. Continue monitoring border zone drop-off migration

---

The full code is modular, reproducible, and processed ~25 million trips without loading everything into memory.

**Data science isn't just about building models — it's about asking the right questions.**

*What would YOU audit next?*

---

*Tags: #DataScience #Python #NYC #CongestionPricing #DuckDB #DataEngineering*
