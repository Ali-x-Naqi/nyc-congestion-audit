🚕 Just completed a deep-dive audit of NYC's new congestion pricing system using 25M+ taxi trip records.

Key findings that surprised me:

📊 $20.2M in surcharges collected (Q1 2025)
⚠️ 23% of zone-entry trips had NO surcharge recorded
👻 587K "ghost trips" detected = $15.6M suspicious
🌧️ Rain has almost zero effect on taxi demand

The technical approach:
• DuckDB for big data processing (no pandas memory issues!)
• Automated web scraping from NYC TLC portal
• Streamlit dashboard with 4 interactive tabs
• Weather API integration for "Rain Tax" analysis

Most interesting finding? Vendor #2 accounts for 60% of all suspicious trips. That's $10.6M worth of potential fraud from ONE vendor.

Data science isn't just about predictions — sometimes the most valuable insights come from auditing the systems we trust.

Full pipeline is modular, reproducible, and handles missing 2025 data with weighted imputation.

What would you audit next? 👇

#DataScience #Python #NYC #CongestionPricing #DataEngineering #DuckDB #Analytics
