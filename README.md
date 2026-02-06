# NYC Congestion Pricing Audit 2025

Analysis of the Manhattan Congestion Relief Zone toll impact on NYC taxi industry.

## Setup

```bash
pip install -r requirements.txt
```

## Run Pipeline

```bash
python pipeline.py
```

## Run Dashboard

```bash
streamlit run dashboard/app.py
```

## Structure

```
DS_ASS_01/
├── pipeline.py           # Main ETL and analysis
├── config/
│   ├── settings.py       # Configuration
│   └── zones.py          # Zone mappings
├── src/
│   ├── scraper.py        # Web scraping
│   ├── schema.py         # Schema unification
│   ├── ghost_filter.py   # Ghost trip detection
│   ├── zone_analysis.py  # Zone analytics
│   ├── weather.py        # Weather API
│   └── aggregations.py   # DuckDB aggregations
├── dashboard/
│   └── app.py            # Streamlit dashboard
├── data/
│   ├── raw/              # Downloaded data
│   ├── processed/        # Aggregated data
│   └── audit_log/        # Ghost trips
└── outputs/              # Reports
```

## Technical Stack

- **DuckDB**: Big data processing
- **Streamlit**: Interactive dashboard
- **Folium**: Geospatial mapping
- **Plotly**: Visualizations
- **Open-Meteo API**: Weather data
