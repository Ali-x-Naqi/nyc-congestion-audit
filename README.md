# NYC Congestion Pricing Audit 🚖

**Assignment 01 - Section SE-6B - Roll 23F-3052**

A data science pipeline and dashboard analyzing the impact of NYC's 2025 Congestion Pricing implementation on the taxi industry, traffic flow, and revenue.

## 📊 Features
- **Big Data Processing**: Handles 100M+ rows efficiently using DuckDB
- **Automated Ingestion**: Web scraper for TLC parquet files (PowerShell-based)
- **Ghost Trip Detection**: Filters fraudulent trips (impossible speed, teleporters)
- **Zone Analysis**: Geospatial analysis of congestion zone compliance
- **Weather Impact**: Correlates demand with precipitation data
- **Interactive Dashboard**: 4-tab Streamlit app for visual audit

## 🚀 Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Run Pipeline (ETL)
Downloads data, processes it, and generates aggregations:
```bash
python pipeline.py
```

### 3. Launch Dashboard
Opens the interactive report in your browser:
```bash
streamlit run dashboard/app.py
```

## 📁 Project Structure
```
DS_ASS_01/
├── pipeline.py          # Main ETL orchestrator
├── dashboard/           # Streamlit app code
├── src/                 # Modular logic (scraper, ghost filter, etc.)
├── config/              # Configuration (zones, settings)
├── data/                # Data directories (raw, processed, audit_log)
└── outputs/             # Generated reports (PDF/Markdown, JSON)
```

## 📋 Requirements Fulfilled
- [x] Automated Web Scraping
- [x] Modular Pipeline (.py scripts)
- [x] Big Data Stack (DuckDB)
- [x] Ghost Trip Detection
- [x] December 2025 Imputation
- [x] Rain Elasticity Analysis
- [x] Streamlit Dashboard
