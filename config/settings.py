import os
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
AUDIT_DIR = DATA_DIR / "audit_log"
OUTPUT_DIR = BASE_DIR / "outputs"

for d in [RAW_DIR, PROCESSED_DIR, AUDIT_DIR, OUTPUT_DIR]:
    d.mkdir(parents=True, exist_ok=True)

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
TLC_PAGE_URL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
TAXI_ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
TAXI_ZONE_SHAPEFILE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"

WEATHER_API_URL = "https://archive-api.open-meteo.com/v1/archive"
CENTRAL_PARK_LAT = 40.7829
CENTRAL_PARK_LON = -73.9654

YEARS_TO_PROCESS = [2023, 2024, 2025]
MONTHS = list(range(1, 13))

CONGESTION_START_DATE = "2025-01-05"

UNIFIED_SCHEMA = [
    "pickup_time",
    "dropoff_time",
    "pickup_loc",
    "dropoff_loc",
    "trip_distance",
    "fare",
    "total_amount",
    "congestion_surcharge",
    "tip_amount",
    "taxi_type",
    "vendor_id"
]

GHOST_TRIP_MAX_SPEED_MPH = 65
GHOST_TRIP_MIN_TIME_MINUTES = 1
GHOST_TRIP_TELEPORTER_FARE = 20
