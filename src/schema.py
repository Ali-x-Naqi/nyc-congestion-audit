import duckdb
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.settings import UNIFIED_SCHEMA

YELLOW_COLUMN_MAP = {
    'tpep_pickup_datetime': 'pickup_time',
    'tpep_dropoff_datetime': 'dropoff_time',
    'PULocationID': 'pickup_loc',
    'DOLocationID': 'dropoff_loc',
    'trip_distance': 'trip_distance',
    'fare_amount': 'fare',
    'total_amount': 'total_amount',
    'congestion_surcharge': 'congestion_surcharge',
    'tip_amount': 'tip_amount',
    'VendorID': 'vendor_id'
}

GREEN_COLUMN_MAP = {
    'lpep_pickup_datetime': 'pickup_time',
    'lpep_dropoff_datetime': 'dropoff_time',
    'PULocationID': 'pickup_loc',
    'DOLocationID': 'dropoff_loc',
    'trip_distance': 'trip_distance',
    'fare_amount': 'fare',
    'total_amount': 'total_amount',
    'congestion_surcharge': 'congestion_surcharge',
    'tip_amount': 'tip_amount',
    'VendorID': 'vendor_id'
}

def get_unified_query(parquet_path, taxi_type):
    if taxi_type == 'yellow':
        col_map = YELLOW_COLUMN_MAP
        pickup_col = 'tpep_pickup_datetime'
        dropoff_col = 'tpep_dropoff_datetime'
    else:
        col_map = GREEN_COLUMN_MAP
        pickup_col = 'lpep_pickup_datetime'
        dropoff_col = 'lpep_dropoff_datetime'
    
    query = f"""
    SELECT 
        {pickup_col} as pickup_time,
        {dropoff_col} as dropoff_time,
        CAST(PULocationID AS INTEGER) as pickup_loc,
        CAST(DOLocationID AS INTEGER) as dropoff_loc,
        CAST(trip_distance AS DOUBLE) as trip_distance,
        CAST(fare_amount AS DOUBLE) as fare,
        CAST(total_amount AS DOUBLE) as total_amount,
        CAST(COALESCE(congestion_surcharge, 0) AS DOUBLE) as congestion_surcharge,
        CAST(COALESCE(tip_amount, 0) AS DOUBLE) as tip_amount,
        '{taxi_type}' as taxi_type,
        CAST(VendorID AS INTEGER) as vendor_id
    FROM read_parquet('{parquet_path}')
    """
    return query

def create_unified_view(con, parquet_files):
    queries = []
    for pf in parquet_files:
        path = Path(pf)
        taxi_type = 'yellow' if 'yellow' in path.name else 'green'
        queries.append(get_unified_query(str(path).replace('\\', '/'), taxi_type))
    
    combined_query = " UNION ALL ".join(queries)
    con.execute(f"CREATE OR REPLACE VIEW unified_trips AS {combined_query}")
    return con

def get_schema_info(parquet_path):
    con = duckdb.connect()
    result = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}')").fetchall()
    con.close()
    return result

def validate_parquet_schema(parquet_path, taxi_type):
    expected_pickup = 'tpep_pickup_datetime' if taxi_type == 'yellow' else 'lpep_pickup_datetime'
    schema = get_schema_info(parquet_path)
    columns = [row[0] for row in schema]
    return expected_pickup in columns

if __name__ == "__main__":
    test_path = "data/raw/yellow_tripdata_2025-01.parquet"
    if Path(test_path).exists():
        print(get_schema_info(test_path))
