import duckdb
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.zones import CONGESTION_ZONE_IDS, BORDER_ZONE_IDS
from config.settings import CONGESTION_START_DATE

def create_zone_tables(con):
    zone_ids_str = ','.join(map(str, CONGESTION_ZONE_IDS))
    border_ids_str = ','.join(map(str, BORDER_ZONE_IDS))
    
    con.execute(f"""
        CREATE OR REPLACE TABLE congestion_zones AS 
        SELECT unnest([{zone_ids_str}]) as zone_id
    """)
    
    con.execute(f"""
        CREATE OR REPLACE TABLE border_zones AS 
        SELECT unnest([{border_ids_str}]) as zone_id
    """)
    
    return con

def is_zone_entry_trip(pickup_loc, dropoff_loc):
    return pickup_loc not in CONGESTION_ZONE_IDS and dropoff_loc in CONGESTION_ZONE_IDS

def calculate_surcharge_compliance(con):
    result = con.execute(f"""
        SELECT 
            COUNT(*) as total_zone_entry_trips,
            SUM(CASE WHEN congestion_surcharge > 0 THEN 1 ELSE 0 END) as trips_with_surcharge,
            SUM(CASE WHEN congestion_surcharge = 0 OR congestion_surcharge IS NULL THEN 1 ELSE 0 END) as trips_without_surcharge,
            SUM(CASE WHEN congestion_surcharge > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as compliance_rate
        FROM clean_trips
        WHERE pickup_time >= '{CONGESTION_START_DATE}'
        AND pickup_loc NOT IN (SELECT zone_id FROM congestion_zones)
        AND dropoff_loc IN (SELECT zone_id FROM congestion_zones)
    """).fetchdf()
    return result

def get_missing_surcharge_locations(con, top_n=3):
    result = con.execute(f"""
        SELECT 
            pickup_loc,
            COUNT(*) as total_entries,
            SUM(CASE WHEN congestion_surcharge = 0 OR congestion_surcharge IS NULL THEN 1 ELSE 0 END) as missing_surcharge,
            SUM(CASE WHEN congestion_surcharge = 0 OR congestion_surcharge IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as missing_rate
        FROM clean_trips
        WHERE pickup_time >= '{CONGESTION_START_DATE}'
        AND pickup_loc NOT IN (SELECT zone_id FROM congestion_zones)
        AND dropoff_loc IN (SELECT zone_id FROM congestion_zones)
        GROUP BY pickup_loc
        HAVING COUNT(*) >= 100
        ORDER BY missing_rate DESC
        LIMIT {top_n}
    """).fetchdf()
    return result

def compare_quarterly_volumes(con):
    result = con.execute("""
        SELECT 
            taxi_type,
            CASE 
                WHEN pickup_time >= '2024-01-01' AND pickup_time < '2024-04-01' THEN 'Q1_2024'
                WHEN pickup_time >= '2025-01-01' AND pickup_time < '2025-04-01' THEN 'Q1_2025'
            END as quarter,
            COUNT(*) as trip_count
        FROM clean_trips
        WHERE dropoff_loc IN (SELECT zone_id FROM congestion_zones)
        AND (
            (pickup_time >= '2024-01-01' AND pickup_time < '2024-04-01')
            OR (pickup_time >= '2025-01-01' AND pickup_time < '2025-04-01')
        )
        GROUP BY taxi_type, quarter
        ORDER BY taxi_type, quarter
    """).fetchdf()
    return result

def get_zone_trip_summary(con):
    result = con.execute("""
        SELECT 
            CASE 
                WHEN pickup_loc IN (SELECT zone_id FROM congestion_zones) 
                    AND dropoff_loc IN (SELECT zone_id FROM congestion_zones) THEN 'within_zone'
                WHEN pickup_loc NOT IN (SELECT zone_id FROM congestion_zones) 
                    AND dropoff_loc IN (SELECT zone_id FROM congestion_zones) THEN 'entering_zone'
                WHEN pickup_loc IN (SELECT zone_id FROM congestion_zones) 
                    AND dropoff_loc NOT IN (SELECT zone_id FROM congestion_zones) THEN 'exiting_zone'
                ELSE 'outside_zone'
            END as trip_type,
            COUNT(*) as trip_count,
            AVG(total_amount) as avg_total,
            AVG(congestion_surcharge) as avg_surcharge
        FROM clean_trips
        WHERE pickup_time >= '{CONGESTION_START_DATE}'
        GROUP BY trip_type
    """).fetchdf()
    return result

def calculate_total_surcharge_revenue(con, year=2025):
    result = con.execute(f"""
        SELECT 
            SUM(congestion_surcharge) as total_surcharge_revenue,
            COUNT(*) as total_trips_with_surcharge,
            AVG(congestion_surcharge) as avg_surcharge
        FROM clean_trips
        WHERE YEAR(pickup_time) = {year}
        AND congestion_surcharge > 0
    """).fetchdf()
    return result

if __name__ == "__main__":
    print("Zone analysis module loaded successfully")
    print(f"Congestion Zone IDs: {len(CONGESTION_ZONE_IDS)} zones")
    print(f"Border Zone IDs: {len(BORDER_ZONE_IDS)} zones")
