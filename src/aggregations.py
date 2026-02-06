import duckdb
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.zones import CONGESTION_ZONE_IDS, BORDER_ZONE_IDS

def aggregate_hourly_speeds(con, year, quarter):
    start_month = (quarter - 1) * 3 + 1
    end_month = quarter * 3
    
    zone_ids_str = ','.join(map(str, CONGESTION_ZONE_IDS))
    
    result = con.execute(f"""
        SELECT 
            EXTRACT(HOUR FROM pickup_time) as hour,
            EXTRACT(DOW FROM pickup_time) as day_of_week,
            AVG(
                CASE 
                    WHEN EXTRACT(EPOCH FROM (dropoff_time - pickup_time)) > 60 
                    THEN trip_distance / (EXTRACT(EPOCH FROM (dropoff_time - pickup_time)) / 3600.0)
                    ELSE NULL 
                END
            ) as avg_speed_mph,
            COUNT(*) as trip_count
        FROM clean_trips
        WHERE YEAR(pickup_time) = {year}
        AND MONTH(pickup_time) >= {start_month}
        AND MONTH(pickup_time) <= {end_month}
        AND pickup_loc IN ({zone_ids_str})
        AND dropoff_loc IN ({zone_ids_str})
        GROUP BY hour, day_of_week
        ORDER BY day_of_week, hour
    """).fetchdf()
    
    return result

def aggregate_daily_trips(con, year=2025):
    result = con.execute(f"""
        SELECT 
            DATE_TRUNC('day', pickup_time) as date,
            COUNT(*) as trip_count,
            SUM(total_amount) as total_revenue,
            AVG(congestion_surcharge) as avg_surcharge
        FROM clean_trips
        WHERE YEAR(pickup_time) = {year}
        GROUP BY DATE_TRUNC('day', pickup_time)
        ORDER BY date
    """).fetchdf()
    
    result['date'] = result['date'].dt.date
    return result

def aggregate_monthly_tips_surcharges(con, year=2025):
    result = con.execute(f"""
        SELECT 
            MONTH(pickup_time) as month,
            AVG(congestion_surcharge) as avg_surcharge,
            AVG(CASE WHEN fare > 0 THEN tip_amount / fare * 100 ELSE 0 END) as avg_tip_pct,
            SUM(congestion_surcharge) as total_surcharge,
            SUM(tip_amount) as total_tips,
            COUNT(*) as trip_count
        FROM clean_trips
        WHERE YEAR(pickup_time) = {year}
        GROUP BY MONTH(pickup_time)
        ORDER BY month
    """).fetchdf()
    
    return result

def aggregate_zone_dropoffs(con):
    border_ids_str = ','.join(map(str, BORDER_ZONE_IDS))
    
    result = con.execute(f"""
        SELECT 
            dropoff_loc,
            YEAR(pickup_time) as year,
            COUNT(*) as dropoff_count
        FROM clean_trips
        WHERE dropoff_loc IN ({border_ids_str})
        AND YEAR(pickup_time) IN (2024, 2025)
        GROUP BY dropoff_loc, YEAR(pickup_time)
        ORDER BY dropoff_loc, year
    """).fetchdf()
    
    return result

def calculate_border_effect(con):
    border_ids_str = ','.join(map(str, BORDER_ZONE_IDS))
    
    result = con.execute(f"""
        WITH zone_year_counts AS (
            SELECT 
                dropoff_loc,
                YEAR(pickup_time) as year,
                COUNT(*) as dropoff_count
            FROM clean_trips
            WHERE dropoff_loc IN ({border_ids_str})
            AND YEAR(pickup_time) IN (2024, 2025)
            GROUP BY dropoff_loc, YEAR(pickup_time)
        ),
        pivoted AS (
            SELECT 
                dropoff_loc,
                MAX(CASE WHEN year = 2024 THEN dropoff_count ELSE 0 END) as count_2024,
                MAX(CASE WHEN year = 2025 THEN dropoff_count ELSE 0 END) as count_2025
            FROM zone_year_counts
            GROUP BY dropoff_loc
        )
        SELECT 
            dropoff_loc,
            count_2024,
            count_2025,
            CASE 
                WHEN count_2024 > 0 
                THEN (count_2025 - count_2024) * 100.0 / count_2024 
                ELSE 0 
            END as pct_change
        FROM pivoted
        ORDER BY pct_change DESC
    """).fetchdf()
    
    return result

def aggregate_for_imputation(con, taxi_type, month, years=[2023, 2024]):
    result = con.execute(f"""
        SELECT 
            EXTRACT(HOUR FROM pickup_time) as hour,
            EXTRACT(DOW FROM pickup_time) as day_of_week,
            COUNT(*) as trip_count,
            AVG(fare) as avg_fare,
            AVG(total_amount) as avg_total,
            AVG(tip_amount) as avg_tip
        FROM unified_trips
        WHERE taxi_type = '{taxi_type}'
        AND MONTH(pickup_time) = {month}
        AND YEAR(pickup_time) IN ({','.join(map(str, years))})
        GROUP BY YEAR(pickup_time), hour, day_of_week
    """).fetchdf()
    
    return result

if __name__ == "__main__":
    print("Aggregations module loaded successfully")
