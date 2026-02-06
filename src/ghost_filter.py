"""
Ghost Trip Filter Module
=========================
Detects and filters fraudulent/impossible taxi trips for audit purposes.

This module implements three detection rules:
1. Impossible Physics: Average speed > 65 MPH (impossible in NYC traffic)
2. Teleporter: Trip time < 1 minute but fare > $20 (GPS/meter fraud)
3. Stationary Ride: Distance = 0 but fare > 0 (meter running without travel)

Ghost trips are separated into an audit log for investigation.
Clean trips are kept for analysis.
"""

import duckdb
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.settings import (
    GHOST_TRIP_MAX_SPEED_MPH, 
    GHOST_TRIP_MIN_TIME_MINUTES, 
    GHOST_TRIP_TELEPORTER_FARE, 
    AUDIT_DIR
)


def get_ghost_trip_filter_query():
    """
    Returns SQL query for calculating trip speed and detecting ghost trips.
    
    Detection Rules:
        - impossible_physics: avg_speed_mph > 65 (config value)
        - teleporter: trip_time < 1 min AND fare > $20
        - stationary_ride: distance = 0 AND fare > 0
        
    Returns:
        str: SQL query with ghost_type column
    """
    return f"""
    WITH trip_with_speed AS (
        SELECT 
            *,
            -- Calculate trip duration in minutes
            EXTRACT(EPOCH FROM (dropoff_time - pickup_time)) / 60.0 as trip_duration_min,
            -- Calculate average speed (distance / hours)
            CASE 
                WHEN EXTRACT(EPOCH FROM (dropoff_time - pickup_time)) > 0 
                THEN trip_distance / (EXTRACT(EPOCH FROM (dropoff_time - pickup_time)) / 3600.0)
                ELSE 0 
            END as avg_speed_mph
        FROM unified_trips
    )
    SELECT 
        *,
        -- Apply ghost trip detection rules
        CASE 
            WHEN avg_speed_mph > {GHOST_TRIP_MAX_SPEED_MPH} THEN 'impossible_physics'
            WHEN trip_duration_min < {GHOST_TRIP_MIN_TIME_MINUTES} AND fare > {GHOST_TRIP_TELEPORTER_FARE} THEN 'teleporter'
            WHEN trip_distance = 0 AND fare > 0 THEN 'stationary_ride'
            ELSE NULL
        END as ghost_type
    FROM trip_with_speed
    """


def filter_ghost_trips(con):
    """
    Apply ghost trip detection and create clean/ghost views.
    
    Creates three DuckDB views:
        - trips_with_ghost_flag: All trips with ghost_type column
        - clean_trips: Trips where ghost_type IS NULL (valid trips)
        - ghost_trips: Trips where ghost_type IS NOT NULL (fraudulent)
        
    Args:
        con: DuckDB connection with unified_trips view
        
    Returns:
        con: DuckDB connection with new views created
    """
    # Build query with speed calculation and ghost detection
    query = f"""
    WITH trip_with_speed AS (
        SELECT 
            *,
            EXTRACT(EPOCH FROM (dropoff_time - pickup_time)) / 60.0 as trip_duration_min,
            CASE 
                WHEN EXTRACT(EPOCH FROM (dropoff_time - pickup_time)) > 0 
                THEN trip_distance / (EXTRACT(EPOCH FROM (dropoff_time - pickup_time)) / 3600.0)
                ELSE 0 
            END as avg_speed_mph
        FROM unified_trips
    )
    SELECT *,
        CASE 
            WHEN avg_speed_mph > {GHOST_TRIP_MAX_SPEED_MPH} THEN 'impossible_physics'
            WHEN trip_duration_min < {GHOST_TRIP_MIN_TIME_MINUTES} AND fare > {GHOST_TRIP_TELEPORTER_FARE} THEN 'teleporter'
            WHEN trip_distance = 0 AND fare > 0 THEN 'stationary_ride'
            ELSE NULL
        END as ghost_type
    FROM trip_with_speed
    """
    
    # Create view with all trips and ghost type flag
    con.execute(f"CREATE OR REPLACE VIEW trips_with_ghost_flag AS {query}")
    
    # Create view for clean trips (no fraud detected)
    con.execute("""
        CREATE OR REPLACE VIEW clean_trips AS 
        SELECT * FROM trips_with_ghost_flag WHERE ghost_type IS NULL
    """)
    
    # Create view for ghost trips (fraud detected - for audit)
    con.execute("""
        CREATE OR REPLACE VIEW ghost_trips AS 
        SELECT * FROM trips_with_ghost_flag WHERE ghost_type IS NOT NULL
    """)
    
    return con


def get_ghost_trip_summary(con):
    """
    Get summary statistics grouped by ghost trip type.
    
    Returns DataFrame with:
        - ghost_type: Type of fraud detected
        - count: Number of trips
        - avg_fare: Average fare amount
        - avg_distance: Average trip distance
        
    Args:
        con: DuckDB connection with ghost_trips view
        
    Returns:
        pd.DataFrame: Summary by ghost type
    """
    result = con.execute("""
        SELECT 
            ghost_type,
            COUNT(*) as count,
            AVG(fare) as avg_fare,
            AVG(trip_distance) as avg_distance
        FROM ghost_trips
        GROUP BY ghost_type
    """).fetchdf()
    return result


def get_suspicious_vendors(con, top_n=5):
    """
    Identify vendors with highest ghost trip rates.
    
    Used to flag vendors for audit based on suspicious activity.
    
    Args:
        con: DuckDB connection with ghost_trips view
        top_n: Number of top vendors to return (default: 5)
        
    Returns:
        pd.DataFrame: Top vendors by ghost trip count with:
            - vendor_id: Vendor identifier
            - ghost_trip_count: Number of ghost trips
            - total_suspicious_fare: Sum of fares from ghost trips
            - pct_of_all_ghost_trips: Percentage of total ghost trips
    """
    result = con.execute(f"""
        SELECT 
            vendor_id,
            COUNT(*) as ghost_trip_count,
            SUM(fare) as total_suspicious_fare,
            COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as pct_of_all_ghost_trips
        FROM ghost_trips
        GROUP BY vendor_id
        ORDER BY ghost_trip_count DESC
        LIMIT {top_n}
    """).fetchdf()
    return result


def save_ghost_trips_audit(con, output_path=None):
    """
    Export ghost trips to parquet file for audit trail.
    
    Args:
        con: DuckDB connection with ghost_trips view
        output_path: Optional custom output path (default: AUDIT_DIR/ghost_trips.parquet)
        
    Returns:
        str: Path to saved audit file
    """
    if output_path is None:
        output_path = AUDIT_DIR / "ghost_trips.parquet"
    
    # Export to parquet using COPY command (efficient for large data)
    con.execute(f"""
        COPY (SELECT * FROM ghost_trips) 
        TO '{str(output_path).replace(chr(92), '/')}' (FORMAT PARQUET)
    """)
    return str(output_path)


def get_ghost_trip_stats(con):
    """
    Get overall ghost trip statistics.
    
    Args:
        con: DuckDB connection with trip views
        
    Returns:
        dict: Statistics including total, ghost, clean counts and ghost rate
    """
    total_trips = con.execute("SELECT COUNT(*) FROM trips_with_ghost_flag").fetchone()[0]
    ghost_trips = con.execute("SELECT COUNT(*) FROM ghost_trips").fetchone()[0]
    clean_trips = con.execute("SELECT COUNT(*) FROM clean_trips").fetchone()[0]
    
    return {
        'total_trips': total_trips,
        'ghost_trips': ghost_trips,
        'clean_trips': clean_trips,
        'ghost_rate': ghost_trips / total_trips * 100 if total_trips > 0 else 0
    }


if __name__ == "__main__":
    print("Ghost filter module loaded successfully")
