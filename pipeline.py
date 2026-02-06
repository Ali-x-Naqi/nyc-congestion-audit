"""
NYC Congestion Pricing Audit Pipeline
=====================================
Main ETL and Analysis Script for analyzing the Manhattan Congestion Relief Zone
toll impact on the NYC taxi industry throughout 2025.

This modular pipeline consists of 7 steps:
1. Data Download - Automated web scraping from TLC website
2. Schema Unification - Combine Yellow and Green taxi data with unified schema
3. Ghost Trip Filter - Detect and log fraudulent/impossible trips
4. Zone Analysis - Congestion zone compliance and revenue analysis
5. Aggregations - Pre-compute data for visualizations
6. Weather Analysis - Rain elasticity of demand calculation
7. December Imputation - Handle missing data with weighted averages

Technical Stack:
- DuckDB: Big data processing (avoids loading full dataset into memory)
- Pandas: Only for visualization-ready aggregated data
- Open-Meteo API: Weather data integration

Author: Data Science Assignment 01
Date: February 2026
"""

import duckdb
import pandas as pd
from pathlib import Path
import sys
from datetime import datetime

# Add parent directory to path for module imports
sys.path.insert(0, str(Path(__file__).parent))

# Import configuration settings
from config.settings import (
    RAW_DIR, PROCESSED_DIR, AUDIT_DIR, OUTPUT_DIR,
    YEARS_TO_PROCESS, CONGESTION_START_DATE
)
from config.zones import CONGESTION_ZONE_IDS, BORDER_ZONE_IDS

# Import data ingestion modules
from src.scraper import (
    download_all_data, check_data_availability, get_missing_months,
    download_taxi_zones
)

# Import data processing modules
from src.schema import create_unified_view, get_unified_query
from src.ghost_filter import (
    filter_ghost_trips, get_ghost_trip_summary, 
    get_suspicious_vendors, save_ghost_trips_audit
)
from src.zone_analysis import (
    create_zone_tables, calculate_surcharge_compliance,
    get_missing_surcharge_locations, compare_quarterly_volumes,
    calculate_total_surcharge_revenue
)
from src.aggregations import (
    aggregate_hourly_speeds, aggregate_daily_trips,
    aggregate_monthly_tips_surcharges, calculate_border_effect
)
from src.weather import (
    fetch_precipitation_data, calculate_rain_elasticity, find_wettest_month
)


class CongestionPricingPipeline:
    """
    Main pipeline class for NYC Congestion Pricing Audit.
    
    Orchestrates the complete ETL and analysis workflow using DuckDB
    for big data processing to handle 100M+ taxi trip records efficiently.
    
    Attributes:
        con: DuckDB connection for in-memory analytical queries
        results: Dictionary storing intermediate results for report generation
    """
    
    def __init__(self):
        """Initialize DuckDB connection and results storage."""
        # DuckDB provides fast analytical queries without loading full data into memory
        self.con = duckdb.connect()
        self.results = {}
        
    def step_1_download_data(self, skip_download=False):
        """
        STEP 1: Download TLC Trip Data
        
        Automated web scraping of NYC TLC parquet files for Yellow and Green taxis.
        Uses PowerShell Invoke-WebRequest to bypass CloudFront CDN restrictions.
        
        Args:
            skip_download: If True, use existing local files only (for re-runs)
            
        Returns:
            dict: {'downloaded': list of file paths, 'failed': list of failed downloads}
        """
        print("=" * 60)
        print("STEP 1: Checking TLC Trip Data")
        print("=" * 60)
        
        # Download taxi zone lookup table for geospatial mapping
        download_taxi_zones()
        
        # Check what files already exist locally
        from src.scraper import check_local_files
        local_files = check_local_files()
        print(f"Found {len(local_files)} local parquet files")
        
        if skip_download:
            print("Skipping downloads - using local data only")
            self.results['needs_imputation'] = True
            return {'downloaded': [f['path'] for f in local_files], 'failed': []}
        
        # Check what's available on TLC website for 2025
        availability = check_data_availability(2025)
        print(f"Available 2025 Yellow months: {availability['yellow']}")
        print(f"Available 2025 Green months: {availability['green']}")
        
        # Detect missing December 2025 for imputation
        missing = get_missing_months(2025)
        self.results['missing_months'] = missing
        
        # Flag if December data needs to be imputed
        if 12 in missing['yellow'] or 12 in missing['green']:
            print("December 2025 data not available - will impute later")
            self.results['needs_imputation'] = True
        else:
            self.results['needs_imputation'] = False
        
        # Download all available data (2024 for comparison, 2025 for analysis)
        result = download_all_data(years=[2025], include_comparison=[2024])
        print(f"Downloaded {len(result['downloaded'])} files")
        if result['failed']:
            print(f"Failed: {result['failed']}")
        
        return result
    
    def step_2_create_unified_view(self):
        """
        STEP 2: Create Unified Schema View
        
        Combines Yellow and Green taxi parquet files into a single DuckDB view
        with unified column names. This follows the "Aggregation First" rule:
        - All data stays in DuckDB (no pd.read_parquet on full data)
        - Only aggregated results are converted to Pandas for visualization
        
        Unified Schema:
            pickup_time, dropoff_time, pickup_loc, dropoff_loc,
            trip_distance, fare, total_amount, congestion_surcharge,
            tip_amount, taxi_type, vendor_id
            
        Returns:
            int: Total number of trips in unified view
        """
        print("=" * 60)
        print("STEP 2: Creating Unified Schema View")
        print("=" * 60)
        
        # Find all parquet files in raw data directory
        parquet_files = list(RAW_DIR.glob("*.parquet"))
        print(f"Found {len(parquet_files)} parquet files")
        
        if not parquet_files:
            print("No parquet files found. Please run step 1 first.")
            return
        
        # Build UNION ALL query for all parquet files
        # Each file gets its schema mapped via get_unified_query()
        queries = []
        for pf in parquet_files:
            # Determine taxi type from filename
            taxi_type = 'yellow' if 'yellow' in pf.name else 'green'
            # Get SQL query with proper column mappings
            queries.append(get_unified_query(str(pf).replace('\\', '/'), taxi_type))
        
        # Create view that combines all files (DuckDB reads parquet lazily)
        combined_query = " UNION ALL ".join(queries)
        self.con.execute(f"CREATE OR REPLACE VIEW unified_trips AS {combined_query}")
        
        # Count total trips (this is fast in DuckDB)
        count = self.con.execute("SELECT COUNT(*) FROM unified_trips").fetchone()[0]
        print(f"Unified view created with {count:,} total trips")
        
        return count
    
    def step_3_filter_ghost_trips(self):
        """
        STEP 3: Filter Ghost Trips (Fraud Detection)
        
        Identifies and removes suspicious trips using three detection rules:
        
        1. Impossible Physics: Average speed > 65 MPH
           (Physically impossible in NYC traffic)
           
        2. Teleporter: Trip time < 1 minute but fare > $20
           (Indicates GPS manipulation or meter fraud)
           
        3. Stationary Ride: Trip distance = 0 but fare > 0
           (Meter running without actual travel)
        
        Ghost trips are stored in a separate audit log for investigation.
        Clean trips are stored in a separate view for analysis.
        
        Returns:
            dict: {'clean': count of valid trips, 'ghost': count of fraudulent trips}
        """
        print("=" * 60)
        print("STEP 3: Filtering Ghost Trips")
        print("=" * 60)
        
        # Apply ghost trip detection rules and create clean_trips view
        filter_ghost_trips(self.con)
        
        # Get summary statistics by ghost trip type
        summary = get_ghost_trip_summary(self.con)
        print("\nGhost Trip Summary:")
        print(summary)
        
        # Identify vendors with highest ghost trip rates
        suspicious = get_suspicious_vendors(self.con)
        print("\nTop 5 Suspicious Vendors:")
        print(suspicious)
        self.results['suspicious_vendors'] = suspicious
        
        # Save ghost trips to parquet for audit trail
        audit_path = save_ghost_trips_audit(self.con)
        print(f"\nAudit log saved to: {audit_path}")
        
        # Calculate ghost trip statistics
        clean_count = self.con.execute("SELECT COUNT(*) FROM clean_trips").fetchone()[0]
        ghost_count = self.con.execute("SELECT COUNT(*) FROM ghost_trips").fetchone()[0]
        print(f"\nClean trips: {clean_count:,}")
        print(f"Ghost trips: {ghost_count:,}")
        print(f"Ghost rate: {ghost_count/(clean_count+ghost_count)*100:.2f}%")
        
        return {'clean': clean_count, 'ghost': ghost_count}
    
    def step_4_zone_analysis(self):
        """
        STEP 4: Congestion Zone Analysis
        
        Analyzes the impact of the congestion toll on trips entering the
        Manhattan Congestion Relief Zone (south of 60th Street).
        
        Calculates:
        - Surcharge Compliance Rate: % of zone-entry trips with surcharge
        - Missing Surcharge Locations: Top pickup spots avoiding the toll
        - Quarterly Comparison: Q1 2024 vs Q1 2025 trip volumes
        - Total Revenue: Estimated 2025 surcharge collection
        
        Returns:
            dict: All zone analysis results for report generation
        """
        print("=" * 60)
        print("STEP 4: Congestion Zone Analysis")
        print("=" * 60)
        
        # Create zone-specific views for analysis
        create_zone_tables(self.con)
        
        # Calculate compliance rate for trips entering the zone
        compliance = calculate_surcharge_compliance(self.con)
        print("\nSurcharge Compliance Rate:")
        print(compliance)
        self.results['compliance'] = compliance
        
        # Find locations with highest missing surcharge rates
        missing_locations = get_missing_surcharge_locations(self.con)
        print("\nTop 3 Locations with Missing Surcharges:")
        print(missing_locations)
        self.results['missing_surcharge_locations'] = missing_locations
        
        # Compare Q1 2024 (before toll) vs Q1 2025 (after toll)
        quarterly = compare_quarterly_volumes(self.con)
        print("\nQ1 2024 vs Q1 2025 Trip Volumes:")
        print(quarterly)
        self.results['quarterly_comparison'] = quarterly
        
        # Calculate total surcharge revenue for 2025
        revenue = calculate_total_surcharge_revenue(self.con)
        print("\n2025 Surcharge Revenue:")
        print(revenue)
        self.results['surcharge_revenue'] = revenue
        
        return self.results
    
    def step_5_aggregations(self):
        """
        STEP 5: Create Aggregations for Visualization
        
        Pre-aggregates data in DuckDB before converting to Pandas.
        This follows the "Aggregation First" rule - plotting libraries
        can't handle 100M+ rows, so we reduce data size here.
        
        Creates:
        - velocity_q1_2024.csv: Hourly speeds by day (Before toll)
        - velocity_q1_2025.csv: Hourly speeds by day (After toll)  
        - daily_trips_2025.csv: Daily trip counts for weather analysis
        - tips_surcharge.csv: Monthly tip % vs surcharge trends
        - border_effect.csv: Drop-off changes at zone borders
        
        Returns:
            bool: True when all aggregations complete
        """
        print("=" * 60)
        print("STEP 5: Creating Aggregations for Visualization")
        print("=" * 60)
        
        # Aggregate hourly speeds for velocity heatmaps (Hour x Day of Week)
        print("Aggregating Q1 2024 speeds...")
        velocity_2024 = aggregate_hourly_speeds(self.con, 2024, 1)
        velocity_2024.to_csv(PROCESSED_DIR / "velocity_q1_2024.csv", index=False)
        
        print("Aggregating Q1 2025 speeds...")
        velocity_2025 = aggregate_hourly_speeds(self.con, 2025, 1)
        velocity_2025.to_csv(PROCESSED_DIR / "velocity_q1_2025.csv", index=False)
        
        # Aggregate daily trips for weather correlation analysis
        print("Aggregating daily trips...")
        daily = aggregate_daily_trips(self.con, 2025)
        daily.to_csv(PROCESSED_DIR / "daily_trips_2025.csv", index=False)
        
        # Aggregate monthly tips and surcharges for "crowding out" analysis
        print("Aggregating monthly tips and surcharges...")
        tips = aggregate_monthly_tips_surcharges(self.con, 2025)
        tips.to_csv(PROCESSED_DIR / "tips_surcharge.csv", index=False)
        self.results['tips_surcharge'] = tips
        
        # Calculate border effect (drop-off changes at zone edges)
        print("Calculating border effect...")
        border = calculate_border_effect(self.con)
        
        # Add coordinates for map visualization
        zone_lookup = pd.read_csv(RAW_DIR / "taxi_zone_lookup.csv")
        zone_coords = {
            142: (40.7736, -73.9830), 143: (40.7725, -73.9870),
            151: (40.7968, -73.9664), 236: (40.7804, -73.9530),
            237: (40.7689, -73.9595), 238: (40.7915, -73.9744),
            239: (40.7800, -73.9795), 262: (40.7767, -73.9530),
            263: (40.7756, -73.9595)
        }
        
        border['lat'] = border['dropoff_loc'].map(lambda x: zone_coords.get(x, (40.77, -73.97))[0])
        border['lon'] = border['dropoff_loc'].map(lambda x: zone_coords.get(x, (40.77, -73.97))[1])
        border = border.merge(zone_lookup[['LocationID', 'Zone']], 
                             left_on='dropoff_loc', right_on='LocationID', how='left')
        border = border.rename(columns={'Zone': 'zone_name'})
        border.to_csv(PROCESSED_DIR / "border_effect.csv", index=False)
        self.results['border_effect'] = border
        
        print("Aggregations complete!")
        return True
    
    def step_6_weather_analysis(self):
        """
        STEP 6: Weather Data Analysis (Rain Tax)
        
        Fetches weather data from Open-Meteo API and calculates the
        "Rain Elasticity of Demand" - how sensitive taxi usage is to rain.
        
        Calculates:
        - Daily precipitation for Central Park, NYC
        - Correlation between precipitation and trip counts
        - Rain Elasticity Score (elastic if |correlation| > 0.5)
        - Wettest month identification for visualization
        
        Returns:
            dict: Rain elasticity results including correlation and interpretation
        """
        print("=" * 60)
        print("STEP 6: Weather Data Analysis")
        print("=" * 60)
        
        # Fetch 2025 precipitation data from Open-Meteo API
        print("Fetching 2025 weather data...")
        weather_df = fetch_precipitation_data(2025)
        
        if weather_df is None:
            print("Could not fetch weather data")
            return None
        
        # Load pre-aggregated daily trip counts
        print("Loading daily trip counts...")
        daily_trips = pd.read_csv(PROCESSED_DIR / "daily_trips_2025.csv")
        daily_trips['date'] = pd.to_datetime(daily_trips['date'])
        
        # Join weather with trip counts
        merged = pd.merge(weather_df, daily_trips, on='date', how='inner')
        merged.to_csv(PROCESSED_DIR / "weather_trips.csv", index=False)
        
        # Find wettest month for focused visualization
        wettest_month = find_wettest_month(weather_df)
        print(f"Wettest month: {wettest_month}")
        
        # Calculate rain elasticity (correlation + regression)
        elasticity = calculate_rain_elasticity(weather_df, daily_trips)
        print(f"Rain Elasticity Results:")
        print(f"  Correlation: {elasticity['correlation']:.4f}")
        print(f"  Slope: {elasticity['slope']:.2f}")
        print(f"  R-squared: {elasticity['r_squared']:.4f}")
        print(f"  Interpretation: {elasticity['interpretation']}")
        
        self.results['rain_elasticity'] = elasticity
        self.results['wettest_month'] = wettest_month
        
        return elasticity
    
    def step_7_impute_december(self):
        """
        STEP 7: December 2025 Imputation
        
        If December 2025 data is not yet published on the TLC site,
        impute it using a weighted average:
        - 30% weight: December 2023 patterns
        - 70% weight: December 2024 patterns
        
        This weighted approach assumes 2024 is more representative
        while still incorporating longer-term seasonal patterns.
        
        Returns:
            DataFrame: Imputed December 2025 data by hour and day of week
        """
        print("=" * 60)
        print("STEP 7: December 2025 Imputation Check")
        print("=" * 60)
        
        # Skip if December data is already available
        if not self.results.get('needs_imputation', False):
            print("December 2025 data available - no imputation needed")
            return
        
        print("Imputing December 2025 using weighted average:")
        print("  30% weight: December 2023")
        print("  70% weight: December 2024")
        
        # Get December 2023 patterns by hour and day of week
        dec_2023 = self.con.execute("""
            SELECT 
                EXTRACT(HOUR FROM pickup_time) as hour,
                EXTRACT(DOW FROM pickup_time) as dow,
                COUNT(*) as count_2023,
                AVG(fare) as fare_2023,
                AVG(total_amount) as total_2023
            FROM unified_trips
            WHERE MONTH(pickup_time) = 12 AND YEAR(pickup_time) = 2023
            GROUP BY 1, 2
        """).fetchdf()
        
        # Get December 2024 patterns by hour and day of week
        dec_2024 = self.con.execute("""
            SELECT 
                EXTRACT(HOUR FROM pickup_time) as hour,
                EXTRACT(DOW FROM pickup_time) as dow,
                COUNT(*) as count_2024,
                AVG(fare) as fare_2024,
                AVG(total_amount) as total_2024
            FROM unified_trips
            WHERE MONTH(pickup_time) = 12 AND YEAR(pickup_time) = 2024
            GROUP BY 1, 2
        """).fetchdf()
        
        # Merge and apply weighted average (30% 2023, 70% 2024)
        imputed = dec_2023.merge(dec_2024, on=['hour', 'dow'], how='outer')
        imputed['imputed_count'] = imputed['count_2023'].fillna(0) * 0.3 + imputed['count_2024'].fillna(0) * 0.7
        imputed['imputed_fare'] = imputed['fare_2023'].fillna(0) * 0.3 + imputed['fare_2024'].fillna(0) * 0.7
        imputed['imputed_total'] = imputed['total_2023'].fillna(0) * 0.3 + imputed['total_2024'].fillna(0) * 0.7
        
        # Save imputed data
        imputed.to_csv(PROCESSED_DIR / "december_2025_imputed.csv", index=False)
        print(f"Imputed data saved to: {PROCESSED_DIR / 'december_2025_imputed.csv'}")
        
        return imputed
    
    def generate_report_data(self):
        """
        Generate JSON report with all key findings for audit_report.pdf.
        
        Exports:
        - Total surcharge revenue
        - Rain elasticity score
        - Top 5 suspicious vendors
        - Compliance rate
        - Missing surcharge locations
        
        Returns:
            dict: Complete report data in JSON-serializable format
        """
        print("=" * 60)
        print("Generating Report Data")
        print("=" * 60)
        
        # Compile all results into report structure
        report = {
            'generated_at': datetime.now().isoformat(),
            'total_surcharge_revenue': self.results.get('surcharge_revenue', {}).to_dict() 
                if hasattr(self.results.get('surcharge_revenue', {}), 'to_dict') else {},
            'rain_elasticity': self.results.get('rain_elasticity', {}),
            'suspicious_vendors': self.results.get('suspicious_vendors', pd.DataFrame()).to_dict() 
                if hasattr(self.results.get('suspicious_vendors', pd.DataFrame()), 'to_dict') else {},
            'compliance_rate': self.results.get('compliance', pd.DataFrame()).to_dict() 
                if hasattr(self.results.get('compliance', pd.DataFrame()), 'to_dict') else {},
            'missing_surcharge_locations': self.results.get('missing_surcharge_locations', pd.DataFrame()).to_dict() 
                if hasattr(self.results.get('missing_surcharge_locations', pd.DataFrame()), 'to_dict') else {}
        }
        
        # Save report data as JSON
        import json
        with open(OUTPUT_DIR / "report_data.json", 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        print(f"Report data saved to: {OUTPUT_DIR / 'report_data.json'}")
        return report
    
    def run_full_pipeline(self, skip_download=True):
        """
        Execute the complete 7-step ETL and analysis pipeline.
        
        Args:
            skip_download: If True, use existing local data (faster for re-runs)
            
        Returns:
            dict: All pipeline results for further analysis
        """
        print("\n" + "=" * 60)
        print("NYC CONGESTION PRICING AUDIT PIPELINE")
        print("=" * 60 + "\n")
        
        # Execute all pipeline steps in sequence
        self.step_1_download_data(skip_download=skip_download)
        self.step_2_create_unified_view()
        self.step_3_filter_ghost_trips()
        self.step_4_zone_analysis()
        self.step_5_aggregations()
        self.step_6_weather_analysis()
        self.step_7_impute_december()
        self.generate_report_data()
        
        print("\n" + "=" * 60)
        print("PIPELINE COMPLETE")
        print("=" * 60)
        print(f"\nOutputs saved to: {OUTPUT_DIR}")
        print(f"Processed data saved to: {PROCESSED_DIR}")
        print(f"Audit logs saved to: {AUDIT_DIR}")
        print("\nRun the dashboard with: streamlit run dashboard/app.py")
        
        return self.results
    
    def close(self):
        """Close DuckDB connection and cleanup resources."""
        self.con.close()


def main():
    """
    Main entry point for the NYC Congestion Pricing Audit Pipeline.
    
    Usage:
        python pipeline.py
        
    The pipeline will:
    1. Use existing local data (or download if not available)
    2. Process all taxi trip records using DuckDB
    3. Generate aggregations for the Streamlit dashboard
    4. Save audit report data to outputs/
    """
    pipeline = CongestionPricingPipeline()
    try:
        results = pipeline.run_full_pipeline()
        return results
    finally:
        pipeline.close()


if __name__ == "__main__":
    main()
