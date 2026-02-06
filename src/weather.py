import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.settings import WEATHER_API_URL, CENTRAL_PARK_LAT, CENTRAL_PARK_LON, RAW_DIR

def fetch_precipitation_data(year=2025):
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"
    
    params = {
        "latitude": CENTRAL_PARK_LAT,
        "longitude": CENTRAL_PARK_LON,
        "start_date": start_date,
        "end_date": end_date,
        "daily": "precipitation_sum,rain_sum",
        "timezone": "America/New_York"
    }
    
    try:
        response = requests.get(WEATHER_API_URL, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()
        
        df = pd.DataFrame({
            'date': pd.to_datetime(data['daily']['time']),
            'precipitation_mm': data['daily']['precipitation_sum'],
            'rain_mm': data['daily']['rain_sum']
        })
        
        return df
    except Exception as e:
        print(f"Error fetching weather data: {e}")
        return None

def save_weather_data(df, year=2025):
    output_path = RAW_DIR / f"weather_{year}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)

def load_weather_data(year=2025):
    cache_path = RAW_DIR / f"weather_{year}.csv"
    if cache_path.exists():
        return pd.read_csv(cache_path, parse_dates=['date'])
    else:
        df = fetch_precipitation_data(year)
        if df is not None:
            save_weather_data(df, year)
        return df

def find_wettest_month(weather_df):
    weather_df['month'] = weather_df['date'].dt.month
    monthly_precip = weather_df.groupby('month')['precipitation_mm'].sum()
    wettest_month = monthly_precip.idxmax()
    return wettest_month

def calculate_rain_elasticity(weather_df, trips_df):
    merged = pd.merge(
        weather_df[['date', 'precipitation_mm']], 
        trips_df,
        on='date',
        how='inner'
    )
    
    correlation = merged['precipitation_mm'].corr(merged['trip_count'])
    
    from scipy import stats
    slope, intercept, r_value, p_value, std_err = stats.linregress(
        merged['precipitation_mm'].fillna(0),
        merged['trip_count']
    )
    
    return {
        'correlation': correlation,
        'slope': slope,
        'r_squared': r_value ** 2,
        'p_value': p_value,
        'interpretation': 'elastic' if abs(slope) > 1000 else 'inelastic'
    }

def get_wettest_month_data(weather_df, trips_df, wettest_month):
    weather_month = weather_df[weather_df['date'].dt.month == wettest_month]
    trips_month = trips_df[trips_df['date'].dt.month == wettest_month]
    
    merged = pd.merge(
        weather_month[['date', 'precipitation_mm']], 
        trips_month,
        on='date',
        how='inner'
    )
    
    return merged

if __name__ == "__main__":
    print("Fetching 2025 weather data...")
    df = fetch_precipitation_data(2025)
    if df is not None:
        print(f"Got {len(df)} days of weather data")
        wettest = find_wettest_month(df)
        print(f"Wettest month: {wettest}")
