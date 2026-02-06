import requests
import duckdb

url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
}

print("Testing HEAD request...")
try:
    resp = requests.head(url, headers=headers, timeout=10)
    print(f"HEAD status: {resp.status_code}")
    print(f"Content-Length: {resp.headers.get('Content-Length', 'N/A')}")
except Exception as e:
    print(f"HEAD error: {e}")

print("\nTesting DuckDB direct read...")
try:
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    result = con.execute(f"SELECT COUNT(*) FROM read_parquet('{url}')").fetchone()
    print(f"DuckDB row count: {result[0]}")
except Exception as e:
    print(f"DuckDB error: {e}")

print("\nTesting requests GET (first 1KB)...")
try:
    resp = requests.get(url, headers=headers, stream=True, timeout=30)
    print(f"GET status: {resp.status_code}")
    chunk = next(resp.iter_content(1024))
    print(f"Got {len(chunk)} bytes")
except Exception as e:
    print(f"GET error: {e}")
