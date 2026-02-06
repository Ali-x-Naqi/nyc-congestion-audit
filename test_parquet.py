import duckdb
from pathlib import Path

RAW_DIR = Path("data/raw")
con = duckdb.connect()

for pf in RAW_DIR.glob("*.parquet"):
    try:
        count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{str(pf).replace(chr(92), '/')}')" ).fetchone()[0]
        print(f"OK: {pf.name} - {count:,} rows")
    except Exception as e:
        print(f"ERROR: {pf.name} - {str(e)[:80]}")
