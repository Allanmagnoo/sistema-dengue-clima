import duckdb
import glob
import os

# Path to Silver (where assertions failed)
SILVER_PATH = r"D:\_data-science\GitHub\eco-sentinel\data\silver\inmet"

con = duckdb.connect()

print("🔍 Inspecting Silver Layer for Filenames that resulted in NULL UFs...")

try:
    # Now we have 'filename' in the parquet!
    print("\nSample rows with NULL UF + Filename:")
    con.execute(f"SELECT filename, uf FROM read_parquet('{SILVER_PATH}/**/*.parquet', hive_partitioning=1) WHERE uf IS NULL OR uf = '' LIMIT 5")
    rows = con.fetchall()
    for r in rows:
        print(f"Filename: {r[0]}")
        print(f"Extracted UF: '{r[1]}'")
        print("-" * 20)

except Exception as e:
    print(f"Error: {e}")
