import duckdb

# Path to Silver
SILVER_PATH = r"D:\_data-science\GitHub\eco-sentinel\data\silver\inmet"

con = duckdb.connect()

print("🔍 Inspecting Silver Layer for NULL UFs distribution...")

try:
    # Is it a specific year?
    print("\nDistribution by Year for NULL UFs:")
    con.execute(f"SELECT ano, COUNT(*) FROM read_parquet('{SILVER_PATH}/**/*.parquet', hive_partitioning=1) WHERE uf IS NULL OR uf = '' GROUP BY ano")
    print(con.fetchall())
    
    # Let's see if we can find the original filename or metadata that produced these NULLs
    # Oh wait, we didn't persist 'filename' in the silver table, only extracted metadata.
    # But we can check other columns.
    
    print("\nSample rows with NULL UF:")
    con.execute(f"SELECT * FROM read_parquet('{SILVER_PATH}/**/*.parquet', hive_partitioning=1) WHERE uf IS NULL OR uf = '' LIMIT 5")
    cols = [desc[0] for desc in con.description]
    rows = con.fetchall()
    print(f"Columns: {cols}")
    for r in rows:
        print(r)

except Exception as e:
    print(f"Error: {e}")
