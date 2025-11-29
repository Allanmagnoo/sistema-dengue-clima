import duckdb

# Path to Silver (where assertions failed)
SILVER_PATH = r"D:\_data-science\GitHub\eco-sentinel\data\silver\inmet"

con = duckdb.connect()

print("🔍 Inspecting Silver Layer for NULL UFs...")

# Check if we can read the parquet files
try:
    # Count NULL UFs
    count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{SILVER_PATH}/**/*.parquet', hive_partitioning=1) WHERE uf IS NULL OR uf = ''").fetchone()[0]
    print(f"Total rows with NULL UF: {count}")
    
    if count > 0:
        # Show examples. Note: In Parquet hive partitioning, the partition key 'uf' is usually inferred from directory.
        # If we wrote it with partition_by(uf), it might not be in the file content itself, but DuckDB reads it from path.
        # Let's see what 'uf' column contains.
        print("\nExamples of rows with NULL/Empty UF:")
        con.execute(f"SELECT * FROM read_parquet('{SILVER_PATH}/**/*.parquet', hive_partitioning=1) WHERE uf IS NULL OR uf = '' LIMIT 5")
        print(con.fetchall())
        
except Exception as e:
    print(f"Error: {e}")
