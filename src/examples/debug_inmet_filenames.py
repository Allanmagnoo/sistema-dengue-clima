import duckdb
import glob
import os

# Path to Bronze
BRONZE_PATH = r"D:\_data-science\GitHub\eco-sentinel\data\bronze\inmet"

con = duckdb.connect()

# Query to find filenames that fail the regex
# We read only the filename column to be fast
print("🔍 Scanning filenames for regex failures...")

query = f"""
SELECT DISTINCT filename 
FROM read_csv('{BRONZE_PATH}/**/*.CSV', 
    header=False, 
    skip=9, 
    sep=';',
    filename=True,
    encoding='latin-1',
    all_varchar=True,
    auto_detect=False,
    null_padding=True,
    ignore_errors=True,
    columns={{'c0': 'VARCHAR'}} -- Read just one column + filename
)
WHERE regexp_extract(filename, 'INMET_[A-Z]{{2}}_([A-Z]{{2}})_', 1) = ''
LIMIT 20
"""

try:
    failures = con.execute(query).fetchall()
    if not failures:
        print("✅ No regex failures found! All filenames match the pattern.")
    else:
        print(f"❌ Found {len(failures)} examples of non-matching filenames:")
        for f in failures:
            print(f" - {os.path.basename(f[0])}")
            
except Exception as e:
    print(f"Error: {e}")
