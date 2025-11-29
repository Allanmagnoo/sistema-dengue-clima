import duckdb
import glob
import os

BRONZE_PATH = r"D:\_data-science\GitHub\eco-sentinel\data\bronze\inmet"

con = duckdb.connect()

# Strategy: 
# The assertion passed on the filename scan (debug_inmet_filenames.py) but failed in the main script.
# This implies the Regex logic inside the main script might be subtly different or the way DuckDB handles 'filename' in the complex query is different.

# Let's try to replicate the transformation logic EXACTLY as it is in the main script, but inspecting the filename that produces NULL UF.
print("🔍 Debugging transformation logic for NULL UF...")

# We create a temporary table with the same logic
con.execute(f"""
    CREATE TABLE debug_raw AS 
    SELECT filename
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
        columns={{'c0': 'VARCHAR'}}
    )
""")

# Now apply the regex and filter for failures
query_debug = """
SELECT DISTINCT filename, 
       regexp_extract(filename, 'INMET_[A-Z]{2}_([A-Z]{2})_', 1) as extracted_uf
FROM debug_raw
WHERE regexp_extract(filename, 'INMET_[A-Z]{2}_([A-Z]{2})_', 1) = ''
LIMIT 10
"""

results = con.execute(query_debug).fetchall()

if not results:
    print("✅ Logic seems correct in isolation. No NULL UFs generated.")
else:
    print(f"❌ Found filenames generating NULL UF:")
    for r in results:
        print(f"File: {r[0]} -> Extracted: '{r[1]}'")
