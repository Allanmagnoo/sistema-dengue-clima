import duckdb
import pathlib
import glob
import os

base_dir = str(pathlib.Path(__file__).parent.parent.parent / "data" / "bronze" / "inmet")
files = glob.glob(os.path.join(base_dir, "**/*.CSV"), recursive=True)
print(f"Found {len(files)} files.")
if files:
    first_file = files[0]
    print(f"Testing read on: {first_file}")
    
    con = duckdb.connect()
    try:
        # Test the exact query logic
        con.execute(f"""
            SELECT * FROM read_csv('{first_file}', 
            header=False, 
            skip=9, 
            sep=';',
            filename=True,
            encoding='latin-1' 
        ) LIMIT 5
        """)
        print("Read successful:")
        print(con.fetchall())
    except Exception as e:
        print(f"Error reading: {e}")
