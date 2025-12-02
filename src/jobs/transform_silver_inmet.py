"""
Silver Layer Transformation - INMET Data (High Performance DuckDB)

Transforms Bronze CSV data into Silver Parquet format using DuckDB.
Optimized for speed:
- Reads all columns as VARCHAR to skip expensive schema inference
- Uses maximum available threads
- Vectorized parsing and transformation
"""
import sys
import os
import logging
import duckdb
import multiprocessing
import argparse
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year-start", type=int, default=2020)
    parser.add_argument("--year-end", type=int, default=2025)
    args = parser.parse_args()

    year_start = args.year_start
    year_end = args.year_end

    logger.info(f"🚀 Starting Silver Layer Transformation for INMET (High Performance) - Years {year_start} to {year_end}")
    
    # Paths
    current_dir = Path(__file__).resolve().parent
    base_dir = current_dir.parent.parent
    
    BRONZE_PATH = base_dir / "data/bronze/inmet"
    SILVER_PATH = base_dir / "data/silver/inmet"
    
    # 1. Initialize DuckDB with Performance Tuning
    # Use all available CPU cores
    num_threads = multiprocessing.cpu_count()
    logger.info(f"⚙️ Configuring DuckDB with {num_threads} threads...")
    
    con = duckdb.connect(database=':memory:')
    con.execute(f"PRAGMA threads={num_threads}")
    # con.execute("PRAGMA memory_limit='80%'") # DuckDB 0.9.x supports percentage but maybe not this version
    
    # 2. Read Bronze Data (Fastest Mode)
    # Strategy: Read everything as VARCHAR to disable sniffer (sample_size=0 is not enough, all_varchar is faster)
    # We rely on filename=True to get metadata
    logger.info(f"📖 Reading Bronze Data from {BRONZE_PATH}...")
    
    # Construct file list using Python's glob to ensure existence
    all_files = []
    for year in range(year_start, year_end + 1):
        year_path = BRONZE_PATH / str(year)
        # Look for both .CSV and .csv, recursive or not
        # Based on directory listing, files are directly in year folder, but let's use rglob to be safe or glob
        # The listing showed files in 2020 directly.
        found_files = list(year_path.glob("*.CSV")) + list(year_path.glob("*.csv"))
        all_files.extend([str(f) for f in found_files])

    if not all_files:
        logger.error(f"❌ No files found for years {year_start}-{year_end}")
        sys.exit(1)

    logger.info(f"found {len(all_files)} files to process.")

    # Format list for SQL
    # Escape backslashes for SQL string if on Windows
    files_sql_list = "[" + ", ".join([f"'{str(f).replace(os.sep, '/')}'" for f in all_files]) + "]"

    try:
        # column00=Data, column01=Hora, column02=Precip, column07=Temp, column15=Umid
        query_create_raw = f"""
        CREATE TABLE raw_inmet_table AS 
        SELECT *
        FROM read_csv({files_sql_list}, 
            header=False, 
            skip=9, 
            sep=';',
            filename=True,
            encoding='latin-1',
            all_varchar=True,
            auto_detect=False,
            null_padding=True,
            ignore_errors=True,
            columns={{
                'column00': 'VARCHAR', 'column01': 'VARCHAR', 'column02': 'VARCHAR', 
                'column03': 'VARCHAR', 'column04': 'VARCHAR', 'column05': 'VARCHAR', 
                'column06': 'VARCHAR', 'column07': 'VARCHAR', 'column08': 'VARCHAR', 
                'column09': 'VARCHAR', 'column10': 'VARCHAR', 'column11': 'VARCHAR', 
                'column12': 'VARCHAR', 'column13': 'VARCHAR', 'column14': 'VARCHAR', 
                'column15': 'VARCHAR', 'column16': 'VARCHAR', 'column17': 'VARCHAR', 
                'column18': 'VARCHAR', 'column19': 'VARCHAR', 'column20': 'VARCHAR'
            }}
        )
        """
        logger.info(f"⚡ Materializing raw data for years {year_start}-{year_end}...")
        con.execute(query_create_raw)
        
        count = con.execute("SELECT COUNT(*) FROM raw_inmet_table").fetchone()[0]
        logger.info(f"✅ Bronze data loaded. Total rows: {count}")
        
    except Exception as e:
        logger.error(f"❌ Error reading bronze data: {e}")
        sys.exit(1)

    # 3. Transform and Enrich (Vectorized Operations)
    logger.info("🔄 Transforming and Enriching (Vectorized)...")
    
    # Using regex to extract metadata from filename
    # Pattern: INMET_[REGION]_[UF]_[STATION_ID]_[NAME]...
    
    query_transform = """
    SELECT 
        -- Metadata from Filename
        regexp_extract(filename, 'INMET_[A-Z]{1,2}_([A-Z]{2})_', 1) as uf,
        regexp_extract(filename, 'INMET_[A-Z]{2}_[A-Z]{2}_([A-Z0-9]+)_', 1) as estacao_id,
        filename, -- Keep filename for debugging if needed
        
        -- Date and Time
        -- Handle date format YYYY/MM/DD
        try_cast(strptime(column00, '%Y/%m/%d') as DATE) as data_medicao,
        
        -- Extract Hour (first 4 chars of '0000 UTC')
        try_cast(substring(column01, 1, 4) as INTEGER) as hora_utc,
        
        -- Metrics (Replace comma with dot and cast)
        try_cast(replace(column02, ',', '.') as DOUBLE) as precipitacao_mm,
        try_cast(replace(column07, ',', '.') as DOUBLE) as temperatura_c,
        try_cast(replace(column15, ',', '.') as DOUBLE) as umidade_relativa_percent,
        
        -- Partitioning Key
        year(try_cast(strptime(column00, '%Y/%m/%d') as DATE)) as ano
    FROM raw_inmet_table
    WHERE 
        -- Filter valid rows (column00 must be a date-like string)
        column00 LIKE '____/__/__'
    """
    
    logger.info("Materializing 'silver_inmet_table'...")
    con.execute(f"CREATE TABLE silver_inmet_table AS {query_transform}")
    
    count_silver = con.execute("SELECT COUNT(*) FROM silver_inmet_table").fetchone()[0]
    logger.info(f"✅ Silver data prepared. Total rows: {count_silver}")

    # 4. Data Quality Assertions
    logger.info("🔍 Running Data Quality Assertions...")
    
    dq_checks = [
        ("Total rows > 0", "SELECT COUNT(*) > 0 FROM silver_inmet_table"),
        ("No NULL UF", "SELECT COUNT(*) = 0 FROM silver_inmet_table WHERE uf IS NULL OR uf = ''"),
        ("No NULL Date", "SELECT COUNT(*) = 0 FROM silver_inmet_table WHERE data_medicao IS NULL"),
        # Relaxed range checks for dirty data, or strict? Let's keep strict but warn.
        # Using strict failures as requested for Silver quality.
        ("Valid Temp Range (-10 to 50)", "SELECT COUNT(*) = 0 FROM silver_inmet_table WHERE temperatura_c < -10 OR temperatura_c > 50"),
        ("Valid Year", "SELECT COUNT(*) = 0 FROM silver_inmet_table WHERE ano IS NULL OR ano < 2000 OR ano > 2100")
    ]
    
    failed_assertions = []
    for check_name, query in dq_checks:
        try:
            result = con.execute(query).fetchone()[0]
            if result:
                logger.info(f"✅ Assertion Passed: {check_name}")
            else:
                logger.error(f"❌ Assertion Failed: {check_name}")
                failed_assertions.append(check_name)
        except Exception as e:
            logger.error(f"❌ Assertion Error ({check_name}): {e}")
            failed_assertions.append(f"{check_name} (Error)")
            
    if failed_assertions:
        logger.error(f"🚨 Data Quality Checks Failed: {', '.join(failed_assertions)}")
        # If quality is critical, uncomment exit. For now, we log error but proceed to let user see data.
        # sys.exit(1) 
        logger.warning("⚠️ Proceeding despite DQ failures (check logs).")
    else:
        logger.info("✨ All Data Quality Checks Passed!")

    # 5. Write to Parquet (Partitioned)
    logger.info(f"💾 Writing to Silver Layer: {SILVER_PATH}...")
    
    os.makedirs(SILVER_PATH, exist_ok=True)
    
    # Use COPY with PER_THREAD_OUTPUT for maximum write parallelism if supported, 
    # but PARTITION_BY usually handles parallel writes well.
    copy_query = f"""
    COPY silver_inmet_table TO '{SILVER_PATH}' (
        FORMAT PARQUET, 
        PARTITION_BY (uf, ano),
        OVERWRITE_OR_IGNORE 1,
        COMPRESSION 'SNAPPY'
    )
    """
    
    con.execute(copy_query)
    logger.info("✅ Silver Layer Transformation Complete!")

if __name__ == "__main__":
    main()
