import duckdb
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_schemas():
    con = duckdb.connect()
    
    current_dir = Path(__file__).resolve().parent
    base_dir = current_dir.parent.parent
    dengue_path = base_dir / "data/silver/infodengue"
    inmet_path = base_dir / "data/silver/inmet"
    
    logger.info(f"Checking Silver Dengue Schema from {dengue_path}...")
    try:
        con.execute(f"CREATE VIEW sd AS SELECT * FROM read_parquet('{dengue_path}/**/*.parquet', hive_partitioning=1)")
        logger.info(con.execute("DESCRIBE sd").fetchall())
        logger.info("Sample Data:")
        logger.info(con.execute("SELECT * FROM sd LIMIT 5").fetchall())
    except Exception as e:
        logger.error(f"Error reading Dengue: {e}")

    logger.info(f"Checking Silver INMET Schema from {inmet_path}...")
    try:
        con.execute(f"CREATE VIEW si AS SELECT * FROM read_parquet('{inmet_path}/**/*.parquet', hive_partitioning=1)")
        logger.info(con.execute("DESCRIBE si").fetchall())
        logger.info("Sample Data:")
        logger.info(con.execute("SELECT * FROM si LIMIT 5").fetchall())
    except Exception as e:
        logger.error(f"Error reading INMET: {e}")

if __name__ == "__main__":
    check_schemas()
