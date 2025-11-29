
import duckdb
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def inspect_schema(parquet_path: Path, table_name: str):
    """Reads a partitioned Parquet dataset and describes its schema."""
    if not parquet_path.exists():
        logger.error(f"Directory not found: {parquet_path}")
        # Try to list parent to give more context
        if parquet_path.parent.exists():
            logger.error(f"Parent directory content: {list(parquet_path.parent.iterdir())}")
        return
        
    logger.info(f"Inspecting schema for {table_name} at {parquet_path}")
    con = duckdb.connect(database=':memory:')
    
    try:
        # Use hive_partitioning=1 to automatically detect partitions like 'uf' and 'ano'
        con.execute(f"CREATE VIEW {table_name} AS SELECT * FROM read_parquet('{parquet_path}/**/*.parquet', hive_partitioning=1)")
        result = con.execute(f"DESCRIBE {table_name}").fetchdf()
        logger.info(f"Schema for {table_name}:\n{result.to_string()}")
    except Exception as e:
        logger.error(f"Failed to inspect schema for {table_name}. Error: {e}")
    finally:
        con.close()

def main():
    base_dir = Path(__file__).resolve().parent.parent.parent
    
    # Paths to Silver layer data
    inmet_path = base_dir / "data/silver/inmet"
    dengue_path = base_dir / "data/silver/infodengue"
    
    logger.info("--- Inspecting INMET Silver Schema ---")
    inspect_schema(inmet_path, "silver_inmet")
    
    logger.info("\n--- Inspecting Dengue Silver Schema ---")
    inspect_schema(dengue_path, "silver_dengue")

if __name__ == "__main__":
    main()
