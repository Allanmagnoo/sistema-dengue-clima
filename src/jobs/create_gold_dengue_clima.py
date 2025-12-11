import duckdb
import duckdb
import logging
from pathlib import Path
import sys

# Ensure current dir is in path for imports
current_dir = Path(__file__).resolve().parent
if str(current_dir) not in sys.path:
    sys.path.append(str(current_dir))

from renaming_utils import rename_parquet_recursive

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("🚀 Starting Gold Layer Creation: Dengue + Clima OBT (Final Fix)")
    
    current_dir = Path(__file__).resolve().parent
    base_dir = current_dir.parent.parent
    
    # --- Paths ---
    SILVER_INMET_PATH = base_dir / "data/silver/silver_inmet"
    SILVER_DENGUE_PATH = base_dir / "data/silver/silver_dengue"
    MAPPING_TABLE_PATH = base_dir / "data/silver/silver_mapping_estacao_geocode.parquet"
    GOLD_PATH = base_dir / "data/gold"
    
    GOLD_PATH.mkdir(parents=True, exist_ok=True)
    
    # --- Step 0: Check Mapping Table ---
    if not MAPPING_TABLE_PATH.exists():
        logger.info("⚠️ Mapping table not found. Creating it now...")
        try:
            # Import and run the mapping creation script
            import sys
            sys.path.append(str(current_dir))
            from create_mapping_estacao_geocode import main as create_mapping
            create_mapping()
            logger.info("✅ Mapping table created.")
        except Exception as e:
            logger.error(f"❌ Failed to create mapping table: {e}")
            return

    con = duckdb.connect(database=':memory:')
    
    # --- Step 1: Load Data ---
    logger.info("📖 Step 1: Reading Silver layer data...")
    
    # Read INMET data
    con.execute(f"""
        CREATE VIEW silver_inmet AS 
        SELECT * 
        FROM read_parquet('{SILVER_INMET_PATH}/**/*.parquet', hive_partitioning=1, union_by_name=True)
    """)
    
    # Read Dengue data
    con.execute(f"""
        CREATE VIEW silver_dengue AS 
        SELECT * 
        FROM read_parquet('{SILVER_DENGUE_PATH}/**/*.parquet', hive_partitioning=1)
    """)
    
    # Read Mapping table
    con.execute(f"""
        CREATE VIEW mapping_table AS 
        SELECT * 
        FROM read_parquet('{str(MAPPING_TABLE_PATH)}')
    """)

    # --- Step 2: Prepare Climate Data (Daily -> Weekly) ---
    logger.info("🔄 Step 2: Preparing Climate Data (Mapping & Aggregation)...")
    
    # 2.1 Map Station -> Geocode and fix Data Types
    con.execute("""
        CREATE TABLE inmet_geocoded AS
        SELECT 
            i.data_medicao,
            i.temperatura_c,
            i.precipitacao_mm,
            CAST(m.geocode AS BIGINT) as geocode
        FROM silver_inmet i
        INNER JOIN mapping_table m ON i.estacao_id = m.estacao_id
        WHERE i.data_medicao IS NOT NULL
    """)
    
    # 2.2 Calculate Epidemiological Week for Climate Data
    # Using ISO week for alignment. 
    # Note: InfoDengue SE might differ slightly, but this is the standard approx.
    # We create a 'join_key' = YYYYWW
    
    con.execute("""
        CREATE TABLE inmet_weekly AS
        SELECT 
            geocode,
            CAST(year(data_medicao) * 100 + week(data_medicao) AS INTEGER) as join_key,
            MIN(data_medicao) as data_inicio_semana_clim,
            AVG(temperatura_c) as inmet_temp_media,
            MIN(temperatura_c) as inmet_temp_min,
            MAX(temperatura_c) as inmet_temp_max,
            SUM(precipitacao_mm) as inmet_precip_tot
        FROM inmet_geocoded
        GROUP BY geocode, year(data_medicao), week(data_medicao)
    """)
    
    # --- Step 3: Create Lags for Climate Data ---
    logger.info("⏳ Step 3: Generating Climate Lags (1-4 weeks)...")
    
    con.execute("""
        CREATE TABLE inmet_lags AS
        SELECT 
            *,
            LAG(inmet_temp_media, 1) OVER (PARTITION BY geocode ORDER BY join_key) as inmet_temp_media_lag1,
            LAG(inmet_temp_media, 2) OVER (PARTITION BY geocode ORDER BY join_key) as inmet_temp_media_lag2,
            LAG(inmet_temp_media, 3) OVER (PARTITION BY geocode ORDER BY join_key) as inmet_temp_media_lag3,
            LAG(inmet_temp_media, 4) OVER (PARTITION BY geocode ORDER BY join_key) as inmet_temp_media_lag4,
            
            LAG(inmet_precip_tot, 1) OVER (PARTITION BY geocode ORDER BY join_key) as inmet_precip_tot_lag1,
            LAG(inmet_precip_tot, 2) OVER (PARTITION BY geocode ORDER BY join_key) as inmet_precip_tot_lag2,
            LAG(inmet_precip_tot, 3) OVER (PARTITION BY geocode ORDER BY join_key) as inmet_precip_tot_lag3,
            LAG(inmet_precip_tot, 4) OVER (PARTITION BY geocode ORDER BY join_key) as inmet_precip_tot_lag4
        FROM inmet_weekly
    """)

    # --- Step 4: Main Join ---
    logger.info("🔗 Step 4: Joining Dengue and Climate Data...")
    
    # Prepare Dengue with compatible join_key
    con.execute("""
        CREATE VIEW dengue_prepared AS
        SELECT 
            *,
            CAST(ano_epidemiologico * 100 + semana_epidemiologica AS INTEGER) as join_key
        FROM silver_dengue
    """)
    
    query_join = """
    CREATE TABLE gold_with_climate AS
    SELECT
        d.geocode,
        d.nome_municipio,
        d.uf,
        d.data_inicio_semana,
        d.semana_epidemiologica,
        d.ano_epidemiologico,
        
        -- Dengue Metrics
        d.casos_notificados,
        d.casos_estimados,
        d.casos_confirmados,
        d.incidencia_100k,
        d.nivel_alerta,
        d.populacao,
        
        -- InfoDengue Climate (Comparison)
        d.temperatura_media as id_temp_media,
        d.umidade_media as id_umidade_media,
        
        -- INMET Climate (Ground Truth)
        c.inmet_temp_media,
        c.inmet_temp_min,
        c.inmet_temp_max,
        c.inmet_precip_tot,
        
        -- INMET Lags
        c.inmet_temp_media_lag1,
        c.inmet_temp_media_lag2,
        c.inmet_temp_media_lag3,
        c.inmet_temp_media_lag4,
        c.inmet_precip_tot_lag1,
        c.inmet_precip_tot_lag2,
        c.inmet_precip_tot_lag3,
        c.inmet_precip_tot_lag4
        
    FROM dengue_prepared d
    LEFT JOIN inmet_lags c 
        ON d.geocode = c.geocode 
        AND d.join_key = c.join_key
    ORDER BY d.geocode, d.ano_epidemiologico, d.semana_epidemiologica
    """
    
    con.execute(query_join)
    
    # --- Step 5: Create Case Lags (NEW!) ---
    logger.info("🦟 Step 5: Generating Case Lags (1-4 weeks)...")
    
    con.execute("""
        CREATE TABLE gold_final AS
        SELECT 
            *,
            -- Lags de casos (semanas anteriores)
            LAG(casos_notificados, 1) OVER (PARTITION BY geocode ORDER BY ano_epidemiologico, semana_epidemiologica) as casos_lag1,
            LAG(casos_notificados, 2) OVER (PARTITION BY geocode ORDER BY ano_epidemiologico, semana_epidemiologica) as casos_lag2,
            LAG(casos_notificados, 3) OVER (PARTITION BY geocode ORDER BY ano_epidemiologico, semana_epidemiologica) as casos_lag3,
            LAG(casos_notificados, 4) OVER (PARTITION BY geocode ORDER BY ano_epidemiologico, semana_epidemiologica) as casos_lag4,
            
            -- Média móvel de casos (últimas 4 semanas)
            AVG(casos_notificados) OVER (
                PARTITION BY geocode 
                ORDER BY ano_epidemiologico, semana_epidemiologica 
                ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
            ) as casos_media_4sem
        FROM gold_with_climate
    """)
    
    # --- Step 6: Quality Check & Write ---
    logger.info("💾 Step 6: Writing to Parquet...")
    
    # Check count
    total_rows = con.execute("SELECT COUNT(*) FROM gold_final").fetchone()[0]
    logger.info(f"  Total rows: {total_rows}")
    
    # Check climate coverage
    climate_rows = con.execute("SELECT COUNT(*) FROM gold_final WHERE inmet_temp_media IS NOT NULL").fetchone()[0]
    percentage = (climate_rows/total_rows*100) if total_rows > 0 else 0
    logger.info(f"  Rows with INMET climate data: {climate_rows} ({percentage:.1f}%)")
    
    if climate_rows == 0:
        logger.warning("⚠️ Warning: Still no climate data joined. Checking sample keys...")
        sample_d = con.execute("SELECT geocode, join_key FROM dengue_prepared LIMIT 1").fetchone()
        sample_c = con.execute("SELECT geocode, join_key FROM inmet_lags LIMIT 1").fetchone()
        logger.info(f"  Sample Dengue Key: {sample_d}")
        logger.info(f"  Sample INMET Key: {sample_c}")
    
    # Write partitioned by UF to handle large data
    output_path = GOLD_PATH / 'gold_dengue_clima'
    con.execute(f"""
        COPY gold_final TO '{output_path}' (
            FORMAT PARQUET, 
            PARTITION_BY (uf), 
            OVERWRITE_OR_IGNORE 1,
            COMPRESSION 'SNAPPY'
        )
    """)
    
    logger.info(f"✅ Gold Layer created at: {output_path}")
    con.close()
    
    # Rename files
    rename_parquet_recursive(output_path, "gold_dengue_clima")

if __name__ == "__main__":
    main()