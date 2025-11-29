"""
Silver Layer Transformation - Dengue Data (DuckDB Version)

Transforms Bronze CSV data into Silver Parquet format using DuckDB.
- Reads raw CSVs from data/bronze/infodengue/municipios
- Enriches with municipality metadata (UF, Name) from DTB
- Standardizes schema and types
- Writes partitioned Parquet to data/silver/infodengue
"""
import sys
import os
import logging
import duckdb
import pandas as pd
from pathlib import Path

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from src.utils.dtb_reader import DTBReader

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    logger.info("🚀 Starting Silver Layer Transformation (DuckDB)")
    
    # Paths
    # Determine project root relative to this script
    current_dir = Path(__file__).resolve().parent
    base_dir = current_dir.parent.parent
    
    BRONZE_PATH = base_dir / "data/bronze/infodengue/municipios/disease=dengue"
    SILVER_PATH = base_dir / "data/silver/infodengue"
    
    # 1. Load Municipality Metadata
    logger.info("📖 Loading Municipality Metadata...")
    dtb_reader = DTBReader()
    try:
        municipios_list = dtb_reader.get_all_municipios()
        
        # Convert to Pandas DataFrame for DuckDB registration
        df_mun = pd.DataFrame(municipios_list)
        
        # UF Mapping
        uf_map = {
            11: 'RO', 12: 'AC', 13: 'AM', 14: 'RR', 15: 'PA', 16: 'AP', 17: 'TO',
            21: 'MA', 22: 'PI', 23: 'CE', 24: 'RN', 25: 'PB', 26: 'PE', 27: 'AL', 28: 'SE', 29: 'BA',
            31: 'MG', 32: 'ES', 33: 'RJ', 35: 'SP',
            41: 'PR', 42: 'SC', 43: 'RS',
            50: 'MS', 51: 'MT', 52: 'GO', 53: 'DF'
        }
        
        # Add UF column based on first 2 digits of geocode
        df_mun['uf_code'] = df_mun['geocode'].astype(str).str[:2].astype(int)
        df_mun['uf'] = df_mun['uf_code'].map(uf_map)
        
        logger.info(f"✅ Loaded {len(df_mun)} municipalities metadata")
        
    except Exception as e:
        logger.error(f"❌ Error loading metadata: {e}")
        sys.exit(1)

    # 2. Initialize DuckDB
    con = duckdb.connect(database=':memory:')
    
    # Register metadata table
    con.register('municipios', df_mun)
    
    # 3. Read Bronze Data (Recursive CSV read)
    logger.info(f"📖 Reading Bronze Data from {BRONZE_PATH}...")
    
    # DuckDB can read hive partitioned folders recursively
    # Pattern: data/bronze/infodengue/municipios/disease=dengue/year=*/*.csv
    # Note: DuckDB hive partitioning support might need explicit hive_partitioning=1
    
    try:
        # Use CREATE TABLE instead of VIEW to materialize data and avoid complex query crash
        query_create_table = f"""
        CREATE TABLE raw_dengue_table AS 
        SELECT 
            *,
            try_cast(NULLIF(regexp_extract(filename, 'year=([0-9]+)', 1), '') AS INTEGER) as year_partition,
            try_cast(replace(list_last(string_split(filename, chr(92))), '.csv', '') AS BIGINT) as geocode_from_filename
        FROM read_csv('{BRONZE_PATH}/*/*.csv', 
            header=True, 
            filename=True,
            columns={{
                'data_iniSE': 'DATE',
                'SE': 'INTEGER',
                'casos_est': 'DOUBLE',
                'casos_est_min': 'DOUBLE',
                'casos_est_max': 'DOUBLE',
                'casos': 'VARCHAR',
                'p_rt1': 'DOUBLE',
                'p_inc100k': 'DOUBLE',
                'Localidade_id': 'INTEGER',
                'nivel': 'INTEGER',
                'id': 'BIGINT',
                'versao_modelo': 'VARCHAR',
                'tweet': 'VARCHAR',
                'Rt': 'DOUBLE',
                'pop': 'DOUBLE',
                'tempmin': 'DOUBLE',
                'umidmax': 'DOUBLE',
                'receptivo': 'INTEGER',
                'transmissao': 'INTEGER',
                'nivel_inc': 'INTEGER',
                'umidmed': 'DOUBLE',
                'umidmin': 'DOUBLE',
                'tempmed': 'DOUBLE',
                'tempmax': 'DOUBLE',
                'casprov': 'VARCHAR',
                'casprov_est': 'DOUBLE',
                'casprov_est_min': 'DOUBLE',
                'casprov_est_max': 'DOUBLE',
                'casconf': 'VARCHAR',
                'notif_accum_year': 'VARCHAR'
            }}
        )
        """
        logger.info("Creating materialized table 'raw_dengue_table'...")
        con.execute(query_create_table)
        
        count = con.execute("SELECT COUNT(*) FROM raw_dengue_table").fetchone()[0]
        logger.info(f"✅ Bronze data table created. Total rows: {count}")
        
        # Materialize municipios table to avoid Pandas-DuckDB bridge issues during complex join
        logger.info("Materializing 'municipios_table'...")
        con.execute("CREATE TABLE municipios_table AS SELECT * FROM municipios")
        
    except Exception as e:
        logger.error(f"❌ Error reading bronze data: {e}")
        sys.exit(1)

    # 4. Transform and Enrich
    logger.info("🔄 Transforming and Enriching...")
    
    query_transform = """
    SELECT 
        r.geocode_from_filename as geocode,
        m.uf,
        m.nome as nome_municipio,
        r.data_iniSE as data_inicio_semana,
        r.SE as semana_epidemiologica,
        try_cast(NULLIF(r.casos, '') as INTEGER) as casos_notificados,
        r.casos_est as casos_estimados,
        r.casos_est_min as casos_estimados_min,
        r.casos_est_max as casos_estimados_max,
        try_cast(NULLIF(r.casconf, '') as INTEGER) as casos_confirmados,
        try_cast(NULLIF(r.notif_accum_year, '') as INTEGER) as notificacoes_acumuladas,
        r.p_inc100k as incidencia_100k,
        r.nivel as nivel_alerta,
        r.receptivo as condicoes_receptivas,
        r.transmissao as evidencia_transmissao,
        r.pop as populacao,
        r.tempmed as temperatura_media,
        r.umidmed as umidade_media,
        try_cast(NULLIF(r.casprov, '') as INTEGER) as casos_provaveis,
        r.casprov_est as casos_provaveis_estimados,
        r.casprov_est_min as casos_provaveis_estimados_min,
        r.casprov_est_max as casos_provaveis_estimados_max,
        r.year_partition as ano_epidemiologico
    FROM raw_dengue_table r
    INNER JOIN municipios_table m ON r.geocode_from_filename = m.geocode
    WHERE r.geocode_from_filename IS NOT NULL
    """
    
    # Materialize the final result first to check if JOIN crashes
    logger.info("Materializing 'silver_dengue_table'...")
    con.execute(f"CREATE TABLE silver_dengue_table AS {query_transform}")
    
    count_silver = con.execute("SELECT COUNT(*) FROM silver_dengue_table").fetchone()[0]
    logger.info(f"✅ Silver data prepared. Total rows: {count_silver}")

    # 4.1 Data Quality Assertions
    logger.info("🔍 Running Data Quality Assertions...")
    
    dq_checks = [
        ("Total rows > 0", "SELECT COUNT(*) > 0 FROM silver_dengue_table"),
        ("No NULL Geocodes", "SELECT COUNT(*) = 0 FROM silver_dengue_table WHERE geocode IS NULL"),
        ("No NULL Data Inicio Semana", "SELECT COUNT(*) = 0 FROM silver_dengue_table WHERE data_inicio_semana IS NULL"),
        ("No NULL UF", "SELECT COUNT(*) = 0 FROM silver_dengue_table WHERE uf IS NULL"),
        ("Valid UF Length", "SELECT COUNT(*) = 0 FROM silver_dengue_table WHERE length(uf) != 2"),
        ("Non-negative Notified Cases", "SELECT COUNT(*) = 0 FROM silver_dengue_table WHERE casos_notificados < 0"),
        ("Year Partition Logic", "SELECT COUNT(*) = 0 FROM silver_dengue_table WHERE ano_epidemiologico IS NULL OR ano_epidemiologico < 2000 OR ano_epidemiologico > 2100")
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
        # Decide whether to stop or continue. For strict Silver layer, we stop.
        sys.exit(1)
    else:
        logger.info("✨ All Data Quality Checks Passed!")

    # 5. Write to Parquet (Partitioned)
    logger.info(f"💾 Writing to Silver Layer: {SILVER_PATH}...")
    
    # Ensure output directory exists (DuckDB creates it, but good practice)
    os.makedirs(SILVER_PATH, exist_ok=True)
    
    copy_query = f"""
    COPY silver_dengue_table TO '{SILVER_PATH}' (
        FORMAT PARQUET, 
        PARTITION_BY (uf, ano_epidemiologico),
        OVERWRITE_OR_IGNORE 1,
        COMPRESSION 'SNAPPY'
    )
    """
    
    con.execute(copy_query)
    logger.info("✅ Silver Layer Transformation Complete!")

if __name__ == "__main__":
    main()
