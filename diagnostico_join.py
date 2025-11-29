"""
Diagnóstico detalhado do JOIN Gold
"""
import duckdb
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("🔍 Diagnóstico detalhado do JOIN Gold")
    
    current_dir = Path(__file__).resolve().parent
    base_dir = current_dir
    
    con = duckdb.connect(database=':memory:')
    
    SILVER_INMET_PATH = base_dir / "data/silver/inmet"
    SILVER_DENGUE_PATH = base_dir / "data/silver/infodengue"
    MAPPING_TABLE_PATH = base_dir / "data/silver/mapping_estacao_geocode.parquet"
    
    # 1. Carregar dados
    logger.info("📖 Carregando dados...")
    con.execute(f"CREATE VIEW silver_inmet AS SELECT * FROM read_parquet('{SILVER_INMET_PATH}/**/*.parquet', hive_partitioning=1, union_by_name=True)")
    con.execute(f"CREATE VIEW silver_dengue AS SELECT * FROM read_parquet('{SILVER_DENGUE_PATH}/**/*.parquet', hive_partitioning=1)")
    con.execute(f"CREATE VIEW mapping_table AS SELECT * FROM read_parquet('{str(MAPPING_TABLE_PATH)}')")
    
    # 2. Verificar colunas de dengue
    logger.info("\n📋 Colunas Silver Dengue:")
    cols_dengue = con.execute("DESCRIBE silver_dengue").fetchall()
    for row in cols_dengue:
        logger.info(f"  - {row[0]}: {row[1]}")
        
    # 3. Verificar geocodes disponíveis
    logger.info("\n🗺️ Análise de Geocodes:")
    
    # Geocodes no Dengue
    dengue_geocodes = con.execute("SELECT COUNT(DISTINCT geocode) FROM silver_dengue").fetchone()[0]
    logger.info(f"  Geocodes únicos em Dengue: {dengue_geocodes}")
    
    # Geocodes no Mapeamento
    mapping_geocodes = con.execute("SELECT COUNT(DISTINCT geocode) FROM mapping_table").fetchone()[0]
    logger.info(f"  Geocodes únicos no Mapeamento: {mapping_geocodes}")
    
    # Overlap
    overlap = con.execute("""
        SELECT COUNT(DISTINCT d.geocode) 
        FROM silver_dengue d
        INNER JOIN mapping_table m ON d.geocode = m.geocode
    """).fetchone()[0]
    logger.info(f"  Geocodes em comum (Dengue INTERSECT Mapeamento): {overlap}")
    
    # 4. Verificar dados climáticos após mapeamento
    logger.info("\n🌡️ Análise de Dados Climáticos Mapeados:")
    
    con.execute("""
        CREATE TABLE silver_inmet_with_geocode AS
        SELECT i.*, m.geocode
        FROM silver_inmet i
        INNER JOIN mapping_table m ON i.estacao_id = m.estacao_id
    """)
    
    inmet_count = con.execute("SELECT COUNT(*) FROM silver_inmet_with_geocode").fetchone()[0]
    logger.info(f"  Total de registros climáticos mapeados: {inmet_count}")
    
    if inmet_count > 0:
        # Verificar datas
        min_date = con.execute("SELECT MIN(data_medicao) FROM silver_inmet_with_geocode").fetchone()[0]
        max_date = con.execute("SELECT MAX(data_medicao) FROM silver_inmet_with_geocode").fetchone()[0]
        logger.info(f"  Período climático: {min_date} a {max_date}")
        
        # Verificar geocodes com dados climáticos
        unique_climate_geocodes = con.execute("SELECT COUNT(DISTINCT geocode) FROM silver_inmet_with_geocode").fetchone()[0]
        logger.info(f"  Municípios com dados climáticos: {unique_climate_geocodes}")
    
    # 5. Verificar datas em Dengue
    min_dengue = con.execute("SELECT MIN(data_inicio_semana) FROM silver_dengue").fetchone()[0]
    max_dengue = con.execute("SELECT MAX(data_inicio_semana) FROM silver_dengue").fetchone()[0]
    logger.info(f"\n📅 Período Dengue: {min_dengue} a {max_dengue}")
    
    con.close()
    logger.info("✅ Diagnóstico concluído")

if __name__ == "__main__":
    main()