"""
Debug do JOIN Gold - Investigação de chaves
"""
import duckdb
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("🔍 Debug do JOIN Gold")
    
    current_dir = Path(__file__).resolve().parent
    base_dir = current_dir
    
    con = duckdb.connect(database=':memory:')
    
    SILVER_INMET_PATH = base_dir / "data/silver/inmet"
    SILVER_DENGUE_PATH = base_dir / "data/silver/infodengue"
    MAPPING_TABLE_PATH = base_dir / "data/silver/mapping_estacao_geocode.parquet"
    
    # Carregar dados
    con.execute(f"CREATE VIEW silver_inmet AS SELECT * FROM read_parquet('{SILVER_INMET_PATH}/**/*.parquet', hive_partitioning=1, union_by_name=True)")
    con.execute(f"CREATE VIEW silver_dengue AS SELECT * FROM read_parquet('{SILVER_DENGUE_PATH}/**/*.parquet', hive_partitioning=1)")
    con.execute(f"CREATE VIEW mapping_table AS SELECT * FROM read_parquet('{str(MAPPING_TABLE_PATH)}')")
    
    # Recriar lógica de preparação climática
    con.execute("""
        CREATE TABLE inmet_geocoded AS
        SELECT i.data_medicao, m.geocode
        FROM silver_inmet i
        INNER JOIN mapping_table m ON i.estacao_id = m.estacao_id
        WHERE i.data_medicao IS NOT NULL
    """)
    
    con.execute("""
        CREATE TABLE inmet_weekly AS
        SELECT 
            geocode,
            year(data_medicao) as ano,
            week(data_medicao) as semana,
            COUNT(*) as qtd
        FROM inmet_geocoded
        GROUP BY geocode, year(data_medicao), week(data_medicao)
    """)
    
    # Verificar tipos de dados das chaves
    logger.info("\n📋 Tipos de dados das chaves:")
    
    dengue_types = con.execute("DESCRIBE silver_dengue").fetchdf()
    inmet_types = con.execute("DESCRIBE inmet_weekly").fetchdf()
    
    logger.info("Dengue:")
    logger.info(dengue_types[dengue_types['column_name'].isin(['geocode', 'ano_epidemiologico', 'semana_epidemiologica'])])
    
    logger.info("\nINMET Weekly:")
    logger.info(inmet_types[inmet_types['column_name'].isin(['geocode', 'ano', 'semana'])])
    
    # Verificar amostra de valores
    logger.info("\n👀 Amostra de valores (Dengue):")
    print(con.execute("SELECT geocode, ano_epidemiologico, semana_epidemiologica FROM silver_dengue LIMIT 5").fetchdf())
    
    logger.info("\n👀 Amostra de valores (INMET Weekly):")
    print(con.execute("SELECT geocode, ano, semana FROM inmet_weekly LIMIT 5").fetchdf())
    
    # Tentar um JOIN em apenas um geocode conhecido
    sample_geocode = con.execute("SELECT geocode FROM inmet_weekly LIMIT 1").fetchone()[0]
    logger.info(f"\n🔗 Testando JOIN para geocode {sample_geocode}:")
    
    join_test = con.execute(f"""
        SELECT 
            d.geocode, d.ano_epidemiologico, d.semana_epidemiologica,
            c.ano, c.semana
        FROM silver_dengue d
        INNER JOIN inmet_weekly c 
        ON d.geocode = c.geocode 
        AND d.ano_epidemiologico = c.ano 
        AND d.semana_epidemiologica = c.semana
        WHERE d.geocode = {sample_geocode}
    """).fetchdf()
    
    logger.info(f"Registros encontrados no JOIN: {len(join_test)}")
    if len(join_test) == 0:
        logger.info("❌ JOIN falhou mesmo para geocode conhecido. Investigando incompatibilidade de semana/ano.")
        
        # Ver o que temos no dengue para esse geocode
        logger.info("Dengue data para este geocode (ano/semana):")
        print(con.execute(f"SELECT ano_epidemiologico, semana_epidemiologica FROM silver_dengue WHERE geocode = {sample_geocode} ORDER BY 1,2 LIMIT 5").fetchdf())
        
        # Ver o que temos no inmet para esse geocode
        logger.info("INMET data para este geocode (ano/semana):")
        print(con.execute(f"SELECT ano, semana FROM inmet_weekly WHERE geocode = {sample_geocode} ORDER BY 1,2 LIMIT 5").fetchdf())

    con.close()
    logger.info("✅ Debug concluído")

if __name__ == "__main__":
    main()