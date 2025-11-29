"""
Verificação final da camada Gold enriquecida
"""
import duckdb
import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("🔍 Verificação Final da Camada Gold Enriquecida")
    
    current_dir = Path(__file__).resolve().parent
    base_dir = current_dir
    
    con = duckdb.connect(database=':memory:')
    gold_path = base_dir / "data/gold/dengue_clima.parquet"
    
    if gold_path.exists():
        con.execute(f"CREATE VIEW gold AS SELECT * FROM read_parquet('{gold_path}')")
        
        # Verificar amostra com dados climáticos
        logger.info("\n📊 Amostra de registros com dados climáticos INMET:")
        sample = con.execute("""
            SELECT 
                nome_municipio, 
                semana_epidemiologica, 
                casos_confirmados, 
                inmet_temp_media, 
                inmet_temp_media_lag1,
                inmet_precip_tot
            FROM gold 
            WHERE inmet_temp_media IS NOT NULL 
            LIMIT 5
        """).fetchdf()
        print(sample)
        
        # Estatísticas de correlação (simples)
        logger.info("\n📈 Correlação (Dengue vs Clima INMET) onde dados existem:")
        corr = con.execute("""
            SELECT 
                CORR(casos_confirmados, inmet_temp_media) as corr_temp,
                CORR(casos_confirmados, inmet_temp_media_lag1) as corr_temp_lag1,
                CORR(casos_confirmados, inmet_precip_tot) as corr_precip
            FROM gold 
            WHERE inmet_temp_media IS NOT NULL
        """).fetchdf()
        print(corr)
        
    con.close()
    logger.info("✅ Verificação concluída")

if __name__ == "__main__":
    main()