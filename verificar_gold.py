"""
Verificação da qualidade dos dados na camada Gold
"""
import duckdb
import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("🔍 Verificando qualidade dos dados na camada Gold")
    
    current_dir = Path(__file__).resolve().parent
    base_dir = current_dir
    
    con = duckdb.connect(database=':memory:')
    
    # Verificar arquivo Gold
    gold_path = base_dir / "data/gold/dengue_clima.parquet"
    
    if gold_path.exists():
        logger.info("✅ Arquivo Gold encontrado")
        
        # Ler dados
        df = con.execute(f"SELECT * FROM read_parquet('{gold_path}')").fetchdf()
        
        logger.info(f"📊 Total de registros: {len(df)}")
        logger.info(f"📊 Colunas: {list(df.columns)}")
        
        # Verificar valores nulos
        logger.info("\n📋 Valores nulos por coluna:")
        null_counts = df.isnull().sum()
        for col, count in null_counts.items():
            if count > 0:
                percentage = (count / len(df)) * 100
                logger.info(f"  {col}: {count} ({percentage:.1f}%)")
        
        # Estatísticas das colunas climáticas
        climate_cols = ['temperatura_media_diaria', 'precipitacao_total_diaria']
        
        logger.info("\n🌡️ Estatísticas das variáveis climáticas:")
        for col in climate_cols:
            if col in df.columns:
                non_null_data = df[col].dropna()
                if len(non_null_data) > 0:
                    logger.info(f"\n{col}:")
                    logger.info(f"  Registros não-nulos: {len(non_null_data)}")
                    logger.info(f"  Média: {non_null_data.mean():.2f}")
                    logger.info(f"  Min: {non_null_data.min():.2f}")
                    logger.info(f"  Max: {non_null_data.max():.2f}")
                else:
                    logger.info(f"\n{col}: Todos os valores são nulos")
        
        # Verificar cobertura temporal
        if 'data_inicio_semana' in df.columns:
            logger.info(f"\n📅 Período temporal:")
            df['data_inicio_semana'] = pd.to_datetime(df['data_inicio_semana'])
            logger.info(f"  Data inicial: {df['data_inicio_semana'].min()}")
            logger.info(f"  Data final: {df['data_inicio_semana'].max()}")
        
        # Verificar cobertura geográfica
        if 'geocode' in df.columns:
            unique_geocodes = df['geocode'].nunique()
            logger.info(f"\n🗺️ Cobertura geográfica:")
            logger.info(f"  Total de municípios únicos: {unique_geocodes}")
            
            # Verificar municípios com e sem dados climáticos
            if 'temperatura_media_diaria' in df.columns:
                municipios_com_clima = df[df['temperatura_media_diaria'].notna()]['geocode'].nunique()
                municipios_sem_clima = unique_geocodes - municipios_com_clima
                logger.info(f"  Municípios com dados climáticos: {municipios_com_clima}")
                logger.info(f"  Municípios sem dados climáticos: {municipios_sem_clima}")
        
        # Amostra dos dados
        logger.info(f"\n🔍 Amostra dos dados (primeiras 5 linhas):")
        print(df.head())
        
    else:
        logger.error("❌ Arquivo Gold não encontrado")
    
    con.close()
    logger.info("✅ Verificação concluída")

if __name__ == "__main__":
    main()