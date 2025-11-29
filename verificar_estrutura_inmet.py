"""
Verificar estrutura real dos dados Silver INMET
"""
import duckdb
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("🔍 Verificando estrutura dos dados Silver INMET")
    
    current_dir = Path(__file__).resolve().parent
    base_dir = current_dir
    
    con = duckdb.connect(database=':memory:')
    
    # Verificar estrutura dos arquivos INMET Silver
    inmet_silver_path = base_dir / "data/silver/inmet"
    
    if inmet_silver_path.exists():
        # Ler apenas um arquivo como amostra
        sample_files = list(inmet_silver_path.glob("*.parquet"))[:3]
        
        if sample_files:
            logger.info(f"📁 Encontrados {len(sample_files)} arquivos")
            
            try:
                # Ler com union_by_name para ver estrutura
                con.execute(f"""
                    CREATE VIEW sample_inmet AS 
                    SELECT * FROM read_parquet('{sample_files[0]}')
                """)
                
                # Obter colunas
                result = con.execute("DESCRIBE sample_inmet").fetchall()
                logger.info("📋 Colunas encontradas:")
                for row in result:
                    logger.info(f"  - {row[0]}: {row[1]}")
                
                # Amostra de dados
                logger.info("\n🔍 Amostra de dados:")
                sample_data = con.execute("SELECT * FROM sample_inmet LIMIT 5").fetchdf()
                print(sample_data)
                
                # Verificar se existe a coluna geocode
                has_geocode = any(row[0] == 'geocode' for row in result)
                logger.info(f"\n✅ Tem coluna 'geocode': {has_geocode}")
                
                if not has_geocode:
                    logger.warning("❌ A coluna 'geocode' não existe nos dados Silver INMET!")
                    logger.info("💡 É necessário adicionar o geocode baseado no estacao_id")
                    
                    # Verificar se existe estacao_id
                    has_estacao_id = any(row[0] == 'estacao_id' for row in result)
                    logger.info(f"✅ Tem coluna 'estacao_id': {has_estacao_id}")
                    
                    if has_estacao_id:
                        # Verificar valores únicos de estacao_id
                        estacoes = con.execute("SELECT DISTINCT estacao_id FROM sample_inmet LIMIT 10").fetchdf()
                        logger.info("\n📡 Amostra de estações:")
                        print(estacoes)
                        
            except Exception as e:
                logger.error(f"❌ Erro ao ler arquivo: {e}")
        else:
            logger.warning("❌ Nenhum arquivo Parquet encontrado em data/silver/inmet")
    else:
        logger.error("❌ Diretório Silver INMET não encontrado")
    
    con.close()
    logger.info("✅ Verificação concluída")

if __name__ == "__main__":
    main()