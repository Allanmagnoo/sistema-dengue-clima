import duckdb
import logging
from pathlib import Path
import sys

# Configuração de Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("🚀 Criando Painel Híbrido: Semanal (Clima) + Anual (Saneamento/IBGE)")
    
    current_dir = Path(__file__).resolve().parent
    base_dir = current_dir.parent.parent
    
    # --- Caminhos ---
    GOLD_WEEKLY_PATH = base_dir / "data/gold/gold_dengue_clima"
    GOLD_ANNUAL_PATH = base_dir / "data/gold/paineis/painel_municipal_dengue_saneamento.parquet"
    OUTPUT_PATH = base_dir / "data/gold/paineis"
    OUTPUT_FILE = OUTPUT_PATH / "painel_hibrido_semanal.parquet"
    
    OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
    
    if not GOLD_ANNUAL_PATH.exists():
        logger.error(f"Painel Anual não encontrado: {GOLD_ANNUAL_PATH}")
        return

    # --- DuckDB ---
    con = duckdb.connect(database=':memory:')
    
    # 1. Carregar Painel Semanal (Dengue + Clima)
    logger.info("📖 Lendo Painel Semanal (Dengue + Clima)...")
    con.execute(f"""
        CREATE VIEW weekly AS 
        SELECT * 
        FROM read_parquet('{GOLD_WEEKLY_PATH}/**/*.parquet', hive_partitioning=1)
    """)
    
    # 2. Carregar Painel Anual (Saneamento + IBGE)
    logger.info("📖 Lendo Painel Anual (Saneamento + IBGE)...")
    con.execute(f"""
        CREATE VIEW annual AS 
        SELECT * 
        FROM read_parquet('{str(GOLD_ANNUAL_PATH)}')
    """)
    
    # 3. Join
    logger.info("🔄 Realizando Join Híbrido...")
    
    # A estratégia é enriquecer o painel semanal com as colunas anuais.
    # Join key: geocode (semanal) = id_municipio (anual) AND ano_epidemiologico (semanal) = ano (anual)
    # id_municipio no anual é string (ex: '1234567'), no semanal geocode é bigint.
    
    query = """
    SELECT 
        w.*,
        
        -- Features de Saneamento (Anuais)
        a.agua_cobertura_total_pct,
        a.esgoto_cobertura_total_pct,
        a.agua_perda_distribuicao_pct,
        
        -- Features Demográficas (Censo 2022 - Constante ou Anual se houver)
        a.densidade_domiciliada,
        
        -- Features Calculadas do Anual (opcional, mas já temos no semanal algumas coisas)
        -- a.populacao_total -- Já tem 'populacao' no semanal, vamos comparar ou manter o do semanal que varia semanalmente? 
        -- O semanal tem populacao vindo do InfoDengue que é estimativa anual.
        
    FROM weekly w
    LEFT JOIN annual a 
        ON w.geocode = CAST(a.id_municipio AS BIGINT)
        AND w.ano_epidemiologico = a.ano
    """
    
    con.execute(f"CREATE TABLE hybrid AS {query}")
    
    # 4. Verificações
    total = con.execute("SELECT COUNT(*) FROM hybrid").fetchone()[0]
    matched = con.execute("SELECT COUNT(*) FROM hybrid WHERE agua_cobertura_total_pct IS NOT NULL").fetchone()[0]
    pct = (matched/total*100) if total > 0 else 0
    
    logger.info(f"📊 Total de linhas: {total}")
    logger.info(f"📊 Linhas com dados de saneamento: {matched} ({pct:.1f}%)")
    
    # 5. Salvar
    logger.info(f"💾 Salvando em: {OUTPUT_FILE}")
    # Partition by UF para performance se ficar muito grande, mas o original já é particionado.
    # Vamos salvar particionado por UF também.
    
    # Como o output é um arquivo único no nome da função, mas parquet suporta pasta.
    # Se o nome termina em .parquet e usamos COPY, ele cria uma pasta se usarmos PARTITION_BY.
    # Vamos apontar para uma pasta.
    
    OUTPUT_DIR_HYBRID = OUTPUT_PATH / "painel_hibrido_semanal"
    
    con.execute(f"""
        COPY hybrid TO '{OUTPUT_DIR_HYBRID}' (
            FORMAT PARQUET, 
            PARTITION_BY (uf), 
            OVERWRITE_OR_IGNORE 1,
            COMPRESSION 'SNAPPY'
        )
    """)
    
    logger.info("✅ Painel Híbrido concluído.")
    con.close()

if __name__ == "__main__":
    main()
