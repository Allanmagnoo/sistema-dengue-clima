"""
Análise de Compatibilidade entre Dados INMET e Dengue

Este script analisa a estrutura dos dados e propõe soluções para criar
uma camada Gold com dados climáticos e de dengue compatíveis.
"""
import duckdb
import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("🔍 Analisando compatibilidade entre INMET e Dengue")
    
    current_dir = Path(__file__).resolve().parent
    base_dir = current_dir
    
    con = duckdb.connect(database=':memory:')
    
    # 1. Análise dos dados INMET Bronze
    logger.info("📊 Analisando dados INMET Bronze...")
    
    inmet_bronze_path = base_dir / "data/bronze/inmet/2024"
    
    # Ler amostra de arquivos INMET
    sample_files = list(inmet_bronze_path.glob("*.CSV"))[:5]
    
    logger.info("Amostra de arquivos INMET:")
    for f in sample_files:
        logger.info(f"  - {f.name}")
        # Extrair metadata do filename
        parts = f.name.split('_')
        if len(parts) >= 5:
            regiao = parts[1]
            uf = parts[2] 
            estacao_id = parts[3]
            nome_estacao = parts[4]
            logger.info(f"    Região: {regiao}, UF: {uf}, Estação: {estacao_id}, Nome: {nome_estacao}")
    
    # 2. Análise da estrutura dos dados Silver existentes
    logger.info("📊 Analisando estrutura Silver existente...")
    
    # Verificar se existe mapeamento estacao_geocode
    mapping_path = base_dir / "data/silver/mapping_estacao_geocode.parquet"
    if mapping_path.exists():
        con.execute(f"CREATE VIEW mapping_estacao_geocode AS SELECT * FROM read_parquet('{mapping_path}')")
        mapping_count = con.execute("SELECT COUNT(*) FROM mapping_estacao_geocode").fetchone()[0]
        logger.info(f"✅ Mapeamento estacao_geocode encontrado: {mapping_count} registros")
        
        # Mostrar amostra do mapeamento
        sample_mapping = con.execute("SELECT * FROM mapping_estacao_geocode LIMIT 5").fetchdf()
        logger.info("Amostra do mapeamento:")
        print(sample_mapping)
    else:
        logger.warning("❌ Mapeamento estacao_geocode não encontrado")
    
    # 3. Problemas identificados e soluções
    logger.info("🎯 Problemas identificados e soluções propostas:")
    
    logger.info("\n1. **Falta de geocode nos dados INMET**")
    logger.info("   Solução: Usar o mapeamento estacao_id → geocode já criado")
    
    logger.info("\n2. **Dados climáticos com NaN após join**")
    logger.info("   Causa: Arquivos INMET com schemas diferentes (colunas faltando)")
    logger.info("   Solução: Usar union_by_name=True ao ler arquivos Parquet")
    
    logger.info("\n3. **Cobertura geográfica limitada**")
    logger.info("   Apenas 373 municípios com dados climáticos dos 5.570+ municípios brasileiros")
    logger.info("   Solução: Aceitar limitação ou buscar dados de mais estações")
    
    logger.info("\n4. **Desalinhamento temporal**")
    logger.info("   Dados dengue: semanal, Dados clima: diário")
    logger.info("   Solução: Agregar clima para semana epidemiológica")
    
    # 4. Verificar dados disponíveis
    logger.info("\n📈 Verificando disponibilidade de dados:")
    
    # Contar total de municípios no DTB
    dtb_path = base_dir / "data/bronze/DTB_2024/RELATORIO_DTB_BRASIL_2024_MUNICIPIOS.xls"
    if dtb_path.exists():
        try:
            df_dtb = pd.read_excel(dtb_path)
            logger.info(f"Total de municípios no DTB: {len(df_dtb)}")
            
            # Verificar colunas disponíveis
            logger.info(f"Colunas DTB: {list(df_dtb.columns)}")
            
        except Exception as e:
            logger.error(f"Erro ao ler DTB: {e}")
    
    # 5. Propor melhorias na estrutura
    logger.info("\n💡 Propostas de melhoria:")
    
    logger.info("""
    **ESTRUTURA PROPOSTA PARA CAMADA GOLD:**
    
    Tabela: gold_dengue_clima
    - geocode (INT): Código IBGE do município
    - nome_municipio (VARCHAR): Nome do município  
    - uf (VARCHAR): Sigla da UF
    - data_inicio_semana (DATE): Início da semana epidemiológica
    - semana_epidemiologica (INT): Semana no formato YYYYWW
    - ano_epidemiologico (INT): Ano da semana
    - casos_notificados (INT): Casos de dengue notificados
    - casos_estimados (FLOAT): Casos estimados pelo modelo
    - casos_confirmados (INT): Casos confirmados
    - incidencia_100k (FLOAT): Incidência por 100 mil habitantes
    - nivel_alerta (INT): Nível de alerta (1-4)
    - populacao (FLOAT): População do município
    - temperatura_media_semanal (FLOAT): Média de temperatura na semana
    - precipitacao_total_semanal (FLOAT): Soma da precipitação na semana
    - umidade_media_semanal (FLOAT): Média da umidade relativa
    - estacao_id (VARCHAR): ID da estação meteorológica (para referência)
    - distancia_estacao_km (FLOAT): Distância aproximada até a estação
    
    **CONSIDERAÇÕES:**
    1. Usar left join para manter todos os municípios com dados de dengue
    2. Permitir valores NULL para dados climáticos quando não houver estação próxima
    3. Adicionar flags indicando qualidade/Fonte dos dados climáticos
    4. Documentar limitações de cobertura geográfica
    
    **PRÓXIMOS PASSOS:**
    1. Executar transform_silver_inmet.py para gerar dados Silver
    2. Executar transform_silver_dengue.py para gerar dados Silver  
    3. Verificar qualidade dos dados Silver gerados
    4. Ajustar script Gold para lidar com dados climáticos faltantes
    5. Adicionar análise de distância entre municípios e estações
    """)
    
    con.close()
    logger.info("✅ Análise de compatibilidade concluída")

if __name__ == "__main__":
    main()