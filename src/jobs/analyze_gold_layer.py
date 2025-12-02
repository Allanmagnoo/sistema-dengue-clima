"""
Análise da Camada Gold - Dengue + Clima
Gera estatísticas e validações sobre a camada Gold criada.
"""
import duckdb
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("📊 Starting Gold Layer Analysis")
    
    current_dir = Path(__file__).resolve().parent
    base_dir = current_dir.parent.parent
    
    GOLD_PATH = base_dir / "data/gold/dengue_clima_partitioned"
    
    if not GOLD_PATH.exists():
        logger.error(f"❌ Gold layer not found at: {GOLD_PATH}")
        return
    
    con = duckdb.connect(database=':memory:')
    
    # Load Gold data
    logger.info(f"📖 Reading Gold layer from: {GOLD_PATH}")
    con.execute(f"""
        CREATE VIEW gold_data AS 
        SELECT * 
        FROM read_parquet('{GOLD_PATH}/**/*.parquet', hive_partitioning=1)
    """)
    
    # === ANÁLISE GERAL ===
    logger.info("\n" + "="*80)
    logger.info("📈 ANÁLISE GERAL")
    logger.info("="*80)
    
    # Total de registros
    total = con.execute("SELECT COUNT(*) FROM gold_data").fetchone()[0]
    logger.info(f"Total de registros: {total:,}")
    
    # Período coberto
    periodo = con.execute("""
        SELECT MIN(ano_epidemiologico) as ano_min, 
               MAX(ano_epidemiologico) as ano_max,
               MIN(data_inicio_semana) as data_min,
               MAX(data_inicio_semana) as data_max
        FROM gold_data
    """).fetchone()
    logger.info(f"Período: {periodo[0]} - {periodo[1]}")
    logger.info(f"Datas: {periodo[2]} até {periodo[3]}")
    
    # Distribuição por UF
    logger.info("\n📍 Distribuição por UF:")
    dist_uf = con.execute("""
        SELECT uf, COUNT(*) as total, COUNT(inmet_temp_media) as com_clima
        FROM gold_data
        GROUP BY uf
        ORDER BY total DESC
    """).fetchall()
    
    for uf, total, com_clima in dist_uf[:10]:  # Top 10
        pct = (com_clima/total*100) if total > 0 else 0
        logger.info(f"  {uf}: {total:,} registros ({com_clima:,} com clima - {pct:.1f}%)")
    
    # === ANÁLISE DE COBERTURA CLIMÁTICA ===
    logger.info("\n" + "="*80)
    logger.info("🌡️ ANÁLISE DE COBERTURA CLIMÁTICA INMET")
    logger.info("="*80)
    
    cobertura = con.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(inmet_temp_media) as com_clima,
            ROUND(COUNT(inmet_temp_media) * 100.0 / COUNT(*), 2) as percentual
        FROM gold_data
    """).fetchone()
    logger.info(f"Registros totais: {cobertura[0]:,}")
    logger.info(f"Com dados INMET: {cobertura[1]:,} ({cobertura[2]}%)")
    logger.info(f"Sem dados INMET: {cobertura[0] - cobertura[1]:,}")
    
    # Cobertura por ano
    logger.info("\n📅 Cobertura climática por ano:")
    cob_ano = con.execute("""
        SELECT 
            ano_epidemiologico,
            COUNT(*) as total,
            COUNT(inmet_temp_media) as com_clima,
            ROUND(COUNT(inmet_temp_media) * 100.0 / COUNT(*), 2) as pct
        FROM gold_data
        GROUP BY ano_epidemiologico
        ORDER BY ano_epidemiologico
    """).fetchall()
    
    for ano, total, com_clima, pct in cob_ano:
        logger.info(f"  {ano}: {com_clima:,}/{total:,} ({pct}%)")
    
    # === ESTATÍSTICAS DE DENGUE ===
    logger.info("\n" + "="*80)
    logger.info("🦟 ESTATÍSTICAS DE DENGUE")
    logger.info("="*80)
    
    stats_dengue = con.execute("""
        SELECT 
            SUM(casos_notificados) as total_casos,
            AVG(casos_notificados) as media_casos,
            MAX(casos_notificados) as max_casos,
            SUM(CASE WHEN nivel_alerta >= 3 THEN 1 ELSE 0 END) as alertas_altos
        FROM gold_data
        WHERE casos_notificados IS NOT NULL
    """).fetchone()
    
    logger.info(f"Total de casos notificados: {stats_dengue[0]:,}")
    logger.info(f"Média de casos por semana/município: {stats_dengue[1]:.2f}")
    logger.info(f"Máximo de casos (uma semana/município): {stats_dengue[2]:,}")
    logger.info(f"Registros com nível de alerta alto (≥3): {stats_dengue[3]:,}")
    
    # Top 10 municípios com mais casos
    logger.info("\n🔝 Top 10 municípios com mais casos (histórico):")
    top_municipios = con.execute("""
        SELECT 
            nome_municipio,
            uf,
            SUM(casos_notificados) as total_casos,
            AVG(incidencia_100k) as inc_media
        FROM gold_data
        WHERE casos_notificados IS NOT NULL
        GROUP BY nome_municipio, uf
        ORDER BY total_casos DESC
        LIMIT 10
    """).fetchall()
    
    for i, (mun, uf, casos, inc) in enumerate(top_municipios, 1):
        logger.info(f"  {i}. {mun}/{uf}: {casos:,} casos (incidência média: {inc:.1f}/100k)")
    
    # === ESTATÍSTICAS CLIMÁTICAS ===
    logger.info("\n" + "="*80)
    logger.info("🌡️ ESTATÍSTICAS CLIMÁTICAS (INMET)")
    logger.info("="*80)
    
    stats_clima = con.execute("""
        SELECT 
            AVG(inmet_temp_media) as temp_media,
            MIN(inmet_temp_min) as temp_min_abs,
            MAX(inmet_temp_max) as temp_max_abs,
            AVG(inmet_precip_tot) as precip_media
        FROM gold_data
        WHERE inmet_temp_media IS NOT NULL
    """).fetchone()
    
    logger.info(f"Temperatura média geral: {stats_clima[0]:.2f}°C")
    logger.info(f"Temperatura mínima absoluta: {stats_clima[1]:.2f}°C")
    logger.info(f"Temperatura máxima absoluta: {stats_clima[2]:.2f}°C")
    logger.info(f"Precipitação média semanal: {stats_clima[3]:.2f}mm")
    
    # === QUALIDADE DOS DADOS ===
    logger.info("\n" + "="*80)
    logger.info("✅ VALIDAÇÕES DE QUALIDADE")
    logger.info("="*80)
    
    # Check nulls em campos críticos
    nulls = con.execute("""
        SELECT 
            COUNT(*) - COUNT(geocode) as null_geocode,
            COUNT(*) - COUNT(data_inicio_semana) as null_data,
            COUNT(*) - COUNT(semana_epidemiologica) as null_semana,
            COUNT(*) - COUNT(nome_municipio) as null_municipio
        FROM gold_data
    """).fetchone()
    
    logger.info(f"NULL geocode: {nulls[0]}")
    logger.info(f"NULL data_inicio_semana: {nulls[1]}")
    logger.info(f"NULL semana_epidemiologica: {nulls[2]}")
    logger.info(f"NULL nome_municipio: {nulls[3]}")
    
    # Verificar duplicatas
    dups = con.execute("""
        SELECT COUNT(*) 
        FROM (
            SELECT geocode, data_inicio_semana, COUNT(*) as cnt
            FROM gold_data
            GROUP BY geocode, data_inicio_semana
            HAVING cnt > 1
        )
    """).fetchone()[0]
    logger.info(f"Duplicatas (geocode + data): {dups}")
    
    # === SAMPLE DATA ===
    logger.info("\n" + "="*80)
    logger.info("📋 AMOSTRA DE DADOS (5 registros com clima)")
    logger.info("="*80)
    
    sample = con.execute("""
        SELECT 
            nome_municipio,
            uf,
            data_inicio_semana,
            casos_notificados,
            inmet_temp_media,
            inmet_precip_tot
        FROM gold_data
        WHERE inmet_temp_media IS NOT NULL
        ORDER BY RANDOM()
        LIMIT 5
    """).fetchall()
    
    for row in sample:
        logger.info(f"  {row[0]}/{row[1]} - {row[2]} | Casos: {row[3]}, Temp: {row[4]:.1f}°C, Precip: {row[5]:.1f}mm")
    
    logger.info("\n" + "="*80)
    logger.info("✅ Análise concluída!")
    logger.info("="*80)
    
    con.close()

if __name__ == "__main__":
    main()
