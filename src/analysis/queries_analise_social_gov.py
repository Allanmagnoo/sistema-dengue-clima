"""
📊 RELATÓRIO DE IMPACTO SOCIAL E GOVERNAMENTAL - ECO-SENTINEL (V2 - Otimizado)
------------------------------------------------------------------------------
Ajustes: Uso de 'casos_notificados' para maior cobertura e filtros climáticos ajustados.
"""

import duckdb
import pandas as pd
from pathlib import Path
import logging

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.float_format', '{:.2f}'.format)

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("EcoSentinelReport")

def run_query(con, title, query, description):
    print("\n" + "="*80)
    logger.info(f"🔎 ANÁLISE: {title}")
    print("="*80)
    logger.info(f"📝 Objetivo: {description}\n")
    try:
        df = con.execute(query).fetchdf()
        if df.empty:
            logger.warning("⚠️ Nenhum resultado encontrado.")
        else:
            print(df)
    except Exception as e:
        logger.error(f"❌ Erro: {e}")

def main():
    current_dir = Path(__file__).resolve().parent
    base_dir = current_dir.parent.parent
    gold_path = base_dir / "data/gold/dengue_clima.parquet"
    
    con = duckdb.connect(database=':memory:')
    con.execute(f"CREATE VIEW dengue_gov AS SELECT * FROM read_parquet('{gold_path}')")

    # 1. EPIDEMIA (Usando Notificados para evitar NaNs)
    query_epidemia = """
    SELECT 
        uf,
        nome_municipio,
        MAX(incidencia_100k) as pico_incidencia,
        SUM(casos_notificados) as total_notificacoes,
        MAX(semana_epidemiologica) as ultima_semana
    FROM dengue_gov
    WHERE ano_epidemiologico = 2024
    GROUP BY uf, nome_municipio
    HAVING MAX(incidencia_100k) > 300
    ORDER BY pico_incidencia DESC
    LIMIT 10;
    """
    run_query(con, "TOP 10 MUNICÍPIOS CRÍTICOS (Incidência)", query_epidemia, 
              "Municípios com surtos explosivos de dengue em 2024.")

    # 2. ALERTA PRECOCE (Mantido, pois funcionou bem)
    query_alerta = """
    WITH RecentStats AS (
        SELECT 
            geocode, nome_municipio, uf, semana_epidemiologica, casos_notificados,
            LAG(casos_notificados, 1) OVER (PARTITION BY geocode ORDER BY semana_epidemiologica) as casos_prev
        FROM dengue_gov WHERE ano_epidemiologico = 2024
    )
    SELECT 
        nome_municipio, uf, semana_epidemiologica, 
        casos_notificados as atual, casos_prev as anterior,
        ROUND(((casos_notificados - casos_prev) / NULLIF(casos_prev, 0)) * 100, 1) as crescimento_pct
    FROM RecentStats
    WHERE casos_notificados > 50 AND casos_prev > 10
    ORDER BY semana_epidemiologica DESC, crescimento_pct DESC
    LIMIT 10;
    """
    run_query(con, "ALERTA DE SURTO IMINENTE", query_alerta, 
              "Municípios com aceleração súbita de casos nas últimas semanas.")

    # 3. CLIMA (Relaxado e focado em capitais/grandes centros com dados)
    query_clima = """
    SELECT 
        nome_municipio, uf, semana_epidemiologica,
        casos_notificados,
        inmet_precip_tot as chuva_atual,
        inmet_precip_tot_lag3 as chuva_lag3,
        inmet_temp_media_lag3 as temp_lag3
    FROM dengue_gov
    WHERE inmet_precip_tot_lag3 IS NOT NULL
      AND casos_notificados > 50
      AND (inmet_precip_tot_lag3 > 20 OR inmet_temp_media_lag3 > 25) -- Filtro mais brando
    ORDER BY casos_notificados DESC
    LIMIT 10;
    """
    run_query(con, "ANÁLISE BIO-CLIMÁTICA (Municípios Monitorados)", query_clima, 
              "Relação entre condições favoráveis ao vetor (calor/chuva há 3 semanas) e casos atuais.")

    # 4. RANKING ESTADUAL (Corrigido com SUM(casos_notificados))
    query_regional = """
    SELECT 
        uf,
        CAST(SUM(casos_notificados) AS INTEGER) as total_notificacoes,
        ROUND((SUM(casos_notificados) / (SUM(populacao) / COUNT(DISTINCT semana_epidemiologica))) * 100000, 0) as incidencia_media
    FROM dengue_gov
    WHERE ano_epidemiologico = 2024
    GROUP BY uf
    ORDER BY total_notificacoes DESC
    LIMIT 10;
    """
    run_query(con, "RANKING DE ESTADOS (Volume Total)", query_regional, 
              "Estados com maior carga absoluta de doença no sistema de saúde.")

    con.close()

if __name__ == "__main__":
    main()
