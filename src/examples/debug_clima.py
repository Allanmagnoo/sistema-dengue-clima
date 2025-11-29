import duckdb
import pandas as pd

# Conectar ao DuckDB
conn = duckdb.connect()

# Carregar a tabela Gold
gold_df = conn.execute("""
    SELECT * 
    FROM 'data/gold/dengue_clima.parquet'
    LIMIT 50
""").df()

print("=== ANÁLISE DA CAMADA GOLD ===")
print(f"Total de registros: {len(gold_df)}")
print(f"\nColunas disponíveis: {list(gold_df.columns)}")
print(f"\nResumo de valores NaN:")
print(gold_df.isnull().sum())

print(f"\n=== AMOSTRA DE DADOS ===")
print(gold_df.head(10))

# Verificar dados de clima diário
print(f"\n=== VERIFICANDO DADOS DE CLIMA DIÁRIO ===")
clima_diario = conn.execute("""
    SELECT 
        COUNT(*) as total_registros,
        COUNT(DISTINCT estacao_id) as estacoes_unicas,
        MIN(data) as data_minima,
        MAX(data) as data_maxima,
        AVG(temperatura_media) as temp_media_geral,
        AVG(precipitacao_total) as precip_media_geral
    FROM 'data/silver/inmet/**/*.parquet'
    WHERE ano = 2024
""").df()

print(clima_diario)

# Verificar mapeamento
print(f"\n=== VERIFICANDO MAPEAMENTO ===")
mapping = conn.execute("""
    SELECT COUNT(*) as total_mapping
    FROM 'data/silver/mapping_estacao_geocode.parquet'
""").df()

print(mapping)

# Verificar dados de dengue
print(f"\n=== VERIFICANDO DADOS DE DENGUE ===")
dengue = conn.execute("""
    SELECT 
        COUNT(*) as total_dengue,
        COUNT(DISTINCT geocode) as geocodes_dengue,
        MIN(data_inicio_semana) as semana_min,
        MAX(data_inicio_semana) as semana_max
    FROM 'data/silver/infodengue/**/*.parquet'
    WHERE ano_epidemiologico = 2024
""").df()

print(dengue)