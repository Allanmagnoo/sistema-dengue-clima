import duckdb
import pandas as pd

# Conectar ao DuckDB
conn = duckdb.connect()

print("=== VERIFICANDO SOBREPOSIÇÃO DE GEOCODES ===")

# Verificar geocodes únicos em cada fonte
geocodes_dengue = conn.execute("""
    SELECT DISTINCT geocode
    FROM 'data/silver/infodengue/**/*.parquet'
    WHERE ano_epidemiologico = 2024
""").df()

geocodes_clima = conn.execute("""
    SELECT DISTINCT m.geocode
    FROM 'data/silver/inmet/**/*.parquet' i
    JOIN 'data/silver/mapping_estacao_geocode.parquet' m ON i.estacao_id = m.estacao_id
    WHERE i.ano = 2024
""").df()

print(f"Geocodes únicos em dengue: {len(geocodes_dengue)}")
print(f"Geocodes únicos em clima: {len(geocodes_clima)}")

# Verificar intersecção
intersecao = conn.execute("""
    SELECT COUNT(*) as comuns
    FROM (
        SELECT DISTINCT geocode
        FROM 'data/silver/infodengue/**/*.parquet'
        WHERE ano_epidemiologico = 2024
    ) d
    INNER JOIN (
        SELECT DISTINCT m.geocode
        FROM 'data/silver/inmet/**/*.parquet' i
        JOIN 'data/silver/mapping_estacao_geocode.parquet' m ON i.estacao_id = m.estacao_id
        WHERE i.ano = 2024
    ) c ON d.geocode = c.geocode
""").df()

print(f"Geocodes em comum: {intersecao['comuns'].iloc[0]}")

# Verificar alguns exemplos de geocodes de dengue
print(f"\n=== AMOSTRA DE GEOCODES DE DENGUE ===")
print(geocodes_dengue.head(10))

print(f"\n=== AMOSTRA DE GEOCODES DE CLIMA ===")
print(geocodes_clima.head(10))