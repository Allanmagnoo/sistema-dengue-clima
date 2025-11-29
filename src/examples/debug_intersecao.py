import duckdb
import pandas as pd

# Conectar ao DuckDB
conn = duckdb.connect()

print("=== VERIFICANDO INTERSECÇÃO DE GEOCODES ===")

# Verificar intersecção de geocodes
intersecao = conn.execute("""
    SELECT COUNT(*) as comuns
    FROM (
        SELECT DISTINCT geocode
        FROM 'data/silver/infodengue/**/*.parquet'
        WHERE ano_epidemiologico = 2024
    ) d
    INNER JOIN (
        SELECT DISTINCT m.geocode
        FROM read_parquet('data/silver/inmet/**/*.parquet', union_by_name=True) i
        JOIN 'data/silver/mapping_estacao_geocode.parquet' m ON i.estacao_id = m.estacao_id
        WHERE i.ano = 2024
    ) c ON d.geocode = c.geocode
""").df()

print(f"Geocodes em comum: {intersecao['comuns'].iloc[0]}")

# Verificar quais geocodes específicos estão em comum
geocodes_comuns = conn.execute("""
    SELECT d.geocode, 
           ANY_VALUE(dengue.nome_municipio) as nome_municipio,
           COUNT(*) as ocorrencias_dengue
    FROM (
        SELECT DISTINCT geocode
        FROM 'data/silver/infodengue/**/*.parquet'
        WHERE ano_epidemiologico = 2024
    ) d
    INNER JOIN (
        SELECT DISTINCT m.geocode
        FROM read_parquet('data/silver/inmet/**/*.parquet', union_by_name=True) i
        JOIN 'data/silver/mapping_estacao_geocode.parquet' m ON i.estacao_id = m.estacao_id
        WHERE i.ano = 2024
    ) c ON d.geocode = c.geocode
    JOIN 'data/silver/infodengue/**/*.parquet' dengue ON d.geocode = dengue.geocode
    GROUP BY d.geocode
    ORDER BY ocorrencias_dengue DESC
    LIMIT 20
""").df()

print(f"\n=== GEOCODES EM COMUM (TOP 20) ===")
print(geocodes_comuns)