import duckdb
import pandas as pd

# Conectar ao DuckDB
conn = duckdb.connect()

print("=== VERIFICANDO ESTRUTURA DOS DADOS DE CLIMA ===")

# Verificar uma amostra dos dados de clima
clima_sample = conn.execute("""
    SELECT *
    FROM 'data/silver/inmet/**/*.parquet'
    LIMIT 5
""").df()

print("Colunas dos dados de clima:")
print(clima_sample.columns.tolist())
print("\nAmostra de dados de clima:")
print(clima_sample)

print("\n=== VERIFICANDO MAPEAMENTO ===")
mapping_sample = conn.execute("""
    SELECT *
    FROM 'data/silver/mapping_estacao_geocode.parquet'
    LIMIT 5
""").df()

print("Colunas do mapeamento:")
print(mapping_sample.columns.tolist())
print("\nAmostra do mapeamento:")
print(mapping_sample)

print("\n=== VERIFICANDO DADOS DE DENGUE ===")
dengue_sample = conn.execute("""
    SELECT *
    FROM 'data/silver/infodengue/**/*.parquet'
    LIMIT 5
""").df()

print("Colunas dos dados de dengue:")
print(dengue_sample.columns.tolist())
print("\nAmostra de dados de dengue:")
print(dengue_sample)

# Verificar quantos geocodes têm dados de clima
print("\n=== VERIFICANDO COBERTURA DE GEOCODES ===")
cobertura = conn.execute("""
    SELECT 
        COUNT(DISTINCT d.geocode) as geocodes_dengue,
        COUNT(DISTINCT m.geocode) as geocodes_mapping,
        COUNT(DISTINCT c.estacao_id) as estacoes_clima
    FROM 'data/silver/infodengue/**/*.parquet' d
    CROSS JOIN 'data/silver/mapping_estacao_geocode.parquet' m
    CROSS JOIN 'data/silver/inmet/**/*.parquet' c
""").df()

print(cobertura)