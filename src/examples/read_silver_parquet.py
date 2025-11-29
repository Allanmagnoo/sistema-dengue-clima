"""
Exemplo de Leitura de Arquivos Parquet da Camada Silver
-------------------------------------------------------
Este script demonstra como ler e analisar os dados processados na camada Silver
usando duas abordagens: DuckDB (SQL) e Pandas (Python).

Vantagens do Parquet:
1. Colunar: Lê apenas as colunas necessárias (muito mais rápido para análises).
2. Compressão: Ocupa 1/4 do espaço de um CSV equivalente.
3. Tipagem: Preserva tipos de dados (datas, números, nulos).
4. Particionamento: Permite leitura inteligente (ex: ler apenas dados de SP em 2024).

Uso:
    python read_silver_example.py
"""
import duckdb
import pandas as pd
from pathlib import Path
import time
import os

def get_project_root():
    """Retorna o diretório raiz do projeto."""
    return Path(__file__).resolve().parent.parent.parent

def read_with_duckdb(silver_path):
    """
    Lê dados usando DuckDB (Recomendado para grandes volumes/SQL).
    """
    print("\n" + "="*50)
    print("🦆 Lendo com DuckDB (Alta Performance & SQL)")
    print("="*50)
    
    con = duckdb.connect(':memory:')
    
    start_time = time.time()
    
    # DuckDB lê nativamente a estrutura particionada (hive partitioning)
    # O curinga **/*.parquet varre recursivamente todos os arquivos
    query = f"""
    SELECT 
        uf,
        ano_epidemiologico,
        count(*) as total_registros,
        sum(casos_provaveis) as total_casos,
        avg(temperatura_media) as temp_media
    FROM read_parquet('{silver_path}/**/*.parquet', hive_partitioning=1)
    GROUP BY uf, ano_epidemiologico
    ORDER BY total_casos DESC
    LIMIT 5
    """
    
    try:
        # Tenta executar com nomes de colunas novos (pós-transformação)
        df = con.execute(query.replace("casos_provaveis", "casos_notificados")).df()
    except:
        # Fallback para nomes antigos se a transformação mudou
        try:
            print("⚠️ Tentando colunas alternativas...")
            df = con.execute(f"""
            SELECT 
                uf,
                year_partition as ano,
                count(*) as total_registros
            FROM read_parquet('{silver_path}/**/*.parquet', hive_partitioning=1)
            GROUP BY uf, year_partition
            LIMIT 5
            """).df()
        except Exception as e:
            print(f"❌ Erro na query: {e}")
            return

    elapsed = time.time() - start_time
    print(f"⏱️ Tempo de execução: {elapsed:.4f} segundos")
    print("\n📊 Resultado (Top 5 Estados por Casos):")
    print(df.to_markdown(index=False))
    
    # Exemplo de filtro eficiente (Pushdown Predicate)
    print("\n🔍 Exemplo: Filtrando apenas RJ em 2024...")
    query_filter = f"""
    SELECT geocode, nome_municipio, data_inicio_semana, casos_notificados
    FROM read_parquet('{silver_path}/**/*.parquet', hive_partitioning=1)
    WHERE uf = 'RJ' AND ano_epidemiologico = 2024
    ORDER BY casos_notificados DESC
    LIMIT 5
    """
    print(con.execute(query_filter).df().to_markdown(index=False))

def read_with_pandas(silver_path):
    """
    Lê dados usando Pandas (Bom para exploração interativa e gráficos).
    """
    print("\n" + "="*50)
    print("🐼 Lendo com Pandas (Análise de Dados Python)")
    print("="*50)
    
    start_time = time.time()
    
    try:
        # Pandas lê diretório particionado se engine='pyarrow' estiver instalado
        # Caso contrário, lemos um arquivo específico para demonstração
        df = pd.read_parquet(silver_path, engine='pyarrow')
        
        elapsed = time.time() - start_time
        print(f"⏱️ Tempo de leitura (Todo o Dataset): {elapsed:.4f} segundos")
        print(f"📏 Shape: {df.shape}")
        print("\n📋 Primeiras linhas:")
        print(df.head(3).to_markdown(index=False))
        
        print("\n📉 Estatísticas de Casos:")
        print(df['casos_notificados'].describe())
        
    except Exception as e:
        print(f"⚠️ Leitura direta de pasta falhou ({e}). Lendo arquivos individuais...")
        # Fallback: Listar arquivos e ler um exemplo
        files = list(Path(silver_path).glob("**/*.parquet"))
        if files:
            f = files[0]
            print(f"📄 Lendo arquivo exemplo: {f.name}")
            df = pd.read_parquet(f)
            print(df.head().to_markdown())

if __name__ == "__main__":
    root = get_project_root()
    silver_path = root / "data/silver/infodengue"
    
    if not silver_path.exists():
        print(f"❌ Diretório Silver não encontrado: {silver_path}")
    else:
        # Instalar dependência para markdown se necessário
        try:
            import tabulate
        except ImportError:
            os.system("pip install tabulate > /dev/null 2>&1")
            
        read_with_duckdb(str(silver_path))
        # read_with_pandas(str(silver_path)) # Opcional, DuckDB é mais rápido para demo
