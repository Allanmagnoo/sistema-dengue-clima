import cudf
import os
import glob
import sys

def test_rapids_read():
    print("=== Iniciando teste do Rapids (cuDF) ===")
    print(f"Python: {sys.version}")
    print(f"cuDF version: {cudf.__version__}")
    
    # Caminho base relativo ao script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(script_dir))
    base_path = os.path.join(project_root, "data", "bronze", "infodengue", "municipios", "disease=dengue", "year=2024")
    
    print(f"\nProcurando arquivos em: {base_path}")
    
    # Encontrar arquivos CSV
    csv_files = glob.glob(os.path.join(base_path, "*.csv"))
    
    if not csv_files:
        print(f"❌ Nenhum arquivo CSV encontrado")
        return
    
    print(f"✓ Encontrados {len(csv_files)} arquivos CSV")
    test_file = csv_files[0]
    print(f"\n📂 Testando com arquivo: {os.path.basename(test_file)}")
    
    try:
        # Leitura com cuDF
        print("\n⏳ Lendo arquivo com cuDF...")
        gdf = cudf.read_csv(test_file)
        
        print(f"✓ Arquivo lido com sucesso!")
        print(f"✓ Tipo: {type(gdf)}")
        print(f"✓ Shape: {gdf.shape}")
        print(f"✓ Colunas: {list(gdf.columns)}")
        print(f"✓ Memória: {gdf.memory_usage(deep=True).sum() / 1024:.2f} KB")
        
        # Mostrar apenas estatísticas básicas
        print(f"\n📊 Primeiras linhas (apenas primeiras 3 colunas):")
        first_cols = gdf.columns[:3]
        print(gdf[first_cols].head().to_pandas())
        
        print("\n✅ Teste do Rapids concluído com SUCESSO!")
        print("=" * 50)
        
    except Exception as e:
        print(f"\n❌ ERRO ao usar Rapids: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    test_rapids_read()
