import cudf
import os
import glob

def test_rapids_read():
    print("Iniciando teste do Rapids (cuDF)...")
    
    # Caminho base relativo ao script
    # O script está em src/jobs/
    # Os dados estão em data/bronze/...
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(script_dir))
    base_path = os.path.join(project_root, "data", "bronze", "infodengue", "municipios", "disease=dengue", "year=2024")
    
    print(f"Procurando arquivos em: {base_path}")
    
    # Encontrar o primeiro arquivo CSV no diretório
    csv_files = glob.glob(os.path.join(base_path, "*.csv"))
    
    if not csv_files:
        print(f"Nenhum arquivo CSV encontrado em {base_path}")
        return

    test_file = csv_files[0]
    print(f"Lendo arquivo: {test_file}")
    
    try:
        # Leitura com cuDF
        gdf = cudf.read_csv(test_file)
        
        print("\n--- Primeiras 5 linhas ---")
        print(gdf.head())
        
        print("\n--- Informações do DataFrame ---")
        print(gdf.info())
        
        print("\n--- Tipo do Objeto ---")
        print(type(gdf))
        
        print("\nTeste do Rapids concluído com SUCESSO!")
        
    except Exception as e:
        print(f"\nERRO ao usar Rapids: {e}")

if __name__ == "__main__":
    test_rapids_read()
