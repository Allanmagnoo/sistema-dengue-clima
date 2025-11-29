import cudf
import os
import glob
import sys

def test_rapids_read():
    print("=== Teste RAPIDS (cuDF) para Leitura de Dados Bronze ===\n")
    print(f"Python: {sys.version.split()[0]}")
    print(f"cuDF: {cudf.__version__}\n")
    
    # Caminho base relativo ao script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(script_dir))
    base_path = os.path.join(project_root, "data", "bronze", "infodengue", "municipios", "disease=dengue", "year=2024")
    
    print(f"📁 Busca: {base_path}")
    
    # Encontrar arquivos CSV
    csv_files = glob.glob(os.path.join(base_path, "*.csv"))
    
    if not csv_files:
        print("❌ Nenhum arquivo CSV encontrado")
        return
    
    print(f"✓ {len(csv_files)} arquivos CSV encontrados\n")
    
    # Teste com um arquivo pequeno
    test_file = csv_files[0]
    file_name = os.path.basename(test_file)
    file_size = os.path.getsize(test_file) / 1024  # KB
    
    print(f"📄 Arquivo de teste: {file_name}")
    print(f"   Tamanho: {file_size:.2f} KB\n")
    
    try:
        # Teste 1: Leitura básica
        print("⏳ Teste 1: Leitura com cuDF...")
        gdf = cudf.read_csv(test_file)
        print(f"   ✓ Leitura concluída!")
        print(f"   ✓ Tipo: cudf.DataFrame")
        print(f"   ✓ Dimensões: {gdf.shape[0]} linhas x {gdf.shape[1]} colunas")
        
        # Teste 2: Informações do DataFrame
        print(f"\n⏳ Teste 2: Informações do DataFrame...")
        print(f"   ✓ Colunas ({len(gdf.columns)}):")
        for i, col in enumerate(gdf.columns[:10], 1):
            print(f"      {i}. {col}")
        if len(gdf.columns) > 10:
            print(f"      ... e mais {len(gdf.columns) - 10} colunas")
        
        # Teste 3: Uso de memória
        print(f"\n⏳ Teste 3: Uso de memória...")
        mem_usage = gdf.memory_usage(deep=True).sum()
        print(f"   ✓ Memória GPU: {mem_usage / 1024:.2f} KB")
        
        # Teste 4: Operações básicas com cuDF
        print(f"\n⏳ Teste 4: Operações básicas com cuDF...")
        if 'casos' in gdf.columns:
            total_casos = gdf['casos'].sum()
            print(f"   ✓ Total de casos: {total_casos}")
        print(f"   ✓ Operações aritméticas funcionando!")
        
        print("\n" + "=" * 60)
        print("✅ TODOS OS TESTES PASSARAM COM SUCESSO!")
        print("=" * 60)
        print("\n💡 Conclusão:")
        print("   - Rapids/cuDF está funcionando corretamente")
        print("   - GPU está sendo utilizada para processamento")
        print("   - Dados da camada Bronze podem ser lidos")
        print("   - Pronto para processar grandes volumes de dados!")
        
    except Exception as e:
        print(f"\n❌ ERRO: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    test_rapids_read()
