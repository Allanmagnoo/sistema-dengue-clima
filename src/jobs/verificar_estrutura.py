"""
Verificar estrutura de diretórios e arquivos após processamento
"""
import os
from pathlib import Path

def check_structure():
    base_dir = Path(".")
    
    print("📁 Verificando estrutura de diretórios...")
    
    # Verificar diretórios esperados
    dirs_to_check = [
        "data/silver",
        "data/silver/inmet", 
        "data/silver/infodengue",
        "data/gold"
    ]
    
    for dir_path in dirs_to_check:
        full_path = base_dir / dir_path
        if full_path.exists():
            print(f"✅ {dir_path} - Existe")
            # Contar arquivos
            files = list(full_path.rglob("*.parquet")) if full_path.is_dir() else []
            print(f"   Arquivos .parquet: {len(files)}")
            if files:
                print(f"   Primeiros 3 arquivos:")
                for f in files[:3]:
                    print(f"     - {f.name}")
        else:
            print(f"❌ {dir_path} - Não existe")
    
    # Verificar arquivos de mapeamento
    mapping_file = base_dir / "data/silver/mapping_estacao_geocode.parquet"
    if mapping_file.exists():
        print(f"✅ Mapeamento estacao_geocode: {mapping_file}")
    else:
        print(f"❌ Mapeamento estacao_geocode: Não encontrado")

if __name__ == "__main__":
    check_structure()