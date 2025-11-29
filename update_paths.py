import os
import re
from pathlib import Path

def update_paths_in_file(file_path):
    """Atualiza caminhos absolutos para relativos em arquivos Python"""
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        
        # Use regex to match the complete path pattern including subdirectories
        # Pattern to match: r"D:\_data-science\GitHub\eco-sentinel\data\silver\inmet" or similar
        import re
        
        # Match raw strings with the complete path including subdirectories
        pattern = r'r"D:\\_data-science\\GitHub\\eco-sentinel(\\[^"]*)"'
        matches = re.findall(pattern, content)
        
        for match in matches:
            # Extract the subdirectory part (e.g., \data\silver\inmet)
            subdirs = match.strip('\\')
            if subdirs:
                # Convert backslashes to forward slashes for pathlib
                path_parts = subdirs.replace('\\', '/').split('/')
                # Build the replacement with pathlib
                path_components = ' / '.join(f'"{part}"' for part in path_parts)
                replacement = f'str(pathlib.Path(__file__).parent.parent.parent / {path_components})'
            else:
                # Just the base path
                replacement = 'str(pathlib.Path(__file__).parent.parent.parent)'
            
            # Replace the complete match
            full_match = f'r"D:\\_data-science\\GitHub\\eco-sentinel{match}"'
            content = content.replace(full_match, replacement)
        
        # Also match regular strings (non-raw)
        pattern_regular = r'"D:\\_data-science\\GitHub\\eco-sentinel(\\[^"]*)"'
        matches_regular = re.findall(pattern_regular, content)
        
        for match in matches_regular:
            # Extract the subdirectory part
            subdirs = match.strip('\\')
            if subdirs:
                # Convert backslashes to forward slashes for pathlib
                path_parts = subdirs.replace('\\', '/').split('/')
                # Build the replacement with pathlib
                path_components = ' / '.join(f'"{part}"' for part in path_parts)
                replacement = f'str(pathlib.Path(__file__).parent.parent.parent / {path_components})'
            else:
                # Just the base path
                replacement = 'str(pathlib.Path(__file__).parent.parent.parent)'
            
            # Replace the complete match
            full_match = f'"D:\\_data-science\\GitHub\\eco-sentinel{match}"'
            content = content.replace(full_match, replacement)
        
        # Se houve mudanças, salvar o arquivo
        if content != original_content:
            # Add pathlib import if not present
            if 'import pathlib' not in content and 'from pathlib import' not in content:
                # Find the first import statement and add pathlib after it
                lines = content.split('\n')
                import_line_index = -1
                for i, line in enumerate(lines):
                    if line.strip().startswith('import ') or line.strip().startswith('from '):
                        import_line_index = i
                        break
                
                if import_line_index >= 0:
                    lines.insert(import_line_index + 1, 'import pathlib')
                    content = '\n'.join(lines)
            
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"✅ Atualizado: {file_path}")
            return True
        else:
            print(f"ℹ️  Sem alterações: {file_path}")
            return False
            
    except Exception as e:
        print(f"❌ Erro ao processar {file_path}: {e}")
        return False

def main():
    # Diretório base do projeto
    project_dir = Path("d:/_data-science/GitHub/sistema-dengue-clima")
    
    # Arquivos a serem atualizados
    files_to_update = [
        "src/examples/debug_silver_inmet_dist.py",
        "src/examples/debug_inmet_regex_deep.py",
        "src/examples/debug_inmet_transformation.py",
        "src/examples/debug_silver_inmet.py",
        "src/examples/debug_inmet_filenames.py",
        "src/examples/debug_inmet_read.py",
        "src/examples/debug_silver_filename.py"
    ]
    
    updated_count = 0
    
    for file_path in files_to_update:
        full_path = project_dir / file_path
        if full_path.exists():
            if update_paths_in_file(full_path):
                updated_count += 1
        else:
            print(f"⚠️  Arquivo não encontrado: {full_path}")
    
    print(f"\n📊 Total de arquivos atualizados: {updated_count}")

if __name__ == "__main__":
    main()