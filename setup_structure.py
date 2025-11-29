import os
from pathlib import Path

def setup_project():
    project_name = "eco-sentinel"
    base_path = Path(".")

    # Estrutura de Diretórios (Híbrida: Astro + Data Engineering)
    structure = [
        "data/bronze",          # Data Lake Local
        "data/silver",
        "data/gold",
        "docs/architecture",    # Documentação
        "src/common",           # Código reutilizável
        "src/connectors",       # Scripts de extração (APIs)
        "src/jobs/silver",      # Transformações Spark/Pandas
        "src/jobs/gold",
        "notebooks",            # Sandboxing
        "tests/unit",           # Testes
    ]

    files_to_create = {
        "src/__init__.py": "",
        "src/common/__init__.py": "",
        "src/common/logging_config.py": "# Placeholder para config de logs\n",
        "src/connectors/__init__.py": "",
        "src/connectors/infodengue_api.py": "# Placeholder conector InfoDengue\n",
        "README.md": "# Será sobrescrito pelo conteúdo completo\n",
        ".env": "AIRFLOW_ENV=dev\n",
        ".gitignore": "" # Será anexado
    }

    print(f"🚀 Configurando estrutura Sênior para {project_name} com Astronomer...")

    # 1. Criar Pastas
    for folder in structure:
        dir_path = base_path / folder
        dir_path.mkdir(parents=True, exist_ok=True)
        print(f"✅ Pasta: {folder}/")

    # 2. Criar Arquivos placeholder (sem sobrescrever se já existir lógica)
    for file_path, content in files_to_create.items():
        p = base_path / file_path
        if not p.exists():
            p.write_text(content, encoding='utf-8')
            print(f"📄 Arquivo: {file_path}")
    
    # 3. Atualizar .gitignore (Preservar o do Astro e adicionar o nosso)
    gitignore_path = base_path / ".gitignore"
    custom_ignores = "\n\n# --- ECO-SENTINEL CUSTOM ---\ndata/\n.env\n.vscode/\n__pycache__/\n*.parquet\n*.json\n"
    
    current_content = gitignore_path.read_text() if gitignore_path.exists() else ""
    if "ECO-SENTINEL" not in current_content:
        with open(gitignore_path, "a") as f:
            f.write(custom_ignores)
        print("🛡️ .gitignore atualizado.")

    print("\n🏁 Estrutura pronta! Agora atualize o README.md e o requirements.txt.")

if __name__ == "__main__":
    setup_project()