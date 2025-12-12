import sys
from pathlib import Path
from google.cloud import bigquery

# Import functions from the optimized script
# Ensuring the src directory is in path if needed, though running from root should work if installed as package
# or we can just append path.
sys.path.append(str(Path(__file__).parent))
from upload_to_bigquery_optimized import upload_dataset, create_dataset_if_not_exists, get_project_root

def batch_run():
    project_id = "sistema-dengue-clima"
    dataset_id = "01_brz_raw_sistema_dengue"
    
    root = get_project_root()
    
    # List of paths provided by user (relative to data/bronze or absolute)
    # We'll assume they are in data/bronze based on the user request context
    paths_to_upload = [
        "data/bronze/2Planilhas_AE2021",
        "data/bronze/Descricoes_limites_Setores_Censitarios_20251010",
        "data/bronze/DTB_2024",
        "data/bronze/inmet",
        "data/bronze/Instituto Brasileiro de Geografia e Estatística",
        "data/bronze/Planilhas_AE2019",
        "data/bronze/Planilhas_AE2020",
        "data/bronze/br_mdr_snis_municipio_agua_esgoto.csv",
        "data/bronze/relatorio_indicadores_Brasil.xlsx"
    ]
    
    client = bigquery.Client(project=project_id)
    create_dataset_if_not_exists(client, dataset_id, project_id)
    
    print(f"🚀 Starting batch upload of {len(paths_to_upload)} items...")

    for path_str in paths_to_upload:
        full_path = root / path_str
        if not full_path.exists():
            print(f"⚠️ Path not found, skipping: {full_path}")
            continue
            
        print(f"📂 Processing: {path_str}")
        try:
            upload_dataset(full_path, dataset_id, project_id)
        except Exception as e:
            print(f"❌ Error processing {path_str}: {e}")

if __name__ == "__main__":
    batch_run()
