import duckdb
import pandas as pd
from pathlib import Path
import re
from loguru import logger
import unicodedata

# --- Configurações ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
DTB_FILE_PATH = BRONZE_DIR / "DTB_2024" / "RELATORIO_DTB_BRASIL_2024_MUNICIPIOS.xls"
INMET_BRONZE_DIR = BRONZE_DIR / "inmet"
OUTPUT_PATH = SILVER_DIR / "silver_mapping_estacao_geocode.parquet"

def normalize_text(text):
    """Remove acentos e converte para minúsculas."""
    if not isinstance(text, str):
        return text
    # Normaliza para NFD (Canonical Decomposition) e remove caracteres não-espaçados (acentos)
    nfkd_form = unicodedata.normalize('NFD', text)
    only_ascii = nfkd_form.encode('ASCII', 'ignore').decode('utf-8')
    return only_ascii.lower().strip()

def main():
    """
    Cria uma tabela de mapeamento de estacao_id para geocode, tratando inconsistências nos nomes dos municípios.
    """
    logger.info("--- Iniciando a criação da tabela de mapeamento estacao_id -> geocode ---")

    # 1. Ler o arquivo DTB e criar o mapeamento de município para geocode
    logger.info(f"Lendo arquivo DTB de: {DTB_FILE_PATH}")
    try:
        df_dtb = pd.read_excel(DTB_FILE_PATH, skiprows=6, usecols=["Nome_Município", "Código Município Completo"])
        # A ordem das colunas lidas pelo usecols pode não ser garantida. Corrigindo a atribuição.
        # Check columns by name instead of assuming order
        df_dtb = df_dtb.rename(columns={"Nome_Município": "nome_municipio", "Código Município Completo": "geocode"})
        
        df_dtb["geocode"] = df_dtb["geocode"].astype(str).str.strip()
        df_dtb["nome_municipio_normalizado"] = df_dtb["nome_municipio"].astype(str).apply(normalize_text)
        logger.info(f"{len(df_dtb)} municípios lidos do arquivo DTB.")
    except Exception as e:
        logger.error(f"Falha ao ler ou processar o arquivo DTB: {e}")
        return

    # 2. Extrair metadados (estacao_id, nome_municipio) dos arquivos do INMET
    logger.info(f"Extraindo metadados das estações do diretório: {INMET_BRONZE_DIR}")
    station_pattern = re.compile(r"INMET_.*?_([A-Z0-9]{4})_(.*?)_\d{2}-\d{2}-\d{4}")
    stations_data = []
    
    # Use recursive glob to find all CSVs in all years
    files = list(INMET_BRONZE_DIR.rglob("*.CSV")) + list(INMET_BRONZE_DIR.rglob("*.csv"))
    
    seen_stations = set()
    
    for file_path in files:
        match = station_pattern.search(file_path.name)
        if match:
            estacao_id = match.group(1)
            if estacao_id in seen_stations:
                continue
                
            nome_municipio_raw = match.group(2).replace("_", " ")
            stations_data.append({
                "estacao_id": estacao_id,
                "nome_municipio_inmet": nome_municipio_raw,
                "nome_municipio_normalizado": normalize_text(nome_municipio_raw)
            })
            seen_stations.add(estacao_id)
            
    df_stations = pd.DataFrame(stations_data)
    if df_stations.empty:
        logger.warning("Nenhuma estação encontrada!")
        return

    df_stations["nome_municipio_normalizado"] = df_stations["nome_municipio_normalizado"].astype(str)
    logger.info(f"{len(df_stations)} estações únicas extraídas.")

    # 3. Juntar as informações para criar o mapeamento final
    logger.info("Juntando informações da DTB e dos metadados das estações.")
    df_mapping = pd.merge(df_stations, df_dtb, on="nome_municipio_normalizado", how="left")

    # Verificação de mapeamentos falhos
    unmapped_stations = df_mapping[df_mapping["geocode"].isnull()]
    if not unmapped_stations.empty:
        logger.warning(f"{len(unmapped_stations)} estações não puderam ser mapeadas para um geocode:")
        for _, row in unmapped_stations.iterrows():
            logger.warning(f"  - Estação: {row['estacao_id']}, Nome no arquivo: '{row['nome_municipio_inmet']}'")

    # 4. Salvar a tabela de mapeamento em formato Parquet
    final_mapping = df_mapping[df_mapping["geocode"].notna()][["estacao_id", "geocode"]]
    if not final_mapping.empty:
        logger.info(f"Salvando {len(final_mapping)} mapeamentos bem-sucedidos em: {OUTPUT_PATH}")
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect()
        con.execute(f"COPY final_mapping TO '{str(OUTPUT_PATH)}' (FORMAT 'PARQUET')")
        con.close()
    else:
        logger.error("Nenhum mapeamento bem-sucedido para salvar. O arquivo Parquet não será criado.")

    logger.info("--- Tabela de mapeamento criada com sucesso! ---")

if __name__ == "__main__":
    main()