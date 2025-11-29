
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
INMET_BRONZE_DIR = BRONZE_DIR / "inmet" / "2024"
OUTPUT_PATH = SILVER_DIR / "mapping_estacao_geocode.parquet"

def normalize_text(text):
    """Remove acentos e converte para minúsculas."""
    if not isinstance(text, str):
        return text
    # Normaliza para NFD (Canonical Decomposition) e remove caracteres não-espaçados (acentos)
    nfkd_form = unicodedata.normalize('NFD', text)
    only_ascii = nfkd_form.encode('ASCII', 'ignore').decode('utf-8')
    return only_ascii.lower().strip()

def create_mapping_table():
    """
    Cria uma tabela de mapeamento de estacao_id para geocode, tratando inconsistências nos nomes dos municípios.
    """
    logger.info("--- Iniciando a criação da tabela de mapeamento estacao_id -> geocode ---")

    # 1. Ler o arquivo DTB e criar o mapeamento de município para geocode
    logger.info(f"Lendo arquivo DTB de: {DTB_FILE_PATH}")
    try:
        df_dtb = pd.read_excel(DTB_FILE_PATH, skiprows=6, usecols=["Nome_Município", "Código Município Completo"])
        # A ordem das colunas lidas pelo usecols pode não ser garantida. Corrigindo a atribuição.
        df_dtb.columns = ["nome_municipio", "geocode"] # Temporário para manter a estrutura
        # Verificação e reordenação se necessário
        if df_dtb.iloc[0]['geocode'].isnumeric():
            df_dtb.columns = ["nome_municipio", "geocode"]
        else:
            df_dtb.columns = ["geocode", "nome_municipio"]

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
    for file_path in INMET_BRONZE_DIR.glob("*.CSV"):
        match = station_pattern.search(file_path.name)
        if match:
            estacao_id = match.group(1)
            nome_municipio_raw = match.group(2).replace("_", " ")
            stations_data.append({
                "estacao_id": estacao_id,
                "nome_municipio_inmet": nome_municipio_raw,
                "nome_municipio_normalizado": normalize_text(nome_municipio_raw)
            })
    df_stations = pd.DataFrame(stations_data)
    df_stations["nome_municipio_normalizado"] = df_stations["nome_municipio_normalizado"].astype(str)
    logger.info(f"{len(df_stations)} estações extraídas dos nomes dos arquivos.")

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
    create_mapping_table()