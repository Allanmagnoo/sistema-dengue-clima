import pandas as pd
import logging
from pathlib import Path
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def transform_snis_data(source_path, target_path):
    """
    Transforms SNIS Bronze CSV to Silver Parquet.
    Focuses on Water and Sewage coverage indicators for Dengue analysis.
    """
    file_path = source_path / "br_mdr_snis_municipio_agua_esgoto.csv"
    
    if not file_path.exists():
        logger.error(f"File not found: {file_path}")
        return

    logger.info(f"Reading SNIS CSV: {file_path}")
    
    # Read CSV (handling potential encoding or separator issues if any, but default seems comma)
    # The file has a header on line 1 (index 0).
    try:
        df = pd.read_csv(file_path, low_memory=False)
    except Exception as e:
        logger.error(f"Error reading CSV: {e}")
        return

    logger.info(f"Raw shape: {df.shape}")

    # Standardize column names
    df.columns = [c.strip().lower() for c in df.columns]

    # Fix specific typo in source file
    if 'populacao_atentida_esgoto' in df.columns:
        df.rename(columns={'populacao_atentida_esgoto': 'populacao_atendida_esgoto'}, inplace=True)

    # Select relevant columns for Dengue analysis
    # Core indicators: Coverage, Urbanization, Losses
    selected_cols = [
        'ano',
        'id_municipio',
        'sigla_uf',
        'populacao_urbana',
        'populacao_atendida_agua',
        'populacao_urbana_atendida_agua',
        'populacao_atendida_esgoto',
        'populacao_urbana_atendida_esgoto',
        'indice_atendimento_total_agua',
        'indice_atendimento_urbano_agua',
        'indice_coleta_esgoto',
        'indice_tratamento_esgoto',
        'indice_perda_distribuicao_agua',
        'volume_agua_produzido',
        'volume_esgoto_coletado',
        'investimento_total_municipio',
        'investimento_total_prestador'
    ]

    # Filter only existing columns
    existing_cols = [c for c in selected_cols if c in df.columns]
    missing_cols = [c for c in selected_cols if c not in df.columns]
    
    if missing_cols:
        logger.warning(f"Missing columns in source: {missing_cols}")
    
    df_silver = df[existing_cols].copy()

    # Data Type Conversions
    if 'id_municipio' in df_silver.columns:
        df_silver['id_municipio'] = df_silver['id_municipio'].astype(str).str.replace(r'\.0$', '', regex=True)
    
    if 'ano' in df_silver.columns:
        df_silver['ano'] = pd.to_numeric(df_silver['ano'], errors='coerce').fillna(0).astype(int)

    # Numeric conversion for indicators (handling strings with commas or other issues)
    # The sample showed dots for decimals (e.g. 324.92), so standard float conversion should work.
    # But some columns might have nulls or formatting issues.
    numeric_cols = [c for c in df_silver.columns if c not in ['ano', 'id_municipio', 'sigla_uf']]
    for col in numeric_cols:
        df_silver[col] = pd.to_numeric(df_silver[col], errors='coerce')

    # Semantic Renaming (Optional, but good for Silver)
    # Mapping to clearer names
    rename_map = {
        'populacao_atendida_agua': 'agua_pop_atendida_total',
        'populacao_urbana_atendida_agua': 'agua_pop_atendida_urbana',
        'populacao_atendida_esgoto': 'esgoto_pop_atendida_total',
        'populacao_urbana_atendida_esgoto': 'esgoto_pop_atendida_urbana',
        'indice_atendimento_total_agua': 'agua_cobertura_total_pct',
        'indice_atendimento_urbano_agua': 'agua_cobertura_urbana_pct',
        'indice_coleta_esgoto': 'esgoto_cobertura_total_pct',
        'indice_tratamento_esgoto': 'esgoto_tratamento_pct',
        'indice_perda_distribuicao_agua': 'agua_perda_distribuicao_pct',
        'volume_agua_produzido': 'agua_volume_produzido_1000m3',
        'volume_esgoto_coletado': 'esgoto_volume_coletado_1000m3'
    }
    df_silver.rename(columns=rename_map, inplace=True)

    # Output path
    target_dir = target_path / "snis"
    target_dir.mkdir(parents=True, exist_ok=True)
    output_file = target_dir / "snis_agregado_1995_2021.parquet"

    logger.info(f"Saving to Parquet: {output_file}")
    
    # Partition by year might be too many files if we have 25 years * small files.
    # But for query efficiency it's often good. 
    # However, one single parquet for 21MB (estimated) is fine.
    # 119k rows is small for Parquet. No need to partition.
    
    df_silver.to_parquet(output_file, index=False)
    logger.info("SNIS transformation completed successfully.")

if __name__ == "__main__":
    BASE_DIR = Path(r"d:\_data-science\GitHub\sistema-dengue-clima\data")
    BRONZE_DIR = BASE_DIR / "bronze"
    SILVER_DIR = BASE_DIR / "silver"
    
    transform_snis_data(BRONZE_DIR, SILVER_DIR)
