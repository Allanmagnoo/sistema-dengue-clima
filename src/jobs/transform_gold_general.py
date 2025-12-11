import pandas as pd
import logging
from pathlib import Path
import pyarrow.dataset as ds
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def transform_gold_municipal_panel(data_path):
    """
    Consolidates Silver data (Dengue, SNIS, IBGE) into a Gold municipal panel.
    Granularity: Municipality (7 digits) x Year
    """
    silver_path = data_path / "silver"
    gold_path = data_path / "gold"
    
    # 1. Load IBGE Data (Aggregated from Census Sectors to Municipality)
    logger.info("Loading IBGE Census data...")
    ibge_file = silver_path / "ibge" / "censo_agregado_2022.parquet"
    if not ibge_file.exists():
        logger.error(f"IBGE file not found: {ibge_file}")
        return

    df_ibge = pd.read_parquet(ibge_file)
    
    # Extract municipality code (first 7 digits of sector code)
    # The 'id_setor_censitario' is usually 15 digits. First 7 are municipality.
    if 'id_setor_censitario' in df_ibge.columns:
        df_ibge['id_municipio'] = df_ibge['id_setor_censitario'].astype(str).str.slice(0, 7)
    
    # Aggregate IBGE metrics by Municipality
    # Sum: Population, Households
    # Mean (Weighted?): Density. Better to recalculate density = Sum(Pop) / Sum(Area)
    
    ibge_agg = df_ibge.groupby('id_municipio').agg({
        'populacao_total': 'sum',
        'domicilios_total': 'sum',
        'domicilios_particulares': 'sum',
        'area_total_km2': 'sum',
        'area_domiciliada_km2': 'sum'
    }).reset_index()
    
    # Recalculate densities at municipal level
    ibge_agg['densidade_demografica_geral'] = ibge_agg['populacao_total'] / ibge_agg['area_total_km2']
    ibge_agg['densidade_demografica_domiciliada'] = ibge_agg['populacao_total'] / ibge_agg['area_domiciliada_km2']
    
    logger.info(f"IBGE Aggregated Shape: {ibge_agg.shape}")

    # 2. Load SNIS Data
    logger.info("Loading SNIS data...")
    snis_file = silver_path / "snis" / "snis_agregado_1995_2021.parquet"
    if not snis_file.exists():
         logger.error(f"SNIS file not found: {snis_file}")
         return
         
    df_snis = pd.read_parquet(snis_file)
    # Ensure id_municipio is string 7 digits
    df_snis['id_municipio'] = df_snis['id_municipio'].astype(str).str.replace(r'\.0$', '', regex=True)
    
    logger.info(f"SNIS Shape: {df_snis.shape}")

    # 3. Load Dengue Data (Partitioned)
    logger.info("Loading Dengue data...")
    dengue_path = silver_path / "silver_dengue"
    if not dengue_path.exists():
        logger.error(f"Dengue path not found: {dengue_path}")
        return
        
    # Using PyArrow dataset for partitioned reading
    dataset = ds.dataset(dengue_path, format="parquet", partitioning="hive")
    # Columns needed: id_municipio (usually in data or derived?), data/ano, cases count
    # Let's read a sample to check schema if needed, but assuming standard columns
    # We need to aggregate cases by municipality and year.
    # Note: The partitioning is by 'uf' and 'ano_epidemiologico'.
    
    # We will read all and aggregate. For large datasets, DuckDB or doing it in chunks is better.
    # Assuming it fits in memory for now (since it's filtered by user scope likely).
    
    # We need 'dt_notificacao' or similar to count cases.
    # Let's inspect columns first via a small read if we were unsure, but let's assume standard SINAN.
    # Usually: 'id_municipio_residencia' or 'id_municipio_notificacao'. We want residence for risk analysis.
    
    # Read specific columns to save memory
    columns_needed = ['municipio_codigo', 'dt_notificacao'] # Adjust column names based on previous knowledge or inspection
    
    # Inspect first file to get column names
    sample_file = next(dengue_path.glob("**/*.parquet"))
    sample_df = pd.read_parquet(sample_file)
    logger.info(f"Dengue Sample Columns: {sample_df.columns.tolist()}")
    
    # Standardize column name for aggregation
    # Looking for municipality code. usually 'ibge_code' or 'municipio_codigo'.
    # Looking for date.
    
    muni_col = next((c for c in sample_df.columns if 'geocode' in c.lower()), None)
    
    if not muni_col:
        logger.error("Could not identify municipality column in Dengue data (expected 'geocode')")
        return

    # Load full dataset (subset of cols)
    # The InfoDengue data is weekly ('data_inicio_semana', 'semana_epidemiologica').
    # We need to aggregate 'casos_notificados' or 'casos_estimados' by year.
    # 'casos_estimados' is often better for analysis.
    
    # Read columns: geocode, ano_epidemiologico (partition), casos_estimados
    # Note: 'ano_epidemiologico' is a partition key, so it might not be in the file content but in metadata.
    # When using dataset.to_table(), partition keys are included if selected.
    
    dengue_table = dataset.to_table(columns=[muni_col, 'ano_epidemiologico', 'casos_estimados']) 
    df_dengue = dengue_table.to_pandas()
    
    # Rename for consistency
    df_dengue.rename(columns={muni_col: 'id_municipio', 'ano_epidemiologico': 'ano'}, inplace=True)
    df_dengue['id_municipio'] = df_dengue['id_municipio'].astype(str).str.replace(r'\.0$', '', regex=True)
    
    # Aggregate: Sum cases per Municipality per Year
    dengue_agg = df_dengue.groupby(['id_municipio', 'ano'])['casos_estimados'].sum().reset_index()
    dengue_agg.rename(columns={'casos_estimados': 'total_casos_dengue'}, inplace=True)
    
    logger.info(f"Dengue Aggregated Shape: {dengue_agg.shape}")

    # 4. JOIN EVERYTHING (The "Gold" Logic)
    # Base spine: Dengue occurrences (Municipality x Year) OR SNIS (Municipality x Year).
    # Since we want to analyze risk even where there are 0 cases (maybe?), a full cross join of (All Munis x All Years) would be ideal.
    # For now, let's outer join Dengue and SNIS.
    
    df_gold = pd.merge(dengue_agg, df_snis, on=['id_municipio', 'ano'], how='outer')
    
    # Fill 0 cases for years present in SNIS but not in Dengue (assuming surveillance was active)
    # Be careful: missing data vs 0 cases. 
    # For this MVP, we leave NaNs where data is missing, but 'total_casos_dengue' could be 0 if the row exists.
    # If the row was created by SNIS side, dengue cases are NaN. Let's fill with 0? 
    # Better to leave as NaN if we aren't sure if that year had data collected.
    # But usually SINAN covers all Brazil.
    
    # Join IBGE (Static 2022 Data)
    # We join by 'id_municipio'. This replicates 2022 IBGE data for ALL years in the panel.
    # This is a common simplification when annual census is not available.
    df_gold = pd.merge(df_gold, ibge_agg, on='id_municipio', how='left')
    
    # Calculate Incidence (Cases / Pop * 100k)
    # Using 'populacao_total' from IBGE 2022 as denominator for all years.
    # Warning: For 1995 this is inaccurate, but for 2015-2024 it's acceptable.
    df_gold['incidencia_dengue_100k'] = (df_gold['total_casos_dengue'] / df_gold['populacao_total']) * 100000
    
    # Sort
    df_gold.sort_values(by=['id_municipio', 'ano'], inplace=True)
    
    # Save Gold
    target_dir = gold_path / "paineis"
    target_dir.mkdir(parents=True, exist_ok=True)
    output_file = target_dir / "painel_municipal_dengue_saneamento.parquet"
    
    logger.info(f"Saving Gold Panel: {output_file}")
    logger.info(f"Final Shape: {df_gold.shape}")
    logger.info(f"Columns: {df_gold.columns.tolist()}")
    
    df_gold.to_parquet(output_file, index=False)
    logger.info("Gold transformation completed.")

if __name__ == "__main__":
    BASE_DIR = Path(r"d:\_data-science\GitHub\sistema-dengue-clima\data")
    transform_gold_municipal_panel(BASE_DIR)
