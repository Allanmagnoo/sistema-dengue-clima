"""
Upload INMET Data to BigQuery

Specialized script to handle INMET CSV files which have:
- 8 lines of metadata before the header
- Semicolon (;) as separator
- European decimal format (comma as decimal separator)
"""

import os
import sys
import logging
from pathlib import Path
from google.cloud import bigquery
import pandas as pd
from joblib import Parallel, delayed

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_project_root():
    return Path(__file__).resolve().parent.parent.parent

def read_inmet_file(f):
    """Reads a single INMET CSV file with special handling"""
    try:
        # Read CSV skipping first 8 metadata rows, using ; separator
        df = pd.read_csv(
            f,
            sep=';',
            skiprows=8,
            encoding='latin1',
            decimal=','  # European decimal format
        )
        
        # Extract station info from filename (e.g., INMET_CO_DF_A001_BRASILIA_...)
        filename = f.stem
        parts = filename.split('_')
        if len(parts) >= 5:
            df['regiao'] = parts[1]
            df['uf'] = parts[2]
            df['estacao_id'] = parts[3]
            df['estacao_nome'] = parts[4]
        
        # Extract year from filename or path
        for part in f.parts:
            if part.isdigit() and len(part) == 4:
                df['ano'] = int(part)
                break
        
        return df
        
    except Exception as e:
        logger.error(f"❌ Error reading {f}: {e}")
        return pd.DataFrame()

def main():
    project_id = "sistema-dengue-clima"
    dataset_id = "01_brz_raw_sistema_dengue"
    table_name = "inmet"
    
    root = get_project_root()
    inmet_path = root / "data" / "bronze" / "inmet"
    
    if not inmet_path.exists():
        logger.error(f"❌ Path not found: {inmet_path}")
        return
    
    # Ensure dataset exists
    client = bigquery.Client(project=project_id)
    dataset_ref = f"{project_id}.{dataset_id}"
    try:
        client.get_dataset(dataset_ref)
        logger.info(f"Dataset {dataset_ref} exists.")
    except Exception:
        logger.info(f"Creating dataset {dataset_ref}...")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)
    
    # Find all CSV files
    files = list(inmet_path.rglob("*.CSV")) + list(inmet_path.rglob("*.csv"))
    logger.info(f"Found {len(files)} INMET files.")
    
    if not files:
        logger.warning("No CSV files found!")
        return
    
    # Read files in parallel
    logger.info("Reading files in parallel...")
    dfs = Parallel(n_jobs=-1, verbose=5)(
        delayed(read_inmet_file)(f) for f in files
    )
    
    # Filter empty and concatenate
    dfs = [d for d in dfs if not d.empty]
    logger.info(f"Successfully read {len(dfs)} files.")
    
    if not dfs:
        logger.error("All files failed to read!")
        return
    
    # Concatenate all dataframes
    df = pd.concat(dfs, ignore_index=True)
    logger.info(f"Total rows: {len(df)}")
    
    # Clean column names for BigQuery
    # Remove accents and special characters
    import unicodedata
    
    def sanitize_column(col):
        # Normalize unicode to decompose accented characters
        col = unicodedata.normalize('NFKD', col)
        # Remove non-ASCII characters (accents)
        col = col.encode('ASCII', 'ignore').decode('ASCII')
        # Replace special characters
        col = (col.replace(' ', '_')
                  .replace('.', '_')
                  .replace('(', '')
                  .replace(')', '')
                  .replace('/', '_')
                  .replace('-', '_')
                  .replace(',', '')
                  .lower()
                  .strip())
        # Remove double underscores
        while '__' in col:
            col = col.replace('__', '_')
        return col
    
    df.columns = [sanitize_column(col) for col in df.columns]

    
    # Convert object columns to string
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str)
    
    # Upload to BigQuery
    logger.info(f"📤 Uploading {len(df)} rows to {dataset_id}.{table_name}...")
    
    try:
        df.to_gbq(
            destination_table=f"{dataset_id}.{table_name}",
            project_id=project_id,
            if_exists='replace',
            progress_bar=True
        )
        logger.info(f"✅ Successfully uploaded {table_name}")
    except Exception as e:
        logger.error(f"❌ Failed to upload: {e}")

if __name__ == "__main__":
    main()
