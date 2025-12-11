
import os
import sys
import logging
import argparse
from pathlib import Path
from google.cloud import bigquery
import pandas as pd
import db_dtypes

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_project_root():
    return Path(__file__).resolve().parent.parent.parent

def upload_dataset(dataset_path, bq_dataset_id, project_id, if_exists='replace'):
    """Uploads a folder of parquet files or a single parquet file to BigQuery"""
    client = bigquery.Client(project=project_id)
    
    # Determine table name from folder/file name
    table_name = dataset_path.stem
    table_id = f"{project_id}.{bq_dataset_id}.{table_name}"
    
    logger.info(f"🚀 Processing {dataset_path} -> {table_id}")
    
    df = pd.DataFrame()
    
    try:
        if dataset_path.is_file():
            df = pd.read_parquet(dataset_path)
        elif dataset_path.is_dir():
            # Read all parquet files in directory
            files = list(dataset_path.rglob("*.parquet"))
            if not files:
                logger.warning(f"⚠️ No parquet files found in {dataset_path}")
                return
            
            dfs = []
            for f in files:
                try:
                    chunk = pd.read_parquet(f)
                    # Handle hive partitioning if needed (naive approach)
                    if 'uf' not in chunk.columns:
                        parts = [p for p in f.parts if p.startswith('uf=')]
                        if parts:
                            chunk['uf'] = parts[0].replace('uf=', '')
                    dfs.append(chunk)
                except Exception as e:
                    logger.error(f"❌ Error reading {f}: {e}")
            
            if dfs:
                df = pd.concat(dfs, ignore_index=True)
        
        if df.empty:
            logger.warning("⚠️ Empty dataframe, skipping upload.")
            return

        # Upload to BigQuery
        logger.info(f"📤 Uploading {len(df)} rows to BigQuery...")
        df.to_gbq(
            destination_table=f"{bq_dataset_id}.{table_name}",
            project_id=project_id,
            if_exists=if_exists,
            progress_bar=True
        )
        logger.info(f"✅ Successfully uploaded {table_name}")

    except Exception as e:
        logger.error(f"❌ Failed to upload {table_name}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Upload Data to BigQuery")
    parser.add_argument("--project-id", required=True, help="GCP Project ID")
    parser.add_argument("--dataset-id", default="dengue_gold", help="BigQuery Dataset ID")
    parser.add_argument("--layers", default="gold", help="Comma-separated layers (gold,silver)")
    
    args = parser.parse_args()
    
    root = get_project_root()
    data_dir = root / 'data'
    
    layers = args.layers.split(',')
    
    for layer in layers:
        layer_path = data_dir / layer
        if not layer_path.exists():
            logger.warning(f"⚠️ Layer {layer} not found at {layer_path}")
            continue
            
        # Iterate over datasets in the layer
        for item in layer_path.iterdir():
            if item.name.startswith('.'): continue
            
            # If it's a directory (dataset) or a parquet file
            if item.is_dir() or (item.is_file() and item.suffix == '.parquet'):
                upload_dataset(item, args.dataset_id, args.project_id)

if __name__ == "__main__":
    main()
