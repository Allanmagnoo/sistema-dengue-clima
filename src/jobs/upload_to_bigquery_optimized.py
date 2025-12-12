import os
import sys
import logging
import argparse
from pathlib import Path
from google.cloud import bigquery
import pandas as pd
import db_dtypes
import joblib
from joblib import Parallel, delayed
import zipfile
import io

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_project_root():
    return Path(__file__).resolve().parent.parent.parent

def read_file(f):
    """Reads a single file into a DataFrame"""
    try:
        if f.suffix == '.parquet':
            df = pd.read_parquet(f)
            # Handle hive partitioning
            if 'uf' not in df.columns:
                parts = [p for p in f.parts if p.startswith('uf=')]
                if parts:
                    df['uf'] = parts[0].replace('uf=', '')
            return df
            
        elif f.suffix == '.csv':
            try:
                return pd.read_csv(f, sep=None, engine='python', encoding='utf-8')
            except UnicodeDecodeError:
                return pd.read_csv(f, sep=None, engine='python', encoding='latin1')
                
        elif f.suffix == '.xlsx':
            return pd.read_excel(f)
            
        elif f.suffix == '.zip':
            # Handle zip: read first valid file inside
            with zipfile.ZipFile(f) as z:
                # Find csv or xlsx inside
                valid_files = [n for n in z.namelist() if n.endswith(('.csv', '.xlsx', '.xls'))]
                if not valid_files:
                    return pd.DataFrame()
                
                target = valid_files[0] # Pick first one
                with z.open(target) as zf:
                    if target.endswith('.csv'):
                        # Zip extraction returns bytes, needing encoding handling
                        try:
                            return pd.read_csv(zf, sep=None, engine='python', encoding='utf-8')
                        except:
                            return pd.read_csv(zf, sep=None, engine='python', encoding='latin1')
                    elif target.endswith(('.xlsx', '.xls')):
                        return pd.read_excel(zf)
        
    except Exception as e:
        logger.error(f"❌ Error reading {f}: {e}")
        return pd.DataFrame() # Return empty on error
    return pd.DataFrame()

def upload_dataset(dataset_path, bq_dataset_id, project_id, if_exists='replace'):
    """Uploads a folder or file to BigQuery"""
    client = bigquery.Client(project=project_id)
    
    table_name = dataset_path.stem
    table_name = table_name.replace('-', '_').replace(' ', '_').replace('.', '_').lower()
    table_id = f"{project_id}.{bq_dataset_id}.{table_name}"
    
    logger.info(f"🚀 Processing {dataset_path} -> {table_id}")
    
    df = pd.DataFrame()
    files_to_read = []
    
    if dataset_path.is_file():
        files_to_read = [dataset_path]
    elif dataset_path.is_dir():
        # Find all supported files
        extensions = ['*.parquet', '*.csv', '*.xlsx', '*.zip']
        for ext in extensions:
            files_to_read.extend(list(dataset_path.rglob(ext)))
    
    if not files_to_read:
        logger.warning(f"⚠️ No data files found in {dataset_path}")
        return

    logger.info(f"Found {len(files_to_read)} files. Reading in parallel...")
    
    # Parallel Read
    dfs = Parallel(n_jobs=-1, verbose=5)(
        delayed(read_file)(f) for f in files_to_read
    )
    
    # Filter empty dfs
    dfs = [d for d in dfs if not d.empty]
    
    if dfs:
        try:
            # Align columns
            df = pd.concat(dfs, ignore_index=True)
        except Exception as e:
            logger.error(f"Failed to concat: {e}")
            return
            
    if df.empty:
        logger.warning("⚠️ Empty dataframe, skipping upload.")
        return

    # Upload
    logger.info(f"📤 Uploading {len(df)} rows to BigQuery table {table_name}...")
    
    # Cast objects to str
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str)

    try:
        df.to_gbq(
            destination_table=f"{bq_dataset_id}.{table_name}",
            project_id=project_id,
            if_exists=if_exists,
            progress_bar=True
        )
        logger.info(f"✅ Successfully uploaded {table_name}")
    except Exception as e:
        logger.error(f"❌ Failed to upload {table_name}: {e}")


def create_dataset_if_not_exists(client, dataset_id, project_id):
    dataset_ref = f"{project_id}.{dataset_id}"
    try:
        client.get_dataset(dataset_ref)
        logger.info(f"Dataset {dataset_ref} already exists.")
    except Exception:
        logger.info(f"Dataset {dataset_ref} not found. Creating...")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"  # Adjust location if needed
        client.create_dataset(dataset)
        logger.info(f"Created dataset {dataset_ref}")

def main():
    parser = argparse.ArgumentParser(description="Upload Data to BigQuery (Optimized)")
    parser.add_argument("--project-id", required=True, help="GCP Project ID")
    parser.add_argument("--dataset-id", default="dengue_gold", help="BigQuery Dataset ID")
    parser.add_argument("--layers", default="gold", help="Comma-separated layers")
    parser.add_argument("--path", help="Specific path to upload (overrides --layers)")
    
    args = parser.parse_args()
    
    root = get_project_root()
    data_dir = root / 'data'
    
    # Ensure dataset exists
    client = bigquery.Client(project=args.project_id)
    create_dataset_if_not_exists(client, args.dataset_id, args.project_id)

    if args.path:
        # Handle specific path
        full_path = Path(args.path)
        if not full_path.is_absolute():
            # Assume relative to project root
            full_path = root / args.path
        
        if not full_path.exists():
            logger.error(f"❌ Path not found: {full_path}")
            return
            
        logger.info(f"🚀 Processing specific path: {full_path}")
        upload_dataset(full_path, args.dataset_id, args.project_id)
        return

    layers = args.layers.split(',')
    
    for layer in layers:
        layer_path = data_dir / layer
        if not layer_path.exists():
            continue
            
        logger.info(f"📂 Processing Layer: {layer}")
        
        for item in layer_path.iterdir():
            if item.name.startswith('.'): continue
            
            if item.is_dir():
                upload_dataset(item, args.dataset_id, args.project_id)
            elif item.is_file() and item.suffix in ['.parquet', '.csv', '.xlsx', '.zip']:
                upload_dataset(item, args.dataset_id, args.project_id)

if __name__ == "__main__":
    main()
