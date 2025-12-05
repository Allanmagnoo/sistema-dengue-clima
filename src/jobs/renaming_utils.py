
import os
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def rename_parquet_recursive(root_path: Path, file_prefix: str):
    """
    Recursively renames parquet files within a partitioned directory structure.
    New name format: {file_prefix}_{partition_v1}_{partition_v2}_{part_index}.parquet
    
    Example:
    root/uf=SP/ano=2024/data_0.parquet 
    -> root/uf=SP/ano=2024/{file_prefix}_SP_2024_part0.parquet
    """
    if not root_path.exists():
        logger.warning(f"Path {root_path} does not exist. Skipping.")
        return

    count = 0
    # Walk top-down
    for root, dirs, files in os.walk(root_path):
        parquet_files = [f for f in files if f.endswith(".parquet")]
        if not parquet_files:
            continue
            
        # Extract partition values from current path relative to root_path?
        # Or just parse the current path parts looking for 'key=value'.
        
        path_parts = Path(root).parts
        
        # Collect partition values in order of appearance
        # Heuristic: any folder named "key=value"
        partition_values = []
        for part in path_parts:
            if "=" in part:
                # e.g. "uf=SP" -> "SP"
                val = part.split("=", 1)[1]
                partition_values.append(val)
        
        # If no partitions found (e.g. root folder), allow renaming too
        
        for i, file in enumerate(parquet_files):
            # Construct new name
            # {prefix}_{val1}_{val2}_..._part{i}.parquet
            
            # avoiding redundant prefix if already present
            # But the prefix is enforced.
            
            parts = [file_prefix] + partition_values
            
            # Handle collision or multiple files
            # If there's only 1 file and it's not "data_0" (generic), maybe we don't need 'part0'?
            # But consistent naming is better.
            # Let's add 'part{i}' always to be safe against multiple files (DuckDB parallel write).
            
            suffix = f"part{i}"
            
            # Join with underscore
            # Clean values (remove illegal chars just in case, though usually fine)
            
            base_name = "_".join(parts)
            new_filename = f"{base_name}_{suffix}.parquet"
            
            old_file_path = Path(root) / file
            new_file_path = Path(root) / new_filename
            
            if old_file_path.name == new_filename:
                continue
                
            # If target exists (very unlikely unless re-running), skip or overwrite?
            if new_file_path.exists():
                logger.warning(f"Target {new_file_path} exists. Skipping {old_file_path.name}")
                continue
                
            try:
                os.rename(old_file_path, new_file_path)
                count += 1
            except Exception as e:
                logger.error(f"Failed to rename {old_file_path}: {e}")

    logger.info(f"Renamed {count} files in {root_path} with prefix '{file_prefix}'")
