import subprocess
import sys
import os
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_command(command):
    logger.info(f"🚀 Running command: {' '.join(command)}")
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        logger.info(result.stdout)
        if result.stderr:
            logger.warning(result.stderr)
        logger.info("✅ Command finished successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Command failed with return code {e.returncode}")
        logger.error(e.stdout)
        logger.error(e.stderr)
        sys.exit(1)

def main():
    logger.info("🏁 Starting Full Silver Layer Transformation (2020-2025)")
    
    current_dir = Path(__file__).resolve().parent
    python_exe = sys.executable
    
    # 1. Transform Dengue Data
    logger.info("--- Step 1: Transforming Dengue Data ---")
    dengue_script = current_dir / "transform_silver_dengue.py"
    run_command([python_exe, str(dengue_script), "--year-start", "2020", "--year-end", "2025"])
    
    # 2. Transform INMET Data
    logger.info("--- Step 2: Transforming INMET Data ---")
    inmet_script = current_dir / "transform_silver_inmet.py"
    run_command([python_exe, str(inmet_script), "--year-start", "2020", "--year-end", "2025"])
    
    logger.info("✨ All transformations completed successfully!")

if __name__ == "__main__":
    main()
