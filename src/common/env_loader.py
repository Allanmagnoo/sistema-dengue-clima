import os
from pathlib import Path
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)

def load_secure_env():
    """
    Loads environment variables from .secrets/.env if available,
    otherwise falls back to .env in project root.
    """
    # Assuming this file is in src/common/
    # Project root is ../../
    current_file = Path(__file__).resolve()
    project_root = current_file.parent.parent.parent
    
    secure_env_path = project_root / ".secrets" / ".env"
    root_env_path = project_root / ".env"
    
    if secure_env_path.exists():
        logger.info(f"Loading environment from secure storage: {secure_env_path}")
        load_dotenv(secure_env_path)
    elif root_env_path.exists():
        logger.warning(f"Loading environment from root (Legacy): {root_env_path}")
        load_dotenv(root_env_path)
    else:
        logger.warning("No .env file found in .secrets/ or root.")
