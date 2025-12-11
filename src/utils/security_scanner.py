import os
import shutil
import re
import json
import logging
from pathlib import Path
from typing import List, Dict
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("security_scan.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("SecurityScanner")

# Constants
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SECURE_DIR = PROJECT_ROOT / ".secrets"
MANIFEST_FILE = SECURE_DIR / "manifest.json"

# Patterns for sensitive files/content
SENSITIVE_PATTERNS = {
    "files": [
        r"\.env.*",
        r".*_credentials\.json",
        r".*_key\.json",
        r".*_secret\.json",
        r"airflow_settings\.yaml",
        r"id_rsa",
        r"id_dsa",
        r".*\.pem"
    ],
    "content": [
        r"API_KEY\s*=\s*['\"].+['\"]",
        r"PASSWORD\s*=\s*['\"].+['\"]",
        r"SECRET\s*=\s*['\"].+['\"]",
        r"TOKEN\s*=\s*['\"].+['\"]"
    ]
}

# Ignore directories
IGNORE_DIRS = {
    ".git", ".venv", "venv", "node_modules", "__pycache__", ".idea", ".vscode", ".secrets", "logs"
}

def load_manifest() -> Dict:
    if MANIFEST_FILE.exists():
        with open(MANIFEST_FILE, "r") as f:
            return json.load(f)
    return {"files": {}, "last_scan": None}

def save_manifest(manifest: Dict):
    manifest["last_scan"] = datetime.now().isoformat()
    with open(MANIFEST_FILE, "w") as f:
        json.dump(manifest, f, indent=4)

def is_sensitive_filename(filename: str) -> bool:
    for pattern in SENSITIVE_PATTERNS["files"]:
        if re.match(pattern, filename, re.IGNORECASE):
            return True
    return False

def scan_file_content(filepath: Path) -> List[str]:
    findings = []
    try:
        # Skip binary files
        if filepath.suffix in ['.parquet', '.pyc', '.joblib', '.pkl']:
            return []
            
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            for pattern in SENSITIVE_PATTERNS["content"]:
                matches = re.findall(pattern, content)
                if matches:
                    findings.extend(matches)
    except Exception as e:
        logger.warning(f"Could not read {filepath}: {e}")
    return findings

def move_to_secure(filepath: Path, manifest: Dict):
    relative_path = filepath.relative_to(PROJECT_ROOT)
    # Create a flat name or preserve structure? Flat is safer to avoid collisions if we map it.
    # But preserving structure is better for organization.
    # Let's use the relative path as the key in manifest.
    
    target_path = SECURE_DIR / filepath.name
    
    # Handle duplicates by appending timestamp if needed, but for now simple overwrite check
    if target_path.exists():
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        target_path = SECURE_DIR / f"{filepath.stem}_{timestamp}{filepath.suffix}"
    
    logger.info(f"Moving sensitive file {filepath} to {target_path}")
    
    # Copy first, then remove (safer)
    shutil.copy2(filepath, target_path)
    os.remove(filepath)
    
    manifest["files"][str(relative_path)] = str(target_path.relative_to(PROJECT_ROOT))

def scan_and_protect():
    if not SECURE_DIR.exists():
        SECURE_DIR.mkdir()
        logger.info(f"Created secure directory at {SECURE_DIR}")

    manifest = load_manifest()
    
    logger.info("Starting security scan...")
    
    files_moved = 0
    issues_found = 0

    for root, dirs, files in os.walk(PROJECT_ROOT):
        # Filter directories
        dirs[:] = [d for d in dirs if d not in IGNORE_DIRS]
        
        for file in files:
            filepath = Path(root) / file
            
            # Check filename
            if is_sensitive_filename(file):
                logger.warning(f"SENSITIVE FILE DETECTED: {filepath}")
                move_to_secure(filepath, manifest)
                files_moved += 1
                continue # Moved, so don't scan content
            
            # Check content (optional, reporting only for now to avoid breaking code)
            findings = scan_file_content(filepath)
            if findings:
                logger.warning(f"POTENTIAL SECRETS IN FILE: {filepath}")
                for find in findings:
                    logger.warning(f"  - Match: {find[:50]}...") # Truncate for log safety
                issues_found += 1

    save_manifest(manifest)
    
    logger.info(f"Scan complete. Moved {files_moved} files. Found {issues_found} files with potential secrets.")
    print(f"Security Scan Complete.\nMoved: {files_moved}\nIssues: {issues_found}\nCheck security_scan.log for details.")

if __name__ == "__main__":
    scan_and_protect()
