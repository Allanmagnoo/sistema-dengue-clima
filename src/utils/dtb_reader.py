"""
DTB (Divisão Territorial Brasileira) Data Reader

Reads IBGE municipality codes from DTB 2024 tables.
"""
import pandas as pd
from pathlib import Path
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)


class DTBReader:
    """Reader for DTB 2024 municipality data."""
    
    def __init__(self, file_path: str = None):
        if file_path is None:
            # Determine project root relative to this script
            # Script is in src/utils/dtb_reader.py -> root is ../../
            current_dir = Path(__file__).resolve().parent
            base_dir = current_dir.parent.parent
            self.municipios_file = base_dir / "data/bronze/DTB_2024/RELATORIO_DTB_BRASIL_2024_MUNICIPIOS.ods"
        else:
            self.municipios_file = Path(file_path)
        
    def get_all_municipios(self) -> List[Dict[str, any]]:
        """
        Read all municipalities from DTB 2024.
        
        Returns:
            List of dicts with 'geocode' and 'nome' keys
        """
        logger.info(f"Reading municipalities from {self.municipios_file}")
        
        try:
            # DTB files have header rows, skip them
            # Try different skiprows values to find the data
            df = pd.read_excel(self.municipios_file, engine='odf', skiprows=6)
            
            logger.info(f"Loaded dataframe with shape: {df.shape}")
            logger.info(f"Columns: {df.columns.tolist()}")
            
            # Find geocode column (should contain 7-digit codes)
            geocode_col = None
            nome_col = None
            
            # Look for columns with expected names
            for col in df.columns:
                col_str = str(col).lower()
                if 'código' in col_str and 'município' in col_str and 'completo' in col_str:
                    geocode_col = col
                if 'nome' in col_str and 'município' in col_str:
                    nome_col = col
            
            # If not found by name, detect by content
            if geocode_col is None:
                for col in df.columns:
                    # Check if column contains 7-digit numeric codes
                    try:
                        sample = df[col].dropna().head(100).astype(str).str.strip()
                        if sample.str.match(r'^\d{7}$').sum() > 50:  # At least 50 valid codes
                            geocode_col = col
                            logger.info(f"Auto-detected geocode column: {col}")
                            break
                    except:
                        continue
            
            if nome_col is None:
                # Find first text column that's not the geocode
                for col in df.columns:
                    if col != geocode_col and df[col].dtype == 'object':
                        # Check if it looks like municipality names
                        sample = df[col].dropna().head(10).astype(str)
                        if sample.str.len().mean() > 3:  # Names are usually longer than 3 chars
                            nome_col = col
                            logger.info(f"Auto-detected name column: {col}")
                            break
            
            if geocode_col is None:
                raise ValueError(f"Could not identify geocode column. Available columns: {df.columns.tolist()}")
            
            # Extract municipalities
            municipios = []
            for _, row in df.iterrows():
                try:
                    geocode_str = str(row[geocode_col]).strip()
                    
                    # Validate: must be exactly 7 digits
                    if len(geocode_str) == 7 and geocode_str.isdigit():
                        municipios.append({
                            'geocode': int(geocode_str),
                            'nome': str(row[nome_col]).strip() if nome_col and pd.notna(row[nome_col]) else f"Município {geocode_str}"
                        })
                except:
                    continue
            
            logger.info(f"✅ Successfully loaded {len(municipios)} municipalities")
            
            if len(municipios) == 0:
                raise ValueError("No valid municipalities found in DTB file")
            
            return municipios
            
        except Exception as e:
            logger.error(f"❌ Error reading DTB file: {e}")
            raise
    
    def get_municipios_by_uf(self, uf: str) -> List[Dict[str, any]]:
        """
        Get municipalities for a specific state (UF).
        
        Args:
            uf: State code (e.g., 'RJ', 'SP')
            
        Returns:
            List of municipalities in that state
        """
        # UF is encoded in the first 2 digits of geocode
        # This is a simplified version - full implementation would need UF mapping
        all_municipios = self.get_all_municipios()
        
        # For now, return all - can be enhanced with UF filtering
        logger.warning("UF filtering not yet implemented, returning all municipalities")
        return all_municipios


if __name__ == "__main__":
    # Test the reader
    logging.basicConfig(level=logging.INFO)
    
    reader = DTBReader()
    municipios = reader.get_all_municipios()
    
    print(f"\n📊 Total municipalities: {len(municipios)}")
    print(f"\n🔍 First 5 municipalities:")
    for m in municipios[:5]:
        print(f"  - {m['geocode']}: {m['nome']}")
    print(f"\n🔍 Last 5 municipalities:")
    for m in municipios[-5:]:
        print(f"  - {m['geocode']}: {m['nome']}")
