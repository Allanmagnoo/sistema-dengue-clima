import logging
import requests
import json
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

class InfoDengueConnector:
    """
    Conector oficial para API InfoDengue.
    Doc: https://info.dengue.mat.br/services/api/doc
    """
    
    BASE_URL = "https://info.dengue.mat.br/api/alertcity"

    def __init__(self, output_path: str = "data/bronze/infodengue", entity_type: str = None):
        """
        Args:
            output_path: Base path for data storage
            entity_type: Optional entity type ('municipios', 'distritos') for organized storage
        """
        self.output_path = Path(output_path)
        if entity_type:
            self.output_path = self.output_path / entity_type
        self.output_path.mkdir(parents=True, exist_ok=True)
        self.session = self._create_retry_session()

    def _create_retry_session(self, retries: int = 3, backoff_factor: float = 1.0):
        """Sessão resiliente para evitar falhas de rede intermitentes."""
        session = requests.Session()
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=(500, 502, 503, 504),
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session

    def fetch_data(self, geocode: int, year: int, disease: str = "dengue") -> List[Dict[str, Any]]:
        """
        Busca dados epidemiológicos completos para um ano específico.
        Parâmetros baseados na doc oficial:
        - ew_start/ew_end: Semana epidemiológica (1 a 53)
        - ey_start/ey_end: Ano (início e fim)
        """
        params = {
            "geocode": geocode,
            "disease": disease,
            "format": "json",
            "ew_start": 1,      # Início do ano epidemiológico
            "ew_end": 53,       # Fim do ano (garante pegar tudo)
            "ey_start": year,
            "ey_end": year      # Busca apenas o ano solicitado
        }
        
        logger.info(f"📡 Request InfoDengue: Cidade={geocode}, Ano={year}, Params={params}")
        
        try:
            response = self.session.get(self.BASE_URL, params=params, timeout=60)
            response.raise_for_status()
            
            # A API retorna uma lista de objetos JSON diretamente
            data = response.json()
            
            if not data:
                logger.warning(f"⚠️ API retornou lista vazia para {geocode} em {year}")
                return []
                
            logger.info(f"✅ Sucesso! {len(data)} semanas epidemiológicas baixadas.")
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erro na API InfoDengue: {e}")
            raise e

    def save_local(self, data: List[Dict[str, Any]], geocode: int, year: int, disease: str):
        """
        Salva em formato Hive-Partitioned:
        data/bronze/infodengue/disease=dengue/year=2024/3304557.json
        """
        if not data:
            return

        partition_dir = self.output_path / f"disease={disease}" / f"year={year}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        
        file_path = partition_dir / f"{geocode}.json"
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"💾 Arquivo salvo: {file_path}")
    
    def fetch_data_csv(self, geocode: int, year: int, disease: str = "dengue") -> str:
        """
        Busca dados em formato CSV.
        Retorna o conteúdo CSV como string.
        """
        params = {
            "geocode": geocode,
            "disease": disease,
            "format": "csv",
            "ew_start": 1,
            "ew_end": 53,
            "ey_start": year,
            "ey_end": year
        }
        
        logger.info(f"📡 Request InfoDengue CSV: Cidade={geocode}, Ano={year}")
        
        try:
            response = self.session.get(self.BASE_URL, params=params, timeout=60)
            response.raise_for_status()
            
            csv_content = response.text
            
            if not csv_content or len(csv_content) < 10:
                logger.warning(f"⚠️ API retornou CSV vazio para {geocode} em {year}")
                return None
            
            # Count lines (excluding header)
            lines = csv_content.strip().split('\n')
            logger.info(f"✅ Sucesso! {len(lines)-1} linhas de dados baixadas.")
            return csv_content
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erro na API InfoDengue: {e}")
            raise e
    
    def save_local_csv(self, csv_content: str, geocode: int, year: int, disease: str):
        """
        Salva CSV em formato Hive-Partitioned:
        data/bronze/infodengue/municipios/disease=dengue/year=2024/3304557.csv
        """
        if not csv_content:
            return
        
        partition_dir = self.output_path / f"disease={disease}" / f"year={year}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        
        file_path = partition_dir / f"{geocode}.csv"
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(csv_content)
        logger.info(f"💾 CSV salvo: {file_path}")

if __name__ == "__main__":
    import time
    
    # Configuração básica de log
    logging.basicConfig(level=logging.INFO)
    
    print("--- 🚀 TESTE CSV CONNECTOR ---")
    
    # Test CSV methods
    connector = InfoDengueConnector(entity_type="municipios")
    
    GEOCODE_TESTE = 3304557  # Rio de Janeiro
    ANO_TESTE = 2024
    
    print(f"\nBaixando dados CSV para Rio de Janeiro ({GEOCODE_TESTE}) em {ANO_TESTE}...")
    csv_data = connector.fetch_data_csv(geocode=GEOCODE_TESTE, year=ANO_TESTE, disease="dengue")
    
    if csv_data:
        connector.save_local_csv(csv_data, geocode=GEOCODE_TESTE, year=ANO_TESTE, disease="dengue")
        print(f"✅ SUCESSO! Verifique: data/bronze/infodengue/municipios/disease=dengue/year={ANO_TESTE}/{GEOCODE_TESTE}.csv")
    else:
        print("❌ FALHA: Nenhum dado retornado.")