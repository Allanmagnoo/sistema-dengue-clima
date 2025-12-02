import logging
import requests
from pathlib import Path
from typing import Dict, Any, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

class IBGEConnector:
    """
    Conector para API de Dados Agregados (SIDRA) e Localidades do IBGE.
    Foco: População e Malha Geográfica.
    """
    SIDRA_URL = "https://apisidra.ibge.gov.br/values"
    LOCALIDADES_URL = "https://servicodados.ibge.gov.br/api/v1/localidades"

    def __init__(self, output_path: str = "data/bronze/ibge"):
        self.output_path = Path(output_path)
        self.output_path.mkdir(parents=True, exist_ok=True)
        self.session = self._create_retry_session()

    def _create_retry_session(self, retries: int = 3, backoff_factor: float = 1.0):
        session = requests.Session()
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=(429, 500, 502, 503, 504),
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=32, pool_maxsize=32)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session

    def get_population_estimate(self, geocode: int, year: int) -> Optional[int]:
        """
        Busca estimativa populacional para um município em um ano específico.
        Usa Tabela 6579 (Estimativas de População).
        """
        # Tabela 6579: Estimativas de População
        # Variável 9324: População residente estimada
        # Classificação 2 (Ano) = year (Nota: SIDRA usa códigos de período específicos, 
        # mas para estimativas anuais simples, às vezes é mais fácil usar a série histórica)
        
        # Simplificação: Para automatização robusta no SIDRA, é preciso mapear os códigos de período.
        # Alternativa: API de Projeções (se disponível) ou API de Pesquisas.
        
        # Endpoint simplificado para teste (Exemplo fictício da estrutura SIDRA):
        # /t/6579/n6/{geocode}/v/9324/p/{year}
        
        # Como o SIDRA é complexo com períodos, vamos usar uma abordagem mais resiliente:
        # Buscar a série histórica e filtrar.
        
        try:
            # Exemplo de URL construída (precisa validar código da tabela para anos recentes)
            # Tabela 6579 cobre 2001-2021+. Para 2024, usar projeções se disponível.
            url = f"{self.SIDRA_URL}/t/6579/n6/{geocode}/v/9324/p/{year}"
            
            logger.info(f"👥 Request IBGE População: {url}")
            response = self.session.get(url, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                for item in data[1:]:
                    v = item.get('V')
                    if v is not None:
                        try:
                            return int(str(v).replace('.', '').replace(',', ''))
                        except Exception:
                            continue
            
            return None

        except Exception as e:
            logger.error(f"❌ Erro IBGE API: {e}")
            return None

    def get_municipality_info(self, geocode: int) -> Dict[str, Any]:
        """
        Busca metadados do município (Nome, UF, Área, etc).
        """
        url = f"{self.LOCALIDADES_URL}/municipios/{geocode}"
        try:
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"❌ Erro IBGE Localidades: {e}")
            return {}

    def save_population_data(self, geocode: int, year: int, population: int):
        import json
        if population is None: return
        
        data = {
            "geocode": geocode,
            "year": year,
            "population": population,
            "source": "IBGE_SIDRA_6579"
        }
        
        path = self.output_path / "population" / f"n6={geocode}" / f"year={year}"
        path.mkdir(parents=True, exist_ok=True)
        file_path = path / f"{geocode}.json"
        
        with open(file_path, 'w') as f:
            json.dump(data, f)

    def fetch_population_series(self, geocode: int, years: list) -> dict:
        series = {}
        for y in years:
            try:
                value = self.get_population_estimate(geocode, y)
                series[y] = value
                if value is not None:
                    self.save_population_data(geocode, y, value)
            except Exception:
                series[y] = None
        return series
