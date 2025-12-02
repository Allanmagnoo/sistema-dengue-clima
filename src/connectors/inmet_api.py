import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time

logger = logging.getLogger(__name__)

class InmetConnector:
    """
    Conector para API de Estações Automáticas do INMET.
    Base URL: https://apitempo.inmet.gov.br/
    """
    BASE_URL = "https://apitempo.inmet.gov.br"

    def __init__(self, output_path: str = "data/bronze/inmet"):
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
            status_forcelist=(500, 502, 503, 504),
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session

    def fetch_daily_data(self, date: str, station_code: str = None):
        """
        Busca dados horários de uma data específica (YYYY-MM-DD).
        Se station_code for fornecido, busca apenas daquela estação.
        Caso contrário, pode buscar de todas (cuidado com volume).
        
        Endpoint: /estacao/{DATA_INI}/{DATA_FIM}/{CODIGO}
        """
        # A API do INMET permite intervalos. Vamos pegar um dia por vez para granularidade.
        # Formato data: YYYY-MM-DD
        
        endpoint = f"/estacao/{date}/{date}/{station_code}" if station_code else f"/estacao/dados/{date}"
        url = f"{self.BASE_URL}{endpoint}"
        
        logger.info(f"🌦️ Request INMET: {url}")
        
        try:
            response = self.session.get(url, timeout=60)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erro API INMET: {e}")
            return []

    def fetch_historical_year(self, year: int, station_code: str):
        """
        Busca dados de um ano inteiro para uma estação.
        Itera dia a dia ou mês a mês (API tem limites de intervalo).
        Recomendado: Intervalos de 1 mês.
        """
        start_date = datetime(year, 1, 1)
        end_date = datetime(year, 12, 31)
        current_date = start_date
        
        all_data = []
        
        while current_date <= end_date:
            # Pega blocos de 1 mês para otimizar
            next_month = current_date + timedelta(days=31)
            chunk_end = min(next_month, end_date)
            
            d_ini = current_date.strftime("%Y-%m-%d")
            d_fim = chunk_end.strftime("%Y-%m-%d")
            
            endpoint = f"/estacao/{d_ini}/{d_fim}/{station_code}"
            url = f"{self.BASE_URL}{endpoint}"
            
            try:
                logger.info(f"Fetching INMET {station_code}: {d_ini} to {d_fim}")
                response = self.session.get(url, timeout=60)
                response.raise_for_status()
                data = response.json()
                all_data.extend(data)
                time.sleep(1) # Rate limit courtesy
            except Exception as e:
                logger.error(f"Failed batch {d_ini}-{d_fim}: {e}")
            
            current_date = chunk_end + timedelta(days=1)
            
        return all_data

    def save_local(self, data: List[dict], station_code: str, year: int):
        if not data:
            return

        partition_dir = self.output_path / f"station={station_code}" / f"year={year}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        
        # Salvar como JSON ou CSV. JSON preserva estrutura bruta.
        file_path = partition_dir / "data.json"
        
        import json
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"💾 Dados salvos: {file_path}")
