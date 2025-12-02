"""
Bulk Municipality Dengue Data Extractor

Extracts dengue data for all Brazilian municipalities from DTB 2024
using InfoDengue API in CSV format.
"""
import logging
import time
import sys
import os
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, List, Optional, Tuple
import csv
import io
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.utils.dtb_reader import DTBReader
from src.connectors.infodengue_api import InfoDengueConnector

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BulkMunicipiosExtractor:
    """Bulk extractor for all municipalities."""
    
    def __init__(self, years: list = [2015, 2023], delay_seconds: float = 1.0, max_workers: int = 8, timeout: float = 60.0, retries: int = 3, backoff_factor: float = 1.0, base_url: Optional[str] = None, auth_token: Optional[str] = None, default_params: Optional[Dict[str, Any]] = None):
        """
        Args:
            years: List of years to extract
            delay_seconds: Delay between API requests (rate limiting)
            max_workers: Number of parallel workers
        """
        self.years = years
        self.delay_seconds = delay_seconds
        self.max_workers = max_workers
        self.dtb_reader = DTBReader()
        self.connector = InfoDengueConnector(entity_type="municipios")
        self.base_url = base_url or self.connector.BASE_URL
        self.timeout = timeout
        self.retries = retries
        self.backoff_factor = backoff_factor
        self.default_params = default_params or {}
        self.session = self._create_retry_session(self.retries, self.backoff_factor)
        if auth_token:
            self.session.headers.update({"Authorization": f"Bearer {auth_token}"})
        self.temp_store: Dict[Tuple[int, int], List[Dict[str, Any]]] = {}
        
        self.lock = threading.Lock()
        self.current_task = 0
        self.success_count = 0
        self.skip_count = 0
        self.error_count = 0
        self.metrics = {
            "requests": 0,
            "bytes_received": 0,
            "latency_ms_total": 0.0,
            "latency_ms_min": None,
            "latency_ms_max": None,
        }

    def _create_retry_session(self, retries: int, backoff_factor: float):
        session = requests.Session()
        retry = Retry(total=retries, read=retries, connect=retries, backoff_factor=backoff_factor, status_forcelist=(429, 500, 502, 503, 504))
        adapter = HTTPAdapter(max_retries=retry, pool_connections=32, pool_maxsize=32)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session

    def _build_params(self, geocode: int, year: int, disease: str = "dengue") -> Dict[str, Any]:
        base = {
            "geocode": geocode,
            "disease": disease,
            "format": "csv",
            "ew_start": 1,
            "ew_end": 53,
            "ey_start": year,
            "ey_end": year,
        }
        base.update(self.default_params)
        return base

    def _request_csv(self, geocode: int, year: int) -> Optional[str]:
        params = self._build_params(geocode, year)
        start = time.perf_counter()
        try:
            resp = self.session.get(self.base_url, params=params, timeout=self.timeout)
            if resp.status_code == 429:
                ra = resp.headers.get("Retry-After")
                wait_s = float(ra) if ra and str(ra).replace('.', '', 1).isdigit() else self.backoff_factor
                time.sleep(wait_s)
                resp = self.session.get(self.base_url, params=params, timeout=self.timeout)
            resp.raise_for_status()
            content = resp.text
            elapsed_ms = (time.perf_counter() - start) * 1000
            size = len(content.encode('utf-8')) if content else 0
            with self.lock:
                self.metrics["requests"] += 1
                self.metrics["bytes_received"] += size
                self.metrics["latency_ms_total"] += elapsed_ms
                self.metrics["latency_ms_min"] = elapsed_ms if self.metrics["latency_ms_min"] is None else min(self.metrics["latency_ms_min"], elapsed_ms)
                self.metrics["latency_ms_max"] = elapsed_ms if self.metrics["latency_ms_max"] is None else max(self.metrics["latency_ms_max"], elapsed_ms)
            return content if content and len(content) >= 10 else None
        except Exception as e:
            with self.lock:
                logger.error(f"❌ Requisição falhou para {geocode}-{year}: {e}")
            return None

    def _parse_csv_records(self, csv_text: str) -> List[Dict[str, Any]]:
        f = io.StringIO(csv_text)
        reader = csv.DictReader(f)
        required_fields = {"data_iniSE", "SE", "casos"}
        if not required_fields.issubset(set(reader.fieldnames or [])):
            return []
        records = []
        for row in reader:
            rec = {k: (v if v != '' else None) for k, v in row.items()}
            records.append(rec)
        return records

    def _validate_and_cache(self, records: List[Dict[str, Any]], geocode: int, year: int) -> bool:
        if not records:
            return False
        valid = 0
        for r in records:
            if r.get("SE") is None or r.get("data_iniSE") is None:
                continue
            valid += 1
        if valid == 0:
            return False
        with self.lock:
            self.temp_store[(geocode, year)] = records
        return True
        
    def process_task(self, task_info):
        """Process a single task (municipality + year)."""
        municipio, year, total_tasks = task_info
        geocode = municipio['geocode']
        nome = municipio['nome']
        
        with self.lock:
            self.current_task += 1
            task_num = self.current_task
        
        # Check if file already exists (resume capability)
        output_file = self.connector.output_path / f"disease=dengue" / f"year={year}" / f"{geocode}.csv"
        
        if output_file.exists():
            with self.lock:
                logger.info(f"[{task_num}/{total_tasks}] ⏭️ Pulando {nome} ({geocode}) - {year} (já existe)")
                self.skip_count += 1
            return
        
        with self.lock:
            logger.info(f"[{task_num}/{total_tasks}] 📥 Baixando {nome} ({geocode}) - {year}")
        
        try:
            csv_data = self._request_csv(geocode, year)
            if csv_data:
                records = self._parse_csv_records(csv_data)
                if self._validate_and_cache(records, geocode, year):
                    self.connector.save_local_csv(csv_data, geocode=geocode, year=year, disease="dengue")
                    with self.lock:
                        self.success_count += 1
                else:
                    with self.lock:
                        logger.warning(f"⚠️ Dados inválidos para {nome} ({geocode}) em {year}")
                        self.error_count += 1
            else:
                with self.lock:
                    logger.warning(f"⚠️ Sem dados para {nome} ({geocode}) em {year}")
                    self.error_count += 1
        except Exception as e:
            with self.lock:
                logger.error(f"❌ Erro em {nome} ({geocode}) - {year}: {e}")
                self.error_count += 1
        
        # Rate limiting per worker
        time.sleep(self.delay_seconds)

    def extract_all(self, resume: bool = True):
        """
        Extract data for all municipalities.
        
        Args:
            resume: If True, skip already downloaded files (handled in process_task)
        """
        logger.info(f"🚀 Iniciando extração em massa de municípios com {self.max_workers} workers")
        
        # Load all municipalities
        municipios = self.dtb_reader.get_all_municipios()
        total_municipios = len(municipios)
        
        logger.info(f"📊 Total de municípios: {total_municipios}")
        logger.info(f"📅 Anos: {self.years}")
        logger.info(f"⏱️ Delay entre requisições: {self.delay_seconds}s")
        
        # Prepare tasks
        tasks = []
        total_tasks = total_municipios * len(self.years)
        
        for municipio in municipios:
            for year in self.years:
                tasks.append((municipio, year, total_tasks))
        
        # Execute in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.process_task, task) for task in tasks]
            
            # Wait for completion (optional: could use as_completed for progress bar)
            for future in as_completed(futures):
                pass
        
        # Summary
        logger.info("\n" + "="*60)
        logger.info("✅ EXTRAÇÃO CONCLUÍDA!")
        logger.info(f"📊 Total de tarefas: {total_tasks}")
        logger.info(f"✅ Sucessos: {self.success_count}")
        logger.info(f"⏭️ Pulados (já existiam): {self.skip_count}")
        logger.info(f"❌ Erros: {self.error_count}")
        avg_latency = (self.metrics["latency_ms_total"] / self.metrics["requests"]) if self.metrics["requests"] else 0
        logger.info(f"⏱️ Latência média: {avg_latency:.2f} ms | Min: {self.metrics['latency_ms_min'] if self.metrics['latency_ms_min'] is not None else 0:.2f} ms | Max: {self.metrics['latency_ms_max'] if self.metrics['latency_ms_max'] is not None else 0:.2f} ms")
        logger.info(f"📦 Bytes recebidos: {self.metrics['bytes_received']}")
        logger.info("="*60)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Bulk extraction of dengue data for all municipalities")
    parser.add_argument("--years", nargs="+", type=int, default=[2015, 2023], help="Years to extract")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between requests (seconds)")
    parser.add_argument("--workers", type=int, default=8, help="Number of parallel workers")
    parser.add_argument("--no-resume", action="store_true", help="Don't skip existing files")
    parser.add_argument("--test", action="store_true", help="Test mode: only first 10 municipalities")
    parser.add_argument("--limit", type=int, help="Limit number of municipalities to process")
    parser.add_argument("--base-url", type=str, help="Override API base URL")
    parser.add_argument("--auth-token", type=str, help="Auth token for API (Bearer)")
    parser.add_argument("--timeout", type=float, default=60.0, help="HTTP timeout seconds")
    parser.add_argument("--retries", type=int, default=3, help="HTTP retries")
    parser.add_argument("--backoff", type=float, default=1.0, help="HTTP backoff factor")
    
    args = parser.parse_args()
    
    extractor = BulkMunicipiosExtractor(
        years=args.years, 
        delay_seconds=args.delay,
        max_workers=args.workers,
        timeout=args.timeout,
        retries=args.retries,
        backoff_factor=args.backoff,
        base_url=args.base_url,
        auth_token=args.auth_token
    )
    
    # Apply test/limit mode
    if args.test or args.limit:
        limit = 10 if args.test else args.limit
        logger.info(f"🧪 MODO LIMITADO: Apenas primeiros {limit} municípios")
        
        # Override get_all_municipios to return limited list
        original_get_all = extractor.dtb_reader.get_all_municipios
        
        # We need to call the original method first to get the data, then slice it
        # But we can't call it inside the lambda easily without recursion if we assign it to self
        # So we fetch it now
        all_municipios = original_get_all()
        limited_municipios = all_municipios[:limit]
        
        extractor.dtb_reader.get_all_municipios = lambda: limited_municipios
    
    extractor.extract_all(resume=not args.no_resume)
