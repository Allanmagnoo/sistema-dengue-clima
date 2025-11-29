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
    
    def __init__(self, years: list = [2024, 2025], delay_seconds: float = 1.0, max_workers: int = 4):
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
        # Each worker will create its own connector instance if needed, 
        # but requests.Session is thread-safe so we can share one connector
        self.connector = InfoDengueConnector(entity_type="municipios")
        
        # Thread-safe counters
        self.lock = threading.Lock()
        self.current_task = 0
        self.success_count = 0
        self.skip_count = 0
        self.error_count = 0
        
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
            # Fetch CSV data
            csv_data = self.connector.fetch_data_csv(
                geocode=geocode,
                year=year,
                disease="dengue"
            )
            
            if csv_data:
                # Save CSV
                self.connector.save_local_csv(
                    csv_data,
                    geocode=geocode,
                    year=year,
                    disease="dengue"
                )
                with self.lock:
                    self.success_count += 1
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
        logger.info("="*60)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Bulk extraction of dengue data for all municipalities")
    parser.add_argument("--years", nargs="+", type=int, default=[2024, 2025], help="Years to extract")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between requests (seconds)")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel workers")
    parser.add_argument("--no-resume", action="store_true", help="Don't skip existing files")
    parser.add_argument("--test", action="store_true", help="Test mode: only first 10 municipalities")
    parser.add_argument("--limit", type=int, help="Limit number of municipalities to process")
    
    args = parser.parse_args()
    
    extractor = BulkMunicipiosExtractor(
        years=args.years, 
        delay_seconds=args.delay,
        max_workers=args.workers
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
