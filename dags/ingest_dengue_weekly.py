from airflow.decorators import dag, task
from pendulum import datetime
import logging
import os
import sys

# Add src to path
sys.path.append(os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "src"))

from src.connectors.infodengue_api import InfoDengueConnector

logger = logging.getLogger("airflow.task")

# Cities to monitor (RJ, SP)
CIDADES_ALVO = [3304557, 3550308]

@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@weekly",
    catchup=False,
    tags=["dengue", "bronze", "ingestion"],
    doc_md="""
    ## Ingestão Semanal de Dengue
    
    Busca dados da última semana epidemiológica para as cidades monitoradas.
    """
)
def ingest_dengue_weekly():
    
    @task
    def ingest_city_data(geocode: int, logical_date=None):
        year = logical_date.year
        logger.info(f"Fetching data for city {geocode}, year {year}")
        
        connector = InfoDengueConnector()
        data = connector.fetch_data(geocode=geocode, year=year)
        
        if data:
            connector.save_local(data, geocode=geocode, year=year, disease="dengue")
            logger.info(f"Saved data for {geocode}")
        else:
            logger.warning(f"No data for {geocode}")

    # Create tasks for each city
    for cidade in CIDADES_ALVO:
        ingest_city_data(geocode=cidade)

ingest_dengue_weekly()
