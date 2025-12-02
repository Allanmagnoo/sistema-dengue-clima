from airflow import DAG
try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python_operator import PythonOperator

import logging
import sys
import os
from datetime import datetime

# Add src to path
sys.path.append(os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "src"))

from src.connectors.ibge_connector import IBGEConnector

logger = logging.getLogger("airflow.task")

# Capitais brasileiras (geocode IBGE 7 dígitos)
CAPITAIS_BR = {
    'AC': 1200401,
    'AL': 2704302,
    'AP': 1600303,
    'AM': 1302603,
    'BA': 2927408,
    'CE': 2304400,
    'DF': 5300108,
    'ES': 3205309,
    'GO': 5208707,
    'MA': 2111300,
    'MT': 5103403,
    'MS': 5002704,
    'MG': 3106200,
    'PA': 1501402,
    'PB': 2507507,
    'PR': 4106902,
    'PE': 2611606,
    'PI': 2211001,
    'RJ': 3304557,
    'RN': 2408102,
    'RS': 4314902,
    'RO': 1100205,
    'RR': 1400100,
    'SC': 4205407,
    'SP': 3550308,
    'SE': 2800308,
    'TO': 1721000
}

# Período (últimos 10 anos + ano atual)
current_year = datetime.now().year
ANOS_HISTORICO = list(range(current_year - 10, current_year + 1))

# Concorrência configurável
DAG_CONCURRENCY = int(os.environ.get("IBGE_CONCURRENCY", "6"))


def _ingest_ibge_population(uf: str, geocode: int, year: int, **kwargs):
    logger.info(f"📥 IBGE População: {uf} geocode={geocode} ano={year}")
    conn = IBGEConnector()
    try:
        value = conn.get_population_estimate(geocode=geocode, year=year)
        if value is not None:
            conn.save_population_data(geocode=geocode, year=year, population=value)
        else:
            logger.warning(f"Sem valor de população para {geocode} em {year}")
    except Exception as e:
        logger.error(f"Erro IBGE {geocode} {year}: {e}")


with DAG(
    dag_id="02_ingest_ibge_population",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bronze", "ingestion", "ibge", "population"],
    max_active_tasks=DAG_CONCURRENCY,
) as dag:

    for uf, geocode in CAPITAIS_BR.items():
        for ano in ANOS_HISTORICO:
            PythonOperator(
                task_id=f"ibge_pop_{uf}_{ano}",
                python_callable=_ingest_ibge_population,
                op_kwargs={
                    "uf": uf,
                    "geocode": geocode,
                    "year": ano,
                },
            )

