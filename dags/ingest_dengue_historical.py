
from airflow import DAG
try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python_operator import PythonOperator

import logging
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "src"))

from datetime import datetime, timedelta
from src.connectors.infodengue_api import InfoDengueConnector

logger = logging.getLogger("airflow.task")

# --- CONFIGURAÇÕES DE NEGÓCIO ---

# Dicionário com os Geocodes (IBGE 7 dígitos) de todas as Capitais do Brasil
CAPITAIS_BR = {
    'AC': 1200401, # Rio Branco
    'AL': 2704302, # Maceió
    'AP': 1600303, # Macapá
    'AM': 1302603, # Manaus
    'BA': 2927408, # Salvador
    'CE': 2304400, # Fortaleza
    'DF': 5300108, # Brasília
    'ES': 3205309, # Vitória
    'GO': 5208707, # Goiânia
    'MA': 2111300, # São Luís
    'MT': 5103403, # Cuiabá
    'MS': 5002704, # Campo Grande
    'MG': 3106200, # Belo Horizonte
    'PA': 1501402, # Belém
    'PB': 2507507, # João Pessoa
    'PR': 4106902, # Curitiba
    'PE': 2611606, # Recife
    'PI': 2211001, # Teresina
    'RJ': 3304557, # Rio de Janeiro
    'RN': 2408102, # Natal
    'RS': 4314902, # Porto Alegre
    'RO': 1100205, # Porto Velho
    'RR': 1400100, # Boa Vista
    'SC': 4205407, # Florianópolis
    'SP': 3550308, # São Paulo
    'SE': 2800308, # Aracaju
    'TO': 1721000  # Palmas
}

# Período de interesse (Solicitação: 2024 e 2025)
ANOS_HISTORICO = [2024, 2025]

def _ingest_dengue_data(uf: str, geocode: int, year: int, **kwargs):
    """
    Task atômica: Baixa dados de uma Capital para um Ano.
    """
    logger.info(f"🚀 Iniciando ingestão: {uf} (Geo: {geocode}) - Ano {year}")
    
    connector = InfoDengueConnector()
    
    # 1. Busca na API
    try:
        dados = connector.fetch_data(geocode=geocode, year=year, disease="dengue")
        
        # 2. Salva no Data Lake (Bronze)
        if dados:
            connector.save_local(dados, geocode=geocode, year=year, disease="dengue")
        else:
            logger.warning(f"⚠️ API retornou vazio para {uf} em {year}")
            
    except Exception as e:
        logger.error(f"❌ Falha crítica em {uf}: {e}")
        # Não damos 'raise' aqui para não parar a DAG inteira; 
        # apenas esta task ficará vermelha (Failed) se o Airflow estiver configurado para isso,
        # ou apenas logamos o erro para análise posterior.

with DAG(
    dag_id="01_ingest_dengue_historical",
    start_date=datetime(2023, 1, 1),
    schedule=None, 
    catchup=False,
    tags=["bronze", "ingestion", "dengue", "capitais"],
    max_active_tasks=4, # Importante: Limita a 4 requests simultâneos para não bloquear IP
    doc_md="""
    # Ingestão Histórica InfoDengue (Nacional)
    
    Monitoramento das 27 Capitais Brasileiras.
    Destaque: Uso de `max_active_tasks` para respeitar rate limits da API pública.
    """
) as dag:

    # Geração Dinâmica de Tasks (27 estados * 2 anos = 54 tasks)
    for uf, geocode in CAPITAIS_BR.items():
        for ano in ANOS_HISTORICO:
            
            task_id = f"get_{uf}_{ano}"
            
            PythonOperator(
                task_id=task_id,
                python_callable=_ingest_dengue_data,
                op_kwargs={
                    "uf": uf,
                    "geocode": geocode,
                    "year": ano
                }
            )