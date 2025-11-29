from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

# Define paths
# Using relative paths from project root
TRANSFORM_SILVER_DENGUE = "src/jobs/transform_silver_dengue.py"
TRANSFORM_SILVER_INMET = "src/jobs/transform_silver_inmet.py"
UPDATE_MAPPING = "src/jobs/create_mapping_estacao_geocode.py"
CREATE_GOLD = "src/jobs/create_gold_dengue_clima.py"
TRAIN_MODEL = "src/models/train_dengue_model.py"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="02_full_data_pipeline",
    default_args=default_args,
    description="Pipeline completo: Silver -> Gold -> Model Training",
    schedule=None, # Manual trigger as requested/implied by "no daily ingestion"
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["silver", "gold", "model", "pipeline"],
    doc_md="""
    # Pipeline Completo de Processamento
    
    Este DAG orquestra todo o processamento de dados após a ingestão manual.
    
    ## Etapas:
    1. **Silver Layer**:
       - Transformação de dados da Dengue (`transform_silver_dengue.py`)
       - Transformação de dados do INMET (`transform_silver_inmet.py`)
       - Atualização do mapeamento Geocode-Estação (`create_mapping_estacao_geocode.py`)
    
    2. **Gold Layer**:
       - Unificação Dengue + Clima (`create_gold_dengue_clima.py`)
    
    3. **Machine Learning**:
       - Treinamento e Previsão do Modelo (`train_dengue_model.py`)
       
    ## Dependências:
    As tarefas da camada Silver rodam em paralelo. A camada Gold aguarda todas as tarefas Silver.
    O treinamento do modelo aguarda a camada Gold.
    """
) as dag:

    # 1. Silver Tasks
    task_silver_dengue = BashOperator(
        task_id="transform_silver_dengue",
        bash_command=f"python {TRANSFORM_SILVER_DENGUE}",
    )

    task_silver_inmet = BashOperator(
        task_id="transform_silver_inmet",
        bash_command=f"python {TRANSFORM_SILVER_INMET}",
    )

    task_update_mapping = BashOperator(
        task_id="update_geocode_mapping",
        bash_command=f"python {UPDATE_MAPPING}",
    )

    # 2. Gold Task
    task_gold = BashOperator(
        task_id="create_gold_layer",
        bash_command=f"python {CREATE_GOLD}",
    )

    # 3. Model Training Task
    task_train_model = BashOperator(
        task_id="train_prediction_model",
        bash_command=f"python {TRAIN_MODEL}",
    )

    # Define Workflow
    [task_silver_dengue, task_silver_inmet, task_update_mapping] >> task_gold >> task_train_model
