from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

# Define paths for scripts
# We use relative paths assuming Airflow runs from project root or has access to it
# In a real Docker setup, these would be absolute paths inside the container
# Scripts reais existentes no projeto
TRANSFORM_SILVER_DENGUE = "src/jobs/transform_silver_dengue.py"
TRANSFORM_SILVER_INMET = "src/jobs/transform_silver_inmet.py"
TRANSFORM_SILVER_IBGE = "src/jobs/transform_silver_ibge.py"
TRANSFORM_SILVER_SNIS = "src/jobs/transform_silver_snis.py"

CREATE_GOLD_DENGUE_CLIMA = "src/jobs/create_gold_dengue_clima.py"
CREATE_GOLD_SANEAMENTO = "src/jobs/create_gold_saneamento.py"
CREATE_GOLD_HYBRID = "src/jobs/create_gold_hybrid.py"

# Script de Carga para PostgreSQL
LOAD_TO_POSTGRES = "src/jobs/bd.py"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="04_transform_pipeline",
    default_args=default_args,
    description="Orchestrates Spark transformations for Silver and Gold layers",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["spark", "silver", "gold", "transformation"],
) as dag:

    # --- Silver Layer Tasks ---
    task_silver_dengue = BashOperator(
        task_id="transform_silver_dengue",
        bash_command=f"python {TRANSFORM_SILVER_DENGUE}",
    )

    task_silver_inmet = BashOperator(
        task_id="transform_silver_inmet",
        bash_command=f"python {TRANSFORM_SILVER_INMET}",
    )
    
    task_silver_ibge = BashOperator(
        task_id="transform_silver_ibge",
        bash_command=f"python {TRANSFORM_SILVER_IBGE}",
    )

    task_silver_snis = BashOperator(
        task_id="transform_silver_snis",
        bash_command=f"python {TRANSFORM_SILVER_SNIS}",
    )

    # --- Gold Layer Tasks (Intermediate) ---
    task_gold_dengue_clima = BashOperator(
        task_id="create_gold_dengue_clima",
        bash_command=f"python {CREATE_GOLD_DENGUE_CLIMA}",
    )
    
    task_gold_saneamento = BashOperator(
        task_id="create_gold_saneamento",
        bash_command=f"python {CREATE_GOLD_SANEAMENTO}",
    )

    # --- Gold Layer Tasks (Final) ---
    task_gold_hybrid = BashOperator(
        task_id="create_gold_hybrid",
        bash_command=f"python {CREATE_GOLD_HYBRID}",
    )

    # --- Load to PostgreSQL ---
    # Carrega incrementalmente todas as camadas (Bronze, Silver, Gold) para o banco
    task_load_postgres = BashOperator(
        task_id="load_to_postgres",
        bash_command=f"python {LOAD_TO_POSTGRES} --mode incremental --layers gold,silver,bronze",
    )

    # --- Dependencies ---
    
    # Gold Dengue+Clima needs Silver Dengue & INMET
    [task_silver_dengue, task_silver_inmet] >> task_gold_dengue_clima
    
    # Gold Saneamento needs Silver Dengue, IBGE & SNIS
    [task_silver_dengue, task_silver_ibge, task_silver_snis] >> task_gold_saneamento
    
    # Gold Hybrid needs both Gold panels
    [task_gold_dengue_clima, task_gold_saneamento] >> task_gold_hybrid
    
    # Load to Postgres runs after all transformations are done
    task_gold_hybrid >> task_load_postgres
