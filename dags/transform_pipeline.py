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
CREATE_GOLD_DENGUE_CLIMA = "src/jobs/create_gold_dengue_clima.py"

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

    # Silver Dengue
    task_silver_dengue = BashOperator(
        task_id="transform_silver_dengue",
        bash_command=f"python {TRANSFORM_SILVER_DENGUE}",
    )

    # Silver INMET
    task_silver_inmet = BashOperator(
        task_id="transform_silver_inmet",
        bash_command=f"python {TRANSFORM_SILVER_INMET}",
    )

    # Gold Dengue + Clima
    task_gold = BashOperator(
        task_id="create_gold_dengue_clima",
        bash_command=f"python {CREATE_GOLD_DENGUE_CLIMA}",
    )

    # Dependency: primeiro silver (dengue e inmet), depois gold
    [task_silver_dengue, task_silver_inmet] >> task_gold
