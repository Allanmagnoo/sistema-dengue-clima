from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

# Define paths for scripts
# We use relative paths assuming Airflow runs from project root or has access to it
# In a real Docker setup, these would be absolute paths inside the container
TRANSFORM_SILVER_SCRIPT = "src/jobs/silver/transform_silver.py"
TRANSFORM_GOLD_SCRIPT = "src/jobs/gold/transform_gold.py"

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

    # Task 1: Run Silver Transformation (Dengue & INMET)
    # We use BashOperator to run the python script which invokes Spark
    task_silver = BashOperator(
        task_id="transform_silver",
        bash_command=f"python {TRANSFORM_SILVER_SCRIPT}",
    )

    # Task 2: Run Gold Transformation
    task_gold = BashOperator(
        task_id="transform_gold",
        bash_command=f"python {TRANSFORM_GOLD_SCRIPT}",
    )

    # Dependency
    task_silver >> task_gold
