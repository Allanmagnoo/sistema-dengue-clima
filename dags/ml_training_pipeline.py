from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

# Define path for ML script
TRAIN_MODEL_V3 = "src/models/train_v3_hybrid.py"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False, # Idealmente True se tiver SMTP configurado
    "email_on_retry": False,
    "retries": 0, # ML training is expensive/long, maybe manual retry is better or just 1
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    dag_id="05_ml_training_pipeline",
    default_args=default_args,
    description="Orchestrates Machine Learning Model Training (XGBoost Hybrid v3)",
    schedule="@weekly", # Re-treinar semanalmente com novos dados
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ml", "xgboost", "training", "v3"],
) as dag:

    # Task 1: Train Model
    # Este script já salva o modelo .joblib e o metadata .json
    task_train_model = BashOperator(
        task_id="train_xgboost_v3",
        bash_command=f"python {TRAIN_MODEL_V3}",
    )

    # Task 2: Validate/Notify (Placeholder)
    # Futuramente, podemos adicionar um passo que lê o metadata.json 
    # e falha a DAG se o R2 for menor que um threshold (ex: 0.5)
    
    # Por enquanto, apenas o treino
    task_train_model
