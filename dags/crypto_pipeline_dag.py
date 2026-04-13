from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data-engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="crypto_batch_pipeline",
    default_args=default_args,
    description="Crypto batch pipeline (CoinGecko → HDFS → Spark)",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    ingestion_task = BashOperator(
        task_id="run_ingestion",
        bash_command="""
cd /opt/airflow &&
export PYTHONPATH=/opt/airflow &&
python -m src.ingestion.coingecko_api
"""
    )

    processing_task = BashOperator(
        task_id="run_spark_processing",
        bash_command="""
cd /opt/airflow &&
export PYTHONPATH=/opt/airflow &&
python -m src.processing.run_pipeline
"""
    )

    ingestion_task >> processing_task