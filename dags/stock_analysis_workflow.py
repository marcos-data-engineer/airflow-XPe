from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOKS_DIR = os.path.join(BASE_DIR, 'notebooks')

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'stock_analysis_pipeline',
    default_args=default_args,
    description='Pipeline ETL Medalhão (Bronze/Silver/Gold) para Análise do Mercado Financeiro',
    schedule_interval='@daily',
    catchup=False,
    tags=['finance', 'stocks', 'etl', 'pyspark']
)

bronze_task = PapermillOperator(
    task_id='ingest_bronze_stocks',
    input_nb=os.path.join(NOTEBOOKS_DIR, 'capture.ipynb'),
    output_nb=os.path.join(NOTEBOOKS_DIR, 'capture_output.ipynb'),
    dag=dag
)

silver_task = PapermillOperator(
    task_id='transform_silver_stocks',
    input_nb=os.path.join(NOTEBOOKS_DIR, 'silver.ipynb'),
    output_nb=os.path.join(NOTEBOOKS_DIR, 'silver_output.ipynb'),
    dag=dag
)

gold_task = PapermillOperator(
    task_id='aggregate_gold_spark',
    input_nb=os.path.join(NOTEBOOKS_DIR, 'gold.ipynb'),
    output_nb=os.path.join(NOTEBOOKS_DIR, 'gold_output.ipynb'),
    dag=dag
)

bronze_task >> silver_task >> gold_task
