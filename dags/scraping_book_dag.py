from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

sys.path.append('/opt/airflow/scripts')

def trigger_producer():
    from scraper.producer import run_producer
    run_producer()

with DAG(
    dag_id='scraping_to_kafka_v1',
    start_date=datetime(2025, 5, 11),
    schedule='@hourly',
    catchup=False,
    tags=['kafka', 'scraping']
) as dag:

    ingest_task = PythonOperator(
        task_id='run_scraping_producer',
        python_callable=trigger_producer
    )