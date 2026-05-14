from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import sys

sys.path.append('/opt/airflow/scripts')

# Producer Kafka
def trigger_producer():
    from scraper.customer_producer import run_producer
    run_producer()

with DAG(
    dag_id='books_medallion_pipeline',
    start_date=datetime(2025, 5, 11),
    schedule='@hourly',
    catchup=False,
    tags=['kafka', 'spark', 'medallion']
) as dag:

    # 1. Producer Kafka
    ingest_task = PythonOperator(
        task_id='run_scraping_producer',
        python_callable=trigger_producer
    )
    
    bronze_task = BashOperator(
        task_id='transform_raw_to_bronze',
        bash_command="""
        spark-submit \
        --driver-memory 512m \
        --executor-memory 512m \
        --packages org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.1026 \
        /opt/airflow/scripts/spark/bronze/book_bronze.py
        """
    )

    silver_task = BashOperator(
        task_id='transform_bronze_to_silver',
        bash_command="""
        spark-submit \
        --driver-memory 512m \
        --executor-memory 512m \
        --packages org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.1026 \
        /opt/airflow/scripts/spark/silver/book_silver.py
        """
    )

    ingest_task >> bronze_task >> silver_task