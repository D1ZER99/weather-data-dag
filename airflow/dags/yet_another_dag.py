from datetime import datetime, timedelta
from airflow import DAG
import logging
from airflow.operators.python import PythonOperator

def print_hello():
    logging.info("Hello World!")

with DAG(
    dag_id='yet_another_dag',
    schedule="@daily",
    start_date=datetime.now() - timedelta(days=2),
    catchup=True,
) as dag:
    hello_world = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello
)