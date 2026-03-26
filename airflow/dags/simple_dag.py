from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Write first simple hello world DAG
with DAG(
    dag_id='hello_world_first_dag',
    schedule="@daily",
    start_date=datetime.now() - timedelta(days=2),
    catchup=True,
) as dag:
    b = BashOperator(
        task_id='simple_command',
        bash_command='echo "Hello World!"'
    )
