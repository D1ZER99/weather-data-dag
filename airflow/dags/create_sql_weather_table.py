from datetime import datetime
import pendulum
import json

from airflow import DAG
from airflow.sdk import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

def create_weather_table():
    hook = SqliteHook(sqlite_conn_id="sqlite_default")
    sql = """
    CREATE TABLE IF NOT EXISTS weather_upd (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TIMESTAMP NOT NULL,
        temp FLOAT
        humidity FLOAT
        cloudiness FLOAT
        wind_speed FLOAT
    );
    """
    conn = hook.get_conn() 
    cur = conn.cursor()
    cur.executescript(sql)
    conn.commit()
    cur.close()
    conn.close()

# Create function to process data
def _process_weather(ti):
    info = ti.xcom_pull("extract_data")
    timestamp = info["dt"] 
    temp = info["main"]["temp"]
    humidity = info["main"]["humidity"]
    cloudiness = info["clouds"]["all"]
    wind_speed = info["wind"]["speed"]
    return timestamp, temp, humidity, cloudiness, wind_speed

#  Define the DAG to create the weather table
with DAG(
    dag_id="create_sql_weather_table",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 3, 1, tz="UTC"),
    catchup=True,
) as dag:
    # Task to create the weather table
    create_table = PythonOperator(
        task_id="create_weather_table",
        python_callable=create_weather_table,
    )

    # Task to check the weather API
    check_api = HttpSensor(
        task_id="check_weather_api",
        http_conn_id="weather_conn_http",
        endpoint="data/2.5/weather",
        request_params={"appid":Variable.get("WEATHER_API_KEY"), "q": "Lviv"}
    )

    # Extract data
    extract_data = HttpOperator(
        task_id="extract_data",
        http_conn_id="weather_conn_http",
        endpoint="data/2.5/weather",
        data={"appid": Variable.get("WEATHER_API_KEY"), "q": "Lviv"},
        method="GET",
        response_filter=lambda x: json.loads(x.text),
        log_response=True
    )

    # Process data
    process_data = PythonOperator(
        task_id="process_data",
        python_callable=_process_weather
    )

    # Inject data
    inject_data = SQLExecuteQueryOperator(
        task_id="inject_data",
        conn_id="sqlite_default",
        sql="""
        INSERT INTO weather (timestamp, temp) VALUES
        ({{ti.xcom_pull(task_ids='process_data')[0]}},
        {{ti.xcom_pull(task_ids='process_data')[1]}},
        {{ti.xcom_pull(task_ids='process_data')[2]}},
        {{ti.xcom_pull(task_ids='process_data')[3]}},
        {{ti.xcom_pull(task_ids='process_data')[4]}});
        """,
)

create_table >> check_api >> extract_data >> process_data >> inject_data




