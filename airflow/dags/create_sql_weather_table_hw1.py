import pendulum
import requests
from urllib.parse import urljoin
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.sdk import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

# One Call 3.0 needs lat/lon (city names alone are not enough for this endpoint)
CITIES = {
    "Lviv": (49.84, 24.03),
    "Kyiv": (50.45, 30.52),
    "Kharkiv": (49.99, 36.23),
    "Odesa": (46.48, 30.72),
    "Zhmerynka": (49.03, 28.11),
}
def _base_url():
    c = BaseHook.get_connection("weather_conn_http")
    scheme = c.schema or "https"
    host = c.host or "api.openweathermap.org"
    return f"{scheme}://{host}/"
def create_table():
    hook = SqliteHook(sqlite_conn_id="sqlite_default")
    sql = """
    CREATE TABLE IF NOT EXISTS weather_lab_table (
        city TEXT NOT NULL,
        run_date TEXT NOT NULL,
        temp REAL,
        humidity REAL,
        clouds REAL,
        wind_speed REAL,
        PRIMARY KEY (city, run_date)
    );
    """
    conn = hook.get_conn()
    try:
        conn.execute(sql)
        conn.commit()
    finally:
        conn.close()
def fetch_and_save(**context):
    day = context["data_interval_start"].in_timezone("UTC").strftime("%Y-%m-%d")
    key = Variable.get("WEATHER_API_KEY")
    url = urljoin(_base_url(), "data/3.0/onecall/day_summary")
    hook = SqliteHook(sqlite_conn_id="sqlite_default")
    conn = hook.get_conn()
    try:
        cur = conn.cursor()
        for city, (lat, lon) in CITIES.items():
            r = requests.get(
                url,
                params={
                    "lat": lat,
                    "lon": lon,
                    "date": day,
                    "units": "metric",
                    "appid": key,
                },
                timeout=60,
            )
            r.raise_for_status()
            j = r.json()
            cur.execute(
                """
                INSERT INTO weather_lab_table (city, run_date, temp, humidity, clouds, wind_speed)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(city, run_date) DO UPDATE SET
                  temp=excluded.temp, humidity=excluded.humidity,
                  clouds=excluded.clouds, wind_speed=excluded.wind_speed
                """,
                (
                    city,
                    day,
                    j.get("temperature", {}).get("afternoon"),
                    j.get("humidity", {}).get("afternoon"),
                    j.get("cloud_cover", {}).get("afternoon"),
                    j.get("wind", {}).get("max", {}).get("speed"),
                ),
            )
        conn.commit()
    finally:
        conn.close()

with DAG(
    dag_id="weather_lab",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 3, 16, tz="UTC"),
    catchup=True,
) as dag:
    t1 = PythonOperator(task_id="create_table", python_callable=create_table)
    t2 = PythonOperator(task_id="fetch_and_save", python_callable=fetch_and_save)
    t1 >> t2
