"""
HW2 DAG: weather pipeline with per-city TaskGroups.

Features implemented:
- TaskGroup per city: extract -> transform -> branch -> load
- XCom-driven data passing between tasks (no shared mutable state)
- BranchPythonOperator routes to normal load or alert + load
- Retry logic and explicit error handling in task callables
"""

from __future__ import annotations

from datetime import timedelta
from urllib.parse import urljoin

import pendulum
import requests
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.utils.task_group import TaskGroup


# City coordinates are stable and explicit for training purposes.
CITIES = {
    "Lviv": (49.8397, 24.0297),
    "Kyiv": (50.4501, 30.5234),
    "Kharkiv": (49.9935, 36.2304),
    "Odesa": (46.4825, 30.7233),
    "Zhmerynka": (49.0345, 28.1061),
}

WIND_ALERT_THRESHOLD = 12.0  # m/s


def _weather_base_url() -> str:
    conn = BaseHook.get_connection("weather_conn_http")
    schema = conn.schema or "https"
    host = conn.host or "api.openweathermap.org"
    if conn.port:
        return f"{schema}://{host}:{conn.port}/"
    return f"{schema}://{host}/"


def _create_table_if_needed() -> None:
    sql = """
    CREATE TABLE IF NOT EXISTS weather_hw2 (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        city TEXT NOT NULL,
        observed_at TEXT NOT NULL,
        temp REAL,
        humidity REAL,
        cloudiness REAL,
        wind_speed REAL,
        is_alert INTEGER NOT NULL DEFAULT 0,
        UNIQUE(city, observed_at)
    );
    """
    hook = SqliteHook(sqlite_conn_id="sqlite_default")
    conn = hook.get_conn()
    try:
        conn.execute(sql)
        conn.commit()
    except Exception as exc:
        raise AirflowException(f"Failed to prepare weather table: {exc}") from exc
    finally:
        conn.close()


def _extract_weather(city: str, lat: float, lon: float) -> dict:
    try:
        api_key = Variable.get("WEATHER_API_KEY")
    except Exception as exc:
        raise AirflowException(f"Missing WEATHER_API_KEY variable: {exc}") from exc

    base_url = _weather_base_url()
    endpoint = "data/2.5/weather"
    url = urljoin(base_url, endpoint)

    try:
        response = requests.get(
            url,
            params={
                "lat": lat,
                "lon": lon,
                "appid": api_key,
                "units": "metric",
            },
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as exc:
        raise AirflowException(f"API request failed for {city}: {exc}") from exc
    except ValueError as exc:
        raise AirflowException(f"Invalid JSON in API response for {city}: {exc}") from exc

    return payload


def _transform_weather(city: str, extract_task_id: str, ti) -> dict:
    raw = ti.xcom_pull(task_ids=extract_task_id)
    if not raw:
        raise AirflowException(f"No XCom payload from {extract_task_id}")

    try:
        transformed = {
            "city": city,
            "observed_at": pendulum.from_timestamp(raw["dt"], tz="UTC").to_iso8601_string(),
            "temp": float(raw["main"]["temp"]),
            "humidity": float(raw["main"]["humidity"]),
            "cloudiness": float(raw["clouds"]["all"]),
            "wind_speed": float(raw["wind"]["speed"]),
        }
    except (KeyError, TypeError, ValueError) as exc:
        raise AirflowException(f"Transform failed for {city}: {exc}") from exc

    return transformed


def _branch_on_wind(transform_task_id: str, group_id: str, ti) -> str:
    record = ti.xcom_pull(task_ids=transform_task_id)
    if not record:
        raise AirflowException(f"No transformed record in XCom from {transform_task_id}")

    wind_speed = float(record["wind_speed"])
    if wind_speed >= WIND_ALERT_THRESHOLD:
        return f"{group_id}.send_alert"
    return f"{group_id}.normal_load"


def _send_alert(transform_task_id: str, city: str, ti) -> dict:
    record = ti.xcom_pull(task_ids=transform_task_id)
    if not record:
        raise AirflowException(f"No transformed record for alert in {city}")
    print(
        f"[ALERT] {city}: wind_speed={record['wind_speed']} m/s "
        f"(threshold={WIND_ALERT_THRESHOLD} m/s)"
    )
    return record


def _load_record(source_task_id: str, alert_flag: int, ti) -> None:
    record = ti.xcom_pull(task_ids=source_task_id)
    if not record:
        raise AirflowException(f"No XCom record to load from {source_task_id}")

    hook = SqliteHook(sqlite_conn_id="sqlite_default")
    conn = hook.get_conn()
    sql = """
    INSERT INTO weather_hw2 (
        city, observed_at, temp, humidity, cloudiness, wind_speed, is_alert
    ) VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(city, observed_at) DO UPDATE SET
        temp = excluded.temp,
        humidity = excluded.humidity,
        cloudiness = excluded.cloudiness,
        wind_speed = excluded.wind_speed,
        is_alert = excluded.is_alert
    """

    try:
        conn.execute(
            sql,
            (
                record["city"],
                record["observed_at"],
                record["temp"],
                record["humidity"],
                record["cloudiness"],
                record["wind_speed"],
                alert_flag,
            ),
        )
        conn.commit()
    except Exception as exc:
        conn.rollback()
        raise AirflowException(f"Load failed for {record.get('city', 'unknown')}: {exc}") from exc
    finally:
        conn.close()


def _build_city_group(city: str, lat: float, lon: float) -> TaskGroup:
    group_id = f"{city.lower()}_tg"
    with TaskGroup(group_id=group_id, tooltip=f"Pipeline for {city}") as group:
        extract = PythonOperator(
            task_id="extract",
            python_callable=_extract_weather,
            op_kwargs={"city": city, "lat": lat, "lon": lon},
        )

        transform = PythonOperator(
            task_id="transform",
            python_callable=_transform_weather,
            op_kwargs={"city": city, "extract_task_id": f"{group_id}.extract"},
        )

        branch = BranchPythonOperator(
            task_id="branch",
            python_callable=_branch_on_wind,
            op_kwargs={
                "transform_task_id": f"{group_id}.transform",
                "group_id": group_id,
            },
        )

        normal_load = PythonOperator(
            task_id="normal_load",
            python_callable=_load_record,
            op_kwargs={
                "source_task_id": f"{group_id}.transform",
                "alert_flag": 0,
            },
        )

        send_alert = PythonOperator(
            task_id="send_alert",
            python_callable=_send_alert,
            op_kwargs={
                "transform_task_id": f"{group_id}.transform",
                "city": city,
            },
        )

        alert_load = PythonOperator(
            task_id="alert_load",
            python_callable=_load_record,
            op_kwargs={
                "source_task_id": f"{group_id}.send_alert",
                "alert_flag": 1,
            },
        )

        extract >> transform >> branch
        branch >> normal_load
        branch >> send_alert >> alert_load

    return group


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
}


with DAG(
    dag_id="hw2_weather_taskgroups",
    description="HW2 weather pipeline with TaskGroups, XComs, branching, retries",
    start_date=pendulum.datetime(2026, 3, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["homework2", "weather", "taskgroup", "branching"],
) as dag:
    start = EmptyOperator(task_id="start")
    create_table = PythonOperator(
        task_id="create_table_if_needed",
        python_callable=_create_table_if_needed,
    )
    finish = EmptyOperator(task_id="finish")

    start >> create_table
    for city_name, (latitude, longitude) in CITIES.items():
        city_group = _build_city_group(city_name, latitude, longitude)
        create_table >> city_group >> finish
