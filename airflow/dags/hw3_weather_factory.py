from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import pendulum
import requests
from airflow.exceptions import AirflowException
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import DAG, BaseHook, Param, Variable


@dataclass(frozen=True)
class CityConfig:
    city: str
    lat: float
    lon: float
    alert_threshold: float = 12.0
    schedule: str = "@daily"

    @property
    def slug(self) -> str:
        return self.city.lower().replace(" ", "_")


CITY_CONFIGS: tuple[CityConfig, ...] = (
    CityConfig(city="Lviv", lat=49.8397, lon=24.0297),
    CityConfig(city="Kyiv", lat=50.4501, lon=30.5234),
    CityConfig(city="Kharkiv", lat=49.9935, lon=36.2304),
    CityConfig(city="Odesa", lat=46.4825, lon=30.7233),
    CityConfig(city="Zhmerynka", lat=49.0345, lon=28.1061),
)

AIRFLOW_HOME = Path(os.environ.get("AIRFLOW_HOME", "/home/d1zer99/airflow"))
HW3_STORAGE_ROOT = AIRFLOW_HOME / "data" / "hw3"

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}


def _weather_base_url() -> str:
    conn = BaseHook.get_connection("weather_conn_http")
    schema = conn.schema or "https"
    host = conn.host or "api.openweathermap.org"
    if conn.port:
        return f"{schema}://{host}:{conn.port}/"
    return f"{schema}://{host}/"


def _ensure_parent_directory(file_path: str) -> Path:
    path = Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _read_json(path: str) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _extract_to_raw_file(city: str, lat: float, lon: float, raw_path: str) -> str:
    output_path = _ensure_parent_directory(raw_path)
    if output_path.exists():
        return raw_path

    try:
        api_key = Variable.get("WEATHER_API_KEY")
    except Exception as exc:
        raise AirflowException(f"Missing WEATHER_API_KEY variable: {exc}") from exc

    url = urljoin(_weather_base_url(), "data/2.5/weather")
    try:
        response = requests.get(
            url,
            params={
                "lat": float(lat),
                "lon": float(lon),
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

    artifact = {
        "city": city,
        "lat": float(lat),
        "lon": float(lon),
        "fetched_at": pendulum.now("UTC").to_iso8601_string(),
        "payload": payload,
    }
    _write_json(output_path, artifact)
    return raw_path


def _assert_artifact_exists(artifact_path: str) -> str:
    if not Path(artifact_path).exists():
        raise AirflowException(f"Expected artifact is missing: {artifact_path}")
    return artifact_path


def _transform_raw_artifact(raw_path: str, transformed_path: str, wind_alert_threshold: float) -> str:
    output_path = _ensure_parent_directory(transformed_path)
    if output_path.exists():
        return transformed_path

    raw_artifact = _read_json(raw_path)
    payload = raw_artifact.get("payload")
    if not payload:
        raise AirflowException(f"Raw payload is missing in artifact: {raw_path}")

    try:
        transformed = {
            "city": raw_artifact["city"],
            "observed_at": pendulum.from_timestamp(payload["dt"], tz="UTC").to_iso8601_string(),
            "temp": float(payload["main"]["temp"]),
            "humidity": float(payload["main"]["humidity"]),
            "cloudiness": float(payload["clouds"]["all"]),
            "wind_speed": float(payload["wind"]["speed"]),
            "wind_alert_threshold": float(wind_alert_threshold),
        }
    except (KeyError, TypeError, ValueError) as exc:
        raise AirflowException(f"Transform failed for artifact {raw_path}: {exc}") from exc

    transformed["is_alert"] = int(transformed["wind_speed"] >= transformed["wind_alert_threshold"])
    transformed["raw_artifact_path"] = raw_path
    _write_json(output_path, transformed)
    return transformed_path


def _run_quality_checks(transformed_path: str, quality_path: str) -> str:
    output_path = _ensure_parent_directory(quality_path)
    transformed = _read_json(transformed_path)

    required_fields = ("city", "observed_at", "temp", "humidity", "cloudiness", "wind_speed", "is_alert")
    missing_fields = [field for field in required_fields if field not in transformed]
    if missing_fields:
        raise AirflowException(f"Transformed artifact is missing fields: {missing_fields}")

    failures: list[str] = []
    if not transformed["city"]:
        failures.append("city must not be empty")
    if not -100.0 <= float(transformed["temp"]) <= 70.0:
        failures.append("temp must be within [-100, 70]")
    if not 0.0 <= float(transformed["humidity"]) <= 100.0:
        failures.append("humidity must be within [0, 100]")
    if not 0.0 <= float(transformed["cloudiness"]) <= 100.0:
        failures.append("cloudiness must be within [0, 100]")
    if not 0.0 <= float(transformed["wind_speed"]) <= 150.0:
        failures.append("wind_speed must be within [0, 150]")

    report = {
        "artifact_path": transformed_path,
        "checked_at": pendulum.now("UTC").to_iso8601_string(),
        "passed": not failures,
        "failures": failures,
    }
    _write_json(output_path, report)

    if failures:
        raise AirflowException(f"Data quality checks failed: {failures}")
    return quality_path


def _prepare_final_table() -> None:
    sql = """
    CREATE TABLE IF NOT EXISTS hw3_weather_final (
        city TEXT NOT NULL,
        observed_at TEXT NOT NULL,
        temp REAL NOT NULL,
        humidity REAL NOT NULL,
        cloudiness REAL NOT NULL,
        wind_speed REAL NOT NULL,
        is_alert INTEGER NOT NULL,
        raw_artifact_path TEXT NOT NULL,
        transformed_artifact_path TEXT NOT NULL,
        quality_report_path TEXT NOT NULL,
        PRIMARY KEY (city, observed_at)
    );
    """
    hook = SqliteHook(sqlite_conn_id="sqlite_default")
    conn = hook.get_conn()
    try:
        conn.execute(sql)
        conn.commit()
    except Exception as exc:
        raise AirflowException(f"Failed to prepare final dataset table: {exc}") from exc
    finally:
        conn.close()


def _load_final_dataset(transformed_path: str, quality_path: str) -> None:
    quality_report = _read_json(quality_path)
    if not quality_report.get("passed"):
        raise AirflowException(f"Quality report did not pass: {quality_path}")

    record = _read_json(transformed_path)
    hook = SqliteHook(sqlite_conn_id="sqlite_default")
    conn = hook.get_conn()
    sql = """
    INSERT INTO hw3_weather_final (
        city,
        observed_at,
        temp,
        humidity,
        cloudiness,
        wind_speed,
        is_alert,
        raw_artifact_path,
        transformed_artifact_path,
        quality_report_path
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(city, observed_at) DO UPDATE SET
        temp = excluded.temp,
        humidity = excluded.humidity,
        cloudiness = excluded.cloudiness,
        wind_speed = excluded.wind_speed,
        is_alert = excluded.is_alert,
        raw_artifact_path = excluded.raw_artifact_path,
        transformed_artifact_path = excluded.transformed_artifact_path,
        quality_report_path = excluded.quality_report_path
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
                int(record["is_alert"]),
                record["raw_artifact_path"],
                transformed_path,
                quality_path,
            ),
        )
        conn.commit()
    except Exception as exc:
        conn.rollback()
        raise AirflowException(f"Failed to load final dataset from {transformed_path}: {exc}") from exc
    finally:
        conn.close()


def _build_params(city_config: CityConfig) -> dict[str, Param]:
    return {
        "city": Param(city_config.city, type="string"),
        "lat": Param(city_config.lat, type="number"),
        "lon": Param(city_config.lon, type="number"),
        "wind_alert_threshold": Param(city_config.alert_threshold, type="number"),
        "storage_root": Param(str(HW3_STORAGE_ROOT), type="string"),
    }


def create_city_dags(city_config: CityConfig) -> tuple[DAG, DAG]:
    ingestion_dag_id = f"hw3_weather_ingestion_{city_config.slug}"
    processing_dag_id = f"hw3_weather_processing_{city_config.slug}"
    dag_params = _build_params(city_config)
    raw_path_template = "{{ params.storage_root }}/raw/{{ params.city | lower }}/{{ ds_nodash }}.json"
    transformed_path_template = (
        "{{ params.storage_root }}/transformed/{{ params.city | lower }}/{{ ds_nodash }}.json"
    )
    quality_path_template = "{{ params.storage_root }}/quality/{{ params.city | lower }}/{{ ds_nodash }}.json"

    with DAG(
        dag_id=ingestion_dag_id,
        description="HW3 ingestion DAG: fetch weather data and persist the raw artifact",
        schedule=city_config.schedule,
        start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
        catchup=False,
        default_args=DEFAULT_ARGS,
        params=dag_params,
        render_template_as_native_obj=True,
        tags=["hw3", "weather", "ingestion", city_config.slug],
    ) as ingestion_dag:
        start_ingestion = EmptyOperator(task_id="start")
        extract_raw_data = PythonOperator(
            task_id="extract_raw_data",
            python_callable=_extract_to_raw_file,
            op_kwargs={
                "city": "{{ params.city }}",
                "lat": "{{ params.lat }}",
                "lon": "{{ params.lon }}",
                "raw_path": raw_path_template,
            },
        )
        trigger_processing = TriggerDagRunOperator(
            task_id="trigger_processing_dag",
            trigger_dag_id=processing_dag_id,
            logical_date="{{ logical_date }}",
            conf={"raw_path": raw_path_template, "city": "{{ params.city }}"},
            reset_dag_run=True,
        )

        start_ingestion >> extract_raw_data >> trigger_processing

    with DAG(
        dag_id=processing_dag_id,
        description="HW3 processing DAG: read stored raw weather data and build the final dataset",
        schedule=None,
        start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
        catchup=False,
        default_args=DEFAULT_ARGS,
        params=dag_params,
        render_template_as_native_obj=True,
        tags=["hw3", "weather", "processing", city_config.slug],
    ) as processing_dag:
        wait_for_ingestion = ExternalTaskSensor(
            task_id="wait_for_ingestion",
            external_dag_id=ingestion_dag_id,
            allowed_states=["success"],
            failed_states=["failed"],
            check_existence=True,
            poll_interval=10.0,
            timeout=600,
            mode="reschedule",
        )
        create_final_table = PythonOperator(
            task_id="create_final_table",
            python_callable=_prepare_final_table,
        )
        ensure_raw_artifact = PythonOperator(
            task_id="ensure_raw_artifact",
            python_callable=_assert_artifact_exists,
            op_kwargs={"artifact_path": raw_path_template},
        )
        transform_raw_data = PythonOperator(
            task_id="transform_raw_data",
            python_callable=_transform_raw_artifact,
            op_kwargs={
                "raw_path": raw_path_template,
                "transformed_path": transformed_path_template,
                "wind_alert_threshold": "{{ params.wind_alert_threshold }}",
            },
        )
        run_quality_checks = PythonOperator(
            task_id="run_quality_checks",
            python_callable=_run_quality_checks,
            op_kwargs={
                "transformed_path": transformed_path_template,
                "quality_path": quality_path_template,
            },
        )
        load_final_dataset = PythonOperator(
            task_id="load_final_dataset",
            python_callable=_load_final_dataset,
            op_kwargs={
                "transformed_path": transformed_path_template,
                "quality_path": quality_path_template,
            },
        )
        finish_processing = EmptyOperator(task_id="finish")

        wait_for_ingestion >> create_final_table >> ensure_raw_artifact
        ensure_raw_artifact >> transform_raw_data >> run_quality_checks >> load_final_dataset >> finish_processing

    return ingestion_dag, processing_dag