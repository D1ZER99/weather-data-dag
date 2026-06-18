from __future__ import annotations
from airflow.sdk import DAG

from hw3_weather_factory import CITY_CONFIGS, create_city_dags


for city_config in CITY_CONFIGS:
    ingestion_dag, processing_dag = create_city_dags(city_config)
    globals()[ingestion_dag.dag_id] = ingestion_dag
    globals()[processing_dag.dag_id] = processing_dag