"""Airflow DAG: runs the Bronze -> Silver -> Gold pipeline every day.

Copy this file into your Airflow `dags/` folder and make sure the `electricity_etl`
package is installed (pip install .) and ELECTRICITYMAPS_API_KEY is set (Airflow
Variable/connection exported as env var). Each layer is its own task so a failed
layer can be retried alone. The run is incremental, so a missed day is caught up.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from electricity_etl import pipeline


def stage(name: str):
    return lambda: pipeline.main(["--stages", name])


with DAG(
    dag_id="electricity_maps_fr_etl",
    start_date=datetime(2025, 1, 1),
    schedule="0 2 * * *",          # every day at 02:00 UTC
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=10)},
    tags=["electricity-maps"],
) as dag:
    bronze = PythonOperator(task_id="bronze", python_callable=stage("bronze"))
    silver = PythonOperator(task_id="silver", python_callable=stage("silver"))
    gold = PythonOperator(task_id="gold", python_callable=stage("gold"))
    bronze >> silver >> gold
