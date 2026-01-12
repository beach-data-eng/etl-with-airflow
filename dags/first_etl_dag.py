# dags/first_etl_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_to_bq

with DAG(
    dag_id="first_etl_gcs_datetime",
    start_date=datetime(2026, 1, 1),
    schedule_interval="*/5 * * * *",   # ⏱ รันทุก 5 นาที
    catchup=False,
    tags=["etl", "beginner"],
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_data,
        op_kwargs={
            "run_dt": "{{ ds_nodash }}_{{ execution_date.hour | printf('%02d') }}"
        },
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_data,
        op_kwargs={
            "run_dt": "{{ ds_nodash }}_{{ execution_date.hour | printf('%02d') }}"
        },
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_to_bq,
        op_kwargs={
            "run_dt": "{{ ds_nodash }}_{{ execution_date.hour | printf('%02d') }}"
        },
    )

    extract >> transform >> load
