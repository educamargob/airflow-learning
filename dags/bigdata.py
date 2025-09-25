from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from big_data_operator import BigDataOperator
from datetime import datetime


def print_result(ti):
    task_instance = ti.xcom_pull(task_ids='query_data')
    print("Resultado da consulta:")
    for row in task_instance:
        print(row)


with DAG(
    'bigdata',
    description='bigdata',
    schedule_interval=None,
    start_date=datetime(2025, 9, 20),
    catchup=False
) as dag:

    big_data = BigDataOperator(task_id="big_data",
                               path_to_csv_file = "/opt/airflow/data/Churn.csv",
                               path_to_save_file = "/opt/airflow/data/Churn.parquet",
                               file_type = 'parquet')
    
    json_big_data = BigDataOperator(task_id="json_big_data",
                               path_to_csv_file = "/opt/airflow/data/Churn.csv",
                               path_to_save_file = "/opt/airflow/data/Churn.json",
                               file_type = 'json')
    
big_data >> json_big_data