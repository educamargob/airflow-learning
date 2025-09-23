from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.providers.http.sensors.http import HttpSensor
import requests

def query_api():
    response = requests.get("https://pokeapi.co/api/v2/pokemon/ditto")
    print(response.text)


with DAG(
    'httpsensor',
    description='httpsensor',
    schedule_interval=None,
    start_date=datetime(2025, 9, 20),
    catchup=False
) as dag:


    check_api = HttpSensor(task_id='check_api', 
                           http_conn_id='connection', #Nome cadastrado no admin do airflow
                           endpoint='pokemon/ditto',
                           poke_interval=5,
                           timeout=10)
    
    process_data = PythonOperator(task_id='process_data',python_callable=query_api)

check_api >> process_data