from airflow import DAG
from airflow import Dataset
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd

mydataset = Dataset("/opt/airflow/data/Churn_new.csv")

def my_file():
    dataset = pd.read_csv("/opt/airflow/data/Churn.csv", sep=";")
    dataset.to_csv("/opt/airflow/data/Churn_new.csv", sep=";")

with DAG(
    'producer',
    description='Producer',
    schedule_interval=None,
    start_date=datetime(2025, 9, 20),
    catchup=False
) as dag:
    
    t1 = PythonOperator(task_id='t1', python_callable=my_file, outlets=[mydataset])

t1