from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


def task_write(**kwargs):
    kwargs['ti'].xcom_push(key='valorxcom1',value='Teste')

def task_read(**kwargs):
    valor = kwargs['ti'].xcom_pull(key='valorxcom1')
    print(f"valor recuperado : {valor}")

with DAG(
    'exemplo_xcom',
    description='Dag com xcom',
    schedule_interval=None,
    start_date=datetime(2025, 9, 20),
    catchup=False
) as dag:
    

    task1 = PythonOperator(task_id="tsk1", python_callable=task_write)
    task2 = PythonOperator(task_id="tsk2", python_callable=task_read)
    

task1 >> task2