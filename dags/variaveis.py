from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime



def print_variable(**context):
    minha_var = Variable.get('minhavar')
    print(f'O valor da variável é: {minha_var}')


with DAG(
    'variaveis',
    description='Dag com variaveis',
    schedule_interval=None,
    start_date=datetime(2025, 9, 20),
    catchup=False
) as dag:
    

    task1 = PythonOperator(task_id="tsk1", python_callable=print_variable)

    
task1