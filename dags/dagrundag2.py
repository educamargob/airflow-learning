from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

with DAG(
    'dagrundag2',
    description='Dag rodada por outra dag',
    schedule_interval=None,
    start_date=datetime(2025, 9, 20),
    catchup=False
) as dag:

    task1 = BashOperator(task_id="tsk1", bash_command='sleep 5')
    task2 = BashOperator(task_id="tsk2", bash_command='sleep 5')
    

task1 >> task2