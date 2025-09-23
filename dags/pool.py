from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from datetime import datetime


with DAG(
    'pool',
    description='pool',
    schedule_interval=None,
    start_date=datetime(2025, 9, 20),
    catchup=False
) as dag:
    

    task1 = BashOperator(task_id="tsk1", bash_command='sleep 5', pool="meupool")
    task2 = BashOperator(task_id="tsk2", bash_command='sleep 5', pool="meupool", priority_weight=5)
    task3 = BashOperator(task_id="tsk3", bash_command='sleep 5', pool="meupool")
    task4 = BashOperator(task_id="tsk4", bash_command='sleep 5', pool="meupool", priority_weight=10)

    
#task1