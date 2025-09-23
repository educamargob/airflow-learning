from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

with DAG(
    'dummy',
    description='Dag com dummy',
    schedule_interval=None,
    start_date=datetime(2025, 9, 20),
    catchup=False
) as dag:
    
    task1 = BashOperator(task_id="tsk1", bash_command='sleep 1')
    task2 = BashOperator(task_id="tsk2", bash_command='sleep 1')
    task3 = BashOperator(task_id="tsk3", bash_command='sleep 1')
    task4 = BashOperator(task_id="tsk4", bash_command='sleep 1')
    task5 = BashOperator(task_id="tsk5", bash_command='sleep 1')
    taskdummy = DummyOperator(task_id="taskdummy")


[task1, task2, task3] >> taskdummy >> [task4, task5,]