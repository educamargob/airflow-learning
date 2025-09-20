from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

with DAG(
    'dagcomplexa',
    description='Dag complexa com varias etapas',
    schedule_interval=None,
    start_date=datetime(2025, 9, 20),
    catchup=False
) as dag:

    task1 = BashOperator(task_id="tsk1", bash_command='sleep 5')
    task2 = BashOperator(task_id="tsk2", bash_command='sleep 5')
    task3 = BashOperator(task_id="tsk3", bash_command='sleep 5')
    task4 = BashOperator(task_id="tsk4", bash_command='sleep 5')
    task5 = BashOperator(task_id="tsk5", bash_command='sleep 5')
    task6 = BashOperator(task_id="tsk6", bash_command='sleep 5')
    task7 = BashOperator(task_id="tsk7", bash_command='sleep 5')
    task8 = BashOperator(task_id="tsk8", bash_command='sleep 5')
    task9 = BashOperator(task_id="tsk9", bash_command='sleep 5', trigger_rule='one_failed')

task1 >> task2
task3 >> task4
[task2, task4] >> task5 >> task6
task6 >> [task7, task8, task9]