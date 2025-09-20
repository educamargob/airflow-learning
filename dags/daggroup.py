from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

with DAG(
    'daggroup',
    description='Dag complexa com group',
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

    task_group = TaskGroup("tsk_group")

    task7 = BashOperator(task_id="tsk7", bash_command='sleep 5', task_group=task_group)
    task8 = BashOperator(task_id="tsk8", bash_command='sleep 5', task_group=task_group)
    task9 = BashOperator(task_id="tsk9", bash_command='sleep 5', task_group=task_group, trigger_rule='one_failed')
    

task1 >> task2
task3 >> task4
[task2, task4] >> task5 >> task6
task6 >> task_group