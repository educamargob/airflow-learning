from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

with  DAG('triggerdag3', description="DAG com trigger",
        schedule_interval=None, start_date=datetime(2025,3,5),
        catchup=False) as dag:

        task1 = BashOperator(task_id="tsk1", bash_command="exit 1")
        task2 = BashOperator(task_id="tsk2", bash_command="exit 1")
        task3 = BashOperator(task_id="tsk3", bash_command="sleep 5", trigger_rule='all_failed')

        [task1,task2] >> task3