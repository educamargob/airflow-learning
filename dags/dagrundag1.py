from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime

with DAG(
    'dagrundag1',
    description='Dag que roda outra dag',
    schedule_interval=None,
    start_date=datetime(2025, 9, 20),
    catchup=False
) as dag:

    task1 = BashOperator(task_id="tsk1", bash_command='sleep 5')
    task2 = TriggerDagRunOperator(task_id="tsk2", trigger_dag_id='dagrundag2')
    

task1 >> task2