from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import json
import os

default_args = {
    'depends_on_past' : False,
    'email' : ['ecamargoborges@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
    }

def process_file(**kwargs):
    with open(Variable.get('path_file')) as f:
        data = json.load(f)
        kwargs['ti'].xcom_push(key='idtemp',value=data['idtemp'])
        kwargs['ti'].xcom_push(key='powerfactor',value=data['powerfactor'])
        kwargs['ti'].xcom_push(key='hydraulicpressure',value=data['hydraulicpressure'])
        kwargs['ti'].xcom_push(key='temperature',value=data['temperature'])
        kwargs['ti'].xcom_push(key='timestamp',value=data['timestamp'])
    os.remove(Variable.get('path_file'))

def avalia_temp(**kwargs):
    temperature= float(kwargs['ti'].xcom_pull(task_ids='get_data', key="temperature"))
    if(temperature >= 24):
        return 'group_check_temp.send_email_alert'
    else:
        return 'group_check_temp.send_email_normal'

#schedule_interval="*/3 * * * * " execução a cada 3 minutos
with DAG('windturbine',description='Dados da turbina',
          schedule_interval=None, start_date=datetime(2025,9,1),
          catchup=False, default_args=default_args, default_view='graph',
          doc_md="## Dag para registrar dados de turbina eólica"):

    group_check_temp = TaskGroup("group_check_temp")
    group_database = TaskGroup("group_database")

    file_sensor_task = FileSensor(
            task_id='file_sensor_task', 
            filepath=Variable.get('path_file'),
            fs_conn_id = 'fs_default',
            poke_interval = 10
    )

    get_data = PythonOperator(
            task_id='get_data',
            python_callable=process_file
    )

    create_table = PostgresOperator(
            task_id="create_table",
            postgres_conn_id='postgres',
            sql="""CREATE TABLE IF NOT EXISTS 
                SENSORS (IDTEMP VARCHAR, POWERFACTOR VARCHAR, HYDRAULICPRESSURE VARCHAR, 
                TEMPERATURE VARCHAR, TIMESTAMP VARCHAR);""",
            task_group=group_database
    )

    insert_data = PostgresOperator(
            task_id='insert_data',
            postgres_conn_id='postgres',
            parameters=(
                '{{ ti.xcom_pull(task_ids="get_data", key="idtemp") }}',
                '{{ ti.xcom_pull(task_ids="get_data", key="powerfactor") }}',
                '{{ ti.xcom_pull(task_ids="get_data", key="hydraulicpressure") }}',
                '{{ ti.xcom_pull(task_ids="get_data", key="temperature") }}',
                '{{ ti.xcom_pull(task_ids="get_data", key="timestamp") }}'
            ),
            sql="""INSERT INTO SENSORS (IDTEMP, POWERFACTOR, HYDRAULICPRESSURE, TEMPERATURE, TIMESTAMP)
                VALUES(%s, %s, %s, %s, %s);""",
            task_group=group_database
    )

    send_email_alert = EmailOperator(
            task_id='send_email_alert',
            to='ecamargoborges@gmail.com',
            subject='Airflow alert',
            html_content="""<h3>Alerta de Temperatura. </h3>
                            <p>Dag: windturbine </p>
                         """,
            task_group=group_check_temp
    )
    
    send_email_normal = EmailOperator(
            task_id='send_email_normal',
            to='ecamargoborges@gmail.com',
            subject='Airflow advise',
            html_content="""<h3>Temperaturas normais. </h3>
                            <p>Dag: windturbine </p>
                         """,
            task_group=group_check_temp
    )

    check_temp_branch = BranchPythonOperator(
            task_id='check_temp_branch',
            python_callable=avalia_temp,
            provide_context=True,
            task_group=group_check_temp
    )


with group_check_temp:
    check_temp_branch >> [send_email_alert,send_email_normal]

with group_database:
    create_table >> insert_data

file_sensor_task >> get_data
get_data >> group_check_temp
get_data >> group_database