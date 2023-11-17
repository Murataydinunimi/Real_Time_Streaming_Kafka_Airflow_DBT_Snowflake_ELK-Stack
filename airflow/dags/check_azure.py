from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.microsoft.azure.sensors.wasb import WasbPrefixSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

from dotenv import load_dotenv
load_dotenv()
import os





dag = DAG(
    'dag_azure_storage',
    start_date=datetime(2023, 1, 1),
    schedule_interval=timedelta(seconds=160), 
    catchup=False,
)


azure_blob_sensor_taxi = WasbPrefixSensor(
    task_id='azure_blob_sensor_taxi_calls',
    container_name='taxi-app-data',
    prefix='transactions.json', 
    wasb_conn_id = 'wasb_conn_id',
    timeout=60 * 60 * 24,  
    poke_interval=120,  
    mode='poke', 
    dag=dag
)

trigger_snowflake_tasks = TriggerDagRunOperator(
    task_id='trigger_pipeline',
    trigger_dag_id='pipeline',
    dag=dag
)

azure_blob_sensor_taxi >> trigger_snowflake_tasks
