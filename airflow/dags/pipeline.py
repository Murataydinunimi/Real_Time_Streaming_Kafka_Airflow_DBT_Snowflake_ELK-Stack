from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator

from docker.types import Mount

## import custom functions
from utils import predict
from utils import move_file_between_containers
from utils import write_ml_mart_data
from utils import load_predictions

import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv(dotenv_path='azure.env')

dag = DAG('pipeline',start_date=datetime.now(), schedule_interval=None,catchup=False)

snowflake_populate_taxi_calls_query = """
    copy into taxi_app.transactions_raw.taxi_calls_json (var) from @azure_stage
file_format=TAXI_APP_FILE_FORMAT;
"""

snowflake_task_populate_taxi_calls = SnowflakeOperator(
    task_id='populate_taxi_calls',
    sql=snowflake_populate_taxi_calls_query,
    snowflake_conn_id='snowflake_conn_id',  
    warehouse='COMPUTE_WH',  
    database='TAXI_APP',  
    schema='TRANSACTIONS',  
    autocommit=True,  
    dag=dag,
)


snowflake_populate_pred_query = """
  copy into taxi_app.transactions_raw.taxi_calls_predictions_json (var) from @AZURE_STAGE_PREDICTIONS
file_format=TAXI_APP_FILE_FORMAT;
"""

snowflake_task_populate_pred = SnowflakeOperator(
    task_id='populate_pred',
    sql=snowflake_populate_pred_query,
    snowflake_conn_id='snowflake_conn_id',  
    warehouse='COMPUTE_WH',  
    database='TAXI_APP',  
    schema='TRANSACTIONS',  
    autocommit=True,  
    dag=dag,
)
 


snowflake_get_ml_mart_data_query = """
        SELECT *,NULL AS DT_INGESTION_TIMESTAMP
FROM transactions.ML_MART;
"""

# Define the SnowflakeOperator task
snowflake_get_ml_mart_data = SnowflakeOperator(
    task_id='get_ml_mart_data',
    sql=snowflake_get_ml_mart_data_query,
    snowflake_conn_id='snowflake_conn_id',  
    warehouse='COMPUTE_WH',  
    database='TAXI_APP',  
    schema='TRANSACTIONS',  
    autocommit=True,  
    dag=dag
)



ml_mart_data_to_local = PythonOperator(
    task_id='snowflake_to_local_file',
    python_callable=write_ml_mart_data,
    provide_context=True,
    dag=dag
)


 
move_taxi_app_file= PythonOperator(
    task_id='move_taxi_app_file',
    python_callable=move_file_between_containers,
    op_args=['taxi-app-data', 'taxi-app-data-uploaded'],
    dag=dag,
)

move_predictions_file= PythonOperator(
    task_id='move_predictions_file',
    python_callable=move_file_between_containers,
    op_args=['predictions', 'predictions-uploaded'],
    dag=dag,
)



run_dbt_taxi_task = DockerOperator(
    task_id='create_taxi_app_model',
    image='custom_dbt_image',
    api_version='auto',
    docker_url='unix://var/run/docker.sock', 
    command='sh -c "cd /dbtlearn && dbt run --models taxi_calls* --project-dir /dbtlearn"',
    mounts=[Mount(source='<your_path_to_the_repo>Real_Time_Streaming_Kafka_Airflow_DBT_Snowflake_ELK-Stack/dbtlearn',target='/dbtlearn',type='bind')],
    network_mode='container:dbt',  
    dag=dag
)

run_dbt_calls_result_task = DockerOperator(
    task_id='create_calls_result_model',
    image='custom_dbt_image',
    api_version='auto',
    docker_url='unix://var/run/docker.sock', 
    command='sh -c "cd /dbtlearn && dbt run --models calls_result --project-dir /dbtlearn"',
    mounts=[Mount(source='<your_path_to_the_repo>Real_Time_Streaming_Kafka_Airflow_DBT_Snowflake_ELK-Stack/dbtlearn',target='/dbtlearn',type='bind')],
    network_mode='container:dbt',  
    dag=dag
)

run_dbt_ml_mart_task = DockerOperator(
    task_id='create_ml_mart_model',
    image='custom_dbt_image',
    api_version='auto',
    docker_url='unix://var/run/docker.sock', 
    command='sh -c "cd /dbtlearn && dbt run --models ml_mart --project-dir /dbtlearn"',
    mounts=[Mount(source='<your_path_to_the_repo>Real_Time_Streaming_Kafka_Airflow_DBT_Snowflake_ELK-Stack/dbtlearn',target='/dbtlearn',type='bind')],
    network_mode='container:dbt',  
    dag=dag
)

run_dbt_metrics_to_kibana_task = DockerOperator(
    task_id='create_metrics_to_kibana_model',
    image='custom_dbt_image',
    api_version='auto',
    docker_url='unix://var/run/docker.sock', 
    command='sh -c "cd /dbtlearn && dbt run --models metrics_to_kibana --project-dir /dbtlearn"',
    mounts=[Mount(source='<your_path_to_the_repo>Real_Time_Streaming_Kafka_Airflow_DBT_Snowflake_ELK-Stack/dbtlearn',target='/dbtlearn',type='bind')],
    network_mode='container:dbt',  
    dag=dag
)
run_dbt_pred_task = DockerOperator(
    task_id='create_predictions_model',
    image='custom_dbt_image',
    api_version='auto',
    docker_url='unix://var/run/docker.sock', 
    command='sh -c "cd /dbtlearn && dbt run --models predictions* --project-dir /dbtlearn"',
    mounts=[Mount(source='<your_path_to_the_repo>Real_Time_Streaming_Kafka_Airflow_DBT_Snowflake_ELK-Stack/dbtlearn',target='/dbtlearn',type='bind')],
    network_mode='container:dbt',  
    dag=dag
)

write_predictions = PythonOperator(
    task_id='write_predictions',
    python_callable=predict,
    provide_context=True,
    dag=dag
)

load_predictions_to_azure = PythonOperator(
    task_id='load_predictions_to_azure',
    python_callable = load_predictions,
    provide_context=True,
    dag=dag
)

snowflake_task_populate_taxi_calls >> move_taxi_app_file >> run_dbt_taxi_task >> run_dbt_calls_result_task >> \
run_dbt_ml_mart_task >> snowflake_get_ml_mart_data >> ml_mart_data_to_local >> write_predictions >> \
load_predictions_to_azure >> snowflake_task_populate_pred >> move_predictions_file >> run_dbt_pred_task >> run_dbt_metrics_to_kibana_task
 

