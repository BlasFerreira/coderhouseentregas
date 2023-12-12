# my_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.docker_operator import DockerOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'my_dag',
    default_args=default_args,
    description='DAG for running Docker container',
    schedule_interval=timedelta(days=1),  
)

run_docker_task = DockerOperator(
    task_id='run_docker_task',
    image='my_service',  
    api_version='auto',
    auto_remove=True,
    command="python main_script.py", 
    dag=dag,
)

run_docker_task
