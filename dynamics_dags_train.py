from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum
from datetime import datetime
import yaml
import os


default_args = {
    'owner': 'airflow',
    'depend_on_past': False,
    'start_date': datetime(2022, 9, 11),
    
}

CONFIG_PATH= "/opt/airflow/include/confs/dynamic_dag.yaml"

with DAG(
    dag_id="conf_file_dag",    
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    default_args=default_args,
    catchup=False,
    schedule_interval=None
) as dag:

    with open(CONFIG_PATH, "r") as yaml_file:
        config =yaml.load(yaml_file, Loader=yaml.FullLoader)


    files =config["file_names"]

    
    for file in files:
        extract = BashOperator(
            task_id=f'extract_{file}',
            bash_command="sleep 1"
        )

        transform = BashOperator(
            task_id=f'transform_{file}',
            bash_command="sleep 3"
        )

        load = BashOperator(
            task_id=f'load_{file}',
            bash_command="sleep 10"
        )


        extract >> transform >> load