import random
import pendulum
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator

default_args = {
    'owner': 'airflow',
    'depend_on_past': False,
    
    'start_date': datetime(2022, 9, 11),
    
}

with DAG(
    dag_id="choose_model",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    default_args=default_args,
    catchup=False,
    schedule_interval=None
) as dag:
    
    run_model=DummyOperator(
        task_id="run_model")
    
    options=['model_1','model_2','model_3','model_4']
    
    choose_model=BranchPythonOperator(
        task_id='branching',
    python_callable=lambda: random.choice(options)
    )
    
    model_1=DummyOperator(
        task_id='model_1'
    )
    
    model_2=DummyOperator(
        task_id='model_2'
    )
        
    model_3=DummyOperator(
        task_id='model_3'
    )
       
    model_4=DummyOperator(
        task_id='model_4'
    )




run_model >> choose_model >> [model_1, model_2, model_3, model_4]








