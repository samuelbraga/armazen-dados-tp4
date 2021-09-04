from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from scripts.transform import transform

from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 9, 7),
    "email": ["samuelfantini@ufmg.br"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG("s3_transformer", default_args=default_args, schedule_interval= '@once') as dag:

    
    # Inicio da Pipeline
    start_of_data_pipeline = DummyOperator(task_id='start_of_data_pipeline', dag=dag)

    # Definindo a tarefa para realização de web scrapping
    transform = PythonOperator(
        task_id='movie_review_web_scraping_stage',
        python_callable=collect_adoro_cinema_data,
        op_kwargs={
            'quantidade_paginas': 1,
        },
    )
    
    # Fim da Pipeline
    end_of_data_pipeline = DummyOperator(task_id='end_of_data_pipeline', dag=dag)