from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from scripts.datamart_s3 import create_datamarts

from datetime import datetime, timedelta

default_args = {
    "owner": "armazen_dados",
    "depends_on_past": False,
    "start_date": datetime.today().strftime('%Y-%m-%d'),
    "email": ["samuelfantini@ufmg.br"],
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG("new_york_tips_datamart", default_args=default_args, schedule_interval= '@once') as dag:

    start_of_data_pipeline = DummyOperator(task_id='start_of_data_pipeline', dag=dag)

    
    transform_stage = PythonOperator(
        task_id='cria_datamarts',
        python_callable=create_datamarts,
        op_kwargs={
            'bucket_name': 'tp-final-armazen-dados',
            'file_key': 'normalized/teste2021_09_07_16_22_00.csv'
        },
    )
    
    # Fim da Pipeline
    end_of_data_pipeline = DummyOperator(task_id='end_of_data_pipeline', dag=dag)

start_of_data_pipeline >> transform_stage >> end_of_data_pipeline