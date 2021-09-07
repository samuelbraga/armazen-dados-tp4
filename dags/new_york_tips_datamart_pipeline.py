from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from scripts.datamart_datatime import create_datamart
from scripts.get_normalized_s3 import get_file

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

    get_file_normalized = PythonOperator(
        task_id='get_normalized_file',
        python_callable=get_file,
        op_kwargs={
            'bucket_name': 'tp-final-armazen-dados',
            'file_key': 'normalized/normalized_yellow_tripdata_2015-01.csv'
        },
    )
    
    datamart_datetime = PythonOperator(
        task_id='cria_datamarts',
        python_callable=create_datamart,
        op_kwargs={
            'bucket_name': 'tp-final-armazen-dados',
            'file_key': 'normalized/normalized_yellow_tripdata_2015-01.csv'
        },
    )
    
    # Fim da Pipeline
    end_of_data_pipeline = DummyOperator(task_id='end_of_data_pipeline', dag=dag)

start_of_data_pipeline >> get_file_normalized >> [ datamart_datetime ] >> end_of_data_pipeline