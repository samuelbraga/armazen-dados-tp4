from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from scripts.transform_data_s3 import transform_data

from datetime import datetime, timedelta

default_args = {
    "owner": "armazen_dados",
    "depends_on_past": False,
    "start_date": datetime.today().strftime('%Y-%m-%d'),
    "email": ["samuelfantini@ufmg.br"],
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG("new_york_tips_etl", default_args=default_args, schedule_interval= '@once') as dag:

    start_of_data_pipeline = DummyOperator(task_id='start_of_data_pipeline', dag=dag)

    
    transform_stage = PythonOperator(
        task_id='transforma_dataframe_arquivo',
        python_callable=transform_data,
        op_kwargs={
            'bucket_name': 'tp-final-armazen-dados',
            'file_key': 'raw/yellow_tripdata_2016-06.csv'
        },
    )
    
    # Fim da Pipeline
    end_of_data_pipeline = DummyOperator(task_id='end_of_data_pipeline', dag=dag)

start_of_data_pipeline >> transform_stage >> end_of_data_pipeline