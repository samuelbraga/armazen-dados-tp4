from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from scripts.get_data_from_s3 import get_data_frame

from datetime import datetime, timedelta

default_args = {
    "owner": "tp4",
    "depends_on_past": False,
    "start_date": datetime.today().strftime('%Y-%m-%d'),
    "email": ["samuelfantini@ufmg.br"],
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG("new-york-tips-etl", default_args=default_args, schedule_interval= '@once') as dag:

    start_of_data_pipeline = DummyOperator(task_id='start_of_data_pipeline', dag=dag)

    
    get_data_frame_stage = PythonOperator(
        task_id='busca_dataframe_do_arquivo',
        python_callable=get_data_frame,
        op_kwargs={
            'bucket_name': 'tp-final-armazen-dados',
            'fileKey': 'raw/yellow_tripdata_2016-12.csv'
        },
    )
    
    # Fim da Pipeline
    end_of_data_pipeline = DummyOperator(task_id='end_of_data_pipeline', dag=dag)

start_of_data_pipeline >> get_data_frame_stage >> end_of_data_pipeline