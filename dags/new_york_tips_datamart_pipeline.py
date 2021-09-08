from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from scripts.datamart_datatime import create_datetime_datamart
from scripts.datamart_fare import create_fare_datamart
from scripts.datamart_location import create_location_datamart
from scripts.datamart_trip import create_trip_datamart
from scripts.get_file_keys import delete_used_file_keys
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
def datamart_pipeline(parent_dag_name, child_dag_name, args):
    dag = DAG(
        dag_id='%s.%s' % (parent_dag_name, child_dag_name),
        default_args=args,
        schedule_interval="@daily")

    start_of_data_pipeline = DummyOperator(task_id='start_of_data_pipeline', dag=dag)

    get_file_normalized = PythonOperator(
        task_id='get_normalized_file',
        python_callable=get_file,
        op_kwargs={
            'bucket_name': 'tp-final-armazen-dados'
        },
        dag=dag
    )
    
    datetime_datamart = PythonOperator(
        task_id='cria_datetime_datamart',
        python_callable=create_datetime_datamart,
        op_kwargs={
            'bucket_name': 'tp-final-armazen-dados'
        },
        dag=dag
    )

    fare_datamart = PythonOperator(
        task_id='cria_fare_datamart',
        python_callable=create_fare_datamart,
        op_kwargs={
            'bucket_name': 'tp-final-armazen-dados'
        },
        dag=dag
    )
    
    location_datamart = PythonOperator(
        task_id='cria_location_datamart',
        python_callable=create_location_datamart,
        op_kwargs={
            'bucket_name': 'tp-final-armazen-dados'
        },
        dag=dag
    )

    trip_datamart = PythonOperator(
        task_id='cria_trip_datamart',
        python_callable=create_trip_datamart,
        op_kwargs={
            'bucket_name': 'tp-final-armazen-dados'
        },
        dag=dag
    )
    # Fim da Pipeline
    end_of_data_pipeline = PythonOperator(
        task_id='end_of_pipeline',
        python_callable=delete_used_file_keys,
        dag=dag
    )

    start_of_data_pipeline >> get_file_normalized >> [ datetime_datamart, fare_datamart, location_datamart, trip_datamart ] >> end_of_data_pipeline

    return dag
