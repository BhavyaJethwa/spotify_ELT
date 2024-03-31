from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from spotify_elt import run_spotify_elt , get_token , get_auth_header , get_songs_by_artists , search_for_artist 

default_args = {
    'owner': 'airflow',
    'depends_on_past' : False,
    'start_date' : datetime(2024 , 3 , 31),
    'email' : ['bhavyajethwa11@gmail.com'],
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'spotify_dag',
    default_args=default_args,
    description="first elt code"
)

run_elt = PythonOperator(task_id = 'complete_spotify_elt',
                         python_callable=run_spotify_elt,
                         dag=dag)


run_elt
