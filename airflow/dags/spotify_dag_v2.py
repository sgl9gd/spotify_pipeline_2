import datetime as dt
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.timetables.interval import CronDataIntervalTimetable
from dotenv import load_dotenv
import os
import sys
import json
from sqlalchemy import create_engine

#Get your main directory (top layer)
main_dir = os.getcwd()
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..',))
# project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',))
sys.path.append('/opt/airflow')

from spotify_etl.extract import get_access_token, get_data
from spotify_etl.transform_load import etl

# Get refresh token, client ID/SECRET info
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
REFRESH_TOKEN = os.getenv('REFRESH_TOKEN')
TOKEN_URL = 'https://accounts.spotify.com/api/token'

# Get access token with refresh token
def get_token_task(**kwargs):
    print(REFRESH_TOKEN)
    access_token = get_access_token(CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN)
    print(access_token)
    kwargs['ti'].xcom_push(key='token', value=access_token)
    return access_token

# Pull raw JSON data
def data_pull_task(**kwargs):
    ti = kwargs['ti']
    access_token = ti.xcom_pull(task_ids='task_get_token', key='token')
    raw_data = get_data(access_token)
    with open('/tmp/recently_played.json','w') as f:
        json.dump(raw_data, f)

# Conduct transformation and load directly into postgres 
def etl_task(**kwargs):
    with open('/tmp/recently_played.json','r') as f:
        raw_data = json.load(f)
    df_songs, df_transform = etl(raw_data)

    # Create sqlalchemy engine
    conn = BaseHook.get_connection('postgres_default')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/airflow')

    # Load into tables
    df_songs.to_sql('my_tracks_played', engine, if_exists='append', index=False)
    df_transform.to_sql('artist_track_count', engine, if_exists='append', index=False)

def cleanup_temp_file(**kwargs):
    file_path = '/tmp/recently_played.json'
    if os.path.exists(file_path):
        os.remove(file_path)

default_args = {
    'owner':'airflow',
    'depends_on_past':False,
    'start_date': dt.datetime(2025,7,21),
    'email': ['sling1778@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1)
}

dag = DAG(
    dag_id = 'spotify_pipeline',
    default_args= default_args,
    description= 'Spotify ETL Pipeline',
    schedule='@daily',
    catchup=False
) 

with dag:
    create_table1 = SQLExecuteQueryOperator(
        task_id='create_table1',
        # postgres_conn_id='postgre_sql',
        conn_id = 'postgres_default',
        sql='''
            CREATE TABLE IF NOT EXISTS my_played_tracks(
            song_name VARCHAR(200),
            artist_name VARCHAR(200),
            played_at VARCHAR(200),
            timestamp VARCHAR(200),
            CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
        )
        '''
    )

    create_table2 = SQLExecuteQueryOperator(
    task_id='create_table2',
    # postgres_conn_id='postgre_sql',
    conn_id = 'postgres_default',
    sql='''
        CREATE TABLE IF NOT EXISTS artist_track_count(
        id VARCHAR(200),
        timestamp VARCHAR(200),
        artist_name VARCHAR(200),
        count VARCHAR(200)
    )
    '''
    )

    get_token = PythonOperator(
        task_id='task_get_token',
        python_callable=get_token_task,
    )

    pull_data = PythonOperator(
        task_id='task_pull_data',
        python_callable=data_pull_task,
    )

    transform_load = PythonOperator(
        task_id='transform_load_task',
        python_callable=etl_task,
    )

    clean_temp = PythonOperator(
        task_id='clean_temp_task',
        python_callable=cleanup_temp_file,
    )

    get_token >> pull_data >> transform_load >> clean_temp