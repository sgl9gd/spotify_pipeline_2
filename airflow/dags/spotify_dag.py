import datetime as dt
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.timetables.interval import CronDataIntervalTimetable
from sqlalchemy import create_engine

# from airflow.utils.dates import days_ago

from transforms.access import create_app
from transforms.etl import spotify_etl

# def run_flask_app():
#     app = create_app()
#     app.run(host='0.0.0.0', debug=False, use_reloader=False, port=8888)

def run_etl_script():
    print('Start')
    df_song, df_transform = spotify_etl
    conn = BaseHook.get_connection('postgres_sql')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    # Eventually we want to make this "append"
    df_song.to_sql('my_played_tracks', engine, if_exists='replace')


default_args = {
    'owner':'airflow',
    'depends_on_past': False,
    'start_date': dt.datetime(2025,3,17),
    'email': ['sling1778@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1)
}

dag = DAG(
    dag_id = 'spotify_dag',
    default_args = default_args,
    description='Spotify ETL Process',
    # timetable=CronDataIntervalTimetable('@daily', timezone="EST"),
    schedule="@daily",
    catchup=False
)

with dag:
    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        # postgres_conn_id='postgre_sql',
        conn_id = 'postgres_default',
        sql="""
            CREATE TABLE IF NOT EXISTS my_played_tracks(
            song_name VARCHAR(200),
            artist_name VARCHAR(200),
            played_at VARCHAR(200),
            timestamp VARCHAR(200),
            CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
        )
        """
    )

    run_flask = PythonOperator(
        task_id='run_flask_app',
        python_callable=run_flask_app,
        dag=dag
    )

    run_etl = PythonOperator(
        task_id='spotify_etl',
        python_callable=run_etl_script,
        dag=dag
    )