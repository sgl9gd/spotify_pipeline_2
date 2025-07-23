import datetime as dt
from airflow import DAG
from airflow.operators import PythonOperator
from airflow.hooks import BaseHook
from airflow.providers import PostgresHook
from airflow.providers import PostgresOperator
from sqlalchemy import create_engine

from airflow.utils.dates import days_ago

from access import create_app
from etl import spotify_etl

def run_flask_app():
    app = create_app()
    app.run(host='0.0.0.0', debug=True, port=8888)

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
    'retry_delay': dt.teimdelta(minutes=1)
}

dag = DAG(
    'sptoify_dag',
    default_args = default_args,
    description='Spotify ETL Process',
    schedule_interval='@daily',
    catchup=False
)

with dag:
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgre_sql',
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
        python_callable=start_flask_app,
        dag=dag
    )

    run_etl = PythonOperator(
        task_id='spotify_etl',
        python_callable=run_etl_script,
        dag=dag
    )