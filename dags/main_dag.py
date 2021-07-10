from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    aws_credentials_id = "aws_credentials",
    redshift_conn_id = "redshift",
    table_name = "staging_events",
    s3_directory = "s3://udacity-dend/log_data",
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    aws_credentials_id = "aws_credentials",
    redshift_conn_id = "redshift",
    table_name = "staging_songs",
    s3_directory = "s3://udacity-dend/song_data",
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id = "redshift",
    table_name = "songplays",
    append_data = True,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id = "redshift",
    table_name = "users",
    sql = "SELECT DISTINCT userId, firstName, lastName, gender, level FROM staging_events WHERE page= 'NextSong' AND userId NOT IN (SELECT DISTINCT user_id FROM users)",
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id = "redshift",
    table_name ="songs",
    sql = "SELECT DISTINCT song_id, title, year, duration, artist_id FROM staging_songs WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs)",
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id = "redshift",
    table_name="artists",
    sql = "SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM staging_songs WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)",
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id = "redshift",
    table_name = "time",
    sql = "SELECT start_time, EXTRACT(hour FROM start_time) AS hour, EXTRACT(day FROM start_time), EXTRACT(week FROM start_time) AS day, EXTRACT(month FROM start_time), EXTRACT(year FROM start_time) AS year, EXTRACT(weekday FROM start_time) AS weekday FROM (SELECT DISTINCT timestamp 'epoch' + s.ts/1000 * INTERVAL '1 second' as start_time FROM staging_events s) WHERE start_time NOT IN (SELECT DISTINCT start_time FROM time)",
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id = "redshift",
    test_cases=[
        {
            'sql': 'SELECT COUNT(*) FROM songplays;',
            'op': 'gt',
            'val': 0
        },
        {
            'sql': 'SELECT COUNT(*) FROM songplays WHERE songid IS NULL;',
            'op': 'eq',
            'val': 0
        }
    ],
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
