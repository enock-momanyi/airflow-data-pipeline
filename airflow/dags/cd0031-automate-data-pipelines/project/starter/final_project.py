from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from final_project_operators.data_quality import DataQualityOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'depends_on_past':False,
    'start_date': pendulum.now(),
    'retries':3,
    'retry_delay':timedelta(minutes=5),
    'catchup': False,
    'email_on_failure': False
    }

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        aws_connection_id='aws_credentials',
        redshift_conn_id="redshift",
        sql = SqlQueries.stage_events_sql
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        aws_connection_id='aws_credentials',
        redshift_conn_id="redshift",
        sql = SqlQueries.stage_songs_sql
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id="redshift",
        sql = SqlQueries.songplay_table
    )
    # modes:
    #       append-only --> "append-only"
    #       truncate-load --> "truncate-load"
    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",
        sql = SqlQueries.user_table,
        mode = "truncate-load"
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift",
        sql = SqlQueries.song_table,
        mode = "truncate-load"
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",
        sql = SqlQueries.artist_table,
        mode = "truncate-load"
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",
        sql = SqlQueries.time_table,
        mode = "truncate-load"
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        tests = SqlQueries.quality_checks
    )
    
    end_operator = DummyOperator(task_id='End_execution')
    
    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_to_redshift
    stage_songs_to_redshift >> load_songplays_table
    stage_events_to_redshift >> load_songplays_table
    load_songplays_table >> load_song_dimension_table >> run_quality_checks
    load_songplays_table >> load_user_dimension_table >> run_quality_checks
    load_songplays_table >> load_artist_dimension_table >> run_quality_checks
    load_songplays_table >> load_time_dimension_table >> run_quality_checks
    run_quality_checks >> end_operator
    
final_project_dag = final_project()
