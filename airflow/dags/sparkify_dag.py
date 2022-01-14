from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (CreateTablesOperator, StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.operators.subdag_operator import SubDagOperator
from sparkify_dimtables_subdag import load_dim_tables_subdag
from helpers import SqlQueries


default_args = {
    'owner': 'Taylor',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 30),
    'depends_on_past' : False,
    'retries' : 3,
    'retry_delay' : timedelta(minutes=5),
    'catchup' : False,
    'email_on_retry' : False
}

dag_name='sparkify_dag2'
dag = DAG(dag_name,
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs = 3
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_redshift_tables = CreateTablesOperator(
    task_id='Create_tables',
    dag=dag,
    redshift_conn_id='redshift'
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table='staging_events',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    region='us-west-2',
    log_json_file = 'log_json_path.json',
    execution_date=default_args['start_date'],
    provide_context=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table='staging_songs',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    region='us-west-2',
    provide_context=True
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql=SqlQueries.songplay_table_insert,
    provide_context=True
)

load_user_dimension_table = SubDagOperator(
    subdag=load_dim_tables_subdag(
        parent_dag_name=dag_name,
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='users',
        sql=SqlQueries.user_table_insert,
        start_date=default_args['start_date'],
        delete_load=True,
    ),
    task_id='Load_user_dim_table',
    dag=dag,
)
    
load_song_dimension_table = SubDagOperator(
    subdag=load_dim_tables_subdag(
        parent_dag_name=dag_name,
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='songs',
        sql=SqlQueries.song_table_insert,
        start_date=default_args['start_date'],
        delete_load=True,
    ),
    task_id='Load_song_dim_table',
    dag=dag,
)

load_artist_dimension_table = SubDagOperator(
    subdag=load_dim_tables_subdag(
        parent_dag_name=dag_name,
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='artists',
        sql=SqlQueries.artist_table_insert,
        start_date=default_args['start_date'],
        delete_load=True,
    ),
    task_id='Load_artist_dim_table',
    dag=dag,
)

load_time_dimension_table = SubDagOperator(
    subdag=load_dim_tables_subdag(
        parent_dag_name=dag_name,
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='time',
        sql=SqlQueries.time_table_insert,
        start_date=default_args['start_date'],
        delete_load=True,
    ),
    task_id='Load_time_dim_table',
    dag=dag,
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = 'redshift',
    tables = ['artists', 'songplays', 'songs', 'time', 'users']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Task dependencies
start_operator >> create_redshift_tables >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator