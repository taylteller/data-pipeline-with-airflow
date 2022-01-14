from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import LoadDimensionOperator
from helpers import SqlQueries


def load_dim_tables_subdag(parent_dag_name,
                           task_id,
                           redshift_conn_id,
                           aws_credentials_id,
                           table,
                           delete_load,
                           sql,
                           *args, **kwargs):
    """
    Returns a subdag to load staging tables data into dimensional tables in Redshift
    """
    
    dag = DAG(f"{parent_dag_name}.{task_id}",**kwargs)

    load_dim_table = LoadDimensionOperator(
        task_id=f"load_{table}_dim_table",
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        sql=sql,
        table=table,
        delete_load=delete_load
    )

    load_dim_table

    return dag