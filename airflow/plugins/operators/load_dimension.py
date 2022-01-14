from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """
    Operator that loads data from staging tables into dimensional tables.
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 sql='',
                 table='',
                 delete_load=False,
                 *args, **kwargs):
        
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table = table
        self.delete_load = delete_load

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        
        if self.delete_load:
            self.log.info(f'Deleting from {self.table} before inserting records')
            redshift.run(f'DELETE FROM {self.table}')
        
        formatted_sql = self.sql.format(self.table)
        redshift.run(formatted_sql)
        self.log.info(f'Completed insertion for {self.table}')