from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    Operator that stages data from S3 onto Redshift.
    """
    
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            REGION '{}'
            TIMEFORMAT as 'epochmillisecs'
            TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
            FORMAT AS json '{}' 
        """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 region='',
                 log_json_file = '',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region= region
        self.log_json_file = log_json_file
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(self.redshift_conn_id)

        self.log.info('Clearing destination table')
        redshift.run(f'DELETE FROM {self.table}')

        self.log.info('Copying {self.table} from S3 to Redshift')
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        
        if self.log_json_file != '':
            self.log_json_file = 's3://{}/{}'.format(self.s3_bucket, self.log_json_file)
            copy_sql = self.copy_sql.format(self.table, s3_path, credentials.access_key, credentials.secret_key, self.region, self.log_json_file)
        else:
            copy_sql = self.copy_sql.format(self.table, s3_path, credentials.access_key, credentials.secret_key, self.region, 'auto')

        redshift.run(copy_sql)

        self.log.info(f"Finished copying {self.table} from S3 to Redshift")