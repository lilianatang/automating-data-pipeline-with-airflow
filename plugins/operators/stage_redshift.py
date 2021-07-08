from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
"""
The stage operator is expected to be able to load any JSON formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided. The operator's parameters should specify where in S3 the file is loaded and what is the target table.
The parameters should be used to distinguish between JSON file. Another important requirement of the stage operator is containing a templated field that allows it to load timestamped files from S3 based on the execution time and run backfills.
"""

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    staging_events_copy = """COPY {table_name} FROM '{s3_directory}' ACCESS_KEY_ID '{key}' SECRET_ACCESS_KEY '{secret_key}' json 's3://udacity-dend/log_json_path.json' compupdate on region 'us-west-2';"""
    staging_songs_copy = """COPY {table_name} FROM '{s3_directory}' ACCESS_KEY_ID '{key}' SECRET_ACCESS_KEY '{secret_key}' json 'auto' compupdate on region 'us-west-2';"""
    @apply_defaults
    def __init__(self,
                 aws_credentials_id,
                 redshift_conn_id,
                 table_name,
                 s3_directory,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.s3_directory = s3_directory
        
    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(self.redshift_conn_id)
        if self.table_name == 'staging_events':
            sql_copy_query = StageToRedshiftOperator.staging_events_copy.format(table_name = self.table_name, s3_directory = self.s3_directory, key_id = credentials.access_key, secret_id = credentials.secret_key)
            redshift.run(sql_copy_query)
            self.log.info('Log Data Was Imported')
        elif self.table_name == "staging_songs":
            sql_copy_query = StageToRedshiftOperator.staging_songs_copy.format(table_name = self.table_name, s3_directory = self.s3_directory, key_id = credentials.access_key, secret_id = credentials.secret_key)
            redshift.run(sql_copy_query)
            self.log.info('Song Data Was Imported')
        else:
            self.log.info('Data Import Failed.')





