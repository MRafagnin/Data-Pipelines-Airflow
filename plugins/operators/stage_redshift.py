from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    copy_sql = """
                COPY {}
                FROM '{}'
                ACCESS_KEY_ID '{}'
                SECRET_ACCESS_KEY '{}'
                {}
            """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_path="auto",
                 delimiter",",
                 data_file="csv",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.delimiter = delimiter
        self.data_file = data_file

    def execute(self, context):

        credentials = AwsHook(self.aws_credentials).get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Deleting Table Data!")
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Copying Data - S3 to Redshift")
        
        if self.data_file == 'csv':
            file_format = "DELIMITER '{}'".format(self.delimiter)
        elif self.data_file == 'json':
            file_format = "FORMAT AS JSON '{}'".format(self.json_path)
        
        key_format = self.s3_key.format(**context)
        self.log.info('Rendered key is ' + key_format)
        s3_path = "s3://{}/{}".format(self.s3_bucket, key_format)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            file_format
        )
        redshift.run(formatted_sql)