from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_statement="",
                 dest_table="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.dest_table=dest_table
        self.sql_statement=sql_statement
        
    def execute(self, context):
        self.log.info('Hooking Redshift!')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('Load Fact Table!')
        insert_query = 'INSERT INTO {} ({})'.format(self.dest_table, self.sql_statement)
        redshift.run(insert_query)
