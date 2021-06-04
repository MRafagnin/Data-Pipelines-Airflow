from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 dest_table='',
                 sql_statement='',
                 update_mode="overwrite", # or "insert"
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dest_table = dest_table
        self.sql_statement = sql_statement
        self.update_mode = update_mode

    def execute(self, context):
        self.log.info('Hooking Redshift!')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('Loading dim table {}'.format(self.dest_table))
        if self.update_mode == 'overwrite':
            update_query = 'TRUNCATE {}; INSERT INTO {} ({})'.format(self.dest_table, self.dest_table, self.sql_statement)
        elif self.update_mode == 'insert':
            update_query = 'INSERT INTO {} ({})'.format(self.dest_table, self.sql_statement)
        redshift.run(update_query)