from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 data_check=[],
                 exp_result=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.data_check=data_check
        self.exp_result=exp_result

    def execute(self, context):
        self.log.info('Data quality checks in progress')
        
        self.log.info('Hooking Redshift')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        checks = zip(self.data_check, self.tables, self.exp_result)
        for i in checks:
            try:
                redshift.run(check[0].format(i[1])) == i[2]
                self.log.info('Quality check Succeeded!')
            except:
                self.log.info('Quality check Failed!')
                raise AssertionError('Quality check Failed!')