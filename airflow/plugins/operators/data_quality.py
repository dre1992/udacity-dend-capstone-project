from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
     Runs the data quality checks against the data the staging data

         :param redshift_conn_id: the connection id to redshift
         :param stmt: the sql statement to run the check
         :param table: the table
         :expected_result: the result to be checked against

     """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 stmt=None,
                 table="",
                 expected_result=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.stmt = stmt
        self.table = table
        self.expected_result = expected_result
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info(f'DataQualityOperator for {self.table}')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        records = redshift.get_records(self.stmt)
        if records[0][0] != self.expected_result:
            raise ValueError(f"{records[0][0]} not matching {self.expected_result}")
        else:
            self.log.info("Quality Check Successful ")
