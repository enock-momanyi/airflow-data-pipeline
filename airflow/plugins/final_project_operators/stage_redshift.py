import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.secrets.metastore import MetastoreBackend
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 aws_connection_id="",
                 redshift_conn_id="",
                 sql="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_connection_id = aws_connection_id
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql

    def execute(self, context):
        aws_connection = MetastoreBackend().get_connection(self.aws_connection_id)
        redshift_hook = PostgresHook(f"{self.redshift_conn_id}")
        redshift_hook.run(self.sql.format(
            access=aws_connection.login,
            secret=aws_connection.password
        ))
        logging.info("Data staged from s3 to redshift")
