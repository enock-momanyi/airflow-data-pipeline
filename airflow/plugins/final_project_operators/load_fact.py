import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql

    def execute(self, context):
        redshift_hook = PostgresHook(f"{self.redshift_conn_id}")
        #creates if not exists
        redshift_hook.run(self.sql["create"])
        redshift_hook.run(self.sql["truncate"])
        redshift_hook.run(self.sql["insert"])
        logging.info("Fact data loaded")
