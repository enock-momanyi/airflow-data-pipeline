import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    modes = ("truncate-load","append-only")
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql="",
                 mode="truncate-load",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.mode = mode

    def execute(self, context):
        if self.mode not in self.modes:
            raise ValueError("f{self.mode} not a valid mode")
        redshift_hook = PostgresHook(f"{self.redshift_conn_id}")
        #creates if not exists
        redshift_hook.run(self.sql["create"])
        if self.mode == "truncate-load":
            redshift_hook.run(self.sql["truncate"])
            redshift_hook.run(self.sql["insert"])
        else:
            redshift_hook.run(self.sql["insert"])
        logging.info("Dimension data loaded")
