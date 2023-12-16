from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging
class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tests=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        for test in self.tests:
            result = redshift_hook.get_records(f"{test['query']}")
            
            if len(result) < 1 or len(result[0]) < 1:
                raise ValueError(f"Data quality check failed. {test['query']} returned no results")
            result = result[0][0]
            if result != test['check']:
                raise ValueError(f"Data Quality Check failed for {test['query']}")
        logging.info(f"All Test Cases for Data Quality passed")