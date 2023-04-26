from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id

    def execute(self, context):
        db = PostgresHook(postgres_conn_id=self.conn_id)

        for table in self.tables:
            query = f"SELECT COUNT(*) FROM {table}"
            expected_result = 1
            records = db.get_recordes(query)
            if len(records) < expected_result or len(records[0]) < expected_result:
                raise ValueError(f"Data quality check failed. {table} returned no results")

            num_records = records[0][0]
            if num_records < expected_result:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
