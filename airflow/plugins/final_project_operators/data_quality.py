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
            records = db.get_recordes(query)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(
                    f"Data quality check failed. {table} returned no results")

            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(
                    f"Data quality check failed. {table} contained 0 rows")

            self.logging.info(
                f"Data quality on table {table} check passed with {records[0][0]} records")
