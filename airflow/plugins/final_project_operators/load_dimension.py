from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 table="",
                 sql="",
                 append_only=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.sql = sql
        self.append_only = append_only

    def execute(self, context):
        db = PostgresHook(postgres_conn_id=self.conn_id)

        if not self.append_only:
            self.log.info(f"Deleting table {self.table} ...")

            db.run(f"DELETE FROM {self.table}")

        insert_statement = f"INSERT INTO {self.table} {self.sql}"
        self.log.info(f"Inserting data into table {self.table} ...")

        db.run(insert_statement)
        self.log.info(f"Done inserting into {self.table}")

