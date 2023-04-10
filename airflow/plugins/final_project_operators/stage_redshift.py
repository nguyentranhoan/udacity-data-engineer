from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    copy_sql = """
        COPY {} 
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}';
    """

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_path="",
                 json_path="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_path = s3_path
        self.json_path = json_path
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        db = PostgresHook(postgres_conn_id=self.conn_id)

        db.run("DELETE FROM {}".format(self.table))

        # Backfill data
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            self.s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_path,
            self.execution_date
        )

        db.run(formatted_sql)
