from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator


class CSVToPostgresOperator(BaseOperator):
    """
    Transfers data from a CSV file to a PostgreSQL table using COPY command.
    """

    template_fields = (
        "csv_filepath",
        "table_name",
    )

    def __init__(
        self,
        csv_filepath: str,
        table_name: str,
        postgres_conn_id: str = "postgres_default",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.csv_filepath = csv_filepath
        self.table_name = table_name
        self.postgres_conn_id = postgres_conn_id

    def _copy_data(self, conn):
        # Generate the SQL command to COPY data
        copy_sql = f"""
        COPY {self.table_name} FROM stdin WITH CSV HEADER
        DELIMITER as ','
        """

        # Use the copy_expert method to load data
        with open(self.csv_filepath, "r") as f:
            cur = conn.cursor()
            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
            cur.close()

    def execute(self, context: dict):
        postgres = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        conn = postgres.get_conn()

        self._copy_data(conn)

        self.log.info(
            f"Data from {self.csv_filepath} copied to {self.table_name} in PostgreSQL."
        )
