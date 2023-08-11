from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator

from airflow.providers.postgres.operators.postgres import PostgresOperator

from algolia.operators.s3_to_local import S3PublicToLocalOperator
from algolia.operators.local_csv_file_transformer import LocalCSVTransformerOperator
from algolia.data_transformers import transform_shopify_configurations
from algolia.operators.csv_to_postgres import CSVToPostgresOperator

S3_BUCKET_ALGOLIA = "alg-data-public"
DAG_ID = "s3_to_postgres"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}


with DAG(
    DAG_ID,
    start_date=datetime(2019, 4, 1),
    end_date=datetime(2019, 4, 7),
    default_args=default_args,
    description="Download Shopify configurations from S3 to local filesystem",
    schedule_interval=timedelta(days=1),
    # Limiting the concurrency to 1 to avoid conflicts on loads and table creations
    concurrency=1,
    catchup=True,
) as dag:
    s3_to_local_task = S3PublicToLocalOperator(
        task_id="s3_to_local",
        s3_bucket="alg-data-public",  # This could be fetched from an Airflow variable
        s3_key="{{ ds }}.csv",
        local_path="raw/s3/{{ ds }}.csv",
    )

    transform_csv = LocalCSVTransformerOperator(
        task_id="transform_csv",
        input_filepath="raw/s3/{{ ds }}.csv",
        output_filepath="processed/s3/{{ ds }}.csv",
        na_values=["N/A"],
        processing_function=transform_shopify_configurations,
    )

    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="algolia_warehouse",
        sql="sql/create_shoppify_configs_table.sql",
    )

    delete_old_data = PostgresOperator(
        task_id="delete_old_data",
        postgres_conn_id="algolia_warehouse",
        sql="DELETE FROM shopify_configs WHERE export_date = '{{ ds }}'",
    )

    load_data_task = CSVToPostgresOperator(
        task_id="load_data",
        csv_filepath="processed/s3/{{ ds }}.csv",
        table_name="shopify_configs",
        postgres_conn_id="algolia_warehouse",
    )

    cleanup_task = BashOperator(
        task_id="cleanup",
        bash_command="rm -rf raw/s3/{{ ds }}.csv processed/s3/{{ ds }}.csv",
    )

    # Task order
    (
        s3_to_local_task
        >> transform_csv
        >> create_table
        >> delete_old_data
        >> load_data_task
        >> cleanup_task
    )
