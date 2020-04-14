import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.python_operator import PythonOperator


PROJECT = os.getenv("GCP_PROJECT")
DATASET_RAW = "geodata_raw"
DATASET_CLEAN = "geodata_clean"
DAG_ID = "ingest_full_gcs_to_clean"


default_args = {
    "owner": "Airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 4, 1),
    "email": ["loic.macherel@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}


class CustomGcsListToBigQueryOperator(GoogleCloudStorageToBigQueryOperator):
    """Pulls list of URIs from gcs and use them as source object for GoogleCloudStorageBigQueryOperator."""

    def __init__(self, bucket, prefix, destination_project_dataset_table, *args, **kwargs):
        source_objects = self.get_files_list(bucket, prefix)
        super().__init__(bucket=bucket,
                         source_objects=source_objects,
                         destination_project_dataset_table=destination_project_dataset_table,
                         *args,
                         **kwargs)

    @staticmethod
    def get_files_list(bucket, prefix):
        hook = GoogleCloudStorageHook()
        return hook.list(bucket=bucket, prefix=prefix, delimiter='.csv')


TABLES_TO_DUMP = {
    'trees': {'prefix': 'trees/'},
    'buildings': {'prefix': 'buildings/'}}


def count_rows(project, dataset, table):
    hook = BigQueryHook(use_legacy_sql=False)
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM `{project}.{dataset}.{table}`")
    res = cursor.fetchone()
    return res[0]


def count_rows_raw_and_clean(project, table):
    raw_count = count_rows(project, DATASET_RAW, table)
    clean_count = count_rows(project, DATASET_CLEAN, table)
    print(f"{table} raw dataset contains {raw_count} rows, {clean_count} rows uploaded to clean dataset")


dag = DAG(DAG_ID, default_args=default_args, schedule_interval=None, catchup=False)

with dag:
    tables_etl = []
    for table, table_config in TABLES_TO_DUMP.items():
        ingestion_query = f"""
            SELECT 
                id, 
                epsg, 
                SAFE.ST_GEOGFROMTEXT(geometry) as geometry, 
                CURRENT_TIMESTAMP() as ingested_at
            FROM {DATASET_RAW}.{table}
            WHERE SAFE.ST_GEOGFROMTEXT(geometry) is not NULL
        """

        gcs_to_bq_operator = CustomGcsListToBigQueryOperator(
            bucket="loic-m-geodata-dump",
            prefix=table_config["prefix"],
            destination_project_dataset_table=table,
            task_id=f"dump_gcs_to_bq_{table}"
        )

        etl_operator = BigQueryOperator(sql=ingestion_query,
                                        destination_dataset_table=f'{PROJECT}.{DATASET_CLEAN}.{table}',
                                        write_disposition="WRITE_TRUNCATE",
                                        task_id=f"raw_to_clean_{table}",
                                        use_legacy_sql=False)

        count_operator = PythonOperator(python_callable=count_rows_raw_and_clean,
                                        op_args=(PROJECT, table),
                                        task_id=f"rows_count_comparison_{table}")

        gcs_to_bq_operator >> etl_operator
        etl_operator >> count_operator




