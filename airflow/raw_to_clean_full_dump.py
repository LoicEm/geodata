import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.operators.python_operator import PythonOperator


PROJECT = os.getenv("GCP_PROJECT")
DATASET_RAW = "geodata_raw"
DATASET_CLEAN = "geodata_clean"
DAG_ID = "ingest_full_raw_to_clean"


default_args = {
    "owner": "Airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 4, 1),
    "email": ["loic.macherel@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

TABLES_TO_DUMP = [
    'trees',
    'buildings'
]


def count_rows(project, dataset, table):
    hook = BigQueryHook()
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {project}.{dataset}.{table}")
    res = cursor.fetchone()
    return res[0]


def count_rows_raw_and_clean(project, table):
    raw_count = count_rows(project, DATASET_RAW, table)
    clean_count =  count_rows(project, DATASET_CLEAN, table)
    print(f"{table} raw dataset contains {raw_count} rows, {clean_count} rows uploaded to cleand dataset")


dag = DAG(DAG_ID, default_args=default_args, schedule_interval='@once ', catchup=False)

with dag:
    tables_etl = []
    for table in TABLES_TO_DUMP:
        ingestion_query = f"""
            SELECT 
                id, 
                epsg, 
                ST_GEOGFROMTEXT(geometry) as geometry, 
                CURRENT_TIMESTAMP() as ingested_at
            FROM {DATASET_RAW}.{table}
            WHERE ST_GEOGFROMTEXT(geometry) is not NULL
        """

        etl_operator = BigQueryOperator(sql=ingestion_query,
                                        destination_dataset_table=f'{PROJECT}.{DATASET_CLEAN}.{table}',
                                        write_disposition="WRITE_TRUNCATE",
                                        task_id=f"full_dump_{table}",
                                        use_legacy_sql=False)

        count_operator = PythonOperator(python_callable=count_rows_raw_and_clean,
                                        op_args=(PROJECT, table),
                                        task_id=f"rows_count_comparison_{table}")

        etl_operator >> count_operator




