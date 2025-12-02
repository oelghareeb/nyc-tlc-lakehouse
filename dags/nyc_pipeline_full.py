from datetime import datetime, timedelta
import pendulum
import requests
import boto3
from botocore.client import Config

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# -----------------------------
# MINIO / BUCKET CONFIG
# -----------------------------
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
BUCKET_NAME = "lakehouse"

# -----------------------------
# SOURCE DATA
# -----------------------------
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
MONTHS = [f"2025-{str(m).zfill(2)}" for m in range(1, 4)] # change the number of months if needed
DBT_PROJECT_DIR = "/opt/airflow/dbt"

# -----------------------------
# FUNCTION: DOWNLOAD & UPLOAD TO MINIO (SKIP IF EXISTS)
# -----------------------------
def download_and_upload_all_files(**kwargs):
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
    )

    # Create bucket if not exists
    buckets = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if BUCKET_NAME not in buckets:
        s3.create_bucket(Bucket=BUCKET_NAME)
        print(f"[INFO] Created bucket {BUCKET_NAME}")

    for month in MONTHS:
        file_name = f"yellow_tripdata_{month}.parquet"
        s3_key = f"raw/{file_name}"

        # Check if file already exists in MinIO
        objects = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=s3_key).get("Contents", [])
        if objects:
            print(f"[INFO] File {s3_key} already exists in MinIO, skipping download")
            continue

        # Download from URL
        url = f"{BASE_URL}/{file_name}"
        local_path = f"/tmp/{file_name}"

        print(f"[INFO] Downloading {url}")
        r = requests.get(url, stream=True)
        r.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

        # Upload to MinIO
        print(f"[INFO] Uploading → {s3_key}")
        s3.upload_file(local_path, BUCKET_NAME, s3_key)

    return True

# -----------------------------
# DEFAULT DAG ARGUMENTS
# -----------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2025, 1, 1, tz="UTC"),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# -----------------------------
# DAG DEFINITION
# -----------------------------
with DAG(
    dag_id="nyc_full_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["nyc", "trino", "iceberg", "dbt"],
) as dag:

    # 1️⃣ RAW INGESTION → MINIO (skip if files exist)
    raw_ingestion = PythonOperator(
        task_id="download_and_upload_raw_files",
        python_callable=download_and_upload_all_files,
    )

    # 2️⃣ CHECK IF ICEBERG TABLE EXISTS
    check_table = SQLExecuteQueryOperator(
        task_id='check_iceberg_table',
        conn_id='trino_default',
        sql="""
            SELECT table_name
            FROM iceberg.information_schema.tables
            WHERE table_schema = 'nyc_bronze'
              AND table_name = 'yellow_tripdata_raw'
        """,
        do_xcom_push=True,
    )

    # 3️⃣ BRANCH: Table exists → dbt_run, else → skip
    def branch_on_table(**context):
        result = context['ti'].xcom_pull(task_ids='check_iceberg_table')
        if result and len(result) > 0:
            return 'dbt_run'
        else:
            return 'table_not_found'

    branch_task = BranchPythonOperator(
        task_id='branch_if_table_exists',
        python_callable=branch_on_table,
        provide_context=True,
    )

    # 4️⃣ DBT RUN + TEST
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run',
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt test',
    )

    table_not_found = BashOperator(
        task_id="table_not_found",
        bash_command='echo "Iceberg table does not exist, skipping DBT tasks"',
    )

    # PIPELINE DEPENDENCIES
    raw_ingestion >> check_table >> branch_task
    branch_task >> dbt_run >> dbt_test
    branch_task >> table_not_found