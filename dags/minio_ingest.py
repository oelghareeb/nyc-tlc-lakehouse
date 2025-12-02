from datetime import datetime, timedelta
import pendulum
import requests
import boto3
from botocore.client import Config

from airflow.operators.python import PythonOperator
from airflow import DAG

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
    dag_id="nyc_ingest_minio",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["nyc", "minio"],
) as dag:

    # 1️⃣ RAW INGESTION → MINIO (skip if files exist)
    raw_ingestion = PythonOperator(
        task_id="download_and_upload_raw_files",
        python_callable=download_and_upload_all_files,
    )

raw_ingestion