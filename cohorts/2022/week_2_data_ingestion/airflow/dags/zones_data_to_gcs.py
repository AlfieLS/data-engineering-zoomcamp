import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = Variable.get("GCP_PROJECT_ID")
BUCKET = Variable.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = Variable.get("BIGQUERY_DATASET", default_var='trips_data_all')

csv_name = "taxi_zone_lookup.csv"
parquet_name = "taxi_zone_lookup.parquet"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

def download_dataset(url, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param url: URL to download the file
    :param local_file: source path & file-name
    :return:
    """
    import requests
    print(f"Downloading {url} to {local_file}")
    response = requests.get(url)
    with open(local_file, 'wb') as f:
        f.write(response.content)

def csv_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

with DAG(
    dag_id="zones_data_to_gcs",
    start_date=datetime(2023, 10, 9),
    schedule_interval="@once"
) as dag:

    download_dataset_task = PythonOperator(
        task_id="download_dataset_task",
        python_callable=download_dataset,
        op_kwargs={
            "url": url,
            "local_file": f"{path_to_local_home}/{csv_name}"
        },
    )

    csv_to_parquet_task = PythonOperator(
        task_id="csv_to_parquet_task",
        python_callable=csv_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{csv_name}"
        },
    )

    upload_to_gcs_task = PythonOperator(
        task_id="upload_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{parquet_name}",
            "local_file": f"{path_to_local_home}/{parquet_name}"
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": f"external_table_zones",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{parquet_name}"],
            },
        },
    )

    download_dataset_task >> csv_to_parquet_task >> upload_to_gcs_task >> bigquery_external_table_task