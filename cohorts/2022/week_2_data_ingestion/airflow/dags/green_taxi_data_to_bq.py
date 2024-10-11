import os
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


PROJECT_ID = Variable.get("GCP_PROJECT_ID")
BUCKET = Variable.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = Variable.get("BIGQUERY_DATASET", default_var='trips_data_all')

date = '{{ execution_date.strftime("%Y-%m") }}'
file_name = f"green_tripdata_{date}.parquet"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"

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

def load_local_file_to_bigquery(file_path, table_id):
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT_ID)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        autodetect=True,
    )

    with open(file_path, "rb") as source_file:
        load_job = client.load_table_from_file(
            source_file,
            table_id,
            job_config=job_config,
        )

    load_job.result()  # Attend la fin du job
    print(f"Le fichier {file_path} a été chargé dans {table_id}.")


with DAG(
    dag_id="green_taxi_data_to_bq",
    start_date=datetime(2022, 1, 1),
    end_date=datetime(2022, 12, 2),
    schedule_interval="@monthly"
) as dag:

    download_dataset_task = PythonOperator(
        task_id="download_dataset_task",
        python_callable=download_dataset,
        op_kwargs={
            "url": url,
            "local_file": f"{path_to_local_home}/{file_name}"
        },
    )

    load_local_file_to_bigquery_task = PythonOperator(
        task_id="load_local_file_to_bigquery_task",
        python_callable=load_local_file_to_bigquery,
        op_kwargs={
            "file_path": f"{path_to_local_home}/{file_name}",
            "table_id": f"{PROJECT_ID}.{BIGQUERY_DATASET}.external_table_green_taxi"
        },
    )
    #62495

    """bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": f"external_table_green_taxi",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{file_name}"],
            },
        },
    )"""

    download_dataset_task >> load_local_file_to_bigquery_task