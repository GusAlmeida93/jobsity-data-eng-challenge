from datetime import datetime, timedelta
from airflow import DAG
import os
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSSynchronizeBucketsOperator, GCSListObjectsOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocSubmitJobOperator, DataprocDeleteClusterOperator
from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryDeleteTableOperator, BigQueryCheckOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
REGION = os.environ["REGION"]
LOCATION = os.environ["LOCATION"]
LANDING_BUCKET_ZONE = os.environ["LANDING_BUCKET_ZONE"]
PROCESSING_BUCKET_ZONE = os.environ["PROCESSING_BUCKET_ZONE"]
CURATED_BUCKET_ZONE = os.environ["CURATED_BUCKET_ZONE"]
DATAPROC_CLUSTER_NAME = os.environ["DATAPROC_CLUSTER_NAME"]
PYSPARK_URI = os.environ["PYSPARK_URI"]
BQ_DATASET_NAME = os.environ["BQ_DATASET_NAME"]
BQ_TABLE_NAME = os.environ["BQ_TABLE_NAME"]

default_args = {
    'owner': 'gustavo almeida',
    'depends_on_past': False,
    'email': ['gusalmeida93@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1}

with DAG(
    dag_id="gcp-pipeline",
    tags=['development', 'cloud storage', 'cloud dataproc', 'google bigquery'],
    default_args=default_args,
    start_date=datetime(year=2021, month=1, day=16),
    schedule_interval='@daily',
    catchup=False
) as dag:

    create_gcs_processing_bucket = GCSCreateBucketOperator(
        task_id="create_gcs_processing_bucket",
        bucket_name=PROCESSING_BUCKET_ZONE,
        storage_class="REGIONAL",
        location=LOCATION,
        labels={"env": "dev", "team": "airflow"}
    )

    gcs_sync_trips_landing_to_processing_zone = GCSSynchronizeBucketsOperator(
        task_id="gcs_sync_trips_landing_to_processing_zone",
        source_bucket=LANDING_BUCKET_ZONE,
        source_object="files/trips/",
        destination_bucket=PROCESSING_BUCKET_ZONE,
        destination_object="files/trips/",
        allow_overwrite=True
    )
    
    list_files_processing_zone = GCSListObjectsOperator(
        task_id="list_files_processing_zone",
        bucket=PROCESSING_BUCKET_ZONE
    )

    dp_cluster_config = {
        "master_config": {
            "num_instances": 1,
            "machine_type_uri": "n1-standard-2",
            "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 100},},
        "worker_config": {
            "num_instances": 2,
            "machine_type_uri": "n1-standard-2",
            "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 100},},
    }

    create_dataproc_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        cluster_name=DATAPROC_CLUSTER_NAME,
        cluster_config=dp_cluster_config,
        region=REGION,
        use_if_exists=True    
        )

    job_py_spark_trips = {
        "reference": {"project_id": GCP_PROJECT_ID},
        "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
        "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
    }

    py_spark_job_submit = DataprocSubmitJobOperator(
        task_id="py_spark_job_submit",
        project_id=GCP_PROJECT_ID,
        location=LOCATION,
        job=job_py_spark_trips,
        asynchronous=True
    )

    dataproc_job_sensor = DataprocJobSensor(
        task_id="dataproc_job_sensor",
        project_id=GCP_PROJECT_ID,
        location=LOCATION,
        dataproc_job_id="{{task_instance.xcom_pull(task_ids='py_spark_job_submit')}}",
        poke_interval=30
    )

    bq_create_dataset_trips = BigQueryCreateEmptyDatasetOperator(
        task_id="bq_create_dataset_trips",
        dataset_id=BQ_DATASET_NAME
    )

    ingest_dt_into_bq_table_trips = GCSToBigQueryOperator(
        task_id="ingest_dt_into_bq_table_trips",
        bucket=CURATED_BUCKET_ZONE,
        source_objects=['ds_trips/*.parquet'],
        destination_project_dataset_table=f'{GCP_PROJECT_ID}:{BQ_DATASET_NAME}.{BQ_TABLE_NAME}',
        source_format='parquet',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        autodetect=True,
        bigquery_conn_id ='bigquery_default',
        google_cloud_storage_conn_id='bigquery_default'
    )

    check_bq_trips_tb_count = BigQueryCheckOperator(
            task_id="check_bq_trips_tb_count",
            sql=f"SELECT COUNT(*) FROM {BQ_DATASET_NAME}.{BQ_TABLE_NAME}",
            use_legacy_sql=False,
            location="us"
        )

    delete_dataproc_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        project_id=GCP_PROJECT_ID,
        region=REGION,
        cluster_name=DATAPROC_CLUSTER_NAME
    )

    delete_bucket_processing_zone = GCSDeleteBucketOperator(
        task_id="delete_bucket_processing_zone",
        bucket_name=PROCESSING_BUCKET_ZONE
    )

create_gcs_processing_bucket >> gcs_sync_trips_landing_to_processing_zone >> list_files_processing_zone >> create_dataproc_cluster >> py_spark_job_submit >> dataproc_job_sensor >> bq_create_dataset_trips >> ingest_dt_into_bq_table_trips >> check_bq_trips_tb_count >> [delete_dataproc_cluster, delete_bucket_processing_zone]
