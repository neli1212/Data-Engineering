import os, boto3
from datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import ShortCircuitOperator, PythonOperator

NETWORK_NAME = 'nyc_data_network'

PROJ_PATH = os.getenv('AIRFLOW_PROJ_DIR', '/tmp')
CACHE_PATH = os.path.join(PROJ_PATH, "kaggle_cache")

S3_ENV = {
    'MINIO_ENDPOINT': 'http://minio:9000',
    'AWS_ACCESS_KEY_ID': 'minioadmin',
    'AWS_SECRET_ACCESS_KEY': 'minioadmin',
    'PYTHONUNBUFFERED': '1'
}

def check_for_new_data():
    s3 = boto3.client("s3", endpoint_url=S3_ENV['MINIO_ENDPOINT'], 
                      aws_access_key_id=S3_ENV['AWS_ACCESS_KEY_ID'], 
                      aws_secret_access_key=S3_ENV['AWS_SECRET_ACCESS_KEY'])
    try:
        s3.head_object(Bucket="config", Key="new_data.flag")
        return True 
    except:
        return False 

def clear_flag():
    s3 = boto3.client("s3", endpoint_url=S3_ENV['MINIO_ENDPOINT'], 
                      aws_access_key_id=S3_ENV['AWS_ACCESS_KEY_ID'], 
                      aws_secret_access_key=S3_ENV['AWS_SECRET_ACCESS_KEY'])
    try:
        s3.delete_object(Bucket="config", Key="new_data.flag")
    except:
        pass

with DAG(
    dag_id='nyc_taxi_pipeline', 
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily', 
    catchup=False
) as dag:

    ingest = DockerOperator(
        task_id='ingest',
        image='nyc_ingestion:latest',
        command='python ingest.py',
        docker_url='unix://var/run/docker.sock',
        network_mode=NETWORK_NAME,
        auto_remove=True,
        mount_tmp_dir=False,
        environment={
            **S3_ENV, 
            'DATASET_NAME': 'elemento/nyc-yellow-taxi-trip-data',
            'KAGGLEHUB_CACHE': '/root/.cache/kagglehub'
        },
        mounts=[{
            "source": CACHE_PATH, 
            "target": "/root/.cache/kagglehub", 
            "type": "bind"
        }]
    )

    condition = ShortCircuitOperator(
        task_id='check_if_new_data',
        python_callable=check_for_new_data
    )


    process = DockerOperator(
        task_id='process',
        image='nyc_processing:latest',
        command='python process.py',
        docker_url='unix://var/run/docker.sock',
        network_mode=NETWORK_NAME,
        environment={
            **S3_ENV, 
            'SPARK_MASTER_URL': 'spark://spark-master:7077'
        },
        auto_remove=True,
        mount_tmp_dir=False,
    )

    aggregate = DockerOperator(
        task_id='aggregate',
        image='nyc_processing:latest',
        command='python aggregate.py',
        docker_url='unix://var/run/docker.sock',
        network_mode=NETWORK_NAME,
        environment={
            **S3_ENV, 
            'SPARK_MASTER_URL': 'spark://spark-master:7077'
        },
        auto_remove=True,
        mount_tmp_dir=False
    )

    cleanup = PythonOperator(
        task_id='cleanup_flag',
        python_callable=clear_flag
    )

    ingest >> condition >> process >> aggregate >> cleanup