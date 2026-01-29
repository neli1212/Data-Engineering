import os, sys, json, socket, logging, boto3
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month, current_timestamp
from pyspark.sql.types import DoubleType, IntegerType

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("SparkProcess")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")

RAW_BUCKET = "raw-data"
TARGET_PATH = "s3a://processed-data/nyc_taxi/"
MANIFEST_KEY = "processed_manifest.json"
CONFIG_BUCKET = "config"

def get_s3_client():
    return boto3.client("s3", endpoint_url=MINIO_ENDPOINT, aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

def load_manifest(s3):
    try:
        obj = s3.get_object(Bucket=CONFIG_BUCKET, Key=MANIFEST_KEY)
        return json.loads(obj['Body'].read().decode('utf-8'))
    except: return []

def save_manifest(s3, processed_files):
    s3.put_object(Bucket=CONFIG_BUCKET, Key=MANIFEST_KEY, Body=json.dumps(processed_files).encode('utf-8'))

def main():
    s3 = get_s3_client()
    
    target_bucket = "processed-data"
    try:
        s3.head_bucket(Bucket=target_bucket)
    except ClientError:
        logger.info(f"Creating missing bucket: {target_bucket}")
        s3.create_bucket(Bucket=target_bucket)

    try:
        all_objects = s3.list_objects_v2(Bucket=RAW_BUCKET)
    except Exception as e:
        logger.error(f"Could not read bucket {RAW_BUCKET}: {e}")
        return

    if 'Contents' not in all_objects:
        logger.info("No files found in raw-data bucket.")
        return

    all_files = [obj['Key'] for obj in all_objects['Contents'] if obj['Key'].endswith('.csv')]
    processed_files = load_manifest(s3)
    new_files = [f for f in all_files if f not in processed_files]
    
    if not new_files:
        logger.info("No new files to process.")
        return

    container_ip = socket.gethostbyname(socket.gethostname())
    spark = SparkSession.builder \
        .appName("NYC Taxi Processing") \
        .master(MASTER_URL) \
        .config("spark.driver.host", container_ip) \
        .config("spark.jars", "/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()

    processed_this_run = []
    try:
        for file_name in new_files:
            path = f"s3a://{RAW_BUCKET}/{file_name}"
            logger.info(f"Starting processing: {file_name}")
            
            df = spark.read.option("header", "true").csv(path)
            
            for c in df.columns:
                df = df.withColumnRenamed(c, c.lower())
            
            df_final = df.withColumn("tpep_pickup_datetime", to_timestamp(col("tpep_pickup_datetime"))) \
                .filter(col("tpep_pickup_datetime").isNotNull()) \
                .withColumn("year", year("tpep_pickup_datetime")) \
                .withColumn("month", month("tpep_pickup_datetime")) \
                .withColumn("processed_at", current_timestamp())

            df_final.repartition("year", "month") \
                .sortWithinPartitions("year", "month") \
                .write.mode("append") \
                .partitionBy("year", "month") \
                .parquet(TARGET_PATH)
            
            processed_this_run.append(file_name)
            logger.info(f"Successfully processed: {file_name}")

        if processed_this_run:
            save_manifest(s3, processed_files + processed_this_run)
            
    finally:
        spark.stop()

if __name__ == "__main__":
    main()