import warnings
warnings.filterwarnings("ignore")

import os
import sys
import json
import socket
import logging
import boto3
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("SparkProcess")

# --- Konfiguration ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")

RAW_BUCKET = "raw-data"
TARGET_PATH = "s3a://processed-data/nyc_taxi/"
MANIFEST_KEY = "processed_manifest.json"
CONFIG_BUCKET = "config"

TAXI_SCHEMA = StructType([
    StructField("VendorID", StringType(), True),
    StructField("tpep_pickup_datetime", StringType(), True),
    StructField("tpep_dropoff_datetime", StringType(), True),
    StructField("passenger_count", StringType(), True),
    StructField("trip_distance", StringType(), True),
    StructField("RatecodeID", StringType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", StringType(), True),
    StructField("DOLocationID", StringType(), True),
    StructField("payment_type", StringType(), True),
    StructField("fare_amount", StringType(), True),
    StructField("extra", StringType(), True),
    StructField("mta_tax", StringType(), True),
    StructField("tip_amount", StringType(), True),
    StructField("tolls_amount", StringType(), True),
    StructField("improvement_surcharge", StringType(), True),
    StructField("total_amount", StringType(), True),
    StructField("congestion_surcharge", StringType(), True),
    StructField("airport_fee", StringType(), True)
])

def get_s3_client():
    return boto3.client("s3", endpoint_url=MINIO_ENDPOINT, aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

def load_manifest(s3):
    try:
        obj = s3.get_object(Bucket=CONFIG_BUCKET, Key=MANIFEST_KEY)
        return json.loads(obj['Body'].read().decode('utf-8'))
    except ClientError:
        return []

def save_manifest(s3, processed_files):
    s3.put_object(Bucket=CONFIG_BUCKET, Key=MANIFEST_KEY, Body=json.dumps(processed_files).encode('utf-8'))

def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    except Exception:
        ip = "127.0.0.1"
    finally:
        s.close()
    return ip

def main():
    logger.info("Starte Processing Job...")
    s3 = get_s3_client()
    
    try:
        all_objects = s3.list_objects_v2(Bucket=RAW_BUCKET)
    except Exception as e:
        logger.error(f"Konnte Bucket {RAW_BUCKET} nicht lesen: {e}")
        sys.exit(1)

    if 'Contents' not in all_objects:
        logger.info("Bucket ist leer.")
        return

    all_files = [obj['Key'] for obj in all_objects['Contents'] if obj['Key'].endswith('.csv')]
    processed_files = load_manifest(s3)
    new_files = [f for f in all_files if f not in processed_files]
    
    if not new_files:
        logger.info("Keine neuen Dateien zu verarbeiten.")
        return

    local_ip = get_local_ip()
    
    # --- OPTIMIERTE SESSION GEGEN 403 FEHLER ---
    spark = SparkSession.builder \
        .appName("NYC Taxi Incremental Processing") \
        .master(MASTER_URL) \
        .config("spark.driver.host", local_ip) \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.files.maxPartitionBytes", "134217728") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    files_processed_successful = []

    try:
        for file_name in new_files:
            file_path = f"s3a://{RAW_BUCKET}/{file_name}"
            logger.info(f"Verarbeite: {file_name}")

            try:
                df = spark.read.option("header", "true").schema(TAXI_SCHEMA).csv(file_path)

                df_transformed = df \
                    .withColumn("tpep_pickup_datetime", to_timestamp(col("tpep_pickup_datetime"))) \
                    .withColumn("tpep_dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime"))) \
                    .withColumn("trip_distance", col("trip_distance").cast(DoubleType())) \
                    .withColumn("total_amount", col("total_amount").cast(DoubleType())) \
                    .withColumn("passenger_count", col("passenger_count").cast(IntegerType()))

                df_clean = df_transformed.filter(
                    (col("trip_distance") > 0) & 
                    (col("total_amount") >= 0) &
                    (col("tpep_pickup_datetime").isNotNull())
                )
                
                df_final = df_clean \
                    .withColumn("year", year("tpep_pickup_datetime")) \
                    .withColumn("month", month("tpep_pickup_datetime")) \
                    .withColumn("processed_at", current_timestamp())

                # SCHREIBEN: partitionBy ist gut, aber mode("append") triggert Checks.
                # mergeSchema=false oben sorgt dafür, dass Spark nicht 403-Fehler wirft.
                df_final.write \
                    .mode("append") \
                    .partitionBy("year", "month") \
                    .parquet(TARGET_PATH)
                
                files_processed_successful.append(file_name)

            except Exception as e:
                logger.error(f"Fehler bei Datei {file_name}: {e}")

        if files_processed_successful:
            processed_files.extend(files_processed_successful)
            save_manifest(s3, processed_files)
            logger.info(f"Batch erfolgreich beendet.")
        
    except Exception as e:
        logger.critical(f"Kritischer Fehler: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()