# =====================================================================
# Imports
# =====================================================================
import os
import sys
import json
import socket
import logging
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, current_timestamp, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# =====================================================================
# Logging Setup
# =====================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("SparkProcess")

# =====================================================================
# Environment Variables
# =====================================================================
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")   
ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")           
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")       
MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")  

# =====================================================================
# Paths & Buckets
# =====================================================================
RAW_BUCKET = "raw-data"                                             
TARGET_PATH = "s3a://processed-data/nyc_taxi/"                     
MANIFEST_KEY = "processed_manifest.json"                            
CONFIG_BUCKET = "config"                                            

# =====================================================================
# Schema Definition
# =====================================================================
taxi_schema = StructType([                                           # Strict schema must obv be changed for other data...
    StructField("vendorid", StringType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("pickup_longitude", DoubleType(), True),
    StructField("pickup_latitude", DoubleType(), True),
    StructField("ratecodeid", IntegerType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("dropoff_longitude", DoubleType(), True),
    StructField("dropoff_latitude", DoubleType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True)
])

# =====================================================================
# Helper Functions
# =====================================================================
def get_s3_client():
    """
    Returns a configured S3 client.
    """
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )

def load_manifest(s3):
    """
    Loads list of already processed files from S3 manifest.

    Args:
        s3 (boto3.client): S3 client.

    Returns:
        list: Filenames already processed.
    """
    try:
        obj = s3.get_object(Bucket=CONFIG_BUCKET, Key=MANIFEST_KEY)
        return json.loads(obj['Body'].read().decode('utf-8'))
    except:
        return []

def save_manifest(s3, processed_files):
    """
    Saves updated processed file list to S3 manifest.

    Args:
        s3 (boto3.client): S3 client.
        processed_files (list): Filenames processed in this run.
    """
    s3.put_object(
        Bucket=CONFIG_BUCKET,
        Key=MANIFEST_KEY,
        Body=json.dumps(processed_files).encode('utf-8')
    )

# =====================================================================
# Main Routine
# =====================================================================
def main():
    """
    Processes raw NYC Taxi CSV files:
        1. Checks new files against manifest.
        2. Reads CSV with strict schema.
        3. Normalizes columns, enriches with year/month and timestamp.
        4. Writes partitioned Parquet to S3.
        5. Updates manifest.
    """
    s3 = get_s3_client()

    # Ensure target bucket exists
    try:
        s3.head_bucket(Bucket="processed-data")
    except:
        s3.create_bucket(Bucket="processed-data")

    # List raw CSV files
    try:
        all_objects = s3.list_objects_v2(Bucket=RAW_BUCKET)
    except Exception as e:
        logger.error(f"Bucket read error: {e}")
        return

    if 'Contents' not in all_objects:
        logger.info("No raw data found.")
        return

    all_files = [obj['Key'] for obj in all_objects['Contents'] if obj['Key'].endswith('.csv')]
    processed_files = load_manifest(s3)
    new_files = [f for f in all_files if f not in processed_files]

    if not new_files:
        logger.info("No new files to process.")
        return

    # Spark Session Setup
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
            logger.info(f"Processing: {file_name}")

            try:
                # Load CSV with strict schema
                df = spark.read \
                    .option("header", "true") \
                    .option("mode", "DROPMALFORMED") \
                    .schema(taxi_schema) \
                    .csv(path)

                # Normalize column names
                for c in df.columns:
                    df = df.withColumnRenamed(c, c.lower())

                # Enrich data with year, month, processed timestamp
                df_final = df \
                    .filter(col("tpep_pickup_datetime").isNotNull()) \
                    .withColumn("year", year("tpep_pickup_datetime")) \
                    .withColumn("month", month("tpep_pickup_datetime")) \
                    .withColumn("processed_at", current_timestamp())

                # Write Parquet partitioned by year/month
                df_final.repartition("year", "month") \
                    .sortWithinPartitions("year", "month") \
                    .write.mode("append") \
                    .partitionBy("year", "month") \
                    .parquet(TARGET_PATH)

                processed_this_run.append(file_name)
                logger.info(f"Successfully processed: {file_name}")

            except Exception as e:
                logger.error(f"Error processing file {file_name}: {e}")
                continue  # Skip to next file

        if processed_this_run:
            save_manifest(s3, processed_files + processed_this_run)

    finally:
        spark.stop()  

if __name__ == "__main__":
    main()
