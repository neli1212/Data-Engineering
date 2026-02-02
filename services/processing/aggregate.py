# =====================================================================
# Imports
# =====================================================================
import os
import sys
import logging
import socket
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum, avg, window, current_timestamp, unix_timestamp, round, expr
)

# =====================================================================
# Logging Setup
# =====================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("AggregateJob")

# =====================================================================
# Environment Variables
# =====================================================================
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")   
ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")            
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")        
MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")  
worker_mem = os.getenv("WORKER_MEMORY", "4g")
exec_mem = os.getenv("EXECUTOR_MEMORY", "2g")
worker_g = int(worker_mem.replace('g', '').replace('G', ''))
if worker_mem.endswith('m'):
    worker_g = worker_g / 1024
exec_g = int(exec_mem.replace('g', '').replace('G', ''))
if exec_mem.endswith('m'):
    exec_g = exec_g / 1024
driver_g = max(1, worker_g - exec_g - 1)  # Mindestens 1g
driver_mem = f"{driver_g}g"
# =====================================================================
# Paths
# =====================================================================
SOURCE_PATH = "s3a://processed-data/nyc_taxi/"           
TARGET_PATH = "s3a://analytics-data/daily_stats/"        

# =====================================================================
# Main Routine
# =====================================================================
def main():
    """
    Runs daily NYC Taxi enhanced aggregation using Spark.

    Steps:
        1. Connect to MinIO and create bucket if needed.
        2. Initialize SparkSession with S3A configuration.
        3. Load Parquet data.
        4. Compute additional columns (trip duration).
        5. Filter invalid trips.
        6. Aggregate daily statistics.
        7. Write results back to S3.

    Returns:
        None
    """
    logger.info("Starting Spark enhanced aggregation...")

    # S3 client
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )
    try:
        s3.create_bucket(Bucket="analytics-data")  
    except:
        pass  

    container_ip = socket.gethostbyname(socket.gethostname())

    # Spark session setup
    spark = SparkSession.builder \
        .appName("NYC Taxi Enhanced Aggregation") \
        .master(MASTER_URL) \
        .config("spark.driver.host", container_ip) \
        .config("spark.jars", "/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar") \
        .config("spark.executor.memory", exec_mem) \
        .config("spark.driver.memory", driver_mem) \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
    
    try:
        df = spark.read.parquet(SOURCE_PATH)

        # Compute trip duration in minutes
        df_enhanced = df.withColumn(
            "duration_min", 
            (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 60
        )

        # Filter invalid trips
        df_clean = df_enhanced.filter(
            (col("trip_distance") > 0) & 
            (col("duration_min") > 0) & 
            (col("total_amount") > 0)
        )

        # Aggregate daily statistics
        stats = df_clean.groupBy(window(col("tpep_pickup_datetime"), "1 day")).agg(
            count("*").alias("total_trips"),
            round(sum("total_amount"), 2).alias("total_revenue"),
            round(sum("trip_distance"), 2).alias("total_distance_miles"),
            round(avg("trip_distance"), 2).alias("avg_distance_miles"),
            round(avg("duration_min"), 1).alias("avg_duration_min"),
            round(avg("total_amount"), 2).alias("avg_cost_per_trip"),
            round(avg("tip_amount"), 2).alias("avg_tip_amount"),
            round((sum("tip_amount") / sum("fare_amount")) * 100, 2).alias("avg_tip_percent"),
            round(sum("trip_distance") / (sum("duration_min") / 60), 2).alias("avg_speed_mph")
        ).withColumn("calculated_at", current_timestamp())

        # Select final columns and sort
        final_stats = stats.select(
            col("window.start").alias("date"),
            "total_trips", 
            "total_revenue",
            "avg_cost_per_trip",
            "avg_distance_miles",
            "avg_duration_min",
            "avg_speed_mph",
            "avg_tip_percent",
            "calculated_at"
        ).orderBy("date")

        # loggs
        logger.info("Writing enhanced statistics...")
        final_stats.show(5) 
        final_stats.coalesce(1).write.mode("overwrite").parquet(TARGET_PATH)
        logger.info("Aggregation completed successfully.")

    except Exception as e:
        logger.error(f"Aggregation failed: {e}")
        sys.exit(1)
    finally:
        spark.stop()  

if __name__ == "__main__":
    main()
