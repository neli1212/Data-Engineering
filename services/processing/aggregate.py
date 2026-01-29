import os, sys, logging, socket, boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, window, current_timestamp

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("AggregateJob")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")

SOURCE_PATH = "s3a://processed-data/nyc_taxi/"
TARGET_PATH = "s3a://analytics-data/daily_stats/"

def main():
    logger.info("Starte Spark Aggregation Job...")
    s3 = boto3.client("s3", endpoint_url=MINIO_ENDPOINT, aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    try:
        s3.create_bucket(Bucket="analytics-data")
    except:
        pass

    container_ip = socket.gethostbyname(socket.gethostname())

    spark = SparkSession.builder \
        .appName("NYC Taxi Aggregation") \
        .master(MASTER_URL) \
        .config("spark.driver.host", container_ip) \
        .config("spark.jars", "/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "1g") \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
    
    try:
        df = spark.read.parquet(SOURCE_PATH)
        
        stats = df.groupBy(window(col("tpep_pickup_datetime"), "1 day")).agg(
            count("*").alias("total_trips"),
            sum("trip_distance").alias("total_distance_miles"),
            sum("total_amount").alias("total_revenue_usd")
        ).withColumn("calculated_at", current_timestamp())

        final_stats = stats.select(
            col("window.start").alias("date"),
            "total_trips", 
            "total_distance_miles", 
            "total_revenue_usd", 
            "calculated_at"
        ).orderBy("date")

        final_stats.coalesce(1).write.mode("overwrite").parquet(TARGET_PATH)
        logger.info("Aggregation erfolgreich beendet.")
    except Exception as e:
        logger.error(f"Fehler: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()