import warnings
warnings.filterwarnings("ignore")

import os
import sys
import logging
import socket
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, window, lit, current_timestamp

# --- Logging statt print ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("AggregateJob")

# --- Konfiguration ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")

# Pfade dynamisch oder Standard
SOURCE_PATH = os.getenv("PATH_PROCESSED", "s3a://processed-data/nyc_taxi/")
TARGET_PATH = os.getenv("PATH_ANALYTICS", "s3a://analytics-data/daily_kpis/")

def get_local_ip():
    """Hack für Docker Networking mit Spark Driver"""
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
    logger.info("Starte Spark Aggregation...")
    
    local_ip = get_local_ip()

    spark = SparkSession.builder \
        .appName("NYC Taxi Aggregation") \
        .master(MASTER_URL) \
        .config("spark.driver.host", local_ip) \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.network.timeout", "600s") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "2000") \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64m") \
        .config("spark.memory.fraction", "0.6") \
        .config("spark.memory.storageFraction", "0.1") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN") # Weniger Spam in den Logs

    try:
        logger.info(f"Lese Daten von: {SOURCE_PATH}")
        
        # Check: Existieren Daten?
        try:
            df = spark.read.parquet(SOURCE_PATH)
        except Exception:
            logger.warning("Keine Daten im Processed-Bucket gefunden. Breche ab.")
            return

        if df.rdd.isEmpty():
            logger.warning("Processed Data ist leer.")
            return

        logger.info("Berechne Daily Stats...")
        
        # Aggregation
        stats = df.groupBy(window(col("tpep_pickup_datetime"), "1 day")).agg(
            count("*").alias("total_trips"),
            sum("trip_distance").alias("total_distance"),
            sum("total_amount").alias("total_revenue")
        ).withColumn("calculated_at", current_timestamp())

        logger.info(f"Speichere Report nach: {TARGET_PATH}")
        
        # OPTIMIERUNG: .coalesce(1)
        # Bleibt bestehen wie gewünscht. Da das Ergebnis der Aggregation (Daily Stats) 
        # sehr klein ist (nur Zeilenanzahl = Anzahl Tage), ist coalesce(1) hier sicher.
        stats.coalesce(1).write \
            .mode("overwrite") \
            .parquet(TARGET_PATH)
            
        logger.info("Aggregation erfolgreich beendet.")

    except Exception as e:
        logger.error(f"Aggregation fehlgeschlagen: {e}")
        sys.exit(1) # Wichtig: Exit Code 1, damit der Orchestrator den Fehler merkt!
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

