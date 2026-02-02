# =====================================================================
# Imports
# =====================================================================
import os
import sys
import logging
import socket
import boto3
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import col

# =====================================================================
# Logging Setup
# =====================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("TrainJob")

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
MODEL_OUTPUT_PATH = "s3a://models/taxi_fare_predictor_v1/"         

# =====================================================================
# Main Routine
# =====================================================================
def main():
    """
    Full data ML training pipeline:
        1. Ensure models bucket exists.
        2. Load all processed NYC Taxi data.
        3. Apply data cleaning filters.
        4. Assemble features.
        5. Split train/test.
        6. Train linear regression model.
        7. Evaluate RMSE and R².
        8. Save model to S3.
        9. Show example predictions.
    """
    # S3 Client
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )
    try:
        s3.create_bucket(Bucket="models")  
        logger.info("Models bucket created/verified")
    except Exception:
        logger.info("Models bucket already exists")

    # Spark session setup
    container_ip = socket.gethostbyname(socket.gethostname())
    spark = SparkSession.builder \
        .appName("NYC Taxi Model Training - Full Data") \
        .master(MASTER_URL) \
        .config("spark.driver.host", container_ip) \
        .config("spark.jars", "/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.files.maxPartitionBytes", "128mb") \
        .config("spark.sql.shuffle.partitions", "500") \
        .config("spark.default.parallelism", "500") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.memory.storageFraction", "0.3") \
        .config("spark.executor.memory", exec_mem) \
        .config("spark.driver.memory", driver_mem) \
        .config("spark.driver.maxResultSize", max_res) \
        .config("spark.rdd.compress", "true")  \
        .config("spark.network.timeout", "600s")  \
        .getOrCreate()

    try:
        # Load all Parquet data
        logger.info(f"Loading ALL data from {SOURCE_PATH}")
        df = spark.read.parquet(SOURCE_PATH)
        available_columns = df.columns
        logger.info(f"Available columns: {available_columns}")

        # Determine target column
        if 'fare_amount' in available_columns:
            label_col = 'fare_amount'
            logger.info("Using 'fare_amount' as target variable")
        elif 'total_amount' in available_columns:
            label_col = 'total_amount'
            logger.warning("Using 'total_amount' as target variable")
        else:
            logger.error("No fare amount column found!")
            raise ValueError("No fare amount column found in data")

        # Apply data cleaning filters
        logger.info("Applying data cleaning filters...")
        data_clean = df.filter(
            (col("trip_distance") > 0) & 
            (col("trip_distance") <= 100) &
            (col(label_col) > 2.5) &
            (col(label_col) < 200) &
            (col("passenger_count") > 0) &
            (col("passenger_count") <= 6)
        ).select(
            col("trip_distance").cast("double"),
            col("passenger_count").cast("double"),
            col(label_col).cast("double").alias("label")
        ).dropna()

        # Partition and cache cleaned data
        data_partitioned = data_clean.repartition(500)
        logger.info("Caching cleaned data for training...")
        data_partitioned.cache()
        total_count = data_partitioned.count()
        logger.info(f"Training on clean data: {total_count:,} records")

        # Assemble feature vectors
        logger.info("Creating feature vectors...")
        assembler = VectorAssembler(
            inputCols=["trip_distance", "passenger_count"],
            outputCol="features"
        )
        final_data = assembler.transform(data_partitioned)

        # Split into train/test sets
        train_data, test_data = final_data.randomSplit([0.8, 0.2], seed=42)
        train_data.cache()

        # Train Linear Regression model
        logger.info(f"Training Linear Regression on {train_data.count():,} samples...")
        lr = LinearRegression(
            featuresCol="features",
            labelCol="label",
            maxIter=100,
            regParam=0.1,
            elasticNetParam=0.5,
            solver="normal",
            aggregationDepth=2
        )
        lr_model = lr.fit(train_data)

        # Evaluate model
        test_data.cache()
        predictions = lr_model.transform(test_data)
        evaluator_rmse = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
        evaluator_r2 = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="r2")
        rmse = evaluator_rmse.evaluate(predictions)
        r2 = evaluator_r2.evaluate(predictions)

        # Log results
        logger.info("="*60)
        logger.info("FULL DATA TRAINING RESULTS (100% of clean data)")
        logger.info("="*60)
        logger.info(f"Total records processed: {total_count:,}")
        logger.info(f"Intercept (base fare): ${lr_model.intercept:.2f}")
        logger.info(f"Coefficient for distance: ${lr_model.coefficients[0]:.2f} per mile")
        logger.info(f"Coefficient for passengers: ${lr_model.coefficients[1]:.2f} per passenger")
        logger.info(f"RMSE (average error): ${rmse:.2f}")
        logger.info(f"R² Score: {r2:.4f}")

        if abs(lr_model.coefficients[1]) > 0.001:
            logger.info(f"Model: Fare = ${lr_model.intercept:.2f} + ${lr_model.coefficients[0]:.2f}*distance + ${lr_model.coefficients[1]:.2f}*passengers")
        else:
            logger.info(f"Model: Fare = ${lr_model.intercept:.2f} + ${lr_model.coefficients[0]:.2f}*distance")

        # Save trained model
        logger.info(f"Saving model to {MODEL_OUTPUT_PATH}...")
        lr_model.write().overwrite().save(MODEL_OUTPUT_PATH)
        logger.info("Model saved successfully!")

        # Show example predictions in pipeline to evaluate
        logger.info("Example predictions from model:")
        example_data = spark.createDataFrame([
            (1.0, 1.0), (3.0, 1.0), (5.0, 1.0), (10.0, 1.0), (20.0, 1.0)
        ], ["trip_distance", "passenger_count"])
        example_features = assembler.transform(example_data)
        example_predictions = lr_model.transform(example_features)
        for row in example_predictions.select("trip_distance", "passenger_count", "prediction").collect():
            logger.info(f"  {row['trip_distance']} miles, {int(row['passenger_count'])} passenger(s): ${row['prediction']:.2f}")

        logger.info("="*60)
        logger.info("TRAINING PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info(f"Model trained on {total_count:,} records")
        logger.info("="*60)

    except Exception as e:
        logger.error(f"Error in training pipeline: {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
