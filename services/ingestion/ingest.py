# =====================================================================
# Imports
# =====================================================================
import os
import boto3
import kagglehub
import glob
import logging
import sys
from botocore.exceptions import ClientError

# =====================================================================
# Logging Setup
# =====================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("Ingest")

# =====================================================================
# Environment Variables
# =====================================================================
ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000").strip()  
KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")                   
SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")            
DATASET = os.getenv("DATASET_NAME", "elemento/nyc-yellow-taxi-trip-data")  

# =====================================================================
# Helper Functions
# =====================================================================
def get_existing_files(s3, bucket):
    """
    Returns all existing object keys in a bucket.

    Args:
        s3 (boto3.client): S3/MinIO client.
        bucket (str): Bucket name.

    Returns:
        set: Set of object keys.
    """
    existing = set()
    paginator = s3.get_paginator('list_objects_v2')  
    try:
        for page in paginator.paginate(Bucket=bucket):
            if 'Contents' in page:
                for obj in page['Contents']:
                    existing.add(obj['Key'])
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchBucket':
            return set()  
    return existing

# =====================================================================
# Main Routine
# =====================================================================
def main():
    """
    Downloads Kaggle dataset, checks existing files, uploads new to S3.

    Args:
        None

    Returns:
        None
    """
    logger.info(f"Connecting to MinIO: {ENDPOINT}")
    s3 = boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET
    )

    # Ensure buckets exist
    for b in ["raw-data", "config"]:
        try:
            s3.create_bucket(Bucket=b)
        except:
            pass  # Ignore if bucket exists

    # Download dataset
    try:
        path = kagglehub.dataset_download(DATASET)
        logger.info(f"Dataset path: {path}")
    except Exception as e:
        logger.error(f"Kaggle download failed: {e}")
        sys.exit(1)

    # Find CSV files
    files = glob.glob(os.path.join(path, "**/*.csv"), recursive=True)
    existing_files = get_existing_files(s3, "raw-data")  

    # Upload new files
    new_files_count = 0
    for f in files:
        name = os.path.basename(f)
        if name not in existing_files:
            try:
                s3.upload_file(f, "raw-data", name)
                new_files_count += 1
            except Exception as e:
                logger.error(f"Upload failed for {name}: {e}")

    # Flag new data if uploaded
    if new_files_count > 0:
        s3.put_object(Bucket="config", Key="new_data.flag", Body="true")


if __name__ == "__main__":
    main()