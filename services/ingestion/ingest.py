import os, boto3, kagglehub, glob, logging, sys
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Ingest")

ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000").strip()
KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
DATASET = os.getenv("DATASET_NAME", "elemento/nyc-yellow-taxi-trip-data")

def main():
    logger.info(f"Connecting to MinIO: {ENDPOINT}")
    s3 = boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=KEY, aws_secret_access_key=SECRET)
    
    for b in ["raw-data", "config"]:
        try: s3.create_bucket(Bucket=b)
        except: pass

    try:
        path = kagglehub.dataset_download(DATASET)
        logger.info(f"Dataset path: {path}")
    except Exception as e:
        logger.error(f"Kaggle failed: {e}")
        sys.exit(1)

    files = glob.glob(os.path.join(path, "**/*.csv"), recursive=True)
    new_files = 0
    for f in files:
        name = os.path.basename(f)
        try:
            s3.head_object(Bucket="raw-data", Key=name)
        except ClientError:
            logger.info(f"Uploading: {name}")
            s3.upload_file(f, "raw-data", name)
            new_files += 1

    if new_files > 0:
        s3.put_object(Bucket="config", Key="new_data.flag", Body="true")

if __name__ == "__main__":
    main()