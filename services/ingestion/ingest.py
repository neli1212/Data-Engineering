import warnings
warnings.filterwarnings("ignore")

import os
import boto3
import kagglehub
import glob
import time
import logging
from concurrent.futures import ThreadPoolExecutor
from botocore.exceptions import ClientError

# --- LOGGING---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("IngestionService")

# --- CONFIG & SECURITY ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")     # Kein Default-Wert "minioadmin" mehr!
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY") # Sicherheit!
DATASET_NAME = os.getenv("DATASET_NAME", "elemento/nyc-yellow-taxi-trip-data")

# Prüfen, ob Credentials da sind (Fail Fast)
if not ACCESS_KEY or not SECRET_KEY:
    logger.critical("FEHLER: AWS_ACCESS_KEY_ID oder AWS_SECRET_ACCESS_KEY fehlen in den Environment Variables!")
    exit(1)

BUCKETS = ["raw-data", "processed-data", "analytics-data", "config"]
HEALTH_FILE = "/tmp/ingestion_healthy"

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )

def update_health_status():
    """
    FEATURE 1: HEALTH CHECKS & OBSERVABILITY
    Schreibt einen Zeitstempel. Docker kann prüfen: "Ist die Datei jünger als 30sek?"
    Wenn das Skript hängt, wird die Datei nicht aktualisiert -> Container gilt als 'unhealthy'.
    """
    try:
        with open(HEALTH_FILE, "w") as f:
            f.write(str(datetime.datetime.now().timestamp()))
    except Exception as e:
        logger.error(f"Health-Status konnte nicht geschrieben werden: {e}")

def validate_data_contract(file_path):
    """
    FEATURE 3: DATA CONTRACT & VALIDATION
    Prüft die Datei physikalisch, bevor sie das System betritt.
    """
    # 1. Dateiendung
    if not file_path.endswith(".csv") and not file_path.endswith(".parquet"):
        return False
    
    # 2. Mindestgröße (1 KB = 1024 Bytes) - verhindert leere/korrupte Files
    if os.path.getsize(file_path) < 1024:
        logger.warning(f"Governance: Datei {os.path.basename(file_path)} ist zu klein (<1KB). Ignoriere.")
        return False

    # 3. Inhaltstest (Header lesen)
    # Wir versuchen, die erste Zeile zu lesen. Wenn das crasht, ist es keine gültige Textdatei.
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            header = f.readline()
            if not header or "," not in header: # Einfacher CSV Check
                logger.warning(f"Governance: Datei {os.path.basename(file_path)} hat keinen gültigen CSV-Header.")
                return False
    except Exception as e:
        logger.warning(f"Governance: Datei {os.path.basename(file_path)} nicht lesbar: {e}")
        return False

    return True

def upload_task(file_path):
    s3 = get_s3_client()
    file_name = os.path.basename(file_path)

    # Schritt 1: Validierung (Data Governance)
    if not validate_data_contract(file_path):
        return False

    try:
        # Schritt 2: Idempotenz-Check
        s3.head_object(Bucket="raw-data", Key=file_name)
        return False # Existiert schon
    except ClientError:
        # Schritt 3: Upload mit Metadaten (FEATURE 2: STRUCTURED METADATA)
        # Wir speichern Infos direkt AM Objekt im Storage.
        metadata = {
            "source": "kaggle",
            "dataset": DATASET_NAME,
            "ingestion_timestamp": datetime.datetime.now().isoformat(),
            "original_size_bytes": str(os.path.getsize(file_path))
        }
        
        logger.info(f"Upload startet: {file_name}")
        s3.upload_file(
            file_path, 
            "raw-data", 
            file_name,
            ExtraArgs={"Metadata": metadata} # Hier werden die Metadaten angehängt
        )
        return True

def run_loop():
    logger.info("Ingestion Service startet...")
    s3 = get_s3_client()

    # Robustheit: Warten auf MinIO
    while True:
        try:
            s3.list_buckets()
            break
        except Exception:
            logger.info("Warte auf MinIO Verbindung...")
            time.sleep(5)

    # Infrastruktur sicherstellen
    for bucket in BUCKETS:
        try: s3.create_bucket(Bucket=bucket)
        except: pass

    while True:
        # Health-Ping senden (Reliability)
        update_health_status()
        logger.info(f"Checking for new data in {DATASET_NAME}...")
        try:
            # Kagglehub Download
            path = kagglehub.dataset_download(DATASET_NAME)
            csv_files = glob.glob(os.path.join(path, "*.csv"))

            new_data_found = False
            
            # Parallel Processing (Scalability)
            with ThreadPoolExecutor(max_workers=4) as executor:
                results = list(executor.map(upload_task, csv_files))
                if any(results):
                    new_data_found = True

            if new_data_found:
                logger.info(">>> Neuer Batch ingestiert. Setze Trigger.")
                s3.put_object(Bucket="config", Key="_new_data_trigger", Body=b"")
            
        except Exception as e:
            logger.error(f"Fehler im Loop: {e}")

        time.sleep(60)

if __name__ == "__main__":
    run_loop()