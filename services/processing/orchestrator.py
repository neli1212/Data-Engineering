import warnings
warnings.filterwarnings("ignore")

import os
import time
import logging
import boto3
import subprocess
from botocore.exceptions import ClientError
import datetime
# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Orchestrator")

# --- Config ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
SPARK_MASTER = "spark://spark-master:7077"
HEALTH_FILE = "/tmp/processing_healthy"

def update_health():
    try:
        with open(HEALTH_FILE, "w") as f:
            f.write(str(datetime.datetime.now().timestamp()))
    except:
        pass
# Spark Submit Command Builder (Wartbarkeit: Zentral definiert)
def get_spark_cmd(script_name, memory="1024m"):
    return [
        "/opt/spark/bin/spark-submit",
        "--master", SPARK_MASTER,
        "--executor-memory", memory,
        "--driver-memory", memory,
        "--jars", "/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
        script_name
    ]

def get_s3_client():
    return boto3.client("s3", endpoint_url=MINIO_ENDPOINT, aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

def check_and_clear_trigger():
    """Prüft atomar auf Trigger und löscht ihn, wenn die Verarbeitung startet."""
    s3 = get_s3_client()
    try:
        # 1. Prüfen
        s3.head_object(Bucket="config", Key="_new_data_trigger")
        logger.info("TRIGGER EMPFANGEN! Neue Daten verfügbar.")
        
        # 2. Löschen (damit wir nicht doppelt starten, während der Job noch läuft)
        # In Production würde man hier eher den Trigger erst NACH Erfolg löschen, 
        # aber für Batch ist das "Consuming" des Triggers am Anfang okay.
        s3.delete_object(Bucket="config", Key="_new_data_trigger")
        return True
    except ClientError:
        return False

def run_pipeline():
    logger.info("Starte Processing Pipeline...")
    
    # ... (Teil 1: Processing wie gehabt) ...
    cmd_process = get_spark_cmd("process.py")
    result_proc = subprocess.run(cmd_process, capture_output=False, text=True) # Output sichtbar lassen!

    if result_proc.returncode != 0:
        logger.error("Processing fehlgeschlagen!")
        return

    logger.info("Processing erfolgreich. Starte Aggregation...")

    # Teil 2: Aggregation
    cmd_agg = get_spark_cmd("aggregate.py")
    result_agg = subprocess.run(cmd_agg, capture_output=False, text=True) # Output sichtbar lassen!

    # HIER WAR DER FEHLER: Wir prüfen result_agg nur, wenn es auch lief
    if result_agg.returncode != 0:
         logger.error("Aggregation fehlgeschlagen!")
    else:
         logger.info("Pipeline erfolgreich abgeschlossen.")

if __name__ == "__main__":
    # Startup Check: Warten bis MinIO da ist
    logger.info("Orchestrator gestartet. Warte auf MinIO...")
    while True:
        try:
            get_s3_client().list_buckets()
            break
        except:
            time.sleep(5)
            
    logger.info("System bereit. Polling loop gestartet.")
    
    while True:
        update_health() # <--- Herzschlag senden
        if check_and_clear_trigger():
            run_pipeline()
        else:
            time.sleep(10)