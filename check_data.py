import os
import boto3
import pandas as pd
import pyarrow.parquet as pq
from io import BytesIO

# Verbindung zu MinIO
s3 = boto3.resource('s3',
                    endpoint_url='http://localhost:9000',
                    aws_access_key_id='minioadmin',
                    aws_secret_access_key='minioadmin')

bucket = s3.Bucket('processed-data')

print("Analysiere bereinigte Parquet-Daten...")

# Wir summieren die Zeilen aus den Metadaten der Parquet-Dateien
total_rows = 0
columns = []

for obj in bucket.objects.filter(Prefix='taxi_trips/'):
    if obj.key.endswith('.parquet'):
        # Wir lesen nur die Metadaten (geht blitzschnell)
        body = obj.get()['Body'].read()
        table = pq.read_table(BytesIO(body))
        total_rows += table.num_rows
        if not columns:
            columns = table.column_names

print(f"\n✅ ERGEBNIS:")
print(f"Anzahl bereinigter Datenpunkte: {total_rows:,}")
print(f"Anzahl der Spalten: {len(columns)}")
print(f"\nVerfügbare Spalten für die ML-App:")
print(columns)