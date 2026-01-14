FROM python:3.9-slim

WORKDIR /app

# Requirements installieren
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Code kopieren
COPY ingest.py .

CMD ["python", "ingest.py"]