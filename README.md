# NYC Taxi Data Engineering Pipeline

Dieses Repository ist eine Daten Pipeline für das Projekt: Data Engineering von Cornelius Rottmair und enthält folgende schritte:
- Ingestion von Kaggle
- Verarbeitung & Parquet-Export via Spark
- Aggregation & Analytics
- ML-Modelltraining
- Airflow DAGs zur Orchestrierung


## Python Environment

Um die Pipeline auszuführen, müssen die benötigten Python-Pakete installiert sein. Dazu kann man entweder die Pakete direkt installieren oder eine isolierte Python Umgebung verwenden. Ein Werkzeug dafür ist Micromamba, das die Installation und Verwaltung der Pakete vereinfacht.

### Micromamba Installation

Micromamba kann schnell installiert werden mit:

```bash
curl -L https://micro.mamba.pm/install.sh | bash  
```
Mit Micromamba installiert (oder irgendwas Condaähnlichem) lassen sich die packete instalieren und aktivieren mit:

```bash
micromamba create -f environment.yml
micromamba activate data-engineering
```
### Docker Installation

Außerdem braucht man Docker, das einfach installiert werden kann mit:

```bash
sudo apt update
sudo apt install -y docker-compose
```
Anschließend sollte die Docker-Gruppen-ID (von Installation zu Installation unterschiedlich) in der Datei `.env` gesetzt werden, damit die Container später die richtigen Berechtigungen haben, mit:

```bash
sed -i "s/^DOCKER_GID=.*/DOCKER_GID=$(getent group docker | cut -d: -f3)/" .env
```
Danach müssen noch Docker-Images für die Pipeline gebaut werden m
```bash
sudo docker build -t nyc_ingestion:latest ./services/ingestion
sudo docker build -t nyc_processing:latest ./services/processing
```

Anschließend kann die Pipeline gestartet werden mit:

```bash
sudo docker compose up
```

Unter [http://localhost:8081](http://localhost:8081) sollte dann Airflow bereitstehen. Die Zugangsdaten stehen in der `.env`-Datei (`admin/admin`).  

Die Pipeline würde ab dann täglich automatisch laufen. Wenn sie sofort ausgeführt werden soll, muss sie manuell aktiviert werden.  

Unter [http://localhost:9001](http://localhost:9001) sollten dann die Daten in Minio (`minioadmin/minioadmin`) sichtbar sein und über Spark* wie im Test-Notebook gezeigt, abrufbar sein.
