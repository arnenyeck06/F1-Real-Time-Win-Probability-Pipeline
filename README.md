# F1-Real-Time-Win-Probability-Pipeline
A 
production-style data pipeline that ingests live Formula 1 telemetry and positioning data from the OpenF1 API, streams it through Apache Kafka, computes per-driver feature vectors on every lap, and runs a LightGBM model to predict each driver's probability of winning updated in real time. 
Built with Python, FastAPI, Redis, and Docker, with a polished React dashboard featuring animated probability gauges, live track positioning, and driver cards. Designed to run locally during race weekends or deploy to AWS for a live audience.

---
Step 1 — Folder Structure
```bash
mkdir f1-win-probability && cd f1-win-probability

mkdir -p producer processor inference training dashboard/src data/raw data/processed

touch docker-compose.yml .env README.md
touch producer/{Dockerfile,main.py}
touch processor/{Dockerfile,main.py}
touch inference/{Dockerfile,main.py}
touch training/train.ipynb
```
