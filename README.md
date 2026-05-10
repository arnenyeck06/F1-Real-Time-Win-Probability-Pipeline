# 🏁 F1 Real-Time Win Probability Pipeline

A production-style data pipeline that ingests live Formula 1 telemetry and positioning data from the OpenF1 API, streams it through Apache Kafka, computes per-driver feature vectors on every lap, and runs a LightGBM model to predict each driver's probability of winning — updated in real time. Built with Python, FastAPI, Redis, and Docker, with a polished React dashboard featuring animated probability gauges, sparklines, and live driver cards. Designed to run locally during race weekends or deploy to AWS for a live audience.

---

## Architecture
OpenF1 API → Kafka → Stream Processor → Redis → LightGBM → FastAPI WebSocket → React Dashboard

## Tech Stack

| Layer | Technology |
|---|---|
| Data Ingestion | Python, OpenF1 API |
| Message Broker | Apache Kafka |
| Stream Processing | Python consumer |
| Feature Store | Redis |
| Database | PostgreSQL |
| ML Model | LightGBM (AUC 0.975) |
| API | FastAPI + WebSocket |
| Dashboard | React + Vite |
| Infrastructure | Docker Compose |

## Model Performance

| Model | Val AUC | Test AUC |
|---|---|---|
| Logistic Regression (baseline) | 0.9739 | 0.9653 |
| LightGBM v2 (final) | 0.9754 | 0.9747 |

## Features

- Real-time win probability per driver updated every second
- 9 engineered features: position, tire age, momentum, pit stops, race completion %
- Animated probability bars with team colors
- Sparkline history per driver
- WebSocket push from inference service to dashboard
- Trained on 68 races across 2022–2024 seasons

## Quick Start

```bash
# Start all services
docker-compose up -d

# Create Kafka topics (first time only)
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --create --topic f1.position --partitions 4 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --create --topic f1.laps --partitions 2 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --create --topic f1.race_control --partitions 1 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --create --topic f1.predictions --partitions 2 --replication-factor 1

# Open dashboard
open http://localhost:3000
```

## Project Structure

f1-win-probability/
├── producer/       # OpenF1 API poller → Kafka
├── processor/      # Kafka consumer → feature engineering → Redis
├── inference/      # LightGBM model → FastAPI WebSocket
├── dashboard/      # React live dashboard
├── training/       # EDA, feature engineering, model training notebook
├── data/           # Raw race data (gitignored)
└── docker-compose.yml

## Training

```bash
cd training
jupyter notebook train.ipynb
```

## Live Race Weekend

Run on race weekends (Saturday qualifying, Sunday race):

```bash
docker-compose up -d
open http://localhost:3000
```

Tear down after:

```bash
docker-compose down
```
