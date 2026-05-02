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
---
Structure:
```bash
f1-win-probability/
├── data/
│   └── raw/
│       ├── all_laps.parquet     ← 68 races, 63,235 rows
│       └── cache/               ← fastf1 cache files
├── inference/
│   ├── model.joblib             ← LightGBM v2 (AUC 0.9747) ✅
│   ├── scaler.joblib            ← StandardScaler for logistic reg ✅
│   ├── feature_cols.joblib      ← list of 9 feature names ✅
│   └── main.py                  ← empty, Phase 6
├── processor/
│   ├── schema.py                ← DriverState dataclass
│   └── main.py                  ← empty, Phase 5
├── producer/
│   └── main.py                  ← empty, Phase 4
├── training/
│   ├── train.ipynb              ← EDA + model training ✅
│   ├── fetch_data.py            ← historical data fetch ✅
│   └── test_fetch.py            ← debug script
├── docker-compose.yml           ← empty, Phase 3
├── .gitignore                   ← ✅
├── .env                         ← empty
└── requirements.txt             ← ✅
```
