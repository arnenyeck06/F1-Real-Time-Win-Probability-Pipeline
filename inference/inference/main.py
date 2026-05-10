import json
import time
import redis
import joblib
import numpy as np
import asyncio
from kafka import KafkaProducer
from datetime import datetime, UTC
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# ── Config ──────────────────────────────────────────────
REDIS_HOST = "localhost"
REDIS_PORT = 6379
KAFKA_BROKER = "localhost:9092"
INFERENCE_INTERVAL = 1

# ── Load model artifacts ─────────────────────────────────
print("Loading model artifacts...")
model = joblib.load("model.joblib")
feature_cols = joblib.load("feature_cols.joblib")
print(f"Model loaded — features: {feature_cols}")

# ── Connections ──────────────────────────────────────────
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
)

# ── FastAPI app ──────────────────────────────────────────
app = FastAPI(title="F1 Win Probability API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)

# ── Connected WebSocket clients ──────────────────────────
connected_clients = []

# ── Driver mapping ───────────────────────────────────────
DRIVER_NAMES = {
    1: 'Verstappen', 11: 'Perez', 44: 'Hamilton',
    63: 'Russell', 16: 'Leclerc', 55: 'Sainz',
    4: 'Norris', 81: 'Piastri', 14: 'Alonso',
    18: 'Stroll', 24: 'Zhou', 20: 'Magnussen',
    3: 'Ricciardo', 22: 'Tsunoda', 23: 'Albon',
    27: 'Hulkenberg', 31: 'Ocon', 10: 'Gasly',
    77: 'Bottas', 2: 'Sargeant'
}

TEAM_COLORS = {
    'Verstappen': '#3671C6', 'Perez': '#3671C6',
    'Hamilton': '#27F4D2', 'Russell': '#27F4D2',
    'Leclerc': '#E8002D', 'Sainz': '#E8002D',
    'Norris': '#FF8000', 'Piastri': '#FF8000',
    'Alonso': '#358C75', 'Stroll': '#358C75',
    'Hulkenberg': '#B6BABD', 'Magnussen': '#B6BABD',
    'Ricciardo': '#6692FF', 'Tsunoda': '#6692FF',
    'Albon': '#64C4FF', 'Sargeant': '#64C4FF',
    'Ocon': '#FF87BC', 'Gasly': '#FF87BC',
    'Bottas': '#C92D4B', 'Zhou': '#C92D4B'
}

def get_driver_features(driver_number):
    key = f"driver:{driver_number}:features"
    data = r.hgetall(key)
    if not data:
        return None
    try:
        return [
            float(data.get('race_completion_pct', 0)),
            float(data.get('position', 20)),
            float(data.get('position_delta', 0)),
            float(data.get('lap_time_rolling_avg', 90)),
            float(data.get('tire_age', 0)),
            float(data.get('compound_encoded', 1)),
            float(data.get('pit_stop_count', 0)),
            float(data.get('momentum_score', 0)),
            float(data.get('is_leader', 0))
        ]
    except:
        return None

def run_inference():
    driver_numbers = list(DRIVER_NAMES.keys())
    feature_matrix = []
    valid_drivers = []

    for driver_num in driver_numbers:
        features = get_driver_features(driver_num)
        if features:
            feature_matrix.append(features)
            valid_drivers.append(driver_num)

    if not feature_matrix:
        return None

    X = np.array(feature_matrix)
    raw_probs = model.predict(X)

    total = sum(raw_probs)
    normalized_probs = [p / total for p in raw_probs] if total > 0 else [1/len(raw_probs)] * len(raw_probs)

    predictions = []
    for i, driver_num in enumerate(valid_drivers):
        name = DRIVER_NAMES.get(driver_num, str(driver_num))
        predictions.append({
            "driver_number": driver_num,
            "driver_name": name,
            "team_color": TEAM_COLORS.get(name, '#FFFFFF'),
            "win_probability": round(normalized_probs[i], 4),
            "position": int(float(r.hget(f"driver:{driver_num}:features", "position") or 20)),
            "race_completion_pct": round(float(r.hget(f"driver:{driver_num}:features", "race_completion_pct") or 0), 3),
            "tire_age": int(float(r.hget(f"driver:{driver_num}:features", "tire_age") or 0)),
            "pit_stop_count": int(float(r.hget(f"driver:{driver_num}:features", "pit_stop_count") or 0))
        })

    predictions.sort(key=lambda x: x['win_probability'], reverse=True)

    return {
        "timestamp": datetime.now(UTC).isoformat(),
        "predictions": predictions
    }

# ── WebSocket endpoint ───────────────────────────────────
@app.websocket("/ws/predictions")
async def websocket_predictions(websocket: WebSocket):
    await websocket.accept()
    connected_clients.append(websocket)
    print(f"Client connected — {len(connected_clients)} total")
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        connected_clients.remove(websocket)
        print(f"Client disconnected — {len(connected_clients)} total")

# ── REST fallback endpoint ───────────────────────────────
@app.get("/predictions/latest")
def get_latest_predictions():
    return run_inference() or {"error": "No data available"}

@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.now(UTC).isoformat()}

# ── Background inference loop ────────────────────────────
async def broadcast_loop():
    while True:
        try:
            payload = run_inference()
            if payload and connected_clients:
                message = json.dumps(payload)
                disconnected = []
                for client in connected_clients:
                    try:
                        await client.send_text(message)
                    except:
                        disconnected.append(client)
                for client in disconnected:
                    connected_clients.remove(client)

            if payload:
                producer.send('f1.predictions', value=payload)
                producer.flush()

        except Exception as e:
            print(f"Broadcast error: {e}")

        await asyncio.sleep(INFERENCE_INTERVAL)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(broadcast_loop())
    print("WebSocket broadcast loop started")

# ── Run ──────────────────────────────────────────────────
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=False)