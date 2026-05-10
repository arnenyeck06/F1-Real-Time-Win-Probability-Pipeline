import asyncio
import json
import joblib
import numpy as np
import redis.asyncio as redis
from contextlib import asynccontextmanager
from datetime import datetime, UTC
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

REDIS_HOST = "localhost"
REDIS_PORT = 6379
INFERENCE_INTERVAL = 1.0

DRIVER_NAMES = {
    1: 'Verstappen', 11: 'Perez', 44: 'Hamilton',
    63: 'Russell', 16: 'Leclerc', 55: 'Sainz',
    4: 'Norris', 81: 'Piastri', 14: 'Alonso',
    18: 'Stroll', 24: 'Zhou', 20: 'Magnussen',
    3: 'Ricciardo', 22: 'Tsunoda', 23: 'Albon',
    27: 'Hulkenberg', 31: 'Ocon', 10: 'Gasly',
    77: 'Bottas', 2: 'Sargeant',
}

# Map model feature names to Redis hash field names.
FEATURE_TO_REDIS = {
    'race_completion_pct': 'race_completion_pct',
    'Position': 'position',
    'position_delta': 'position_delta',
    'lap_time_rolling_avg': 'lap_time_rolling_avg',
    'TyreLife': 'tire_age',
    'compound_encoded': 'compound_encoded',
    'pit_stop_count': 'pit_stop_count',
    'momentum_score': 'momentum_score',
    'is_leader': 'is_leader',
}

FEATURE_DEFAULTS = {
    'race_completion_pct': 0.0,
    'Position': 20.0,
    'position_delta': 0.0,
    'lap_time_rolling_avg': 90.0,
    'TyreLife': 0.0,
    'compound_encoded': 1.0,
    'pit_stop_count': 0.0,
    'momentum_score': 0.0,
    'is_leader': 0.0,
}

print("Loading model artifacts...")
model = joblib.load("model.joblib")
scaler = joblib.load("scaler.joblib")
feature_cols = joblib.load("feature_cols.joblib")
print(f"Model loaded — features: {feature_cols}")


class ConnectionManager:
    def __init__(self):
        self.active: set[WebSocket] = set()
        self.lock = asyncio.Lock()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        async with self.lock:
            self.active.add(ws)

    async def disconnect(self, ws: WebSocket):
        async with self.lock:
            self.active.discard(ws)

    async def broadcast(self, payload: dict):
        async with self.lock:
            targets = list(self.active)
        message = json.dumps(payload, default=str)
        dead = []
        for ws in targets:
            try:
                await ws.send_text(message)
            except Exception:
                dead.append(ws)
        if dead:
            async with self.lock:
                for ws in dead:
                    self.active.discard(ws)


manager = ConnectionManager()
latest_payload: dict | None = None


def parse_features(data: dict) -> list[float] | None:
    row = []
    for col in feature_cols:
        redis_key = FEATURE_TO_REDIS.get(col, col)
        raw = data.get(redis_key)
        if raw is None:
            row.append(FEATURE_DEFAULTS.get(col, 0.0))
            continue
        try:
            row.append(float(raw))
        except (TypeError, ValueError):
            return None
    return row


async def run_inference(r: redis.Redis) -> dict | None:
    feature_matrix = []
    valid_drivers = []
    raw_data: dict[int, dict] = {}

    for driver_num in DRIVER_NAMES:
        data = await r.hgetall(f"driver:{driver_num}:features")
        if not data:
            continue
        row = parse_features(data)
        if row is None:
            continue
        feature_matrix.append(row)
        valid_drivers.append(driver_num)
        raw_data[driver_num] = data

    if not feature_matrix:
        return None

    X = np.array(feature_matrix, dtype=np.float64)
    X_scaled = scaler.transform(X)
    raw_probs = np.asarray(model.predict(X_scaled), dtype=np.float64).ravel()

    total = float(raw_probs.sum())
    if total > 0:
        normalized = raw_probs / total
    else:
        normalized = np.full(len(raw_probs), 1.0 / len(raw_probs))

    predictions = []
    for i, driver_num in enumerate(valid_drivers):
        d = raw_data[driver_num]
        predictions.append({
            "driver_number": driver_num,
            "driver_name": DRIVER_NAMES.get(driver_num, str(driver_num)),
            "win_probability": round(float(normalized[i]), 4),
            "position": int(float(d.get('position', 20))),
            "race_completion_pct": round(float(d.get('race_completion_pct', 0)), 3),
        })

    predictions.sort(key=lambda x: x['win_probability'], reverse=True)

    return {
        "timestamp": datetime.now(UTC).isoformat(),
        "predictions": predictions,
    }


async def inference_loop(r: redis.Redis):
    global latest_payload
    while True:
        try:
            payload = await run_inference(r)
            if payload:
                latest_payload = payload
                await manager.broadcast(payload)
                top = payload['predictions'][:5]
                ts = payload['timestamp']
                print(f"\n{'='*50}\nWin Probability Update — {ts}\n{'='*50}")
                for p in top:
                    bar = '█' * int(p['win_probability'] * 50)
                    print(f"P{p['position']:2d} {p['driver_name']:12s} {p['win_probability']*100:5.1f}% {bar}")
            else:
                print("No driver features in Redis yet...")
        except Exception as e:
            print(f"Inference error: {e}")
        await asyncio.sleep(INFERENCE_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    app.state.redis = r
    task = asyncio.create_task(inference_loop(r))
    print("Inference service started...")
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        await r.aclose()
        print("Inference service stopped.")


app = FastAPI(title="F1 Win Probability Inference", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health():
    return {"status": "ok", "connections": len(manager.active)}


@app.get("/predictions")
async def predictions():
    return latest_payload or {"timestamp": None, "predictions": []}


@app.websocket("/ws/predictions")
async def ws_predictions(ws: WebSocket):
    await manager.connect(ws)
    try:
        if latest_payload:
            await ws.send_text(json.dumps(latest_payload, default=str))
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        await manager.disconnect(ws)
    except Exception as e:
        print(f"WebSocket error: {e}")
        await manager.disconnect(ws)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
