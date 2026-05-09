import json
import time
import redis
import joblib
import numpy as np
from kafka import KafkaProducer
from datetime import datetime, UTC

# ── Config ──────────────────────────────────────────────
REDIS_HOST = "localhost"
REDIS_PORT = 6379
KAFKA_BROKER = "localhost:9092"
INFERENCE_INTERVAL = 1  # seconds between predictions

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

# ── Driver number to name mapping ────────────────────────
DRIVER_NAMES = {
    1: 'Verstappen', 11: 'Perez', 44: 'Hamilton',
    63: 'Russell', 16: 'Leclerc', 55: 'Sainz',
    4: 'Norris', 81: 'Piastri', 14: 'Alonso',
    18: 'Stroll', 24: 'Zhou', 20: 'Magnussen',
    3: 'Ricciardo', 22: 'Tsunoda', 23: 'Albon',
    27: 'Hulkenberg', 31: 'Ocon', 10: 'Gasly',
    77: 'Bottas', 2: 'Sargeant'
}

def get_driver_features(driver_number):
    """Read feature vector from Redis for one driver."""
    key = f"driver:{driver_number}:features"
    data = r.hgetall(key)
    if not data:
        return None

    # Build feature vector in correct order
    try:
        features = [
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
        return features
    except Exception as e:
        print(f"Error parsing features for driver {driver_number}: {e}")
        return None

def run_inference():
    """Read all driver features, run model, publish predictions."""
    driver_numbers = list(DRIVER_NAMES.keys())

    feature_matrix = []
    valid_drivers = []

    for driver_num in driver_numbers:
        features = get_driver_features(driver_num)
        if features:
            feature_matrix.append(features)
            valid_drivers.append(driver_num)

    if not feature_matrix:
        print("No driver features in Redis yet...")
        return

    # Run batch inference
    X = np.array(feature_matrix)
    raw_probs = model.predict(X)

    # Normalize so probabilities sum to 1
    total = sum(raw_probs)
    if total > 0:
        normalized_probs = [p / total for p in raw_probs]
    else:
        normalized_probs = [1/len(raw_probs)] * len(raw_probs)

    # Build predictions payload
    predictions = []
    for i, driver_num in enumerate(valid_drivers):
        predictions.append({
            "driver_number": driver_num,
            "driver_name": DRIVER_NAMES.get(driver_num, str(driver_num)),
            "win_probability": round(normalized_probs[i], 4),
            "position": int(float(r.hget(f"driver:{driver_num}:features", "position") or 20)),
            "race_completion_pct": round(float(r.hget(f"driver:{driver_num}:features", "race_completion_pct") or 0), 3)
        })

    # Sort by win probability
    predictions.sort(key=lambda x: x['win_probability'], reverse=True)

    payload = {
        "timestamp": datetime.now(UTC).isoformat(),
        "predictions": predictions
    }

    # Publish to Kafka
    producer.send('f1.predictions', value=payload)
    producer.flush()

    # Print top 5
    print(f"\n{'='*50}")
    print(f"Win Probability Update — {payload['timestamp']}")
    print(f"{'='*50}")
    for p in predictions[:5]:
        bar = '█' * int(p['win_probability'] * 50)
        print(f"P{p['position']:2d} {p['driver_name']:12s} {p['win_probability']*100:5.1f}% {bar}")

def run():
    print("Inference service started...")
    while True:
        try:
            run_inference()
            time.sleep(INFERENCE_INTERVAL)
        except KeyboardInterrupt:
            print("\nStopping inference service...")
            break
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    run()

