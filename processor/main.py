import json
import time
import redis
import psycopg2
from kafka import KafkaConsumer
from collections import defaultdict
from datetime import datetime, UTC

# ── Config ──────────────────────────────────────────────
KAFKA_BROKER = "localhost:9092"
REDIS_HOST = "localhost"
REDIS_PORT = 6379
DB_CONFIG = {
    "dbname": "f1_pipeline",
    "user": "f1_user",
    "password": "f1_password",
    "host": "localhost",
    "port": 5432
}

# ── Connections ──────────────────────────────────────────
def connect_redis():
    while True:
        try:
            client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            client.ping()
            print("Connected to Redis.")
            return client
        except redis.exceptions.RedisError as e:
            print(f"Redis connection failed: {e}. Retrying in 5s...")
            time.sleep(5)

def connect_postgres():
    while True:
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()
            print("Connected to Postgres.")
            return conn, cursor
        except psycopg2.OperationalError as e:
            print(f"Postgres connection failed: {e}. Retrying in 5s...")
            time.sleep(5)

r = connect_redis()
conn, cursor = connect_postgres()

# ── In-memory driver state ────────────────────────────────
driver_state = defaultdict(lambda: {
    "driver_number": None,
    "position": None,
    "lap_number": 0,
    "lap_time": None,
    "tire_compound": None,
    "tire_age": 0,
    "pit_stop_count": 0,
    "stint": 0,
    "lap_times": [],        # rolling history
    "positions": [],        # rolling history
    "race_completion_pct": 0.0,
    "position_delta": 0,
    "start_position": None,
    "momentum_score": 0.0,
    "is_leader": 0,
    "last_updated": None
})

# ── Feature computation ───────────────────────────────────
def compute_features(driver_num):
    s = driver_state[driver_num]

    # Rolling lap time average (last 3 laps)
    recent_laps = s["lap_times"][-3:]
    lap_time_rolling_avg = sum(recent_laps) / len(recent_laps) if recent_laps else 90.0

    # Momentum score (position change over last 5 laps)
    recent_positions = s["positions"]
    if len(recent_positions) >= 5:
        momentum_score = recent_positions[-5] - recent_positions[-1]
    else:
        momentum_score = 0.0

    # Position delta from start
    position_delta = 0
    if s["start_position"] and s["position"]:
        position_delta = s["start_position"] - s["position"]

    # Is leader
    is_leader = 1 if s["position"] == 1 else 0

    # Compound encoding
    compound_map = {'SOFT': 0, 'MEDIUM': 1, 'HARD': 2, 'INTERMEDIATE': 3}
    compound_encoded = compound_map.get(s.get("tire_compound", "MEDIUM"), 1)

    return {
        "driver_number": driver_num,
        "position": s["position"] or 20,
        "race_completion_pct": s["race_completion_pct"],
        "position_delta": position_delta,
        "lap_time_rolling_avg": lap_time_rolling_avg,
        "tire_age": s["tire_age"],
        "compound_encoded": compound_encoded,
        "pit_stop_count": s["pit_stop_count"],
        "momentum_score": momentum_score,
        "is_leader": is_leader,
        "last_updated": datetime.now(UTC).isoformat()
    }

def write_to_redis(driver_num, features):
    key = f"driver:{driver_num}:features"
    r.hset(key, mapping={k: str(v) for k, v in features.items()})
    r.expire(key, 7200)  # 2 hour TTL

def process_position_event(event):
    driver_num = event.get("driver_number")
    if not driver_num:
        return

    s = driver_state[driver_num]
    s["driver_number"] = driver_num
    s["position"] = event.get("position")

    # Track position history
    if s["position"]:
        s["positions"].append(s["position"])
        if len(s["positions"]) > 20:
            s["positions"].pop(0)

    # Set start position on first lap
    if s["start_position"] is None and s["position"]:
        s["start_position"] = s["position"]

    features = compute_features(driver_num)
    write_to_redis(driver_num, features)

def process_lap_event(event):
    driver_num = event.get("driver_number")
    if not driver_num:
        return

    s = driver_state[driver_num]
    s["lap_number"] = event.get("lap_number", s["lap_number"])
    s["tire_compound"] = event.get("compound", s["tire_compound"])
    s["tire_age"] = event.get("tyre_life", s["tire_age"])
    s["stint"] = event.get("stint", s["stint"])

    # Track lap time history
    lap_duration = event.get("lap_duration")
    if lap_duration:
        s["lap_times"].append(lap_duration)
        if len(s["lap_times"]) > 10:
            s["lap_times"].pop(0)

    # Race completion %
    total_laps = event.get("total_laps", 60)
    s["race_completion_pct"] = s["lap_number"] / total_laps

    features = compute_features(driver_num)
    write_to_redis(driver_num, features)

# ── Main consumer loop ────────────────────────────────────
def run():
    consumer = KafkaConsumer(
        'f1.position',
        'f1.laps',
        'f1.race_control',
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='f1-processor'
    )

    print("Stream processor started — listening to Kafka topics...")

    for message in consumer:
        topic = message.topic
        event = message.value

        try:
            if topic == 'f1.position':
                process_position_event(event)
            elif topic == 'f1.laps':
                process_lap_event(event)

            print(f"Processed {topic} event — driver {event.get('driver_number')}")

        except Exception as e:
            print(f"Error processing {topic}: {e}")

if __name__ == "__main__":
    run()
