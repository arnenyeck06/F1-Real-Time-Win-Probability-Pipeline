import json
import time
import requests
from datetime import datetime, UTC
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

# ── Config ──────────────────────────────────────────────
KAFKA_BROKER = "localhost:9092"
BASE_URL = "https://api.openf1.org/v1"
POLL_INTERVAL = 3  # seconds between API calls

# ── Kafka Producer ───────────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8')
)

def get_latest_session():
    """Get the most recent live or recent session key."""
    resp = requests.get(f"{BASE_URL}/sessions?session_type=Race", timeout=10)
    sessions = resp.json()
    if not sessions:
        return None
    return sessions[-1]['session_key']

def fetch_and_publish_positions(session_key, last_timestamp=None):
    """Fetch position data and publish to f1.position topic."""
    url = f"{BASE_URL}/position?session_key={session_key}"
    if last_timestamp:
        url += f"&date>{last_timestamp}"
    
    resp = requests.get(url, timeout=10)
    data = resp.json()
    if not isinstance(data, list):
        return 0

    for record in data:
        producer.send(
            topic='f1.position',
            key=str(record.get('driver_number', 0)),
            value=record
        )

    return len(data)

def fetch_and_publish_laps(session_key, last_lap=None):
    """Fetch lap data and publish to f1.laps topic."""
    url = f"{BASE_URL}/laps?session_key={session_key}"
    if last_lap:
        url += f"&lap_number>{last_lap}"
    
    resp = requests.get(url, timeout=10)
    data = resp.json()
    if not isinstance(data, list):
        return 0

    for record in data:
        producer.send(
            topic='f1.laps',
            key=str(record.get('driver_number', 0)),
            value=record
        )

    return len(data)

def fetch_and_publish_race_control(session_key):
    """Fetch race control messages and publish to f1.race_control topic."""
    url = f"{BASE_URL}/race_control?session_key={session_key}"
    resp = requests.get(url, timeout=10)
    data = resp.json()
    if not isinstance(data, list):
        return 0

    for record in data:
        producer.send(
            topic='f1.race_control',
            key='race_control',
            value=record
        )

    return len(data)

def run_replay(session_key):
    """Replay mode — simulate live race from historical session."""
    print(f"Starting replay for session {session_key}")
    
    last_timestamp = None
    last_lap = 0
    
    while True:
        try:
            # Position — highest frequency
            pos_count = fetch_and_publish_positions(session_key, last_timestamp)
            last_timestamp = datetime.now(UTC).isoformat()
            
            # Laps — on new lap events
            lap_count = fetch_and_publish_laps(session_key, last_lap)
            if lap_count > 0:
                last_lap += 1
            
            # Race control — every 5 seconds
            rc_count = fetch_and_publish_race_control(session_key)
            
            producer.flush()
            
            print(f"Published — positions: {pos_count} | laps: {lap_count} | race_control: {rc_count}")
            time.sleep(POLL_INTERVAL)

        except KeyboardInterrupt:
            print("\nStopping producer...")
            producer.close()
            break
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    # 2024 Bahrain GP — session key for replay testing
    SESSION_KEY = 9158
    run_replay(SESSION_KEY)
