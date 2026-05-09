import time
import fastf1
import pandas as pd
from pathlib import Path

Path("data/raw/cache").mkdir(parents=True, exist_ok=True)
fastf1.Cache.enable_cache("data/raw/cache")

SEASONS = [2022,2023,2024]
OUTPUT_DIR = Path("data/raw")

def fetch_race(year: int, round_number: int):
    try:
        session = fastf1.get_session(year, round_number, "R")
        session.load(telemetry=False, weather=True, messages=True)
        laps = session.laps.copy()
        laps["Year"] = year
        laps["RoundNumber"] = round_number
        laps["EventName"] = session.event["EventName"]
        print(f"    OK — {len(laps)} laps")
        return laps
    except Exception as e:
        print(f"    SKIP — {e}")
        return None

all_laps = []

for year in SEASONS:
    schedule = fastf1.get_event_schedule(year, include_testing=False)
    print(f"\nFetching {year} — {len(schedule)} rounds")
    for _, event in schedule.iterrows():
        round_num = event["RoundNumber"]
        print(f"  Round {round_num}: {event['EventName']}")
        laps = fetch_race(year, round_num)
        if laps is not None:
            all_laps.append(laps)
        time.sleep(5)

df = pd.concat(all_laps, ignore_index=True)
df.to_parquet(OUTPUT_DIR / "all_laps.parquet", index=False)
print(f"\nDone. Saved {len(df)} lap records to data/raw/all_laps.parquet")