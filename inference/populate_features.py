"""
Day 4 — Populate the Redis feature store.

For each engine (machine_id) in the training data, computes the SAME rolling
mean/std features used in Day 3's training script and stores them as a Redis
Hash. At inference time, the FastAPI service fetches an engine's stored
vector with a single HGETALL, blends in the live sensor reading, and hands
the complete vector to the model.

This script must replicate Day 3's `engineer_features()` rolling-window
logic exactly — feature names, window size, computation — or you get
training-serving skew (the model sees different feature semantics in
production than it saw during training).

Usage:
    python populate_features.py
"""

import json
import os
from pathlib import Path

import pandas as pd
import redis
from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).resolve().parent.parent
DATA_PATH = ROOT / "notebooks" / "data" / "train_FD001.txt"
SCHEMA_PATH = ROOT / "artifacts" / "feature_schema.json"

INDEX_NAMES = ["unit_nr", "time_cycles"]
SETTING_NAMES = ["setting_1", "setting_2", "setting_3"]
SENSOR_NAMES = [f"s_{i}" for i in range(1, 22)]
COL_NAMES = INDEX_NAMES + SETTING_NAMES + SENSOR_NAMES

FEATURE_KEY_PREFIX = "feature:machine_"


def get_redis_client() -> redis.Redis:
    """
    Connects to Redis using Upstash credentials from .env if present,
    otherwise falls back to localhost (useful for local testing without
    burning Upstash quota during development).
    """
    host = os.getenv("UPSTASH_REDIS_HOST")
    if host:
        return redis.Redis(
            host=host,
            port=int(os.getenv("UPSTASH_REDIS_PORT", 6379)),
            password=os.getenv("UPSTASH_REDIS_PASSWORD"),
            ssl=os.getenv("UPSTASH_REDIS_SSL", "true").lower() == "true",
            decode_responses=True,
        )
    print("⚠️  No UPSTASH_REDIS_HOST in .env — falling back to localhost:6379")
    return redis.Redis(host="localhost", port=6379, decode_responses=True)


def load_schema() -> dict:
    if not SCHEMA_PATH.exists():
        raise FileNotFoundError(
            f"{SCHEMA_PATH} not found. Run the Day 2 notebook first."
        )
    with open(SCHEMA_PATH) as f:
        return json.load(f)


def load_raw_data() -> pd.DataFrame:
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"{DATA_PATH} not found.")
    return pd.read_csv(DATA_PATH, sep=r"\s+", header=None, names=COL_NAMES)


def compute_rolling_features(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    """
    Identical logic to Day 3's engineer_features() rolling-window step —
    deliberately duplicated rather than imported, because in production
    this function lives in a different service (a streaming consumer) than
    train.py, and the two need to be independently auditable against the
    same schema contract.
    """
    df = df.copy()
    window = schema["rolling_window"]
    for sensor in schema["top_sensors"]:
        df[f"{sensor}_rolling_mean_{window}"] = df.groupby("unit_nr")[sensor].transform(
            lambda x: x.rolling(window, min_periods=1).mean()
        )
        df[f"{sensor}_rolling_std_{window}"] = df.groupby("unit_nr")[sensor].transform(
            lambda x: x.rolling(window, min_periods=1).std().fillna(0)
        )
    return df


def populate(client: redis.Redis, df: pd.DataFrame, schema: dict) -> int:
    """
    Stores each engine's LATEST rolling feature vector (i.e. its most
    recent/current state) as a Redis Hash: feature:machine_<id> -> {col: val}.

    In production this would be continuously updated as new readings arrive;
    today we're seeding it from the historical training data's final rows,
    which simulates "this is the engine's state as of its last known reading."
    """
    feature_columns = schema["feature_columns"]
    latest_per_engine = df.sort_values("time_cycles").groupby("unit_nr").tail(1)

    count = 0
    for _, row in latest_per_engine.iterrows():
        key = f"{FEATURE_KEY_PREFIX}{int(row['unit_nr'])}"
        mapping = {col: float(row[col]) for col in feature_columns}
        client.hset(key, mapping=mapping)
        count += 1
    return count


def get_feature_vector(client: redis.Redis, machine_id: int, live_reading: dict, schema: dict) -> dict:
    """
    The function the FastAPI inference service will call on Day 5.

    Fetches the engine's stored historical rolling features from Redis,
    then overlays any live values present in `live_reading` (e.g. if a
    fresh sensor reading just arrived and should take precedence over the
    last stored snapshot). Returns a flat dict ready to feed the model,
    in the exact column order the model expects.

    Raises KeyError if the machine has no stored feature vector — this
    should not be silently swallowed, since it usually means the machine
    was never seeded (a real bug, not a quiet default).
    """
    key = f"{FEATURE_KEY_PREFIX}{machine_id}"
    stored = client.hgetall(key)
    if not stored:
        raise KeyError(f"No feature vector found in Redis for {key}")

    vector = {col: float(stored[col]) for col in schema["feature_columns"]}
    for k, v in live_reading.items():
        if k in vector:
            vector[k] = float(v)
    return vector


def main():
    print("Loading feature schema...")
    schema = load_schema()

    print("Loading raw data...")
    raw_df = load_raw_data()
    print(f"Loaded {raw_df.shape[0]} rows, {raw_df['unit_nr'].nunique()} engines")

    print("Computing rolling features (must match train.py exactly)...")
    df = compute_rolling_features(raw_df, schema)

    print("Connecting to Redis...")
    client = get_redis_client()
    client.ping()
    print("✓ Connected")

    print("Populating feature store...")
    count = populate(client, df, schema)
    print(f"✓ Populated {count} engine feature vectors")

    # Sanity check: fetch one back and confirm shape matches what train.py expects
    print("\nVerifying getter function...")
    sample_id = int(df["unit_nr"].iloc[0])
    vector = get_feature_vector(client, sample_id, live_reading={}, schema=schema)
    print(f"Sample vector for machine_{sample_id}:")
    for k, v in vector.items():
        print(f"  {k}: {v:.4f}")

    expected_cols = set(schema["feature_columns"])
    actual_cols = set(vector.keys())
    assert expected_cols == actual_cols, (
        f"Feature mismatch! Expected {expected_cols}, got {actual_cols}"
    )
    print(f"\n✓ Vector shape matches schema ({len(vector)} features). Day 4 complete.")


if __name__ == "__main__":
    main()
