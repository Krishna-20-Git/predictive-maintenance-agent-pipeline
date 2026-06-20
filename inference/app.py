"""
Day 5 — FastAPI inference service.

Exposes POST /score: given a machine_id and a live sensor reading, fetches
that machine's historical rolling-feature vector from Redis (Day 4),
overlays the live reading, runs the Day 3 XGBoost model, and returns a
failure probability.

The model and Redis client are loaded ONCE at startup (not per-request) —
this is what keeps latency low. Loading a joblib model or opening a Redis
connection on every request would add tens of milliseconds of pure
overhead before any actual inference happens.

Run with:
    uvicorn app:app --reload --port 8000
"""

import json
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

import joblib
import pandas as pd
import redis
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

load_dotenv()

ROOT = Path(__file__).resolve().parent.parent
MODEL_PATH = ROOT / "artifacts" / "xgb_model.joblib"
SCHEMA_PATH = ROOT / "artifacts" / "feature_schema.json"

FEATURE_KEY_PREFIX = "feature:machine_"

# Populated at startup via the lifespan handler below.
state: dict = {}


def get_redis_client() -> redis.Redis:
    host = os.getenv("UPSTASH_REDIS_HOST")
    if host:
        return redis.Redis(
            host=host,
            port=int(os.getenv("UPSTASH_REDIS_PORT", 6379)),
            password=os.getenv("UPSTASH_REDIS_PASSWORD"),
            ssl=os.getenv("UPSTASH_REDIS_SSL", "true").lower() == "true",
            decode_responses=True,
        )
    return redis.Redis(host="localhost", port=6379, decode_responses=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ──
    print("Loading model and schema...")
    if not MODEL_PATH.exists():
        raise FileNotFoundError(f"{MODEL_PATH} not found. Run train.py (Day 3) first.")
    if not SCHEMA_PATH.exists():
        raise FileNotFoundError(f"{SCHEMA_PATH} not found. Run the Day 2 notebook first.")

    state["model"] = joblib.load(MODEL_PATH)
    with open(SCHEMA_PATH) as f:
        state["schema"] = json.load(f)

    state["redis"] = get_redis_client()
    state["redis"].ping()

    print(f"✓ Model loaded ({state['model'].n_features_in_} features)")
    print(f"✓ Redis connected")
    yield
    # ── Shutdown ──
    state["redis"].close()
    print("Shut down cleanly.")


app = FastAPI(title="Predictive Maintenance Inference Service", lifespan=lifespan)


class SensorReading(BaseModel):
    """
    Live sensor values for a single machine. All fields optional — only the
    sensors present here override the corresponding stored rolling feature
    in Redis. An empty reading is valid (scores purely off stored history).
    """
    machine_id: int = Field(..., description="Engine/machine identifier", examples=[1])
    sensor_readings: dict[str, float] = Field(
        default_factory=dict,
        description="Raw sensor values, e.g. {'s_11': 48.2}. Field names must "
        "match schema feature column prefixes (e.g. 's_11') — they are NOT "
        "rolling feature names directly; see /score docstring.",
    )


class ScoreResponse(BaseModel):
    machine_id: int
    failure_probability: float
    failure_soon: bool
    timestamp: float
    latency_ms: float


def get_feature_vector(machine_id: int, live_reading: dict, schema: dict, client: redis.Redis) -> dict:
    """
    Same logic as Day 4's populate_features.get_feature_vector — duplicated
    here intentionally. This service and the streaming consumer (Day 7) are
    separate processes; each should independently match the schema contract
    rather than share a hidden runtime import, so a change to one doesn't
    silently change the other's behavior without a code review catching it.
    """
    key = f"{FEATURE_KEY_PREFIX}{machine_id}"
    stored = client.hgetall(key)
    if not stored:
        raise KeyError(f"No feature vector found for machine_id={machine_id}")

    vector = {col: float(stored[col]) for col in schema["feature_columns"]}
    for k, v in live_reading.items():
        if k in vector:
            vector[k] = float(v)
    return vector


@app.get("/health")
def health():
    """Liveness/readiness check — confirms the model and Redis are both reachable."""
    try:
        state["redis"].ping()
        redis_ok = True
    except Exception:
        redis_ok = False

    return {
        "status": "ok" if redis_ok else "degraded",
        "model_loaded": "model" in state,
        "redis_connected": redis_ok,
    }


@app.post("/score", response_model=ScoreResponse)
def score(reading: SensorReading):
    """
    Scores a machine's current failure risk.

    `sensor_readings` keys should match the *rolling feature column names*
    from the schema (e.g. "s_11_rolling_mean_7"), not raw sensor names —
    in a fuller production system, a separate service would compute fresh
    rolling stats from a recent window and pass those in; for this
    portfolio project, the Day 7 streaming consumer does that overlay
    before calling this endpoint, so /score itself stays a pure scoring
    function with no rolling-window computation of its own.
    """
    start = time.perf_counter()

    try:
        vector = get_feature_vector(
            reading.machine_id, reading.sensor_readings, state["schema"], state["redis"]
        )
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))

    feature_columns = state["schema"]["feature_columns"]
    X = pd.DataFrame([vector])[feature_columns]

    probability = float(state["model"].predict_proba(X)[0, 1])
    threshold = 0.5

    elapsed_ms = (time.perf_counter() - start) * 1000

    return ScoreResponse(
        machine_id=reading.machine_id,
        failure_probability=round(probability, 4),
        failure_soon=probability >= threshold,
        timestamp=time.time(),
        latency_ms=round(elapsed_ms, 2),
    )
