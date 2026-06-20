"""
Day 7 — Streaming consumer: raw-telemetry → score → scored-alerts.

Consumes stream:raw-telemetry in batches using a Redis Streams consumer
group, scores each event against the model using that machine's STORED
rolling-feature vector (populated by Day 4 / kept fresh by Day 4's
populate script), and publishes results above ALERT_PROBABILITY_THRESHOLD
to stream:scored-alerts for Spring Boot to pick up in Week 2.

Design note on why we don't recompute rolling features from the live event:
the live event carries a single cycle's RAW sensor readings (s_1..s_21),
but the model expects ROLLING features (mean/std over a 7-cycle window).
A single raw reading is not the same unit as a rolling average — naively
overlaying one into the other's slot would silently corrupt the feature
vector with a wrong value, which is worse than just using the last known
good rolling vector. So this consumer scores off the stored vector and
logs the live raw reading for traceability/audit, rather than fabricating
a rolling stat it doesn't actually have enough history to compute inline.
(A fuller production system would maintain a sliding window per machine
and recompute rolling stats as each new raw reading arrives — out of
scope for this portfolio pipeline, but worth knowing as the honest
limitation here.)

Usage:
    python consumer.py
    python consumer.py --batch-size 20 --threshold 0.3
"""

import argparse
import json
import os
import time
from pathlib import Path

import joblib
import pandas as pd
import redis
from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).resolve().parent.parent
MODEL_PATH = ROOT / "artifacts" / "xgb_model.joblib"
SCHEMA_PATH = ROOT / "artifacts" / "feature_schema.json"

RAW_STREAM = "stream:raw-telemetry"
SCORED_STREAM = "stream:scored-alerts"
CONSUMER_GROUP = "inference-workers"
CONSUMER_NAME = "consumer-1"
FEATURE_KEY_PREFIX = "feature:machine_"


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
    print("⚠️  No UPSTASH_REDIS_HOST in .env — falling back to localhost:6379")
    return redis.Redis(host="localhost", port=6379, decode_responses=True)


def ensure_consumer_group(client: redis.Redis):
    """
    Creates the consumer group if it doesn't already exist. mkstream=True
    means the group can be created even if the stream itself doesn't exist
    yet (e.g. fresh Redis instance with no events produced so far).
    """
    try:
        client.xgroup_create(RAW_STREAM, CONSUMER_GROUP, id="0", mkstream=True)
        print(f"✓ Created consumer group '{CONSUMER_GROUP}' on '{RAW_STREAM}'")
    except redis.ResponseError as e:
        if "BUSYGROUP" in str(e):
            print(f"✓ Consumer group '{CONSUMER_GROUP}' already exists")
        else:
            raise


def get_feature_vector(machine_id: int, schema: dict, client: redis.Redis) -> dict | None:
    """
    Fetches the stored rolling-feature vector for a machine. Returns None
    (rather than raising) if the machine was never seeded — this consumer
    runs unattended, so a missing machine should be skipped and logged,
    not crash the whole batch.
    """
    key = f"{FEATURE_KEY_PREFIX}{machine_id}"
    stored = client.hgetall(key)
    if not stored:
        return None
    return {col: float(stored[col]) for col in schema["feature_columns"]}


def score_event(model, schema: dict, vector: dict) -> float:
    feature_columns = schema["feature_columns"]
    X = pd.DataFrame([vector])[feature_columns]
    return float(model.predict_proba(X)[0, 1])


def publish_alert(client: redis.Redis, machine_id: int, probability: float, raw_event: dict):
    alert = {
        "machine_id": str(machine_id),
        "failure_probability": str(round(probability, 4)),
        "cycle_position": raw_event.get("cycle_position", ""),
        "source_timestamp": raw_event.get("timestamp", ""),
        "scored_timestamp": str(time.time()),
    }
    client.xadd(SCORED_STREAM, alert)


def process_batch(client, model, schema, messages, threshold: float) -> dict:
    """Processes one batch of stream messages. Returns summary counts."""
    stats = {"processed": 0, "skipped_unseeded": 0, "alerts_published": 0}

    for message_id, fields in messages:
        machine_id = int(fields["machine_id"])
        stats["processed"] += 1

        vector = get_feature_vector(machine_id, schema, client)
        if vector is None:
            stats["skipped_unseeded"] += 1
            client.xack(RAW_STREAM, CONSUMER_GROUP, message_id)
            continue

        probability = score_event(model, schema, vector)

        if probability >= threshold:
            publish_alert(client, machine_id, probability, fields)
            stats["alerts_published"] += 1

        client.xack(RAW_STREAM, CONSUMER_GROUP, message_id)

    return stats


def run(batch_size: int, threshold: float, poll_interval: float):
    print("Loading model and schema...")
    if not MODEL_PATH.exists():
        raise FileNotFoundError(f"{MODEL_PATH} not found. Run train.py (Day 3) first.")
    model = joblib.load(MODEL_PATH)
    with open(SCHEMA_PATH) as f:
        schema = json.load(f)
    print(f"✓ Model loaded ({model.n_features_in_} features)")

    client = get_redis_client()
    client.ping()
    print("✓ Redis connected")

    ensure_consumer_group(client)

    print(f"\nConsuming '{RAW_STREAM}' → scoring → publishing to '{SCORED_STREAM}'")
    print(f"Batch size: {batch_size}, alert threshold: {threshold}\n")

    total_processed = 0
    total_alerts = 0

    try:
        while True:
            response = client.xreadgroup(
                CONSUMER_GROUP,
                CONSUMER_NAME,
                {RAW_STREAM: ">"},
                count=batch_size,
                block=int(poll_interval * 1000),
            )

            if not response:
                continue

            for _stream_name, messages in response:
                stats = process_batch(client, model, schema, messages, threshold)
                total_processed += stats["processed"]
                total_alerts += stats["alerts_published"]

                if stats["processed"] > 0:
                    print(
                        f"  batch: {stats['processed']} processed, "
                        f"{stats['alerts_published']} alerts published, "
                        f"{stats['skipped_unseeded']} skipped (unseeded) "
                        f"| totals: {total_processed} processed, {total_alerts} alerts"
                    )

    except KeyboardInterrupt:
        print(f"\n\nStopped by user.")
        print(f"Total processed: {total_processed}, total alerts published: {total_alerts}")


def main():
    parser = argparse.ArgumentParser(description="Stream consumer: score telemetry, publish alerts")
    parser.add_argument("--batch-size", type=int, default=10, help="Messages per batch (default: 10)")
    parser.add_argument(
        "--threshold", type=float, default=float(os.getenv("ALERT_PROBABILITY_THRESHOLD", 0.3)),
        help="Minimum failure probability to publish an alert (default: 0.3 or .env value)",
    )
    parser.add_argument(
        "--poll-interval", type=float, default=2.0,
        help="Seconds to block waiting for new messages before re-polling (default: 2.0)",
    )
    args = parser.parse_args()
    run(batch_size=args.batch_size, threshold=args.threshold, poll_interval=args.poll_interval)


if __name__ == "__main__":
    main()
