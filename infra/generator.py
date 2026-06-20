"""
Day 6 — Mock IoT telemetry generator.

Simulates ~20 machines continuously emitting sensor readings into a Redis
Stream (stream:raw-telemetry). Rather than generating arbitrary random
numbers, this replays each machine's REAL historical sensor trajectory
(from the training data) cycle by cycle, looping back to the start when it
reaches the end — this guarantees every live reading is statistically
identical to what the model was trained on, with zero risk of mismatch
between "what the generator emits" and "what the model expects to see."

Usage:
    python generator.py                  # default rate, all engines
    python generator.py --rate 10        # 10 events/sec total
    python generator.py --machines 5     # only simulate first 5 engines
    python generator.py --once           # emit ONE reading per machine, then exit
                                          # (a single round-robin tick — useful
                                          # for quick pipeline smoke tests)
"""

import argparse
import json
import os
import time
from itertools import cycle as itertools_cycle
from pathlib import Path

import pandas as pd
import redis
from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).resolve().parent.parent
DATA_PATH = ROOT / "notebooks" / "data" / "train_FD001.txt"
STREAM_NAME = "stream:raw-telemetry"

INDEX_NAMES = ["unit_nr", "time_cycles"]
SETTING_NAMES = ["setting_1", "setting_2", "setting_3"]
SENSOR_NAMES = [f"s_{i}" for i in range(1, 22)]
COL_NAMES = INDEX_NAMES + SETTING_NAMES + SENSOR_NAMES


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


def load_engine_trajectories(max_machines: int | None) -> dict[int, list[dict]]:
    """
    Loads each engine's full cycle-by-cycle sensor history as an ordered
    list of readings, keyed by unit_nr. This is what gets replayed in a
    loop to simulate that engine "still running" indefinitely.
    """
    if not DATA_PATH.exists():
        raise FileNotFoundError(
            f"{DATA_PATH} not found. Place train_FD001.txt in notebooks/data/."
        )
    df = pd.read_csv(DATA_PATH, sep=r"\s+", header=None, names=COL_NAMES)

    unit_ids = sorted(df["unit_nr"].unique())
    if max_machines:
        unit_ids = unit_ids[:max_machines]

    trajectories = {}
    for unit_id in unit_ids:
        sub = df[df["unit_nr"] == unit_id].sort_values("time_cycles")
        readings = sub[SENSOR_NAMES].to_dict(orient="records")
        trajectories[int(unit_id)] = readings
    return trajectories


def build_event(machine_id: int, reading: dict, cycle_position: int) -> dict[str, str]:
    """
    Redis Streams (via XADD) only accepts flat string/byte fields — no
    nested JSON. We flatten the reading into a flat dict and stringify
    every value, which the consumer (Day 7) will parse back into floats.
    """
    event = {
        "machine_id": str(machine_id),
        "cycle_position": str(cycle_position),
        "timestamp": str(time.time()),
    }
    for sensor, value in reading.items():
        event[sensor] = str(value)
    return event


def run(rate_per_sec: float, max_machines: int | None, once: bool):
    print("Loading engine trajectories from training data...")
    trajectories = load_engine_trajectories(max_machines)
    print(f"Loaded {len(trajectories)} engine trajectories")

    client = get_redis_client()
    client.ping()
    print(f"✓ Connected to Redis. Streaming to '{STREAM_NAME}'")

    # Each engine replays its own trajectory independently and loops forever
    # (itertools.cycle repeats the sequence indefinitely once exhausted).
    cursors = {
        machine_id: itertools_cycle(enumerate(readings))
        for machine_id, readings in trajectories.items()
    }

    delay_between_events = 1.0 / rate_per_sec if rate_per_sec > 0 else 0
    machine_ids = list(cursors.keys())
    total_emitted = 0

    try:
        while True:
            for machine_id in machine_ids:
                cycle_position, reading = next(cursors[machine_id])
                event = build_event(machine_id, reading, cycle_position)
                client.xadd(STREAM_NAME, event)
                total_emitted += 1

                if total_emitted % 50 == 0:
                    print(f"  emitted {total_emitted} events so far "
                          f"(latest: machine_{machine_id}, cycle {cycle_position})")

                if delay_between_events:
                    time.sleep(delay_between_events)

            if once:
                print(f"\n✓ Single round-robin tick complete. Emitted {total_emitted} "
                      f"events (one per machine, across {len(machine_ids)} machines).")
                break
    except KeyboardInterrupt:
        print(f"\n\nStopped by user. Total events emitted: {total_emitted}")


def main():
    parser = argparse.ArgumentParser(description="Mock IoT telemetry generator")
    parser.add_argument(
        "--rate", type=float, default=2.0,
        help="Events per second, total across all machines (default: 2.0)",
    )
    parser.add_argument(
        "--machines", type=int, default=None,
        help="Limit to the first N machines (default: all)",
    )
    parser.add_argument(
        "--once", action="store_true",
        help="Emit a single full pass through all machines, then exit",
    )
    args = parser.parse_args()

    run(rate_per_sec=args.rate, max_machines=args.machines, once=args.once)


if __name__ == "__main__":
    main()
