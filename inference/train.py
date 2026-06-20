"""
Day 3 — Train an XGBoost classifier to predict imminent engine failure.

Reads artifacts/feature_schema.json (written by the Day 2 notebook) so the
exact same top sensors, rolling window, and RUL threshold are used here as
were used during EDA. This schema file is the single source of truth that
Day 4 (Redis feature store) and the FastAPI service must also honor — any
mismatch between this script's feature engineering and the live feature
store's feature engineering is exactly what "training-serving skew" means.

Usage:
    python train.py
"""

import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import classification_report, roc_auc_score
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier

ROOT = Path(__file__).resolve().parent.parent
DATA_PATH = ROOT / "notebooks" / "data" / "train_FD001.txt"
SCHEMA_PATH = ROOT / "artifacts" / "feature_schema.json"
MODEL_PATH = ROOT / "artifacts" / "xgb_model.joblib"
BASELINE_STATS_PATH = ROOT / "artifacts" / "baseline_stats.json"

INDEX_NAMES = ["unit_nr", "time_cycles"]
SETTING_NAMES = ["setting_1", "setting_2", "setting_3"]
SENSOR_NAMES = [f"s_{i}" for i in range(1, 22)]
COL_NAMES = INDEX_NAMES + SETTING_NAMES + SENSOR_NAMES


def load_schema() -> dict:
    if not SCHEMA_PATH.exists():
        raise FileNotFoundError(
            f"{SCHEMA_PATH} not found. Run the Day 2 notebook "
            "(notebooks/01_eda.ipynb) first — it writes this file."
        )
    with open(SCHEMA_PATH) as f:
        return json.load(f)


def load_raw_data() -> pd.DataFrame:
    if not DATA_PATH.exists():
        raise FileNotFoundError(
            f"{DATA_PATH} not found. Place train_FD001.txt in notebooks/data/ "
            "(same file used in the Day 2 notebook)."
        )
    return pd.read_csv(DATA_PATH, sep=r"\s+", header=None, names=COL_NAMES)


def engineer_features(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    """
    Reproduces EXACTLY the feature engineering from the Day 2 notebook:
    RUL, rolling mean/std per top sensor, and the binary label.

    This function's logic must match the Redis feature store getter that
    will be written on Day 4. Any drift between the two is training-serving
    skew — the #1 thing that silently breaks production ML systems.
    """
    df = df.copy()
    max_cycles = df.groupby("unit_nr")["time_cycles"].transform("max")
    df["RUL"] = max_cycles - df["time_cycles"]

    window = schema["rolling_window"]
    for sensor in schema["top_sensors"]:
        df[f"{sensor}_rolling_mean_{window}"] = df.groupby("unit_nr")[sensor].transform(
            lambda x: x.rolling(window, min_periods=1).mean()
        )
        df[f"{sensor}_rolling_std_{window}"] = df.groupby("unit_nr")[sensor].transform(
            lambda x: x.rolling(window, min_periods=1).std().fillna(0)
        )

    df[schema["label_column"]] = (df["RUL"] < schema["rul_threshold"]).astype(int)
    return df


def train_model(X_train, y_train) -> XGBClassifier:
    # scale_pos_weight compensates for class imbalance (~14% positive rate) —
    # without this, XGBoost would be biased toward predicting "no failure"
    # since that's the majority class.
    neg, pos = (y_train == 0).sum(), (y_train == 1).sum()
    scale_pos_weight = neg / pos

    model = XGBClassifier(
        n_estimators=200,
        max_depth=5,
        learning_rate=0.1,
        scale_pos_weight=scale_pos_weight,
        eval_metric="logloss",
        random_state=42,
    )
    model.fit(X_train, y_train)
    return model


def evaluate(model, X_test, y_test) -> float:
    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]

    print("\n=== Classification report ===")
    print(classification_report(y_test, y_pred, target_names=["healthy", "failure_soon"]))

    auc = roc_auc_score(y_test, y_proba)
    print(f"ROC-AUC: {auc:.4f}")
    return auc


def save_baseline_stats(df: pd.DataFrame, feature_columns: list[str]) -> None:
    """
    Saves mean/std/min/max for every feature column, computed on the FULL
    training set. Day 11's drift detection compares live feature
    distributions against these baseline stats — if a sensor's live mean
    drifts far from its baseline mean, that's a signal the model may need
    retraining.
    """
    stats = {}
    for col in feature_columns:
        stats[col] = {
            "mean": float(df[col].mean()),
            "std": float(df[col].std()),
            "min": float(df[col].min()),
            "max": float(df[col].max()),
        }
    with open(BASELINE_STATS_PATH, "w") as f:
        json.dump(stats, f, indent=2)
    print(f"Saved baseline stats for {len(stats)} features → {BASELINE_STATS_PATH}")


def main():
    print("Loading feature schema from Day 2...")
    schema = load_schema()
    print(f"Top sensors: {schema['top_sensors']}")
    print(f"Rolling window: {schema['rolling_window']}, RUL threshold: {schema['rul_threshold']}")

    print("\nLoading raw data...")
    raw_df = load_raw_data()
    print(f"Loaded {raw_df.shape[0]} rows, {raw_df['unit_nr'].nunique()} engines")

    print("\nEngineering features (must match Day 4 Redis logic exactly)...")
    df = engineer_features(raw_df, schema)

    feature_columns = schema["feature_columns"]
    label_column = schema["label_column"]

    X = df[feature_columns]
    y = df[label_column]
    print(f"\nFeature matrix: {X.shape}, positive class rate: {y.mean():.2%}")

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    print(f"Train: {X_train.shape[0]} rows, Test: {X_test.shape[0]} rows")

    print("\nTraining XGBoost classifier...")
    model = train_model(X_train, y_train)

    auc = evaluate(model, X_test, y_test)

    if auc < 0.85:
        print(
            f"\n⚠️  WARNING: AUC ({auc:.4f}) is below the 0.85 target. "
            "Consider revisiting feature selection in the Day 2 notebook, "
            "or trying different rolling windows / sensor counts."
        )
    else:
        print(f"\n✓ AUC ({auc:.4f}) meets target (>0.85).")

    print(f"\nSaving model → {MODEL_PATH}")
    joblib.dump(model, MODEL_PATH)

    print("\nComputing baseline feature statistics (for Day 11 drift detection)...")
    save_baseline_stats(df, feature_columns)

    print("\n✓ Day 3 complete. Artifacts written:")
    print(f"  - {MODEL_PATH}")
    print(f"  - {BASELINE_STATS_PATH}")


if __name__ == "__main__":
    main()
