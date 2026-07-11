from __future__ import annotations

import json
import os
import random
import signal
import sys
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
from kafka import KafkaProducer
from prometheus_client import Counter, start_http_server

BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"].split(",")
TOPIC = os.environ["TOPIC"]
EPS = float(os.getenv("EVENTS_PER_SECOND", "30"))
PARQUET = os.getenv("PARQUET_PATH", "/data/yellow_tripdata_2023-01.parquet")
OUT_OF_ORDER_FRACTION = float(os.getenv("OUT_OF_ORDER_FRACTION", "0.05"))
METRICS_PORT = int(os.getenv("PRODUCER_METRICS_PORT", "8001"))

events_sent = Counter("producer_events_sent_total", "Events produced to Kafka")
send_errors = Counter("producer_send_errors_total", "Failed sends")

_running = True


def _stop(*_):
    global _running
    _running = False
    print("producer: shutdown signal received", flush=True)


signal.signal(signal.SIGINT, _stop)
signal.signal(signal.SIGTERM, _stop)


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        key_serializer=lambda k: str(k).encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=20,
        max_in_flight_requests_per_connection=1,
    )


def load_rows(path: str) -> pd.DataFrame:
    print(f"producer: reading {path}", flush=True)
    cols = ["tpep_pickup_datetime", "PULocationID", "DOLocationID",
            "passenger_count", "trip_distance", "fare_amount",
            "total_amount", "payment_type"]
    df = pd.read_parquet(path, columns=cols)
    df = df[(df["trip_distance"] > 0) & (df["fare_amount"] >= 0)
            & (df["total_amount"] > 0)].reset_index(drop=True)
    df["passenger_count"] = df["passenger_count"].fillna(1).astype("int16")
    df["payment_type"] = df["payment_type"].fillna(0).astype("int16")
    print(f"producer: {len(df):,} rows ready to stream", flush=True)
    return df


def to_event(row: pd.Series) -> tuple[int, dict]:
    now = datetime.now(timezone.utc)
    if random.random() < OUT_OF_ORDER_FRACTION:
        now = now - timedelta(seconds=random.randint(5, 30))
    pu = int(row["PULocationID"])
    event = {
        "event_ts": now.isoformat(),
        "pu_location_id": pu,
        "do_location_id": int(row["DOLocationID"]),
        "passenger_count": int(row["passenger_count"]),
        "trip_distance": float(row["trip_distance"]),
        "fare_amount": float(row["fare_amount"]),
        "total_amount": float(row["total_amount"]),
        "payment_type": int(row["payment_type"]),
    }
    return pu, event


def main() -> None:
    start_http_server(METRICS_PORT)
    producer = build_producer()
    df = load_rows(PARQUET)

    interval = 1.0 / EPS
    print(f"producer: streaming at ~{EPS} events/sec to '{TOPIC}'", flush=True)

    i = 0
    n = len(df)
    while _running:
        key, event = to_event(df.iloc[i % n])
        try:
            producer.send(TOPIC, key=key, value=event)
            events_sent.inc()
        except Exception as exc:
            send_errors.inc()
            print(f"producer: send error: {exc}", flush=True)
        i += 1
        if i % 500 == 0:
            print(f"producer: {i:,} events sent", flush=True)
        time.sleep(interval)

    producer.flush()
    producer.close()
    print("producer: closed cleanly", flush=True)


if __name__ == "__main__":
    sys.exit(main())
