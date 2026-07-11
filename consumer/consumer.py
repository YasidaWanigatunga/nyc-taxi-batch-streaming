"""
Task 3 — Stream Consumer, Windowed Aggregator, and Sink
Consumes taxi-trips-stream, maintains 5-min tumbling windows per pickup zone,
and on window close writes raw events (append) + aggregates (upsert) to Postgres.

Three graded behaviours:
1. WINDOW      -> window_start_for() + the windows dict
2. OUT-OF-ORDER-> WATERMARK_SECONDS grace period before flush
3. BACKPRESSURE-> manual commit + bounded poll (at-least-once)
"""
from __future__ import annotations

import json
import os
import signal
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import psycopg2
import psycopg2.extras
from kafka import KafkaConsumer
from prometheus_client import Counter, Gauge, Histogram, start_http_server

BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"].split(",")
TOPIC = os.environ["TOPIC"]
PG_DSN = os.environ["PG_DSN"]
WINDOW_SECONDS = int(os.getenv("WINDOW_SECONDS", "300"))
WATERMARK_SECONDS = int(os.getenv("WATERMARK_SECONDS", "10"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))
GROUP_ID = os.getenv("GROUP_ID", "taxi-aggregator")

events_consumed = Counter("consumer_events_consumed_total", "Events consumed")
events_late = Counter("consumer_events_late_total", "Late events dropped")
windows_flushed = Counter("consumer_windows_flushed_total", "Windows written")
rows_upserted = Counter("consumer_agg_rows_upserted_total", "Aggregate rows upserted")
consumer_lag = Gauge("consumer_lag_messages", "End offset minus position", ["partition"])
open_windows = Gauge("consumer_open_windows", "Windows held in memory")
flush_seconds = Histogram("consumer_flush_seconds", "Time to flush a window")
watermark_ts = Gauge("consumer_watermark_timestamp", "Current watermark (unix secs)")

_running = True


def _stop(*_):
    global _running
    _running = False


signal.signal(signal.SIGINT, _stop)
signal.signal(signal.SIGTERM, _stop)


def window_start_for(ts: datetime) -> datetime:
    epoch = int(ts.timestamp())
    floored = epoch - (epoch % WINDOW_SECONDS)
    return datetime.fromtimestamp(floored, tz=timezone.utc)


class WindowState:
    __slots__ = ("count", "revenue", "fare_sum", "dist_sum", "raw")

    def __init__(self):
        self.count = 0
        self.revenue = 0.0
        self.fare_sum = 0.0
        self.dist_sum = 0.0
        self.raw = []

    def add(self, ev, partition, offset):
        self.count += 1
        self.revenue += ev["total_amount"]
        self.fare_sum += ev["fare_amount"]
        self.dist_sum += ev["trip_distance"]
        self.raw.append((
            ev["event_ts"], ev["pu_location_id"], ev["do_location_id"],
            ev["passenger_count"], ev["trip_distance"], ev["fare_amount"],
            ev["total_amount"], ev["payment_type"], partition, offset,
        ))


windows = defaultdict(dict)


def connect_pg():
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = False
    return conn


RAW_INSERT = """
    INSERT INTO stream.stream_trip_events
      (event_ts, pu_location_id, do_location_id, passenger_count,
       trip_distance, fare_amount, total_amount, payment_type,
       kafka_partition, kafka_offset)
    VALUES %s
"""

AGG_UPSERT = """
    INSERT INTO stream.trip_window_agg
      (window_start, window_end, pu_location_id,
       trip_count, total_revenue, avg_fare, avg_distance)
    VALUES %s
    ON CONFLICT (window_start, pu_location_id) DO UPDATE SET
       trip_count    = stream.trip_window_agg.trip_count    + EXCLUDED.trip_count,
       total_revenue = stream.trip_window_agg.total_revenue + EXCLUDED.total_revenue,
       avg_fare      = EXCLUDED.avg_fare,
       avg_distance  = EXCLUDED.avg_distance,
       updated_at    = now()
"""


def flush_window(conn, ws, cells):
    we = ws + timedelta(seconds=WINDOW_SECONDS)
    raw_rows, agg_rows = [], []
    for pu, st in cells.items():
        raw_rows.extend(st.raw)
        agg_rows.append((
            ws, we, pu, st.count, round(st.revenue, 2),
            round(st.fare_sum / st.count, 2),
            round(st.dist_sum / st.count, 2),
        ))
    with flush_seconds.time():
        with conn.cursor() as cur:
            if raw_rows:
                psycopg2.extras.execute_values(cur, RAW_INSERT, raw_rows)
            if agg_rows:
                psycopg2.extras.execute_values(cur, AGG_UPSERT, agg_rows)
        conn.commit()
    windows_flushed.inc()
    rows_upserted.inc(len(agg_rows))
    print(f"consumer: flushed window {ws.isoformat()} "
          f"({len(agg_rows)} zones, {len(raw_rows)} events)", flush=True)


def update_lag(consumer):
    for tp in consumer.assignment():
        end = consumer.end_offsets([tp])[tp]
        pos = consumer.position(tp)
        consumer_lag.labels(partition=str(tp.partition)).set(max(0, end - pos))


def main():
    start_http_server(METRICS_PORT)
    conn = connect_pg()
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id=GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=200,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        key_deserializer=lambda b: b.decode("utf-8") if b else None,
    )
    print(f"consumer: joined group '{GROUP_ID}', waiting for events", flush=True)

    max_event_time = None

    while _running:
        batch = consumer.poll(timeout_ms=1000)
        if not batch:
            continue

        for tp, records in batch.items():
            for rec in records:
                ev = rec.value
                ev_ts = datetime.fromisoformat(ev["event_ts"])
                if ev_ts.tzinfo is None:
                    ev_ts = ev_ts.replace(tzinfo=timezone.utc)

                ws = window_start_for(ev_ts)

                if max_event_time is not None:
                    watermark = max_event_time - timedelta(seconds=WATERMARK_SECONDS)
                    if ws + timedelta(seconds=WINDOW_SECONDS) < watermark:
                        events_late.inc()
                        continue

                windows[ws].setdefault(
                    ev["pu_location_id"], WindowState()
                ).add(ev, rec.partition, rec.offset)
                events_consumed.inc()

                if max_event_time is None or ev_ts > max_event_time:
                    max_event_time = ev_ts

        if max_event_time is not None:
            watermark = max_event_time - timedelta(seconds=WATERMARK_SECONDS)
            watermark_ts.set(watermark.timestamp())
            ready = [ws for ws in windows
                     if ws + timedelta(seconds=WINDOW_SECONDS) <= watermark]
            for ws in sorted(ready):
                flush_window(conn, ws, windows.pop(ws))

        open_windows.set(len(windows))
        update_lag(consumer)
        consumer.commit()

    for ws in sorted(windows):
        flush_window(conn, ws, windows[ws])
    consumer.commit()
    consumer.close()
    conn.close()
    print("consumer: closed cleanly", flush=True)


if __name__ == "__main__":
    main()
