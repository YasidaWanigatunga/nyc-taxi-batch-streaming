CREATE SCHEMA IF NOT EXISTS stream;

CREATE TABLE IF NOT EXISTS stream.stream_trip_events (
    event_id          BIGSERIAL PRIMARY KEY,
    event_ts          TIMESTAMPTZ  NOT NULL,
    ingest_ts         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    pu_location_id    INTEGER      NOT NULL,
    do_location_id    INTEGER      NOT NULL,
    passenger_count   SMALLINT     NOT NULL,
    trip_distance     NUMERIC(10,2) NOT NULL,
    fare_amount       NUMERIC(10,2) NOT NULL,
    total_amount      NUMERIC(10,2) NOT NULL,
    payment_type      SMALLINT     NOT NULL,
    kafka_partition   SMALLINT     NOT NULL,
    kafka_offset      BIGINT       NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_event_ts
    ON stream.stream_trip_events (event_ts DESC);

CREATE TABLE IF NOT EXISTS stream.trip_window_agg (
    window_start      TIMESTAMPTZ  NOT NULL,
    window_end        TIMESTAMPTZ  NOT NULL,
    pu_location_id    INTEGER      NOT NULL,
    trip_count        INTEGER      NOT NULL,
    total_revenue     NUMERIC(14,2) NOT NULL,
    avg_fare          NUMERIC(10,2) NOT NULL,
    avg_distance      NUMERIC(10,2) NOT NULL,
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, pu_location_id)
);

CREATE INDEX IF NOT EXISTS idx_agg_window_start
    ON stream.trip_window_agg (window_start DESC);

CREATE OR REPLACE VIEW stream.latest_window_leaderboard AS
SELECT window_start, window_end, pu_location_id,
       trip_count, total_revenue, avg_fare, avg_distance
FROM stream.trip_window_agg
WHERE window_start = (SELECT MAX(window_start) FROM stream.trip_window_agg)
ORDER BY total_revenue DESC;
