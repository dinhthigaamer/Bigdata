CREATE TABLE IF NOT EXISTS bigdata.station_flow_batch
(
    system_id        LowCardinality(String),
    station_id       String,
    hour_ts          DateTime('UTC'),
    bikes_in         UInt32,
    bikes_out        UInt32,
    net_flow         Int32,
    avg_bikes        Float32,
    min_bikes        UInt16,
    max_bikes        UInt16,
    empty_minutes    UInt16 DEFAULT 0,
    full_minutes     UInt16 DEFAULT 0,
    computed_at      DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(hour_ts)
ORDER BY (system_id, station_id, hour_ts);

CREATE TABLE IF NOT EXISTS bigdata.batch_watermark
(
    job_name       String,
    system_id      LowCardinality(String) DEFAULT '',
    watermark_ts   DateTime64(3, 'UTC'),
    last_run_id    String DEFAULT '',
    status         LowCardinality(String) DEFAULT 'OK',
    rows_processed UInt64 DEFAULT 0,
    updated_at     DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (job_name, system_id);