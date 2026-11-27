CREATE TABLE IF NOT EXISTS bigdata.forecast_rt
(
    system_id     LowCardinality(String),
    station_id    String,
    forecast_ts   DateTime64(3, 'UTC'),
    target_ts     DateTime64(3, 'UTC'),
    horizon_min   UInt16,
    pred_bikes    Float32,
    pred_inflow   Float32 DEFAULT 0,
    pred_outflow  Float32 DEFAULT 0,
    lower_bound   Float32 DEFAULT 0,
    upper_bound   Float32 DEFAULT 0,
    model_name    LowCardinality(String),
    model_version String DEFAULT ''
)
ENGINE = ReplacingMergeTree(forecast_ts)
PARTITION BY toYYYYMMDD(target_ts)
ORDER BY (system_id, station_id, target_ts, horizon_min, model_name)
TTL toDateTime(target_ts) + INTERVAL 7 DAY DELETE;

CREATE TABLE IF NOT EXISTS bigdata.station_cluster_batch
(
    system_id     LowCardinality(String),
    run_id        String,
    station_id    String,
    cluster_id    UInt16,
    cluster_label String DEFAULT '',
    features      Map(String, Float32),
    computed_at   DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (system_id, run_id, station_id);

CREATE TABLE IF NOT EXISTS bigdata.model_metrics
(
    model_name    LowCardinality(String),
    model_version String DEFAULT '',
    system_id     LowCardinality(String) DEFAULT '',
    metric_name   LowCardinality(String),
    metric_value  Float64,
    scope         LowCardinality(String) DEFAULT 'global',
    horizon_min   UInt16 DEFAULT 0,
    run_id        String DEFAULT '',
    evaluated_at  DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (model_name, system_id, metric_name, evaluated_at);