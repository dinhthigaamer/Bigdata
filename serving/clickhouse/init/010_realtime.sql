-- ===== DIMENSION =====
CREATE TABLE IF NOT EXISTS bigdata.dim_station
(
    system_id       LowCardinality(String),
    station_id      String,
    name            String,
    short_name      Nullable(String),
    lat             Float64,
    lon             Float64,
    capacity        Nullable(UInt16),
    region_id       Nullable(String),
    is_charging     UInt8                          DEFAULT 0,
    extra           String                         DEFAULT '{}',
    source_version  LowCardinality(String)         DEFAULT '',
    updated_at      DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (system_id, station_id);

-- ===== TRẠNG THÁI MỚI NHẤT (query với FINAL hoặc argMax) =====
CREATE TABLE IF NOT EXISTS bigdata.station_latest
(
    system_id                LowCardinality(String),
    station_id               String,
    snapshot_ts              DateTime64(3, 'UTC'),
    last_reported            Nullable(DateTime64(3, 'UTC')),
    event_ts                 DateTime64(3, 'UTC')   DEFAULT coalesce(last_reported, snapshot_ts),
    num_bikes_available      UInt16,
    num_bikes_disabled       UInt16                 DEFAULT 0,
    num_docks_available      UInt16                 DEFAULT 0,
    num_docks_disabled       UInt16                 DEFAULT 0,
    num_ebikes_available     Nullable(UInt16),
    is_installed             UInt8,
    is_renting               UInt8,
    is_returning             UInt8,
    vehicle_types_available  Map(String, UInt16),
    source_version           LowCardinality(String) DEFAULT '',
    ingest_ts                DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(event_ts)
ORDER BY (system_id, station_id);

CREATE TABLE IF NOT EXISTS bigdata.station_status_rt
(
    system_id                LowCardinality(String),
    station_id               String,
    snapshot_ts              DateTime64(3, 'UTC'),
    last_reported            Nullable(DateTime64(3, 'UTC')),
    event_ts                 DateTime64(3, 'UTC')   DEFAULT coalesce(last_reported, snapshot_ts)
                                                    CODEC(DoubleDelta, ZSTD(1)),
    num_bikes_available      UInt16,
    num_bikes_disabled       UInt16                 DEFAULT 0,
    num_docks_available      UInt16                 DEFAULT 0,
    num_docks_disabled       UInt16                 DEFAULT 0,
    num_ebikes_available     Nullable(UInt16),
    is_installed             UInt8,
    is_renting               UInt8,
    is_returning             UInt8,
    vehicle_types_available  Map(String, UInt16),
    source_version           LowCardinality(String) DEFAULT '',
    ingest_ts                DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ingest_ts)
PARTITION BY toYYYYMMDD(event_ts)
ORDER BY (system_id, station_id, event_ts)
TTL toDateTime(event_ts) + INTERVAL 30 DAY DELETE;

CREATE TABLE IF NOT EXISTS bigdata.station_flow_rt
(
    station_id   String,
    window_start DateTime64(3, 'UTC'),
    window_end   DateTime64(3, 'UTC'),
    bikes_in     UInt32,
    bikes_out    UInt32,
    net_flow     Int32,
    avg_bikes    Float32,
    ingested_at  DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (station_id, window_start)
TTL toDateTime(window_start) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS bigdata.alerts_log
(
    alert_id     UUID DEFAULT generateUUIDv4(),
    ts           DateTime64(3, 'UTC'),
    station_id   String,
    alert_type   LowCardinality(String),
    severity     LowCardinality(String),
    metric_value Float64 DEFAULT 0,
    threshold    Float64 DEFAULT 0,
    message      String DEFAULT '',
    resolved     UInt8 DEFAULT 0
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (ts, station_id)
TTL toDateTime(ts) + INTERVAL 90 DAY;

CREATE TABLE IF NOT EXISTS bigdata.weather_latest
(
    location_id           LowCardinality(String),
    time                  DateTime('UTC'),
    data_type             Enum8('observed' = 1, 'forecast' = 2),
    latitude              Float64,
    longitude             Float64,
    fetch_time            DateTime64(3, 'UTC'),
    source                LowCardinality(String)  DEFAULT 'open_meteo',
    interval_seconds      UInt32                  DEFAULT 900,
    temperature_2m        Nullable(Float32),
    relative_humidity_2m  Nullable(UInt8),
    precipitation         Nullable(Float32),
    rain                  Nullable(Float32),
    snowfall              Nullable(Float32),
    wind_speed_10m        Nullable(Float32),
    wind_direction_10m    Nullable(UInt16),
    wind_gusts_10m        Nullable(Float32),
    visibility            Nullable(Float32),
    weather_code          Nullable(UInt16)
)
ENGINE = ReplacingMergeTree(fetch_time)
PARTITION BY toYYYYMM(time)
ORDER BY (location_id, time);
