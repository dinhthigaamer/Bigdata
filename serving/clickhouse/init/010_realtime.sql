-- ===== DIMENSION =====
CREATE TABLE IF NOT EXISTS bigdata.dim_station
(
    station_id  String,
    name        String,
    lat         Float64,
    lon         Float64,
    capacity    UInt16,
    address     String DEFAULT '',
    region      LowCardinality(String) DEFAULT '',
    updated_at  DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY station_id;

-- ===== TRẠNG THÁI MỚI NHẤT (query với FINAL hoặc argMax) =====
CREATE TABLE IF NOT EXISTS bigdata.station_latest
(
    station_id      String,
    ts              DateTime64(3, 'UTC'),
    bikes_available UInt16,
    docks_available UInt16,
    capacity        UInt16,
    occupancy_rate  Float32,
    is_renting      UInt8 DEFAULT 1,
    is_returning    UInt8 DEFAULT 1,
    ingested_at     DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ts)
ORDER BY station_id;

-- ===== LỊCH SỬ TRẠNG THÁI (giữ 30 ngày) =====
CREATE TABLE IF NOT EXISTS bigdata.station_status_rt
(
    station_id      String,
    ts              DateTime64(3, 'UTC'),
    bikes_available UInt16,
    docks_available UInt16,
    capacity        UInt16,
    occupancy_rate  Float32,
    is_renting      UInt8 DEFAULT 1,
    is_returning    UInt8 DEFAULT 1,
    ingested_at     DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ts)
ORDER BY (station_id, ts)
TTL toDateTime(ts) + INTERVAL 30 DAY;

-- ===== FLOW THEO CỬA SỔ (streaming) =====
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

-- ===== CẢNH BÁO =====
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

-- ===== THỜI TIẾT MỚI NHẤT =====
CREATE TABLE IF NOT EXISTS bigdata.weather_latest
(
    location_id      String,
    ts               DateTime64(3, 'UTC'),
    temperature_c    Float32,
    humidity         Float32,
    precipitation_mm Float32,
    wind_speed_ms    Float32,
    weather_code     LowCardinality(String) DEFAULT '',
    ingested_at      DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ts)
ORDER BY location_id;
