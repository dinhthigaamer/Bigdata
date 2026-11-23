-- =====================================================================
-- CityBike / GBFS Lambda - ClickHouse schema (001: các bảng đã chắc chắn)
-- Đặt tại serving/clickhouse/init/ (mount vào /docker-entrypoint-initdb.d).
-- Chỉ tự chạy khi volume clickhouse_data còn trống.
--
-- Nếu CLICKHOUSE_DB trong serving/clickhouse/.env KHÁC 'gbfs', hãy
-- find/replace "gbfs." và "gbfs;" trong file này cho khớp.
--
-- Các bảng còn chờ xác nhận (flow, alerts, batch, ML) để ở
-- serving/clickhouse/drafts/unconfirmed_tables.sql - KHÔNG tự chạy.
--
-- Quy ước:
--   * Thời gian lưu UTC.
--   * Cột Nullable(...) tương ứng trường Optional trong model pydantic.
--   * ReplacingMergeTree => ghi lặp/retry là idempotent. Khi đọc cần chính xác
--     tuyệt đối dùng FINAL hoặc argMax(..., version_col).
--   * system_id = định danh hệ thống GBFS (vd 'citibike_nyc', 'bixi_mtl').
-- =====================================================================

CREATE DATABASE IF NOT EXISTS gbfs;

-- ---------------------------------------------------------------------
-- 1. dim_station  <- StationInfo (station_information)
--    `extra` = JSON các trường GBFS ngoài CORE.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gbfs.dim_station
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

-- ---------------------------------------------------------------------
-- 2. station_latest  <- StationStatus (upsert, mỗi trạm 1 dòng sau merge)
--    event_ts tự tính từ last_reported/snapshot_ts nếu bên ghi không gửi.
--    Đọc: SELECT ... FROM gbfs.station_latest FINAL
--    vehicle_types_available: Map(vehicle_type_id -> count)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gbfs.station_latest
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
-- version = event_ts: replay sau khi reset checkpoint không ghi đè trạng thái mới bằng bản cũ
ENGINE = ReplacingMergeTree(event_ts)
ORDER BY (system_id, station_id);

-- ---------------------------------------------------------------------
-- 3. station_status_rt : lịch sử status theo snapshot (giữ 30 ngày)
--    event_ts = coalesce(last_reported, snapshot_ts) - giống cột event_ts của
--    Job A (speed layer). Khóa dedup (system, station, event_ts) khớp với
--    dropDuplicates của Spark, nên replay sau khi reset checkpoint vẫn idempotent.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gbfs.station_status_rt
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

-- ---------------------------------------------------------------------
-- 4. weather_latest  <- WeatherMessage (Open-Meteo minutely_15)
--    Mỗi (location_id, time) 1 dòng; lần fetch mới nhất thắng, nên một mốc
--    từng là 'forecast' sẽ được thay bằng 'observed'. Không đặt TTL để job
--    ML còn dữ liệu quá khứ làm feature.
--    `interval` của message được lưu thành interval_seconds (tránh từ khóa).
--    location_id là khóa join với GBFS - xem ghi chú về ánh xạ system_id.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gbfs.weather_latest
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