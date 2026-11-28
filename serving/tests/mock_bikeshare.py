"""Sinh mock data cho dashboard Grafana (bike-share real-time).

Phủ ĐỦ 6 bảng trong 010_realtime.sql:

    dim_station        <- write_dim_station
    station_latest     <- write_station_status (snapshot cuối)
    station_status_rt  <- lịch sử status theo phút (insert trực tiếp)
                          + snapshot cuối (write_station_status)
    station_flow_rt    <- write_flow_rt
    alerts_log         <- write_alerts
    weather_latest     <- insert trực tiếp (observed + forecast)

Các bảng KHỚP NHAU vì cùng một mô phỏng:
  * status/flow/alerts sinh từ cùng một chuỗi số xe theo phút của từng trạm;
  * số xe luôn thỏa: bikes + bikes_disabled + docks + docks_disabled = capacity;
  * nhu cầu thuê/trả xe phụ thuộc giờ cao điểm VÀ thời tiết (mưa/lạnh -> ít xe
    di chuyển hơn) nên dashboard có thể thấy tương quan weather <-> flow;
  * alert phát theo trạng thái (edge-triggered) và có resolved khi trạm hồi phục.
  * Kết quả xác định theo --seed (RNG riêng cho từng trạm / từng system).

Alert:
    STATION_EMPTY (critical) : 0 xe
    LOW_BIKES     (warning)  : số xe <= low_ratio * sức chứa khả dụng
    STATION_FULL  (critical) : 0 chỗ trống
    LOW_DOCKS     (warning)  : số chỗ trống <= low_ratio * sức chứa khả dụng

Hai chế độ:
  1) Tổng hợp (mặc định): sinh cả dim_station + status + flow + alerts + weather.
  2) --from-dim: ĐỌC dim_station đã có, chỉ sinh phần còn lại khớp các trạm đó
     (weather: 1 location_id = system_id, tại tâm các trạm). Trạm capacity
     NULL/0 bị bỏ qua.

    python mock_bikeshare.py                        # chế độ 1
    python mock_bikeshare.py --stations 100 --minutes 120
    python mock_bikeshare.py --from-dim --system-id citi-bike-nyc --stations 20
    python mock_bikeshare.py --no-alerts --no-weather
    python mock_bikeshare.py --status-step 5        # status_rt mỗi 5 phút
    python mock_bikeshare.py --apply-ddl            # áp DDL rồi seed

Dùng như thư viện:
    from libs.ch_writer import ClickHouseWriter
    from mock_bikeshare import seed_mock_from_dim
    with ClickHouseWriter() as w:
        seed_mock_from_dim(w, minutes=30, system_id="citi-bike-nyc")

Lưu ý: các bảng là ReplacingMergeTree -> truy vấn dashboard nên dùng FINAL
hoặc argMax. weather_latest dùng source mặc định ('open_meteo') nên chạy mock
trùng khóa với dữ liệu thật sẽ bị dedupe theo fetch_time.
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Sequence

from libs.ch_writer import ClickHouseWriter, apply_ddl

log = logging.getLogger("mock_bikeshare")

MockRow = dict[str, Any]
# system_id, station_id, capacity, lat, lon, is_charging
Station = dict[str, Any]

SOURCE_VERSION = "2.3"
DEFAULT_DDL_DIR = "clickhouse/init"
FLOW_TTL_DAYS = 30  # khớp TTL của station_flow_rt / station_status_rt
DEFAULT_LOW_RATIO = 0.10
DEFAULT_FORECAST_HOURS = 48
INSERT_CHUNK = 50_000

# Khu vực Manhattan, New York: góc dưới-trái và kích thước lưới (độ).
LAT_ORIGIN, LON_ORIGIN = 40.705, -74.015
GRID_SPAN = 0.070
JITTER = 0.0012
REGIONS = ("lower_manhattan", "midtown", "upper_manhattan")

# state: (rank, alert_type, severity). Alert chỉ phát khi rank tăng.
_LEVELS: dict[str, tuple[int, str, str]] = {
    "ok": (0, "", ""),
    "low_bikes": (1, "LOW_BIKES", "warning"),
    "low_docks": (1, "LOW_DOCKS", "warning"),
    "empty": (2, "STATION_EMPTY", "critical"),
    "full": (2, "STATION_FULL", "critical"),
}

# Nhiệt độ trung bình tháng (°C) kiểu New York, bán cầu Bắc.
_TEMPERATE_MEAN_C = [1, 2, 6, 12, 18, 23, 26, 25, 21, 15, 9, 4]
_WEATHER_STEP_S = 900  # observed mỗi 15 phút (= DEFAULT interval_seconds)


@dataclass
class MockData:
    """Dữ liệu sinh ra, chia theo bảng đích."""
    dim: list[MockRow] = field(default_factory=list)             # dim_station
    status_history: list[MockRow] = field(
        default_factory=list)  # station_status_rt
    status_latest: list[MockRow] = field(
        default_factory=list)   # status_rt + station_latest
    flow: list[MockRow] = field(
        default_factory=list)            # station_flow_rt
    alerts: list[MockRow] = field(default_factory=list)          # alerts_log
    weather: list[MockRow] = field(
        default_factory=list)         # weather_latest


def _norm_now(now: datetime | None) -> datetime:
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    return now.astimezone(timezone.utc).replace(second=0, microsecond=0)


def _check_args(minutes: int, status_step: int, forecast_hours: int,
                low_ratio: float) -> None:
    if minutes < 1:
        raise ValueError("minutes phải >= 1")
    if status_step < 1:
        raise ValueError("status_step phải >= 1")
    if forecast_hours < 0:
        raise ValueError("forecast_hours phải >= 0")
    if not 0 < low_ratio < 0.5:
        raise ValueError("low_ratio phải nằm trong (0, 0.5)")
    if minutes > FLOW_TTL_DAYS * 24 * 60:
        log.warning("minutes=%d vượt TTL %d ngày, phần cũ sẽ bị xóa khi merge",
                    minutes, FLOW_TTL_DAYS)


# ====================================================================
# Thời tiết (weather_latest)
# ====================================================================
def _seasonal_mean(lat: float, month: int) -> float:
    if abs(lat) < 30:  # nhiệt đới: gần như hằng số
        return 27.0
    idx = (month - 1 + (6 if lat < 0 else 0)) % 12
    return float(_TEMPERATE_MEAN_C[idx])


def _weather_code(wet: bool, intensity: float, temp: float, cloud: float,
                  fog: bool) -> int:
    """Mã WMO (Open-Meteo)."""
    if wet:
        if temp <= 0.5:
            return 71 if intensity < 0.5 else 73 if intensity < 2 else 75
        if intensity >= 6 and temp > 15:
            return 95  # giông
        if intensity < 0.3:
            return 51
        if intensity < 1.0:
            return 61
        return 63 if intensity < 4 else 65
    if fog:
        return 45
    return 0 if cloud < 0.2 else 1 if cloud < 0.5 else 2 if cloud < 0.8 else 3


def _generate_weather(
    rng: random.Random,
    location_id: str,
    lat: float,
    lon: float,
    start: datetime,
    now: datetime,
    forecast_hours: int,
) -> tuple[list[MockRow], list[tuple[datetime, float, float]]]:
    """Mô phỏng 1 chuỗi thời tiết liên tục (bước 15 phút) từ `start`.

    Điểm <= now  -> 'observed' (interval 900s, precipitation = mm / 15 phút).
    Điểm > now, đúng đầu giờ -> 'forecast' (interval 3600s, mm / giờ).
    Trả về (rows, timeline[(ts, mưa mm/h, nhiệt độ)]) để tính nhu cầu thuê xe.
    """
    step = timedelta(seconds=_WEATHER_STEP_S)
    tz = round(lon / 15)  # xấp xỉ múi giờ địa phương theo kinh độ
    end = now + timedelta(hours=forecast_hours)
    n = int((end - start) / step) + 1
    mean = _seasonal_mean(lat, now.month)

    temp_noise = rng.gauss(0, 1)
    wind_noise = rng.gauss(0, 1)
    cloud = rng.random()
    wet = rng.random() < 0.2
    intensity = rng.lognormvariate(-0.5, 0.7) if wet else 0.0
    wind_dir = rng.uniform(0, 360)

    rows: list[MockRow] = []
    timeline: list[tuple[datetime, float, float]] = []

    for k in range(n):
        ts = start + k * step
        hour = (ts.hour + ts.minute / 60 + tz) % 24

        # mưa: chuỗi Markov khô/ướt, cường độ mm/h tự tương quan
        if wet:
            if rng.random() < 0.08:
                wet, intensity = False, 0.0
            else:
                intensity = min(25.0, max(0.05, 0.8 * intensity
                                          + 0.2 * rng.lognormvariate(-0.3, 0.8)))
        elif rng.random() < 0.02:
            wet, intensity = True, rng.lognormvariate(-0.5, 0.7)

        cloud = min(1.0, max(0.0, 0.95 * cloud + 0.025 + rng.gauss(0, 0.07)))
        if wet:
            cloud = max(cloud, 0.85)

        temp_noise = 0.98 * temp_noise + rng.gauss(0, 0.3)
        diurnal = 4.0 * math.cos(2 * math.pi *
                                 (hour - 15) / 24) * (1 - 0.4 * cloud)
        temp = mean + diurnal + 1.5 * temp_noise - (1.5 if wet else 0.0)

        wind_noise = 0.95 * wind_noise + rng.gauss(0, 0.25)
        wind = max(0.5, 9 + 5 * wind_noise + (4 if wet else 0))  # km/h
        gust = wind * rng.uniform(1.25, 1.8)
        wind_dir = (wind_dir + rng.gauss(0, 5)) % 360

        humidity = int(min(100, max(25, round(
            45 + 40 * cloud + (10 if wet else 0) + rng.gauss(0, 2)))))
        fog = (not wet and hour < 9 and humidity >= 88 and wind < 8
               and rng.random() < 0.3)
        if fog:
            visibility = rng.uniform(300, 900)
        elif wet:
            visibility = max(1500.0, 24140 / (1 + 1.2 * intensity))
        else:
            visibility = 24140 * (1 - 0.25 * cloud)

        timeline.append((ts, intensity, temp))

        if ts <= now:
            data_type, interval_s = "observed", _WEATHER_STEP_S
            precip = intensity * _WEATHER_STEP_S / 3600
            fetch = min(now, ts + timedelta(seconds=rng.randint(20, 90)))
            t_out, w_out, g_out = temp, wind, gust
        elif ts.minute == 0:
            data_type, interval_s = "forecast", 3600
            precip = intensity  # mm / giờ
            fetch = now
            # dự báo có sai số nhỏ so với "thực tế" mô phỏng
            t_out = temp + rng.gauss(0, 0.8)
            w_out = max(0.5, wind + rng.gauss(0, 1.5))
            g_out = max(w_out, gust + rng.gauss(0, 2))
        else:
            continue

        snow = wet and temp <= 0.5
        rows.append({
            "location_id": location_id,
            "time": ts,
            "data_type": data_type,
            "latitude": round(lat, 4),
            "longitude": round(lon, 4),
            "fetch_time": fetch,
            # source: bỏ qua -> DEFAULT 'open_meteo'
            "interval_seconds": interval_s,
            "temperature_2m": round(t_out, 1),
            "relative_humidity_2m": humidity,
            "precipitation": round(precip, 2),
            "rain": 0.0 if snow else round(precip, 2),
            "snowfall": round(precip * 0.7, 2) if snow else 0.0,  # cm
            "wind_speed_10m": round(w_out, 1),
            "wind_direction_10m": int(wind_dir) % 360,
            "wind_gusts_10m": round(g_out, 1),
            "visibility": round(visibility, 0),
            "weather_code": _weather_code(wet, intensity, temp, cloud, fog),
        })
    return rows, timeline


# ====================================================================
# Nhu cầu thuê/trả xe theo giờ + thời tiết
# ====================================================================
def _hour_factor(hour: float) -> float:
    """Hệ số theo giờ địa phương: đỉnh sáng ~8h30, chiều ~17h30, đêm thấp."""
    return (0.35
            + 0.9 * math.exp(-(((hour - 8.5) / 1.5) ** 2))
            + 1.0 * math.exp(-(((hour - 17.5) / 2.0) ** 2))
            + 0.4 * math.exp(-(((hour - 13.0) / 3.0) ** 2)))


def _demand_curve(
    timeline: list[tuple[datetime, float, float]] | None,
    start: datetime,
    first_window: datetime,
    minutes: int,
    tz: int,
) -> list[float]:
    out: list[float] = []
    for m in range(minutes):
        ts = first_window + timedelta(minutes=m)
        f = _hour_factor((ts.hour + ts.minute / 60 + tz) % 24)
        if timeline:
            idx = int((ts - start).total_seconds() // _WEATHER_STEP_S)
            _, intensity, temp = timeline[min(len(timeline) - 1, max(0, idx))]
            f /= 1 + 0.35 * intensity  # mưa -> ít người đi
            if temp < 2:
                f *= 0.7               # quá lạnh
        out.append(f)
    return out


def _scale(rng: random.Random, n: int, f: float) -> int:
    """Nhân n với f, làm tròn ngẫu nhiên để giữ kỳ vọng."""
    x = n * f
    k = int(x)
    return k + (1 if rng.random() < x - k else 0)


# ====================================================================
# Alert
# ====================================================================
def _classify(bikes: int, usable: int, low_ratio: float) -> str:
    docks = usable - bikes
    threshold = max(1, int(usable * low_ratio))
    if bikes == 0:
        return "empty"
    if docks == 0:
        return "full"
    if bikes <= threshold:
        return "low_bikes"
    if docks <= threshold:
        return "low_docks"
    return "ok"


def _make_alert(rng: random.Random, system_id: str, station_id: str,
                state: str, bikes: int, usable: int, ts: datetime,
                low_ratio: float) -> MockRow:
    """1 dòng alerts_log."""
    _, alert_type, severity = _LEVELS[state]
    docks = usable - bikes
    threshold = max(1, int(usable * low_ratio))
    on_bikes = state in ("empty", "low_bikes")
    messages = {
        "STATION_EMPTY": f"Trạm {station_id} hết xe (0/{usable})",
        "LOW_BIKES": f"Trạm {station_id} sắp hết xe ({bikes}/{usable})",
        "STATION_FULL": f"Trạm {station_id} hết chỗ trống ({bikes}/{usable} xe)",
        "LOW_DOCKS": f"Trạm {station_id} sắp hết chỗ trống ({docks} chỗ)",
    }
    return {
        "alert_id": uuid.UUID(int=rng.getrandbits(128), version=4),
        "system_id": system_id,
        "ts": ts,
        "station_id": station_id,
        "alert_type": alert_type,
        "severity": severity,
        "metric_value": float(bikes if on_bikes else docks),
        "threshold": float(0 if state in ("empty", "full") else threshold),
        "message": messages[alert_type],
        "resolved": 0,
    }


# ====================================================================
# Mô phỏng 1 trạm
# ====================================================================
def _simulate_station(
    rng: random.Random,
    system_id: str,
    station_id: str,
    capacity: int,
    first_window: datetime,
    minutes: int,
    now: datetime,
    demand: Sequence[float],
    is_charging: bool = False,
    status_step: int = 1,
    with_alerts: bool = True,
    low_ratio: float = DEFAULT_LOW_RATIO,
) -> tuple[list[MockRow], list[MockRow], MockRow, list[MockRow]]:
    """Trả về (flow_rows, status_history, status_final, alerts).

    Bất biến mọi thời điểm:
        bikes + bikes_disabled + docks + docks_disabled == capacity
    Status cuối (ts = now) khớp số xe sau phút flow cuối.
    """
    # Xe / chỗ hỏng cố định theo trạm (chỉ khi trạm đủ lớn).
    disabled_docks = disabled_bikes = 0
    if capacity >= 8:
        disabled_docks = rng.choices([0, 1, 2], weights=[80, 15, 5])[0]
        disabled_bikes = rng.choices([0, 1, 2], weights=[75, 20, 5])[0]
    usable = capacity - disabled_docks - disabled_bikes  # >= 1
    ebike_ratio = rng.uniform(
        0.35, 0.65) if is_charging else rng.uniform(0.05, 0.25)

    def status_row(ts: datetime, bikes: int) -> MockRow:
        ebikes = min(bikes, round(bikes * ebike_ratio))
        return {
            "system_id": system_id,
            "station_id": station_id,
            "snapshot_ts": ts,
            "last_reported": ts - timedelta(seconds=rng.randint(0, 30)),
            "num_bikes_available": bikes,
            "num_bikes_disabled": disabled_bikes,
            "num_docks_available": usable - bikes,
            "num_docks_disabled": disabled_docks,
            "num_ebikes_available": ebikes,
            "is_installed": 1,
            "is_renting": 1,
            "is_returning": 1,
            "vehicle_types_available": {
                "classic": bikes - ebikes,
                "electric_bike": ebikes,
            },
            "source_version": SOURCE_VERSION,
            "ingest_ts": ts,
            # event_ts: DDL tự tính coalesce(last_reported, snapshot_ts)
        }

    flow_rows: list[MockRow] = []
    history: list[MockRow] = []
    alert_rows: list[MockRow] = []

    lo = min(max(1, usable // 4), usable)
    hi = max(lo, min(usable, usable * 3 // 4))
    bikes = rng.randint(lo, hi)
    prev_state = _classify(bikes, usable, low_ratio)
    open_alerts: list[MockRow] = []

    for minute in range(minutes):
        window_start = first_window + timedelta(minutes=minute)
        window_end = window_start + timedelta(minutes=1)
        f = demand[minute]
        # Kéo về mức đầy ~50% (như điều phối xe thực tế) để không kẹt ở biên.
        pull = 0.45 * (0.5 - bikes / usable)
        returned = _scale(rng, rng.choices([0, 1, 2, 3, 4], weights=[35, 35, 20, 8, 2])[0],
                          f * max(0.1, 1 + pull))
        rented = _scale(rng, rng.choices([0, 1, 2, 3, 4], weights=[30, 35, 22, 10, 3])[0],
                        f * max(0.1, 1 - pull))

        bikes_in = min(returned, usable - bikes)
        bikes_out = min(rented, bikes)
        bikes_before = bikes
        bikes += bikes_in - bikes_out

        flow_rows.append({
            "system_id": system_id,
            "station_id": station_id,
            "window_start": window_start,
            "window_end": window_end,
            "bikes_in": bikes_in,
            "bikes_out": bikes_out,
            "net_flow": bikes_in - bikes_out,
            "avg_bikes": round((bikes_before + bikes) / 2.0, 2),
            # ingested_at: DEFAULT now64(3)
        })

        # Lịch sử status_rt: bỏ phút cuối (đó là snapshot cuối, ghi riêng).
        if minute < minutes - 1 and (minute + 1) % status_step == 0:
            history.append(status_row(window_end, bikes))

        if with_alerts:
            state = _classify(bikes, usable, low_ratio)
            cur_type = _LEVELS[state][1]
            # đóng alert không còn đúng với trạng thái hiện tại
            still_open = []
            for al in open_alerts:
                if al["alert_type"] != cur_type:
                    al["resolved"] = 1
                else:
                    still_open.append(al)
            open_alerts = still_open
            # chỉ phát khi chuyển sang mức xấu hơn
            if _LEVELS[state][0] > _LEVELS[prev_state][0]:
                alert = _make_alert(rng, system_id, station_id, state, bikes,
                                    usable, window_end, low_ratio)
                alert_rows.append(alert)
                open_alerts.append(alert)
            prev_state = state

    return flow_rows, history, status_row(now, bikes), alert_rows


# ====================================================================
# Lõi: mô phỏng nhiều trạm / nhiều system
# ====================================================================
def _simulate_systems(
    stations: Sequence[Station],
    *,
    minutes: int,
    seed: int,
    now: datetime,
    with_alerts: bool,
    low_ratio: float,
    status_step: int,
    with_weather: bool,
    forecast_hours: int,
) -> MockData:
    data = MockData()
    first_window = now - timedelta(minutes=minutes)
    start = first_window - timedelta(minutes=first_window.minute % 15)

    by_system: dict[str, list[Station]] = {}
    skipped = 0
    for s in stations:
        cap = s.get("capacity")
        if not cap or cap < 1:
            skipped += 1
            continue
        by_system.setdefault(s["system_id"], []).append(s)
    if skipped:
        log.warning("Bỏ qua %d/%d trạm có capacity NULL/0",
                    skipped, len(stations))
    if not by_system:
        raise ValueError(
            "Mọi trạm đều có capacity NULL/0, không có gì để sinh.")

    for system_id, group in by_system.items():
        lat_c = sum(float(s["lat"]) for s in group) / len(group)
        lon_c = sum(float(s["lon"]) for s in group) / len(group)
        tz = round(lon_c / 15)

        timeline = None
        if with_weather:
            wrng = random.Random(f"{seed}:weather:{system_id}")
            weather_rows, timeline = _generate_weather(
                wrng, system_id, lat_c, lon_c, start, now, forecast_hours)
            data.weather.extend(weather_rows)
        demand = _demand_curve(timeline, start, first_window, minutes, tz)

        for s in group:
            srng = random.Random(f"{seed}:{system_id}:{s['station_id']}")
            f_rows, hist, final, a_rows = _simulate_station(
                srng, system_id, s["station_id"], int(s["capacity"]),
                first_window, minutes, now, demand,
                is_charging=bool(s.get("is_charging")),
                status_step=status_step,
                with_alerts=with_alerts, low_ratio=low_ratio)
            data.flow.extend(f_rows)
            data.status_history.extend(hist)
            data.status_latest.append(final)
            data.alerts.extend(a_rows)
    return data


# ====================================================================
# Chế độ 1: sinh toàn bộ (kèm dim_station)
# ====================================================================
def generate_mock_dashboard_data(
    station_count: int = 50,
    minutes: int = 60,
    seed: int = 42,
    system_id: str = "mock_nyc",
    now: datetime | None = None,
    with_alerts: bool = True,
    with_weather: bool = True,
    low_ratio: float = DEFAULT_LOW_RATIO,
    status_step: int = 1,
    forecast_hours: int = DEFAULT_FORECAST_HOURS,
) -> MockData:
    """Sinh dữ liệu cho cả 6 bảng, khớp schema 010_realtime.sql (không đụng DB).

    - station_id giữ prefix system_id (vd: mock_nyc_001); join theo
      (system_id, station_id).
    - weather: location_id = system_id.
    """
    if station_count < 1:
        raise ValueError("station_count phải >= 1")
    _check_args(minutes, status_step, forecast_hours, low_ratio)

    rng = random.Random(f"{seed}:dim:{system_id}")
    now = _norm_now(now)

    grid_cols = min(10, station_count)
    grid_rows = (station_count + grid_cols - 1) // grid_cols

    dim_rows: list[MockRow] = []
    stations: list[Station] = []
    for i in range(station_count):
        r, c = divmod(i, grid_cols)
        lat = (LAT_ORIGIN + r * (GRID_SPAN / max(grid_rows - 1, 1))
               + rng.uniform(-JITTER, JITTER))
        lon = (LON_ORIGIN + c * (GRID_SPAN / max(grid_cols - 1, 1))
               + rng.uniform(-JITTER, JITTER))
        station_id = f"{system_id}_{i + 1:03d}"
        capacity = rng.randint(20, 40)
        is_charging = 1 if rng.random() < 0.15 else 0
        region = REGIONS[min(len(REGIONS) - 1, r * len(REGIONS) // grid_rows)]

        dim_rows.append({
            "system_id": system_id,
            "station_id": station_id,
            "name": f"Mock Manhattan Station {i + 1:03d}",
            "short_name": None if rng.random() < 0.05 else f"M{i + 1:03d}",
            "lat": round(lat, 6),
            "lon": round(lon, 6),
            "capacity": capacity,
            "region_id": region,
            "is_charging": is_charging,
            "extra": json.dumps({
                "mock": True,
                "has_kiosk": rng.random() < 0.8,
                "rental_methods": ["KEY", "CREDITCARD"],
            }),
            "source_version": SOURCE_VERSION,
            "updated_at": now,
        })
        stations.append({"system_id": system_id, "station_id": station_id,
                         "capacity": capacity, "lat": lat, "lon": lon,
                         "is_charging": is_charging})

    data = _simulate_systems(
        stations, minutes=minutes, seed=seed, now=now,
        with_alerts=with_alerts, low_ratio=low_ratio, status_step=status_step,
        with_weather=with_weather, forecast_hours=forecast_hours)
    data.dim = dim_rows
    return data


# ====================================================================
# Ghi vào ClickHouse
# ====================================================================
def _insert_rows(writer: ClickHouseWriter, table: str,
                 rows: Sequence[MockRow]) -> int:
    """Insert trực tiếp (cho bảng chưa có hàm write_* riêng). Cột vắng mặt
    trong dict sẽ dùng DEFAULT của DDL."""
    if not rows:
        return 0
    cols = list(rows[0].keys())
    for i in range(0, len(rows), INSERT_CHUNK):
        chunk = [[r[c] for c in cols] for r in rows[i:i + INSERT_CHUNK]]
        writer._retry(lambda ch=chunk: writer.client.insert(
            table, ch, column_names=cols))
    return len(rows)


def _write_data(writer: ClickHouseWriter, data: MockData,
                with_alerts: bool, with_weather: bool) -> dict[str, int]:
    result: dict[str, int] = {}
    if data.dim:
        result["dim_station"] = writer.write_dim_station(data.dim)

    # Lịch sử -> station_status_rt; snapshot cuối -> cả status_rt + station_latest.
    n_hist = _insert_rows(writer, "station_status_rt", data.status_history)
    n_last = writer.write_station_status(data.status_latest)
    result["station_status_rt"] = n_hist + n_last
    result["station_latest"] = n_last

    result["station_flow_rt"] = writer.write_flow_rt(data.flow)
    if with_alerts:
        result["alerts_log"] = writer.write_alerts(data.alerts)
    if with_weather:
        result["weather_latest"] = _insert_rows(
            writer, "weather_latest", data.weather)
    return result


def seed_mock_dashboard_data(
    writer: ClickHouseWriter,
    station_count: int = 50,
    minutes: int = 60,
    seed: int = 42,
    system_id: str = "mock_nyc",
    with_alerts: bool = True,
    with_weather: bool = True,
    low_ratio: float = DEFAULT_LOW_RATIO,
    status_step: int = 1,
    forecast_hours: int = DEFAULT_FORECAST_HOURS,
) -> dict[str, int]:
    """Sinh và ghi mock data cho cả 6 bảng."""
    data = generate_mock_dashboard_data(
        station_count=station_count, minutes=minutes, seed=seed,
        system_id=system_id, with_alerts=with_alerts, with_weather=with_weather,
        low_ratio=low_ratio, status_step=status_step,
        forecast_hours=forecast_hours)
    result = _write_data(writer, data, with_alerts, with_weather)
    log.info("Đã seed mock dashboard data: %s", result)
    return result


# ====================================================================
# Chế độ 2: bám theo dim_station đã có
# ====================================================================
def load_stations_from_dim(
    writer: ClickHouseWriter,
    system_id: str | None = None,
    limit: int | None = None,
) -> list[Station]:
    """Đọc danh sách trạm từ dim_station (FINAL vì là ReplacingMergeTree)."""
    sql = ("SELECT system_id, station_id, capacity, lat, lon, is_charging "
           "FROM dim_station FINAL")
    params: dict[str, Any] = {}
    if system_id:
        sql += " WHERE system_id = {sid:String}"
        params["sid"] = system_id
    sql += " ORDER BY system_id, station_id"
    if limit:
        sql += f" LIMIT {int(limit)}"

    res = writer._retry(lambda: writer.client.query(sql, parameters=params))
    return [dict(zip(res.column_names, row)) for row in res.result_rows]


def generate_mock_from_stations(
    stations: Sequence[Station],
    minutes: int = 60,
    seed: int = 42,
    now: datetime | None = None,
    with_alerts: bool = True,
    with_weather: bool = True,
    low_ratio: float = DEFAULT_LOW_RATIO,
    status_step: int = 1,
    forecast_hours: int = DEFAULT_FORECAST_HOURS,
) -> MockData:
    """Sinh status + flow + alerts + weather cho danh sách trạm có sẵn.

    Giữ nguyên system_id / station_id từ dim nên join khớp 100%. `dim` trong
    kết quả rỗng (không ghi lại dim_station).
    """
    if not stations:
        raise ValueError("Danh sách trạm rỗng")
    _check_args(minutes, status_step, forecast_hours, low_ratio)
    return _simulate_systems(
        stations, minutes=minutes, seed=seed, now=_norm_now(now),
        with_alerts=with_alerts, low_ratio=low_ratio, status_step=status_step,
        with_weather=with_weather, forecast_hours=forecast_hours)


def seed_mock_from_dim(
    writer: ClickHouseWriter,
    minutes: int = 60,
    seed: int = 42,
    system_id: str | None = None,
    limit: int | None = None,
    with_alerts: bool = True,
    with_weather: bool = True,
    low_ratio: float = DEFAULT_LOW_RATIO,
    status_step: int = 1,
    forecast_hours: int = DEFAULT_FORECAST_HOURS,
) -> dict[str, int]:
    """Đọc dim_station rồi ghi status + flow + alerts + weather khớp các trạm đó."""
    stations = load_stations_from_dim(writer, system_id=system_id, limit=limit)
    if not stations:
        raise ValueError(
            "dim_station không có trạm nào"
            + (f" cho system_id='{system_id}'" if system_id else "")
            + ". Kiểm tra lại dữ liệu dim hoặc system_id.")
    log.info("Đọc %d trạm từ dim_station", len(stations))

    data = generate_mock_from_stations(
        stations, minutes=minutes, seed=seed, with_alerts=with_alerts,
        with_weather=with_weather, low_ratio=low_ratio,
        status_step=status_step, forecast_hours=forecast_hours)
    result = _write_data(writer, data, with_alerts, with_weather)
    log.info("Đã seed mock theo dim_station: %s", result)
    return result


# ====================================================================
# CLI
# ====================================================================
def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Sinh mock data bike-share (6 bảng realtime) và ghi vào ClickHouse.")
    p.add_argument("--apply-ddl", nargs="?", const=DEFAULT_DDL_DIR, metavar="DDL_DIR",
                   help=f"Áp DDL trước khi seed (mặc định thư mục: {DEFAULT_DDL_DIR}).")
    p.add_argument("--seed-mock", action="store_true",
                   help="Seed mock data (mặc định nếu không có --apply-ddl).")
    p.add_argument("--from-dim", action="store_true",
                   help="Đọc dim_station có sẵn, không sinh lại dim.")
    p.add_argument("--stations", type=int, default=None,
                   help="Tổng hợp: số trạm (mặc định 50). Với --from-dim: giới hạn số trạm.")
    p.add_argument("--minutes", type=int, default=60,
                   help="Số phút lịch sử (mặc định: 60).")
    p.add_argument("--seed", type=int, default=42,
                   help="Random seed (mặc định: 42).")
    p.add_argument("--system-id", default=None,
                   help="Tổng hợp: system_id mock (mặc định mock_nyc). "
                        "Với --from-dim: lọc theo system_id.")
    p.add_argument("--no-alerts", action="store_true",
                   help="Không sinh alerts_log.")
    p.add_argument("--no-weather", action="store_true",
                   help="Không sinh weather_latest.")
    p.add_argument("--status-step", type=int, default=1,
                   help="Khoảng cách (phút) giữa các dòng lịch sử station_status_rt "
                        "(mặc định: 1).")
    p.add_argument("--forecast-hours", type=int, default=DEFAULT_FORECAST_HOURS,
                   help=f"Số giờ dự báo thời tiết (mặc định: {DEFAULT_FORECAST_HOURS}).")
    p.add_argument("--low-ratio", type=float, default=DEFAULT_LOW_RATIO,
                   help=f"Ngưỡng LOW_BIKES/LOW_DOCKS theo tỉ lệ (mặc định: {DEFAULT_LOW_RATIO}).")
    return p.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = _parse_args()

    if args.apply_ddl is not None:
        apply_ddl(args.apply_ddl)

    if not (args.seed_mock or args.from_dim or args.apply_ddl is None):
        return

    common = dict(
        minutes=args.minutes, seed=args.seed,
        with_alerts=not args.no_alerts, with_weather=not args.no_weather,
        low_ratio=args.low_ratio, status_step=args.status_step,
        forecast_hours=args.forecast_hours)

    with ClickHouseWriter() as writer:
        if args.from_dim:
            seed_mock_from_dim(writer, system_id=args.system_id,
                               limit=args.stations, **common)
        else:
            seed_mock_dashboard_data(
                writer, station_count=args.stations or 50,
                system_id=args.system_id or "mock_nyc", **common)


if __name__ == "__main__":
    main()
