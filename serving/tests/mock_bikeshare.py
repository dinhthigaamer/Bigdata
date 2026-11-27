"""Sinh mock data cho dashboard Grafana (bike-share real-time).

Dùng lại ClickHouseWriter / apply_ddl từ ch_writer.py, file này chỉ chứa
logic sinh dữ liệu + CLI.

Hai chế độ:
  1) Tổng hợp toàn bộ (mặc định): sinh cả dim_station + status + flow.
  2) --from-dim: ĐỌC dim_station đã có trong ClickHouse, chỉ sinh
     status + flow khớp với các trạm đó (system_id, station_id, capacity).
     Trạm có capacity NULL/0 bị BỎ QUA để status luôn khớp capacity trong dim.

    python generate_mock_bikeshare.py                       # chế độ 1
    python generate_mock_bikeshare.py --stations 100 --minutes 120
    python generate_mock_bikeshare.py --from-dim            # chế độ 2, mọi system
    python generate_mock_bikeshare.py --from-dim --system-id citi-bike-nyc --stations 20
    python generate_mock_bikeshare.py --apply-ddl           # áp DDL rồi seed

Dùng như thư viện:
    from ch_writer import ClickHouseWriter
    from generate_mock_bikeshare import seed_mock_from_dim
    with ClickHouseWriter() as w:
        seed_mock_from_dim(w, minutes=30, system_id="citi-bike-nyc")
"""
from __future__ import annotations

import argparse
import logging
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Sequence

from libs.ch_writer import ClickHouseWriter, apply_ddl

log = logging.getLogger("generate_mock_bikeshare")

MockRow = dict[str, Any]
Station = dict[str, Any]  # system_id, station_id, capacity, lat, lon

SOURCE_VERSION = "2.3"
DEFAULT_DDL_DIR = "clickhouse/init"
FLOW_TTL_DAYS = 30  # khớp TTL của station_flow_rt trong 010_realtime.sql

# Khu vực Manhattan, New York: góc dưới-trái và kích thước lưới (độ).
LAT_ORIGIN, LON_ORIGIN = 40.705, -74.015
GRID_SPAN = 0.070
JITTER = 0.0012


def _check_args(minutes: int) -> None:
    if minutes < 1:
        raise ValueError("minutes phải >= 1")
    if minutes > FLOW_TTL_DAYS * 24 * 60:
        log.warning("minutes=%d vượt TTL %d ngày của station_flow_rt, "
                    "phần cũ sẽ bị xóa khi merge", minutes, FLOW_TTL_DAYS)


def _simulate_station(
    rng: random.Random,
    system_id: str,
    station_id: str,
    capacity: int,
    first_window: datetime,
    minutes: int,
    now: datetime,
) -> tuple[list[MockRow], MockRow]:
    """Mô phỏng 1 trạm: trả về (flow_rows theo phút, 1 dòng status cuối).

    Số xe luôn nằm trong [0, capacity]; status cuối khớp số xe sau phút flow cuối.
    """
    flow_rows: list[MockRow] = []

    lo = min(max(1, capacity // 4), capacity)
    hi = max(lo, min(capacity, capacity * 3 // 4))
    bikes = rng.randint(lo, hi)

    for minute in range(minutes):
        window_start = first_window + timedelta(minutes=minute)
        returned = rng.choices([0, 1, 2, 3, 4], weights=[35, 35, 20, 8, 2])[0]
        rented = rng.choices([0, 1, 2, 3, 4], weights=[30, 35, 22, 10, 3])[0]

        bikes_in = min(returned, capacity - bikes)
        bikes_out = min(rented, bikes)
        bikes_before = bikes
        bikes += bikes_in - bikes_out

        flow_rows.append({
            "system_id": system_id,
            "station_id": station_id,
            "window_start": window_start,
            "window_end": window_start + timedelta(minutes=1),
            "bikes_in": bikes_in,
            "bikes_out": bikes_out,
            "net_flow": bikes_in - bikes_out,
            "avg_bikes": round((bikes_before + bikes) / 2.0, 2),
            # ingested_at có DEFAULT trong DDL -> để ClickHouse tự điền.
        })

    ebikes = min(bikes, round(bikes * rng.uniform(0.15, 0.40)))
    status_row = {
        "system_id": system_id,
        "station_id": station_id,
        "snapshot_ts": now,
        "last_reported": now,
        "num_bikes_available": bikes,
        "num_bikes_disabled": 0,
        "num_docks_available": capacity - bikes,
        "num_docks_disabled": 0,
        "num_ebikes_available": ebikes,
        "is_installed": 1,
        "is_renting": 1,
        "is_returning": 1,
        "vehicle_types_available": {
            "classic": bikes - ebikes,
            "electric_bike": ebikes,
        },
        "source_version": SOURCE_VERSION,
        "ingest_ts": now,
        # event_ts được DDL tự tính từ last_reported/snapshot_ts.
    }
    return flow_rows, status_row


# ---------- chế độ 1: sinh toàn bộ (dim + status + flow) ----------
def generate_mock_dashboard_data(
    station_count: int = 50,
    minutes: int = 60,
    seed: int = 42,
    system_id: str = "mock_nyc",
    now: datetime | None = None,
) -> tuple[list[MockRow], list[MockRow], list[MockRow]]:
    """Sinh dữ liệu khớp schema trong 010_realtime.sql (không đụng DB).

    Returns:
        dim_rows:    -> dim_station
        status_rows: -> station_status_rt + station_latest
        flow_rows:   -> station_flow_rt

    Lưu ý:
    - Cả dim_station, station_status_rt/station_latest và station_flow_rt đều
      có system_id, nên join theo (system_id, station_id).
    - station_id giữ prefix system_id (vd: mock_nyc_001).
    - Các cột có DEFAULT trong DDL (event_ts, ingested_at) không điền ở đây.
    - Cả 3 bảng fact là ReplacingMergeTree: chạy lại với cửa sổ chồng lấn sẽ
      được dedupe sau merge; truy vấn dashboard nên dùng FINAL hoặc argMax.
    """
    if station_count < 1:
        raise ValueError("station_count phải >= 1")
    _check_args(minutes)

    rng = random.Random(seed)
    now = (now or datetime.now(timezone.utc)).replace(second=0, microsecond=0)
    first_window = now - timedelta(minutes=minutes)

    dim_rows: list[MockRow] = []
    status_rows: list[MockRow] = []
    flow_rows: list[MockRow] = []

    grid_cols = min(10, station_count)
    grid_rows = (station_count + grid_cols - 1) // grid_cols

    for i in range(station_count):
        r, c = divmod(i, grid_cols)
        lat = (LAT_ORIGIN + r * (GRID_SPAN / max(grid_rows - 1, 1))
               + rng.uniform(-JITTER, JITTER))
        lon = (LON_ORIGIN + c * (GRID_SPAN / max(grid_cols - 1, 1))
               + rng.uniform(-JITTER, JITTER))

        station_id = f"{system_id}_{i + 1:03d}"
        capacity = rng.randint(20, 40)

        dim_rows.append({
            "system_id": system_id,
            "station_id": station_id,
            "name": f"Mock Manhattan Station {i + 1:03d}",
            "short_name": f"M{i + 1:03d}",
            "lat": round(lat, 6),
            "lon": round(lon, 6),
            "capacity": capacity,
            "region_id": "manhattan",
            "is_charging": 0,
            "extra": '{"mock":true}',
            "source_version": SOURCE_VERSION,
            "updated_at": now,
        })

        f_rows, s_row = _simulate_station(
            rng, system_id, station_id, capacity, first_window, minutes, now)
        flow_rows.extend(f_rows)
        status_rows.append(s_row)

    return dim_rows, status_rows, flow_rows


def seed_mock_dashboard_data(
    writer: ClickHouseWriter,
    station_count: int = 50,
    minutes: int = 60,
    seed: int = 42,
    system_id: str = "mock_nyc",
) -> dict[str, int]:
    """Sinh và ghi mock data vào các bảng phục vụ dashboard real-time."""
    dim_rows, status_rows, flow_rows = generate_mock_dashboard_data(
        station_count=station_count,
        minutes=minutes,
        seed=seed,
        system_id=system_id,
    )

    n_dim = writer.write_dim_station(dim_rows)
    # Ghi cùng lúc vào station_status_rt và station_latest.
    n_status = writer.write_station_status(status_rows)
    n_flow = writer.write_flow_rt(flow_rows)

    result = {
        "dim_station": n_dim,
        "station_status_rt": n_status,
        "station_latest": n_status,
        "station_flow_rt": n_flow,
    }
    log.info("Đã seed mock dashboard data: %s", result)
    return result


# ---------- chế độ 2: bám theo dim_station đã có ----------
def load_stations_from_dim(
    writer: ClickHouseWriter,
    system_id: str | None = None,
    limit: int | None = None,
) -> list[Station]:
    """Đọc danh sách trạm từ dim_station (FINAL vì là ReplacingMergeTree)."""
    sql = "SELECT system_id, station_id, capacity, lat, lon FROM dim_station FINAL"
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
) -> tuple[list[MockRow], list[MockRow]]:
    """Sinh status + flow cho danh sách trạm có sẵn (không đụng DB).

    Giữ nguyên system_id / station_id từ dim nên join (system_id, station_id)
    khớp 100%. Trạm có capacity NULL hoặc 0 bị bỏ qua, vì không có capacity
    thì không thể sinh số xe khớp dim.

    Returns:
        status_rows: -> station_status_rt + station_latest
        flow_rows:   -> station_flow_rt
    """
    if not stations:
        raise ValueError("Danh sách trạm rỗng")
    _check_args(minutes)

    rng = random.Random(seed)
    now = (now or datetime.now(timezone.utc)).replace(second=0, microsecond=0)
    first_window = now - timedelta(minutes=minutes)

    status_rows: list[MockRow] = []
    flow_rows: list[MockRow] = []
    skipped = 0

    for s in stations:
        capacity = s.get("capacity")
        if not capacity or capacity < 1:
            skipped += 1
            continue

        f_rows, s_row = _simulate_station(
            rng, s["system_id"], s["station_id"], int(capacity),
            first_window, minutes, now)
        flow_rows.extend(f_rows)
        status_rows.append(s_row)

    if skipped:
        log.warning("Bỏ qua %d/%d trạm có capacity NULL/0 trong dim_station",
                    skipped, len(stations))
    if not status_rows:
        raise ValueError(
            "Mọi trạm đều có capacity NULL/0, không có gì để sinh.")
    return status_rows, flow_rows


def seed_mock_from_dim(
    writer: ClickHouseWriter,
    minutes: int = 60,
    seed: int = 42,
    system_id: str | None = None,
    limit: int | None = None,
) -> dict[str, int]:
    """Đọc dim_station rồi ghi status + flow mock khớp với các trạm đó.

    Không ghi lại dim_station (đã có sẵn).
    """
    stations = load_stations_from_dim(writer, system_id=system_id, limit=limit)
    if not stations:
        raise ValueError(
            "dim_station không có trạm nào"
            + (f" cho system_id='{system_id}'" if system_id else "")
            + ". Kiểm tra lại dữ liệu dim hoặc system_id.")
    log.info("Đọc %d trạm từ dim_station", len(stations))

    status_rows, flow_rows = generate_mock_from_stations(
        stations, minutes=minutes, seed=seed)

    n_status = writer.write_station_status(status_rows)
    n_flow = writer.write_flow_rt(flow_rows)

    result = {
        "station_status_rt": n_status,
        "station_latest": n_status,
        "station_flow_rt": n_flow,
    }
    log.info("Đã seed mock theo dim_station: %s", result)
    return result


# ---------- CLI ----------
def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Sinh mock data bike-share và ghi vào ClickHouse qua ch_writer."
    )
    p.add_argument("--apply-ddl", nargs="?", const=DEFAULT_DDL_DIR,
                   metavar="DDL_DIR",
                   help=f"Áp DDL trước khi seed (mặc định thư mục: {DEFAULT_DDL_DIR}).")
    p.add_argument("--seed-mock", action="store_true",
                   help="Seed mock data (là hành vi mặc định nếu không có --apply-ddl).")
    p.add_argument("--from-dim", action="store_true",
                   help="Đọc dim_station có sẵn, chỉ sinh status + flow cho các trạm đó.")
    p.add_argument("--stations", type=int, default=None,
                   help="Chế độ tổng hợp: số trạm (mặc định 50). "
                        "Với --from-dim: giới hạn số trạm (mặc định: tất cả).")
    p.add_argument("--minutes", type=int, default=60,
                   help="Số phút lịch sử flow (mặc định: 60).")
    p.add_argument("--seed", type=int, default=42,
                   help="Random seed (mặc định: 42).")
    p.add_argument("--system-id", default=None,
                   help="Chế độ tổng hợp: system_id của mock (mặc định mock_nyc). "
                        "Với --from-dim: lọc theo system_id (mặc định: tất cả).")
    return p.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = _parse_args()

    if args.apply_ddl is not None:
        apply_ddl(args.apply_ddl)

    if not (args.seed_mock or args.from_dim or args.apply_ddl is None):
        return

    with ClickHouseWriter() as writer:
        if args.from_dim:
            seed_mock_from_dim(
                writer,
                minutes=args.minutes,
                seed=args.seed,
                system_id=args.system_id,
                limit=args.stations,
            )
        else:
            seed_mock_dashboard_data(
                writer,
                station_count=args.stations or 50,
                minutes=args.minutes,
                seed=args.seed,
                system_id=args.system_id or "mock_nyc",
            )


if __name__ == "__main__":
    main()
