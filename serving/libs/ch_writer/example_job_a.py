"""Smoke test theo schema gbfs: ghi dữ liệu mẫu rồi đọc lại. Chạy từ thư mục libs/.

    cd libs && python -m ch_writer.example_job_a

Dữ liệu mẫu lấy từ bản ghi thật trong out/ (biketown_pdx, weather montreal).
Mốc thời gian cố định -> chạy lại nhiều lần không sinh thêm dòng.
"""
from datetime import datetime, timezone

from ch_writer import ClickHouseWriter


def utc(*a) -> datetime:
    return datetime(*a, tzinfo=timezone.utc)


with ClickHouseWriter() as w:
    # StationInfo -> dim_station  (out/gbfs.station_information.biketown_pdx.jsonl)
    w.write_dim_station([{
        "system_id": "biketown_pdx", "station_id": "1440914087503119616",
        "name": "SW 4th at College", "short_name": None,
        "lat": 45.50936198, "lon": -122.6812121, "capacity": 18,
        "region_id": None, "is_charging": False,
        "extra": {"address": "Portland State University, Portland, Oregon, 97258, United States of America",
                  "rental_uris": {"android": "https://pdx.lft.to/lastmile_qr_scan",
                                  "ios": "https://pdx.lft.to/lastmile_qr_scan"}},
        "source_version": "2.3", "updated_at": utc(2026, 11, 12, 19, 54, 1, 613902),
    }])

    # StationStatus -> station_status_rt + station_latest
    # (out/gbfs.station_status.biketown_pdx.jsonl; event_ts bỏ trống -> ClickHouse tự tính
    #  = coalesce(last_reported, snapshot_ts))
    w.write_station_status([{
        "system_id": "biketown_pdx", "station_id": "1440913838395016060",
        "snapshot_ts": utc(2026, 11, 12, 19, 52, 24),
        "last_reported": utc(2026, 1, 22, 20, 33, 34),
        "num_bikes_available": 3, "num_bikes_disabled": 0,
        "num_docks_available": 10, "num_docks_disabled": 0,
        "num_ebikes_available": 3,
        "is_installed": True, "is_renting": True, "is_returning": True,
        "vehicle_types_available": [
            {"vehicle_type_id": "1", "count": 0},
            {"vehicle_type_id": "2", "count": 3},
        ],
        # thư viện tự bỏ: bảng status không có cột extra
        "extra": {"num_scooters_available": 4},
        "source_version": "2.3", "ingest_ts": utc(2026, 11, 12, 19, 54, 0, 795693),
    }])

    # WeatherMessage -> weather_latest  (out/weather.open_meteo.montreal.jsonl)
    # `interval` được thư viện đổi thành interval_seconds
    w.write_weather([{
        "location_id": "montreal",
        "latitude": 45.49215, "longitude": -73.56103,
        # DateTime('UTC'): không cần tzinfo
        "time": datetime(2026, 11, 9, 15, 0, 0),
        "fetch_time": utc(2026, 11, 9, 16, 1, 39, 149531),
        "data_type": "observed", "source": "open_meteo", "interval": 900,
        "temperature_2m": 8.3, "relative_humidity_2m": 90,
        "precipitation": 0.0, "rain": 0.0, "snowfall": 0.0,
        "wind_speed_10m": 9.7, "wind_direction_10m": 336, "wind_gusts_10m": 22.0,
        "visibility": 14900.0, "weather_code": 3,
    }])

    q = w.client.query
    print("dim_station   :", q("SELECT system_id, station_id, name, capacity "
                               "FROM dim_station FINAL").result_rows)
    print("station_latest:", q("SELECT system_id, station_id, event_ts, num_bikes_available, "
                               "vehicle_types_available FROM station_latest FINAL").result_rows)
    print("status_rt     :", q(
        "SELECT count() FROM station_status_rt FINAL").result_rows)
    print("weather       :", q("SELECT location_id, time, data_type, temperature_2m "
                               "FROM weather_latest FINAL").result_rows)
