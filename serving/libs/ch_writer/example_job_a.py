"""Smoke test: ghi dữ liệu mẫu rồi đọc lại. Chạy từ thư mục libs/."""
from datetime import datetime, timezone
from ch_writer import ClickHouseWriter

now = datetime.now(timezone.utc)
with ClickHouseWriter() as w:
    w.write_dim_station([{"station_id": "S001", "name": "Hoan Kiem", "lat": 21.028,
                          "lon": 105.852, "capacity": 20}])
    w.write_station_status([{"station_id": "S001", "ts": now, "bikes_available": 3,
                             "docks_available": 17, "capacity": 20, "occupancy_rate": 0.15}])
    w.write_alerts([{"ts": now, "station_id": "S001", "alert_type": "LOW_BIKES",
                     "severity": "WARN", "metric_value": 3, "threshold": 5,
                     "message": "Sắp hết xe"}])
    w.set_watermark("flow_batch", now, rows_processed=1)
    print(w.client.query("SELECT * FROM station_latest FINAL").result_rows)
    print(w.get_watermark("flow_batch"))
