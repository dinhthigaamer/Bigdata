"""Thư viện ghi ClickHouse dùng chung (Job A streaming, Job B batch, ML).

    from ch_writer import ClickHouseWriter
    with ClickHouseWriter() as w:
        w.write_station_status(rows)   # -> station_status_rt + station_latest
        w.write_flow_rt(flow_rows)
        w.write_alerts(alerts)

Spark Structured Streaming:
    df.writeStream.foreachBatch(w.foreach_batch("station_flow_rt")).start()
"""
from __future__ import annotations

import logging
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import clickhouse_connect

log = logging.getLogger("ch_writer")
Row = Mapping[str, Any]


@dataclass
class ClickHouseConfig:
    host: str = "localhost"
    port: int = 8123
    username: str = "bigdata"
    password: str = "bigdata123"
    database: str = "bigdata"

    @classmethod
    def from_env(cls) -> "ClickHouseConfig":
        return cls(
            host=os.getenv("CH_HOST", "localhost"),
            port=int(os.getenv("CH_PORT", "8123")),
            username=os.getenv("CH_USER", "bigdata"),
            password=os.getenv("CH_PASSWORD", "bigdata123"),
            database=os.getenv("CH_DATABASE", "bigdata"),
        )


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class ClickHouseWriter:
    def __init__(self, config: ClickHouseConfig | None = None, max_retries: int = 5,
                 backoff_s: float = 1.0, buffer_size: int = 5000,
                 flush_interval_s: float = 5.0):
        self.cfg = config or ClickHouseConfig.from_env()
        self.max_retries = max_retries
        self.backoff_s = backoff_s
        self.buffer_size = buffer_size
        self.flush_interval_s = flush_interval_s
        self._client = None
        self._columns: dict[str, list[str]] = {}
        self._buffers: dict[str, list[Row]] = {}
        self._last_flush = time.monotonic()

    # ---------- kết nối ----------
    @property
    def client(self):
        if self._client is None:
            self._client = clickhouse_connect.get_client(
                host=self.cfg.host, port=self.cfg.port,
                username=self.cfg.username, password=self.cfg.password,
                database=self.cfg.database,
            )
        return self._client

    def close(self) -> None:
        try:
            self.flush()
        finally:
            if self._client is not None:
                self._client.close()
                self._client = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def _retry(self, fn, *args, **kwargs):
        for attempt in range(1, self.max_retries + 1):
            try:
                return fn(*args, **kwargs)
            except Exception as e:  # noqa: BLE001
                if attempt == self.max_retries:
                    log.error("ClickHouse lỗi sau %d lần thử: %s", attempt, e)
                    raise
                wait = self.backoff_s * 2 ** (attempt - 1)
                log.warning("ClickHouse lỗi (%s), thử lại sau %.1fs", e, wait)
                self._client = None
                time.sleep(wait)

    # ---------- ghi cơ bản ----------
    def table_columns(self, table: str) -> list[str]:
        if table not in self._columns:
            res = self._retry(lambda: self.client.query(f"DESCRIBE TABLE {table}"))
            self._columns[table] = [r[0] for r in res.result_rows]
        return self._columns[table]

    def insert_rows(self, table: str, rows: Sequence[Row]) -> int:
        """Ghi list[dict]. Cột lạ -> lỗi; cột thiếu -> dùng DEFAULT của bảng."""
        if not rows:
            return 0
        valid = set(self.table_columns(table))
        cols = list(rows[0].keys())
        unknown = [c for c in cols if c not in valid]
        if unknown:
            raise ValueError(f"Bảng {table} không có cột: {unknown}")
        data = [[r.get(c) for c in cols] for r in rows]
        self._retry(lambda: self.client.insert(table, data, column_names=cols))
        log.debug("insert %d dòng vào %s", len(rows), table)
        return len(rows)

    def insert_df(self, table: str, df) -> int:
        if df is None or len(df) == 0:
            return 0
        self._retry(lambda: self.client.insert_df(table, df))
        return len(df)

    # ---------- buffer ----------
    def add(self, table: str, row: Row) -> None:
        self._buffers.setdefault(table, []).append(row)
        if (len(self._buffers[table]) >= self.buffer_size
                or time.monotonic() - self._last_flush >= self.flush_interval_s):
            self.flush()

    def flush(self) -> None:
        for table, rows in list(self._buffers.items()):
            if rows:
                self.insert_rows(table, rows)
                self._buffers[table] = []
        self._last_flush = time.monotonic()

    # ---------- Spark ----------
    def foreach_batch(self, table: str):
        def _write(batch_df, batch_id: int):
            n = self.insert_df(table, batch_df.toPandas())
            log.info("batch %s -> %s: %d dòng", batch_id, table, n)
        return _write

    # ---------- hàm tiện dụng theo bảng ----------
    def write_dim_station(self, rows: Iterable[Row]) -> int:
        return self.insert_rows("dim_station", list(rows))

    def write_station_status(self, rows: Iterable[Row]) -> int:
        """Ghi lịch sử (station_status_rt) và trạng thái mới nhất (station_latest)."""
        rows = list(rows)
        n = self.insert_rows("station_status_rt", rows)
        self.insert_rows("station_latest", rows)
        return n

    def write_flow_rt(self, rows: Iterable[Row]) -> int:
        return self.insert_rows("station_flow_rt", list(rows))

    def write_alerts(self, rows: Iterable[Row]) -> int:
        return self.insert_rows("alerts_log", list(rows))

    def write_weather(self, rows: Iterable[Row]) -> int:
        return self.insert_rows("weather_latest", list(rows))

    def write_flow_batch(self, rows: Iterable[Row]) -> int:
        return self.insert_rows("station_flow_batch", list(rows))

    def write_forecast(self, rows: Iterable[Row]) -> int:
        return self.insert_rows("forecast_rt", list(rows))

    def write_clusters(self, rows: Iterable[Row]) -> int:
        return self.insert_rows("station_cluster_batch", list(rows))

    def write_metrics(self, rows: Iterable[Row]) -> int:
        return self.insert_rows("model_metrics", list(rows))

    # ---------- watermark ----------
    def get_watermark(self, job_name: str) -> datetime | None:
        res = self._retry(lambda: self.client.query(
            "SELECT watermark_ts FROM batch_watermark FINAL WHERE job_name = {j:String}",
            parameters={"j": job_name}))
        return res.result_rows[0][0] if res.result_rows else None

    def set_watermark(self, job_name: str, watermark_ts: datetime,
                      rows_processed: int = 0, status: str = "OK",
                      run_id: str | None = None) -> None:
        self.insert_rows("batch_watermark", [{
            "job_name": job_name, "watermark_ts": watermark_ts,
            "last_run_id": run_id or uuid.uuid4().hex, "status": status,
            "rows_processed": rows_processed, "updated_at": _utcnow(),
        }])


def apply_ddl(ddl_dir: str | Path, config: ClickHouseConfig | None = None) -> None:
    """Áp DDL thủ công (khi volume đã tồn tại, init script của Docker không chạy lại)."""
    cfg = config or ClickHouseConfig.from_env()
    client = clickhouse_connect.get_client(
        host=cfg.host, port=cfg.port, username=cfg.username, password=cfg.password)
    for f in sorted(Path(ddl_dir).glob("*.sql")):
        for stmt in f.read_text(encoding="utf-8").split(";"):
            sql = "\n".join(l for l in stmt.splitlines()
                            if not l.strip().startswith("--")).strip()
            if sql:
                client.command(sql)
        log.info("đã áp %s", f.name)


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    apply_ddl(sys.argv[1] if len(sys.argv) > 1 else "clickhouse/init")
