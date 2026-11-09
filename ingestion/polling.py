"""
Prototype polling GBFS (Citi Bike NYC, Bixi Montréal, ...) và thời tiết Open-Meteo.

GBFS: mỗi (hệ thống, feed) được poll theo `ttl` mà chính feed báo về, bỏ qua snapshot
trùng `last_updated`. Thời tiết: mỗi địa điểm được poll 15 phút một lần.
Mọi task tự backoff khi lỗi và đẩy envelope đã chuẩn hóa ra sink.

Cài đặt:   pip install requests "pydantic>=2"      (thêm: confluent-kafka nếu dùng Kafka)
Chạy thử:  python polling.py --once                 # poll 1 vòng, ghi JSONL vào ./out
Chạy liên tục (file):  python polling.py
Chạy với Kafka:        python polling.py --sink kafka --kafka localhost:9092
"""
import argparse
import logging
import signal
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import requests

from common.envelope import StationInfoEnvelope, StationStatusEnvelope, WeatherEnvelope
from common.schemas.station_info import normalize_information
from common.schemas.station_status import normalize_status
from common.schemas.weather import VARIABLES, parse_weather_response

log = logging.getLogger("gbfs.polling")

SYSTEMS = [
    {
        "url": "https://gbfs.citibikenyc.com/gbfs/2.3/gbfs.json",
        "version": "2.3",
        "system_id": "citi-bike-nyc",
    },
    {
        "url": "https://gbfs.velobixi.com/gbfs/2-2/gbfs.json",
        "version": "2.2",
        "system_id": "velobixi",
    },
]

# feed -> (class envelope, hàm chuẩn hóa)
FEEDS = {
    "station_status": (StationStatusEnvelope, normalize_status),
    "station_information": (StationInfoEnvelope, normalize_information),
}

# Địa điểm lấy thời tiết (location_id khớp với hệ thống GBFS cùng thành phố)
LOCATIONS = [
    {"location_id": "nyc", "latitude": 40.7128, "longitude": -74.0060},
    {"location_id": "montreal", "latitude": 45.5017, "longitude": -73.5673},
]

WEATHER_URL = "https://api.open-meteo.com/v1/forecast"
WEATHER_INTERVAL = 15 * 60     # giây; dữ liệu minutely_15 đổi mỗi 15 phút
WEATHER_PARAMS = {
    "current": "temperature_2m",           # cần để có current.time, current.interval
    "minutely_15": ",".join(VARIABLES),
    "past_minutely_15": 4,                 # 1 giờ quá khứ
    "forecast_minutely_15": 16,            # 4 giờ dự báo
    "timezone": "GMT",
}

MIN_INTERVAL = 5       # giây, không poll dày hơn mức này
DEFAULT_TTL = 60       # dùng khi feed không báo ttl
UNCHANGED_RETRY = 5    # snapshot chưa đổi -> hỏi lại sau ngần này giây
MAX_BACKOFF = 300
HTTP_TIMEOUT = 10


# ---------------------------------------------------------------- HTTP / GBFS
def get_json(session: requests.Session, url: str, params: Optional[dict] = None) -> dict:
    r = session.get(url, params=params, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.json()


def find_feeds(index: dict) -> dict[str, str]:
    """{tên_feed: url}. Hỗ trợ GBFS v2 (data -> en -> feeds) và v3 (data -> feeds)."""
    data = index["data"]
    if "feeds" in data:
        feeds = data["feeds"]
    else:
        feeds = (data.get("en") or next(iter(data.values())))["feeds"]
    return {f["name"]: f["url"] for f in feeds}


# ---------------------------------------------------------------------- Sinks
class JsonlSink:
    """Nối (append) mỗi bản ghi thành một dòng JSON: out/<topic>.<system_id|location_id>.jsonl"""

    def __init__(self, out_dir: str):
        self.dir = Path(out_dir)
        self.dir.mkdir(parents=True, exist_ok=True)

    def send(self, topic: str, system_id: str, records: list) -> None:
        path = self.dir / f"{topic}.{system_id}.jsonl"
        with open(path, "a", encoding="utf-8") as f:
            for r in records:
                f.write(r.model_dump_json() + "\n")

    def close(self) -> None:
        pass


class KafkaSink:
    """Mỗi message là một envelope; key lấy từ envelope.key() (GBFS: system_id:station_id)."""

    def __init__(self, bootstrap: str):
        # import muộn để file sink không cần thư viện
        from confluent_kafka import Producer
        self.producer = Producer({
            "bootstrap.servers": bootstrap,
            "linger.ms": 50,
            "compression.type": "lz4",
        })

    def send(self, topic: str, system_id: str, records: list) -> None:
        for r in records:
            self.producer.produce(topic, key=r.key(),
                                  value=r.model_dump_json())
            self.producer.poll(0)
        self.producer.flush(10)

    def close(self) -> None:
        self.producer.flush(10)


# --------------------------------------------------------------------- Poller
@dataclass
class FeedTask:
    system: dict
    feed_name: str
    url: Optional[str] = None
    next_due: float = 0.0          # time.monotonic()
    last_updated: object = None
    failures: int = 0

    @property
    def label(self) -> str:
        return f"{self.system['system_id']}/{self.feed_name}"


@dataclass
class WeatherTask:
    location: dict
    next_due: float = 0.0
    failures: int = 0

    @property
    def label(self) -> str:
        return f"weather/{self.location['location_id']}"


def discover(session: requests.Session, tasks: list, system: dict) -> None:
    """Đọc gbfs.json và gán URL feed cho mọi task của hệ thống đó."""
    index = get_json(session, system["url"])
    feeds = find_feeds(index)
    for t in tasks:
        if isinstance(t, FeedTask) and t.system is system:
            t.url = feeds.get(t.feed_name)
            if not t.url:
                log.warning("[%s] không có feed %s",
                            system["system_id"], t.feed_name)


def run_task(session, sink, task, all_tasks: list) -> float:
    """Chạy một task (GBFS hoặc thời tiết), trả về số giây tới lần poll kế tiếp."""
    if isinstance(task, WeatherTask):
        return run_weather_task(session, sink, task)
    return run_feed_task(session, sink, task, all_tasks)


def run_weather_task(session, sink, task: WeatherTask) -> float:
    loc = task.location
    try:
        params = {**WEATHER_PARAMS,
                  "latitude": loc["latitude"], "longitude": loc["longitude"]}
        data = get_json(session, WEATHER_URL, params)
        task.failures = 0
        messages = parse_weather_response(data, loc["location_id"])
        records = [WeatherEnvelope.build(m) for m in messages]
        sink.send(WeatherEnvelope.topic, loc["location_id"], records)
        log.info("[%s] %d bản ghi (%d observed, %d forecast)", task.label, len(records),
                 sum(m.data_type == "observed" for m in messages),
                 sum(m.data_type == "forecast" for m in messages))
        return WEATHER_INTERVAL
    except (requests.RequestException, ValueError, KeyError) as e:
        task.failures += 1
        backoff = min(MAX_BACKOFF, MIN_INTERVAL * 2 ** task.failures)
        log.warning("[%s] lỗi (%s) lần %d, thử lại sau %ss",
                    task.label, e, task.failures, backoff)
        return backoff


def run_feed_task(session, sink, task: FeedTask, all_tasks: list) -> float:
    """Poll một feed GBFS."""
    try:
        if not task.url:
            discover(session, all_tasks, task.system)
            if not task.url:
                raise KeyError(f"feed {task.feed_name} không tồn tại")

        feed = get_json(session, task.url)
        task.failures = 0
        ttl = feed.get("ttl")
        interval = max(MIN_INTERVAL, ttl if isinstance(
            ttl, (int, float)) else DEFAULT_TTL)

        last_updated = feed.get("last_updated")
        if last_updated is not None and last_updated == task.last_updated:
            log.debug("[%s] chưa có snapshot mới", task.label)
            return UNCHANGED_RETRY
        task.last_updated = last_updated

        envelope_cls, normalize = FEEDS[task.feed_name]
        system_id = task.system["system_id"]
        records = [envelope_cls.build(feed, m)
                   for m in normalize(feed, system_id)]
        sink.send(envelope_cls.topic, system_id, records)
        log.info("[%s] %d bản ghi (last_updated=%s, ttl=%ss)",
                 task.label, len(records), last_updated, interval)
        return interval

    except (requests.RequestException, ValueError, KeyError) as e:
        task.failures += 1
        backoff = min(MAX_BACKOFF, MIN_INTERVAL * 2 ** task.failures)
        if task.failures >= 3:
            task.url = None  # URL có thể đã đổi -> khám phá lại từ gbfs.json
        log.warning("[%s] lỗi (%s) lần %d, thử lại sau %ss",
                    task.label, e, task.failures, backoff)
        return backoff


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Polling GBFS + thời tiết prototype")
    ap.add_argument("--sink", choices=["file", "kafka"], default="file")
    ap.add_argument("--out-dir", default="out")
    ap.add_argument("--kafka", default="localhost:9092",
                    help="bootstrap servers")
    ap.add_argument("--once", action="store_true",
                    help="poll mỗi task đúng 1 lần rồi thoát")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    sink = KafkaSink(
        args.kafka) if args.sink == "kafka" else JsonlSink(args.out_dir)
    session = requests.Session()
    session.headers["User-Agent"] = "gbfs-polling-prototype/0.1"

    tasks = [FeedTask(system=s, feed_name=name)
             for s in SYSTEMS for name in FEEDS]
    tasks += [WeatherTask(location=loc) for loc in LOCATIONS]

    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())

    try:
        if args.once:
            for t in tasks:
                run_task(session, sink, t, tasks)
            return
        while not stop.is_set():
            task = min(tasks, key=lambda t: t.next_due)
            wait = task.next_due - time.monotonic()
            if wait > 0:
                stop.wait(wait)
                continue
            task.next_due = time.monotonic() + run_task(session, sink, task, tasks)
    finally:
        sink.close()
        session.close()
        log.info("Đã dừng.")


if __name__ == "__main__":
    main()
