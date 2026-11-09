"""Consumer mẫu: đọc envelope từ Kafka hoặc file JSONL, parse về đúng class và xử lý theo loại.

    python consumer_example.py --file out/*.jsonl
    python consumer_example.py --kafka localhost:9092                  # nghe cả 4 topic
    python consumer_example.py --kafka localhost:9092 --topic weather.open_meteo
"""
import argparse
import json
from collections import Counter

from common.envelope import (EnvelopeBase, StationInfoEnvelope, StationStatusEnvelope,
                             WeatherEnvelope, parse_envelope)

ALL_TOPICS = [c.topic for c in (
    StationStatusEnvelope, StationInfoEnvelope, WeatherEnvelope)]


def handle(raw) -> EnvelopeBase:
    """raw: dict, chuỗi hoặc bytes. Ném ValueError nếu sai phiên bản hoặc payload không hợp lệ."""
    env = parse_envelope(raw)
    p = env.payload
    if isinstance(env, StationStatusEnvelope):
        print(
            f"[status ] {env.key()} bikes={p.num_bikes_available} docks={p.num_docks_available}")
    elif isinstance(env, StationInfoEnvelope):
        print(f"[info   ] {env.key()} {p.name} ({p.lat}, {p.lon})")
    elif isinstance(env, WeatherEnvelope):
        print(f"[weather] {env.key()} {p.data_type} temp={p.temperature_2m}°C "
              f"rain={p.rain}mm wind={p.wind_speed_10m}km/h")
    return env


def read_files(paths: list[str], stats: Counter) -> None:
    for path in paths:
        with open(path, encoding="utf-8") as f:
            for n, line in enumerate(f, 1):
                if not line.strip():
                    continue
                try:
                    stats[handle(line).record_type] += 1
                except ValueError as e:  # JSONDecodeError, version lạ, payload sai
                    stats["error"] += 1
                    print(f"Bỏ qua {path}:{n}: {str(e).splitlines()[0]}")


def read_kafka(bootstrap: str, topics: list[str], stats: Counter) -> None:
    # import muộn để đọc file không cần thư viện
    from confluent_kafka import Consumer
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": "gbfs-demo",
                  "auto.offset.reset": "earliest"})
    c.subscribe(topics)
    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Kafka lỗi:", msg.error())
                continue
            try:
                stats[handle(msg.value()).record_type] += 1
            except ValueError as e:
                stats["error"] += 1
                print(f"Bỏ qua message {msg.topic()}[{msg.partition()}]@{msg.offset()}: "
                      f"{str(e).splitlines()[0]}")
    except KeyboardInterrupt:
        pass
    finally:
        c.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", nargs="+", help="một hoặc nhiều file JSONL")
    ap.add_argument("--kafka", default="localhost:9092",
                    help="bootstrap servers")
    ap.add_argument("--topic", action="append",
                    help="topic cần nghe (lặp lại được); mặc định: tất cả")
    args = ap.parse_args()

    stats: Counter = Counter()
    if args.file:
        read_files(args.file, stats)
    else:
        read_kafka(args.kafka, args.topic or ALL_TOPICS, stats)
    print("\nTổng kết:", dict(stats))


if __name__ == "__main__":
    main()
