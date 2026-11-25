import json
import traceback

from serving.libs.ch_writer import ClickHouseWriter
from common.envelope import parse_envelope

PATH = "./out/gbfs.station_information.citi-bike-nyc.jsonl"
BATCH = 2000
LIMIT = None   # đặt = 1 hoặc 5 để test thử vài dòng, None = đọc hết

# Cột phải khớp DDL của bảng dim_station
COLS = [
    "system_id", "station_id", "name", "short_name", "lat", "lon",
    "capacity", "region_id", "is_charging", "extra", "source_version", "updated_at",
]


def to_row(info) -> dict:
    d = info.model_dump()                      # pydantic v1: info.dict()
    # print(d)
    d["extra"] = json.dumps(d.get("extra") or {}, ensure_ascii=False)
    return {c: d.get(c) for c in COLS}


def main():
    total = 0
    batch = []

    with ClickHouseWriter() as w, open(PATH, "r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue

            try:
                info = parse_envelope(line).payload     # StationInfo
                batch.append(to_row(info))
            except Exception as e:
                print(f"[dòng {lineno}] lỗi parse: {e}")
                continue

            if len(batch) >= BATCH:
                total += w.write_dim_station(batch)
                batch.clear()

            if LIMIT and lineno >= LIMIT:
                break

        if batch:
            total += w.write_dim_station(batch)

    print(f"Đã ghi {total} dòng vào dim_station")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("Write error")
        traceback.print_exc()
