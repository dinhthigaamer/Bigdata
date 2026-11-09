"""
Test thử API GBFS của Citi Bike (NYC).
Không cần API key. Cài thư viện:  pip install requests
Chạy:  python test_citibike_gbfs.py
"""
from datetime import datetime, timezone
import json
import requests

# Potential
# GBFS_URL = "https://gbfs.citibikenyc.com/gbfs/2.3/gbfs.json"
GBFS_URL = "https://gbfs.velobixi.com/gbfs/2-2/gbfs.json"
# GBFS_URL = "https://maas.zeus.cooltra.com/gbfs/brussels/3.0/en/gbfs.json"

# Nếu muốn thử qua Citybikes (giới hạn 300 request/giờ khi không có key):
# GBFS_URL = "https://api.citybik.es/gbfs/2/citi-bike-nyc/gbfs.json"


def get_json(url):
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    print(r)
    return r.json()


def find_feeds(index):
    """Trả về dict {tên_feed: url}. Hỗ trợ cả GBFS v2 (data -> en -> feeds) và v3 (data -> feeds)."""
    data = index["data"]
    feeds = data["feeds"] if "feeds" in data else data.get(
        "en", next(iter(data.values())))["feeds"]
    return {f["name"]: f["url"] for f in feeds}


def text_of(value):
    """Tên trạm: v2 là chuỗi, v3 là mảng [{text, language}]."""
    if isinstance(value, list):
        return value[0]["text"] if value else ""
    return value or ""


def fmt_time(ts):
    """Thời gian: v2 là số giây POSIX, v3 là chuỗi ISO 8601."""
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    return str(ts)


def main():
    # 1. Mục lục
    index = get_json(GBFS_URL)
    print(f"GBFS version: {index.get('version')} | ttl: {index.get('ttl')}s")

    feeds = find_feeds(index)
    print("Các feed có sẵn:", ", ".join(feeds))
    with open("feeds.json", "w", encoding="utf-8") as f:
        json.dump(feeds, f, indent=2, ensure_ascii=False)

    # 2. Thông tin trạm (tên, tọa độ) và trạng thái (số xe, chỗ trống)
    info = get_json(feeds["station_information"])
    status = get_json(feeds["station_status"])

    # with open("station_information.json", "w", encoding="utf-8") as f:
    #     json.dump(info, f, indent=2, ensure_ascii=False)
    # # print(info)
    # with open("station_status.json", "w", encoding="utf-8") as f:
    #     json.dump(status, f, indent=2, ensure_ascii=False)

    print(f"\nDữ liệu cập nhật lúc: {fmt_time(status.get('last_updated'))}")

    stations = {s["station_id"]: s for s in info["data"]["stations"]}
    print(f"Số trạm: {len(stations)}")

    # 3. Ghép hai file theo station_id
    rows = []
    for st in status["data"]["stations"]:
        meta = stations.get(st["station_id"])
        if not meta:
            continue
        # v3 dùng num_vehicles_available, v2 dùng num_bikes_available
        bikes = st.get("num_vehicles_available",
                       st.get("num_bikes_available", 0))
        rows.append({
            "name": text_of(meta.get("name")),
            "lat": meta.get("lat"),
            "lon": meta.get("lon"),
            "bikes": bikes,
            "docks": st.get("num_docks_available"),
            "renting": st.get("is_renting"),
        })

    print(f"So tram xe: {len(rows)}")
    # 4. In 10 trạm đầu tiên
    print(f"\n{'Trạm':<40} {'Xe':>4} {'Chỗ trống':>10}  Đang cho thuê")
    print("-" * 75)
    for r in rows[:10]:
        print(
            f"{r['name'][:39]:<40} {r['bikes']:>4} {str(r['docks']):>10}  {r['renting']}")

    # 5. Thống kê nhanh
    print(f"\nTổng số xe sẵn sàng: {sum(r['bikes'] for r in rows)}")
    print(f"Trạm hết xe: {sum(1 for r in rows if r['bikes'] == 0)}")

    # 6. Xem nguyên một phần tử thô để kiểm tra tên trường thực tế
    print("\nVí dụ một phần tử station_status thô:")
    print(status["data"]["stations"][20])


if __name__ == "__main__":
    try:
        main()
    except requests.RequestException as e:
        print("Lỗi mạng/HTTP:", e)
