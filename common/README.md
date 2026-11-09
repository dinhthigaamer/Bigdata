# Schema Message

# Tầng Ingestion: thu thập dữ liệu GBFS và thời tiết

Tầng này poll định kỳ dữ liệu từ các API công khai, chuẩn hóa về một schema chung, bọc mỗi bản ghi trong một **envelope** rồi đẩy ra Kafka (hoặc file JSONL khi phát triển). Các layer phía sau (speed layer, batch layer) chỉ cần đọc envelope, không cần biết nguồn gốc hay phiên bản API.

## 1. Luồng dữ liệu

```
GBFS gbfs.json (Citi Bike 2.3, Bixi 2.2)   Open-Meteo /v1/forecast
        │  station_status / station_information     │  minutely_15
        ▼                                           ▼
   normalize_status / normalize_information   parse_weather_response
        │  StationStatus / StationInfo              │  WeatherMessage
        └──────────────┬────────────────────────────┘
                       ▼
        Envelope (StationStatusEnvelope, StationInfoEnvelope, WeatherEnvelope)
                       ▼
              Sink: Kafka  |  JSONL
```

## 2. Các file


| File                  | Vai trò                                                           |
| --------------------- | ------------------------------------------------------------------ |
| `schema_info.py`      | Model`StationInfo`và`normalize_information()`                     |
| `schema_status.py`    | Model`StationStatus`,`VehicleCount`,`normalize_status()`,`to_dt()` |
| `schema_weather.py`   | Model`WeatherMessage`và`parse_weather_response()`                 |
| `envelope.py`         | Các class envelope,`parse_envelope()`                             |
| `polling.py`          | Vòng lặp poll, các task GBFS và thời tiết, sink file/Kafka   |
| `consumer_example.py` | Consumer mẫu đọc envelope từ Kafka hoặc file                  |
| `requirements.txt`    | Thư viện cần cài                                               |

## 3. Nguồn dữ liệu


| Nguồn                    | `system_id`/`location_id` | Feed                                   | Chu kỳ poll                                  |
| ------------------------- | ------------------------- | -------------------------------------- | --------------------------------------------- |
| Citi Bike NYC (GBFS 2.3)  | `citi-bike-nyc`           | `station_status`,`station_information` | Theo`ttl`của từng feed, tối thiểu 5 giây |
| Bixi Montréal (GBFS 2.2) | `velobixi`                | `station_status`,`station_information` | Theo`ttl`của từng feed, tối thiểu 5 giây |
| Open-Meteo                | `nyc`,`montreal`          | `minutely_15`                          | 15 phút                                      |

Cấu hình nằm ở đầu `polling.py` (`SYSTEMS`, `LOCATIONS`, `WEATHER_PARAMS` và các hằng số interval, backoff).

## 4. Schema chuẩn hóa

Dữ liệu của GBFS 2.2, 2.3 và 3.0 được hợp nhất vào cùng một schema. Các field không thuộc phần lõi được giữ trong `extra`.

**`StationInfo`**: `system_id`, `station_id`, `name`, `short_name`, `lat`, `lon`, `capacity`, `region_id`, `is_charging`, `extra`, `source_version`, `updated_at`.

**`StationStatus`**: `system_id`, `station_id`, `snapshot_ts`, `last_reported`, `num_bikes_available`, `num_bikes_disabled`, `num_docks_available`, `num_docks_disabled`, `num_ebikes_available`, `is_installed`, `is_renting`, `is_returning`, `vehicle_types_available`, `extra`, `source_version`, `ingest_ts`.

**`WeatherMessage`**: `location_id`, `latitude`, `longitude`, `time` (UTC), `fetch_time`, `data_type` (`observed` hoặc `forecast`), `source`, `interval` (giây), cùng các số đo `temperature_2m`, `relative_humidity_2m`, `precipitation`, `rain`, `snowfall`, `wind_speed_10m`, `wind_direction_10m`, `wind_gusts_10m`, `visibility`, `weather_code`.

Điểm cần biết khi chuẩn hóa:

* Tên trạm của GBFS v3 là mảng `[{text, language}]`, được đọc bằng `text_of()`.
* Trạm lỗi dữ liệu bị bỏ qua và ghi log cảnh báo, không làm hỏng cả feed.
* Thời gian luôn quy về UTC. Với Open-Meteo, code đọc `utc_offset_seconds` từ response thay vì giả định UTC.
* Các số đo thời tiết là `Optional`, vì Open-Meteo có thể trả `null`.
* `latitude` và `longitude` của thời tiết là tọa độ lưới do Open-Meteo trả về, không phải tọa độ yêu cầu. Vì vậy dùng `location_id` để join với dữ liệu GBFS.

## 5. Envelope

Mỗi message Kafka là một envelope, chứa đúng một bản ghi:

```json
{
  "schema_version": "1.0",
  "record_type": "station_status",
  "system_id": "citi-bike-nyc",
  "source_version": "2.3",
  "source_last_updated": "2026-11-08T10:00:00Z",
  "source_ttl": 5,
  "fetched_at": "2026-11-08T10:00:02Z",
  "payload": { "...bản ghi StationStatus đã chuẩn hóa..." }
}
```


| Field                              | Ý nghĩa                                                           |
| ---------------------------------- | ------------------------------------------------------------------- |
| `schema_version`                   | Phiên bản envelope. Consumer chỉ chấp nhận cùng major version |
| `record_type`                      | Loại bản ghi, dùng để chọn class khi parse                    |
| `system_id`                        | Nguồn dữ liệu (thời tiết:`open-meteo:<location_id>`)           |
| `source_version`                   | Phiên bản API nguồn (GBFS), thời tiết để trống              |
| `source_last_updated`,`source_ttl` | Metadata cấp feed                                                  |
| `fetched_at`                       | Thời điểm ingestion lấy dữ liệu                               |
| `payload`                          | Bản ghi đã chuẩn hóa                                           |

Mỗi loại dữ liệu có một class riêng, tự định nghĩa payload, topic và key:


| Class                   | `record_type`         | Topic Kafka                | Key                         |
| ----------------------- | --------------------- | -------------------------- | --------------------------- |
| `StationStatusEnvelope` | `station_status`      | `gbfs.station_status`      | `system_id:station_id`      |
| `StationInfoEnvelope`   | `station_information` | `gbfs.station_information` | `system_id:station_id`      |
| `WeatherEnvelope`       | `weather_message`     | `weather.open_meteo`       | `location_id:YYYYMMDDTHHMM` |

Cách dùng:

```python
# Bên gửi
env = StationStatusEnvelope.build(feed, status_model)
sink.send(env.topic, system_id, [env])

# Bên nhận: tự chọn đúng class theo record_type, payload đã được validate
env = parse_envelope(msg.value())
rec = env.payload
```

Khi đổi schema theo kiểu không tương thích ngược, tăng `SCHEMA_VERSION` lên major mới (ví dụ `2.0`). Consumer cũ sẽ từ chối message thay vì parse sai.

## 6. Cài đặt và chạy

```bash
pip install -r requirements.txt     # requests, pydantic>=2, confluent-kafka (chỉ khi dùng Kafka)

python polling.py --once                           # poll mỗi task một lần, ghi JSONL vào ./out
python polling.py                                  # chạy liên tục, ghi JSONL
python polling.py --sink kafka --kafka localhost:9092
python polling.py -v                               # bật log debug
```

Tham số: `--sink file|kafka`, `--out-dir`, `--kafka`, `--once`, `-v`. Nhấn Ctrl-C để dừng gọn.

Sink file ghi nối thêm vào `out/<topic>.<system_id hoặc location_id>.jsonl`, mỗi dòng là một envelope. Sink Kafka dùng `envelope.key()` làm key để cùng một trạm hoặc cùng một địa điểm luôn vào cùng partition.

### Consumer mẫu

```bash
python consumer_example.py --file out/*.jsonl
python consumer_example.py --kafka localhost:9092                      # nghe cả 3 topic
python consumer_example.py --kafka localhost:9092 --topic weather.open_meteo
```

Consumer in thông tin theo từng loại, bỏ qua message lỗi kèm vị trí (file và dòng, hoặc topic, partition, offset) và in thống kê khi kết thúc.

## 7. Hành vi của poller

* **GBFS:** đọc `gbfs.json` để lấy URL các feed, rồi poll từng feed theo `ttl` mà feed báo về. Nếu `last_updated` không đổi so với lần trước thì không gửi lại, và hỏi lại sau 5 giây.
* **Thời tiết:** mỗi địa điểm gọi `minutely_15` với 4 mốc quá khứ (1 giờ) và 16 mốc dự báo (4 giờ), múi giờ GMT, kèm `current` để biết mốc hiện tại. Mốc không sau thời điểm hiện tại là `observed`, các mốc sau là `forecast`.
* **Lỗi:** lỗi mạng, HTTP hoặc dữ liệu không hợp lệ làm task đó backoff (10, 20, 40 giây, ... tối đa 300 giây). Sau 3 lần lỗi liên tiếp, task GBFS đọc lại `gbfs.json` để lấy URL mới. Lỗi ở một task không ảnh hưởng task khác.

## 8. Thêm nguồn dữ liệu mới

1. Viết model Pydantic cho bản ghi chuẩn hóa và hàm parse.
2. Viết class kế thừa `EnvelopeBase` (hoặc `GbfsEnvelope`) với `record_type: Literal[...]`, `payload`, `topic` và `key()`.
3. Thêm class đó vào `Union` của `AnyEnvelope` trong `envelope.py`.
4. Viết task poll và thêm vào danh sách `tasks` trong `main()` của `polling.py`.

## 9. Lưu ý cho các layer phía sau

* **Dedupe thời tiết:** mỗi lần poll gửi lại các mốc dự báo với giá trị mới, nên cùng khóa `(location_id, time)` xuất hiện nhiều lần. Khi xử lý, lấy bản có `fetched_at` mới nhất.
* **Spark Structured Streaming:** cần khai báo schema lồng nhau (envelope bọc payload).
* **Join thời tiết với GBFS:** cần một bảng ánh xạ giữa `location_id` (`nyc`, `montreal`) và `system_id` của GBFS (`citi-bike-nyc`, `velobixi`).
* **Dữ liệu thô:** ingestion hiện chỉ gửi bản ghi đã chuẩn hóa. Nếu batch layer cần xử lý lại từ dữ liệu gốc, cần bổ sung một sink lưu JSON thô (ví dụ vào MinIO).
* **Giới hạn Open-Meteo:** API miễn phí dành cho mục đích phi thương mại. Hai địa điểm poll mỗi 15 phút là khoảng 192 lượt gọi mỗi ngày.

- /common/schema_status.py: schema cho station_status
- /common/schema_info.py: schema cho station_information
- /common/schema_weather.py: schema cho weather
