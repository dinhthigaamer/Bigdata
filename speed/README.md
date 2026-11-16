# Layer 3 — Speed Layer (Spark Structured Streaming)

Thư mục này chứa code xử lý luồng thời gian thực (Job A) cho kiến trúc Lambda của nhóm:

* **`speed/lib/schemas.py`**: Tự động sinh Spark `StructType` trực tiếp từ các model Pydantic trong `common/` (`common/envelope.py` & `common/schemas/*.py`).
* **`speed/lib/transforms.py`**: Parse JSON Envelope từ Kafka, chuẩn hóa timestamp UTC, đặt **Watermark**, loại bỏ **dữ liệu đến trễ (late data)** và **khử trùng lặp** theo `(system_id, station_id, last_reported)`.
* **`speed/jobs/job_a_console.py`**: Prototype Job A đọc Kafka topic `gbfs.station_status` và in kết quả + metrics từng micro-batch ra console.
* **`speed/docker-compose.yml` & `speed/Dockerfile`**: Môi trường Docker chạy riêng cho Layer 3 (`python:3.11.10-slim-bookworm` + OpenJDK 11 + Apache Spark `3.5.3` + Kafka SQL connector), cài dependency từ [../requirements.txt](../requirements.txt).

---

## 1. Chạy riêng Layer 3 bằng `speed/docker-compose.yml`

Mặc định `speed/docker-compose.yml` nối vào network `kafka-docker_default` (để dùng chung container `kafka_l1` từ `kafka-docker/docker-compose.yml`) và đọc `kafka:29092`:

```bash
# 1. Đảm bảo Kafka đang chạy (nếu dùng kafka-docker/)
docker compose -f kafka-docker/docker-compose.yml up -d

# 2. Build & chạy Layer 3
docker compose -f speed/docker-compose.yml up -d --build

# 3. Xem bảng kết quả và metrics từng micro-batch
docker logs -f --tail 50 spark-speed
```

Nếu bạn bật Kafka từ file `docker-compose.yml` ở root (mạng `bikeshare_net`), truyền thêm biến `DOCKER_NETWORK=bikeshare_net`:

```bash
DOCKER_NETWORK=bikeshare_net docker compose -f speed/docker-compose.yml up -d --build
```

> **Lưu ý về `localhost:9092`:** Container trong Docker **không** dùng `host.docker.internal:9092` để nối tới `kafka_l1`, vì broker quảng bá `EXTERNAL://localhost:9092` khiến client trong container tự gọi vào chính mình. Luôn nối qua cùng Docker network (`kafka-docker_default` hoặc `bikeshare_net`) với địa chỉ `kafka:29092`.


---

## 2. Reset Checkpoint khi chuyển giữa dữ liệu test và dữ liệu live

Nếu bạn từng bơm dữ liệu test có timestamp mới hơn (hoặc chạy lại từ đầu sau khi xóa topic Kafka), hãy xóa volume checkpoint cũ trước khi bật lại `spark-speed`:

```bash
docker compose -f speed/docker-compose.yml down -v
docker compose -f speed/docker-compose.yml up -d
```

---

## 5. Lưu ý về Watermark & `last_reported`

* **Fallback khi `last_reported` bị `null`:** Dữ liệu thực tế trong `out/` và feed live có một số trạm `last_reported: null`. Job A tạo cột `event_ts = coalesce(last_reported, snapshot_ts)` để đảm bảo các bản ghi này vẫn tham gia watermark và khử trùng lặp đúng.
* **Chế độ Watermark:** Mặc định `--watermark-mode last_reported` (lọc trễ và dedup trực tiếp trên `last_reported`). Có thể chuyển sang `--watermark-mode snapshot_ts` (dùng `dropDuplicatesWithinWatermark`) nếu muốn giữ cả các trạm ít biến động có `last_reported` cũ hơn ngưỡng watermark so với `snapshot_ts`.
